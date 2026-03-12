"""FastAPI application for the lineage tracker."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import time
import traceback
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

from lineage_tracker.extractor import BigQueryExtractor
from lineage_tracker.models import ColumnMapping, LineageEdge, LineageGraph, ScanConfig
from lineage_tracker.persistence import _edge_to_dict, graph_to_dict, load_graph, save_graph

logger = logging.getLogger(__name__)


class ScanEventBus:
    """Broadcast scan progress events to multiple SSE subscribers.

    Events are pushed from the scan background task (running in a thread
    executor) and consumed by zero or more SSE client connections.
    """

    def __init__(self) -> None:
        self._subscribers: list[asyncio.Queue[dict | None]] = []
        self._history: list[dict] = []
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe(self) -> asyncio.Queue[dict | None]:
        """Create a new subscriber queue. Returns the queue.

        The subscriber receives dicts with {"data"} until
        it gets ``None`` (meaning the scan is done or errored).
        """
        q: asyncio.Queue[dict | None] = asyncio.Queue()
        # Send history so late joiners catch up
        for event in self._history:
            q.put_nowait(event)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[dict | None]) -> None:
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    def publish(self, event_type: str, message: str | None = None) -> None:
        """Publish an event. Thread-safe — can be called from executor threads."""
        payload = {
            "data": json.dumps({
                "type": event_type,
                "message": message,
                "timestamp": time.time(),
            }),
        }
        self._history.append(payload)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._broadcast_sync, payload)
        else:
            self._broadcast_sync(payload)

    def _broadcast_sync(self, payload: dict) -> None:
        for q in self._subscribers:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                pass  # slow consumer, drop event

    def finish(self) -> None:
        """Signal all subscribers that the scan is done."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._finish_sync)
        else:
            self._finish_sync()

    def _finish_sync(self) -> None:
        for q in self._subscribers:
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass

    def reset(self) -> None:
        """Reset state for a new scan."""
        self._history.clear()
        # Clear any leftover subscribers
        for q in self._subscribers:
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass
        self._subscribers.clear()


def create_app(
    project_id: str,
    data_dir: Path,
    no_scan: bool = False,
    initial_scan_config: dict[str, Any] | None = None,
    frontend_dir: Path | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""

    # Initialize BigQuery extractor (lazy — only used when not in no_scan mode)
    extractor: BigQueryExtractor | None = None
    if not no_scan:
        try:
            extractor = BigQueryExtractor(project_id)
        except Exception:
            logger.exception("Failed to initialize BigQuery client")

    # Load cached graph from disk if available
    cached_graph = load_graph(data_dir, project_id)
    if cached_graph is not None:
        logger.info("Loaded cached graph with %d nodes", len(cached_graph.nodes))

    # Shared event bus for scan progress
    event_bus = ScanEventBus()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        event_bus.bind_loop(asyncio.get_event_loop())
        # If initial scan config was provided via CLI, run scan in background
        if initial_scan_config and extractor is not None:
            asyncio.create_task(
                _run_scan(app, extractor, initial_scan_config, event_bus)
            )
        yield

    app = FastAPI(title="SQL Lineage Tracker", version="0.1.0", lifespan=lifespan)

    # Mount compiled frontend assets if available
    _frontend_dir = frontend_dir if frontend_dir and frontend_dir.exists() else None
    if _frontend_dir:
        assets_dir = _frontend_dir / "assets"
        if assets_dir.exists():
            app.mount("/assets", StaticFiles(directory=assets_dir), name="static-assets")

    app.state.project_id = project_id
    app.state.data_dir = data_dir
    app.state.no_scan = no_scan
    app.state.initial_scan_config = initial_scan_config
    app.state.graph: LineageGraph | None = cached_graph
    app.state.scan_in_progress = False
    app.state.extractor = extractor
    app.state.event_bus = event_bus

    @app.get("/api/health")
    async def health() -> dict:
        bq_connected = extractor is not None
        has_graph = app.state.graph is not None
        return {
            "status": "ok",
            "project_id": project_id,
            "no_scan": no_scan,
            "bigquery_connected": bq_connected,
            "has_graph": has_graph,
            "scan_in_progress": app.state.scan_in_progress,
        }

    @app.get("/api/graph")
    async def get_graph() -> JSONResponse:
        graph: LineageGraph | None = app.state.graph
        if graph is None:
            return JSONResponse(
                content={"metadata": None, "nodes": {}, "edges": []},
            )
        return JSONResponse(content=graph_to_dict(graph))

    @app.get("/api/datasets")
    async def list_datasets() -> JSONResponse:
        if extractor is None:
            return JSONResponse(
                status_code=503,
                content={"error": "BigQuery connection not available"},
            )

        datasets = extractor.list_datasets()
        return JSONResponse(content=[
            {"id": ds.id, "table_count": ds.table_count, "view_count": ds.view_count}
            for ds in datasets
        ])

    @app.get("/api/datasets/{dataset_id}/tables")
    async def list_tables(dataset_id: str) -> JSONResponse:
        if extractor is None:
            return JSONResponse(
                status_code=503,
                content={"error": "BigQuery connection not available"},
            )

        tables = extractor.list_tables(dataset_id)
        return JSONResponse(content=[
            {"name": t.name, "type": t.type, "dataset": t.dataset}
            for t in tables
        ])

    @app.get("/api/columns/{dataset_id}/{table_name}")
    async def get_columns(dataset_id: str, table_name: str) -> JSONResponse:
        if extractor is None:
            return JSONResponse(
                status_code=503,
                content={"error": "BigQuery connection not available"},
            )

        columns_map = extractor.get_columns(dataset_id, table_name)
        columns = columns_map.get(table_name, [])
        return JSONResponse(content=[
            {"name": c.name, "data_type": c.data_type}
            for c in columns
        ])

    # --- Manual Edge CRUD ---

    @app.post("/api/manual-edge")
    async def create_manual_edge(request: Request) -> JSONResponse:
        """Create a manual edge between two nodes."""
        graph = _require_graph(app)
        if isinstance(graph, JSONResponse):
            return graph

        body = await request.json()
        source_node = body.get("source_node")
        target_node = body.get("target_node")

        if not source_node or not target_node:
            return JSONResponse(
                status_code=422,
                content={"error": "source_node and target_node are required"},
            )

        # Validate referenced nodes exist
        missing = [n for n in (source_node, target_node) if n not in graph.nodes]
        if missing:
            return JSONResponse(
                status_code=404,
                content={"error": f"Nodes not found: {', '.join(missing)}"},
            )

        # Build edge
        edge_id = f"manual_{source_node}__{target_node}"
        # Ensure unique id if one already exists
        existing_ids = {e.id for e in graph.edges}
        if edge_id in existing_ids:
            counter = 2
            while f"{edge_id}_{counter}" in existing_ids:
                counter += 1
            edge_id = f"{edge_id}_{counter}"

        edge = LineageEdge(
            id=edge_id,
            source_node=source_node,
            target_node=target_node,
            edge_type="manual",
            description=body.get("description"),
            column_mappings=_parse_column_mappings(body.get("column_mappings", [])),
        )

        graph.edges.append(edge)
        await _save_current_graph_async(app)

        return JSONResponse(
            status_code=201,
            content=_edge_to_dict(edge),
        )

    @app.put("/api/manual-edge/{edge_id:path}")
    async def update_manual_edge(edge_id: str, request: Request) -> JSONResponse:
        """Update an existing manual edge."""
        graph = _require_graph(app)
        if isinstance(graph, JSONResponse):
            return graph

        edge = _find_manual_edge(graph, edge_id)
        if edge is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"Manual edge '{edge_id}' not found"},
            )

        body = await request.json()

        # Update fields if provided
        if "description" in body:
            edge.description = body["description"]

        if "column_mappings" in body:
            edge.column_mappings = _parse_column_mappings(body["column_mappings"])

        await _save_current_graph_async(app)

        return JSONResponse(content=_edge_to_dict(edge))

    @app.delete("/api/manual-edge/{edge_id:path}")
    async def delete_manual_edge(edge_id: str) -> JSONResponse:
        """Delete a manual edge."""
        graph = _require_graph(app)
        if isinstance(graph, JSONResponse):
            return graph

        edge = _find_manual_edge(graph, edge_id)
        if edge is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"Manual edge '{edge_id}' not found"},
            )

        graph.edges.remove(edge)
        await _save_current_graph_async(app)

        return JSONResponse(content={"status": "deleted", "id": edge_id})

    @app.post("/api/scan")
    async def start_scan(request: Request) -> JSONResponse:
        """Start an async scan. Body: {target?, datasets?, depth?}."""
        if app.state.no_scan:
            return JSONResponse(
                status_code=400,
                content={"error": "Server started with --no-scan; scanning is disabled"},
            )

        ext = app.state.extractor
        if ext is None:
            return JSONResponse(
                status_code=503,
                content={"error": "BigQuery connection not available"},
            )

        if app.state.scan_in_progress:
            return JSONResponse(
                status_code=409,
                content={"error": "A scan is already in progress"},
            )

        body = await request.json()
        scan_config_dict = {
            "target": body.get("target"),
            "datasets": body.get("datasets", []),
            "depth": body.get("depth"),
        }

        asyncio.create_task(
            _run_scan(app, ext, scan_config_dict, event_bus)
        )

        return JSONResponse(
            status_code=202,
            content={"status": "accepted", "message": "Scan started"},
        )

    @app.post("/api/expand")
    async def expand_node(request: Request) -> JSONResponse:
        """Expand a truncated node by scanning its dependencies."""
        if app.state.no_scan:
            return JSONResponse(
                status_code=400,
                content={"error": "Server started with --no-scan; scanning is disabled"},
            )

        ext = app.state.extractor
        if ext is None:
            return JSONResponse(
                status_code=503,
                content={"error": "BigQuery connection not available"},
            )

        if app.state.scan_in_progress:
            return JSONResponse(
                status_code=409,
                content={"error": "A scan is already in progress"},
            )

        graph: LineageGraph | None = app.state.graph
        if graph is None:
            return JSONResponse(
                status_code=400,
                content={"error": "No graph loaded"},
            )

        body = await request.json()
        node_id = body.get("node_id")
        if not node_id:
            return JSONResponse(
                status_code=422,
                content={"error": "node_id is required"},
            )

        node = graph.nodes.get(node_id)
        if node is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"Node '{node_id}' not found"},
            )

        if node.status != "truncated":
            return JSONResponse(
                status_code=400,
                content={"error": f"Node '{node_id}' is not truncated (status: {node.status})"},
            )

        depth = body.get("depth", 1)

        asyncio.create_task(
            _run_expand(app, ext, node_id, depth, event_bus)
        )

        return JSONResponse(
            status_code=202,
            content={"status": "accepted", "message": f"Expanding node {node_id}"},
        )

    @app.get("/api/scan/events")
    async def scan_events(request: Request) -> EventSourceResponse:
        """Server-Sent Events endpoint for scan progress."""
        queue = event_bus.subscribe()

        async def event_generator() -> AsyncGenerator[dict, None]:
            try:
                while True:
                    # Check if client disconnected
                    if await request.is_disconnected():
                        break

                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    except asyncio.TimeoutError:
                        # Send keepalive comment
                        yield {"comment": "keepalive"}
                        continue

                    if event is None:
                        # Scan finished — close stream
                        break

                    yield event
            finally:
                event_bus.unsubscribe(queue)

        return EventSourceResponse(event_generator())

    # Catch-all route for SPA — must be registered AFTER all /api/* routes
    if _frontend_dir:
        index_html = _frontend_dir / "index.html"

        @app.get("/{full_path:path}")
        async def serve_spa(full_path: str) -> FileResponse:
            if index_html.exists():
                return FileResponse(index_html)
            raise HTTPException(status_code=404, detail="Frontend not found")

    return app


def _require_graph(app: FastAPI) -> LineageGraph | JSONResponse:
    """Return the current graph, or a 400 JSONResponse if none is loaded."""
    graph: LineageGraph | None = app.state.graph
    if graph is None:
        return JSONResponse(status_code=400, content={"error": "No graph loaded"})
    return graph


def _find_manual_edge(graph: LineageGraph, edge_id: str) -> LineageEdge | None:
    """Find a manual edge by ID, or return None."""
    for edge in graph.edges:
        if edge.id == edge_id and edge.edge_type == "manual":
            return edge
    return None


def _parse_column_mappings(raw: list[dict]) -> list[ColumnMapping]:
    """Parse a list of raw dicts into ColumnMapping objects."""
    return [
        ColumnMapping(
            source_columns=m.get("source_columns", []),
            target_column=m["target_column"],
            transformation=m.get("transformation", "unknown"),
            expression=m.get("expression"),
            description=m.get("description"),
        )
        for m in raw
    ]


async def _save_current_graph_async(app: FastAPI) -> None:
    """Persist the current graph to disk without blocking the event loop."""
    graph: LineageGraph | None = app.state.graph
    if graph is not None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, save_graph, graph, app.state.data_dir, app.state.project_id
        )


async def _run_scan(
    app: FastAPI,
    extractor: BigQueryExtractor,
    scan_config_dict: dict[str, Any],
    event_bus: ScanEventBus,
) -> None:
    """Run a scan in a background thread, publishing progress via the event bus."""
    from lineage_tracker.graph import build_graph
    from lineage_tracker.scanner import run_scoped_scan

    config = ScanConfig(
        target=scan_config_dict.get("target"),
        datasets=scan_config_dict.get("datasets", []),
        depth=scan_config_dict.get("depth"),
    )

    app.state.scan_in_progress = True
    event_bus.reset()
    logger.info("Starting scan: %s", scan_config_dict)

    try:
        loop = asyncio.get_event_loop()

        # Run scanner in executor with progress callback
        scan_result = await loop.run_in_executor(
            None,
            lambda: run_scoped_scan(extractor, config, progress=event_bus.publish),
        )

        # Preserve manual edges from current graph
        existing_manual_edges = None
        if app.state.graph is not None:
            existing_manual_edges = [
                e for e in app.state.graph.edges if e.edge_type == "manual"
            ]

        # Build graph in executor with progress callback
        graph = await loop.run_in_executor(
            None,
            lambda: build_graph(
                scan_result,
                config,
                app.state.project_id,
                existing_manual_edges,
                progress=event_bus.publish,
            ),
        )

        app.state.graph = graph
        await _save_current_graph_async(app)

        # Print console report
        from lineage_tracker.graph import format_scan_report

        print(format_scan_report(graph, scan_result.errors))

        event_bus.publish(
            "complete",
            f"Scan complete: {len(graph.nodes)} nodes, {len(graph.edges)} edges",
        )

        logger.info(
            "Scan complete: %d nodes, %d edges",
            len(graph.nodes),
            len(graph.edges),
        )
    except Exception as exc:
        logger.exception("Scan failed")
        error_detail = f"{type(exc).__name__}: {exc}"
        event_bus.publish("error", f"Scan failed: {error_detail}")
    finally:
        app.state.scan_in_progress = False
        event_bus.finish()


async def _run_expand(
    app: FastAPI,
    extractor: BigQueryExtractor,
    node_id: str,
    depth: int,
    event_bus: ScanEventBus,
) -> None:
    """Expand a truncated node by scanning from it and merging into the existing graph."""
    from lineage_tracker.graph import build_graph, format_scan_report
    from lineage_tracker.scanner import ScanResult, run_scoped_scan

    app.state.scan_in_progress = True
    event_bus.reset()
    logger.info("Expanding node: %s (depth=%d)", node_id, depth)

    try:
        loop = asyncio.get_event_loop()
        graph: LineageGraph = app.state.graph  # type: ignore[assignment]

        # Build scan config: scan from the truncated node
        # Include the node's own dataset plus the original scan datasets
        original_datasets = list(graph.metadata.scan_config.datasets) if graph.metadata.scan_config.datasets else []
        node_dataset = node_id.split(".")[0]
        expand_datasets = list(set(original_datasets + [node_dataset]))

        expand_config = ScanConfig(
            target=node_id,
            datasets=expand_datasets,
            depth=depth,
        )

        # Run scan from the truncated node
        scan_result = await loop.run_in_executor(
            None,
            lambda: run_scoped_scan(extractor, expand_config, progress=event_bus.publish),
        )

        # Merge: existing nodes + new scan results (new data overwrites truncated nodes)
        # Deep-copy existing nodes so build_graph mutations don't corrupt
        # the original graph (important if build_graph fails midway).
        merged_nodes = {k: copy.deepcopy(v) for k, v in graph.nodes.items()}
        merged_nodes.update(scan_result.nodes)

        merged_result = ScanResult(
            nodes=merged_nodes,
            errors=scan_result.errors,
        )

        # Preserve manual edges
        existing_manual_edges = [e for e in graph.edges if e.edge_type == "manual"]

        # Rebuild graph with all nodes (uses original scan config for metadata)
        new_graph = await loop.run_in_executor(
            None,
            lambda: build_graph(
                merged_result,
                graph.metadata.scan_config,
                app.state.project_id,
                existing_manual_edges,
                progress=event_bus.publish,
            ),
        )

        app.state.graph = new_graph
        await _save_current_graph_async(app)

        print(format_scan_report(new_graph, scan_result.errors))

        event_bus.publish(
            "complete",
            f"Expansion complete: {len(new_graph.nodes)} nodes, {len(new_graph.edges)} edges",
        )

        logger.info(
            "Expansion complete: %d nodes, %d edges",
            len(new_graph.nodes),
            len(new_graph.edges),
        )
    except Exception as exc:
        logger.exception("Node expansion failed")
        tb = traceback.format_exc()
        error_detail = f"{type(exc).__name__}: {exc}"
        event_bus.publish("error", f"Node expansion failed: {error_detail}")
        logger.error("Expansion traceback:\n%s", tb)
    finally:
        app.state.scan_in_progress = False
        event_bus.finish()
