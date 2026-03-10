"""FastAPI application for the lineage tracker."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

from lineage_tracker.extractor import BigQueryExtractor
from lineage_tracker.models import LineageGraph, ScanConfig
from lineage_tracker.persistence import graph_to_dict, load_graph, save_graph

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

        The subscriber receives dicts with {"event", "data"} until
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
            "event": event_type,
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
        for q in self._subscribers:
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass
        self._history.clear()

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
                        # Scan finished — send final event and close
                        yield {
                            "event": "done",
                            "data": json.dumps({"type": "done", "message": "Stream closed"}),
                        }
                        break

                    yield event
            finally:
                event_bus.unsubscribe(queue)

        return EventSourceResponse(event_generator())

    return app


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
        save_graph(graph, app.state.data_dir, app.state.project_id)

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
    except Exception:
        logger.exception("Scan failed")
        event_bus.publish("error", "Scan failed unexpectedly")
    finally:
        app.state.scan_in_progress = False
        event_bus.finish()
