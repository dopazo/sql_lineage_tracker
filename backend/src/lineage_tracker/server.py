"""FastAPI application for the lineage tracker."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from lineage_tracker.extractor import BigQueryExtractor
from lineage_tracker.models import LineageGraph, ScanConfig
from lineage_tracker.persistence import graph_to_dict, load_graph, save_graph

logger = logging.getLogger(__name__)


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

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        # If initial scan config was provided via CLI, run scan in background
        if initial_scan_config and extractor is not None:
            asyncio.create_task(_run_initial_scan(app, extractor, initial_scan_config))
        yield

    app = FastAPI(title="SQL Lineage Tracker", version="0.1.0", lifespan=lifespan)

    app.state.project_id = project_id
    app.state.data_dir = data_dir
    app.state.no_scan = no_scan
    app.state.initial_scan_config = initial_scan_config
    app.state.graph: LineageGraph | None = cached_graph
    app.state.scan_in_progress = False
    app.state.extractor = extractor

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

    return app


async def _run_initial_scan(
    app: FastAPI,
    extractor: BigQueryExtractor,
    scan_config_dict: dict[str, Any],
) -> None:
    """Run the initial scan from CLI flags in a background thread."""
    from lineage_tracker.graph import build_graph
    from lineage_tracker.scanner import run_scoped_scan

    config = ScanConfig(
        target=scan_config_dict.get("target"),
        datasets=scan_config_dict.get("datasets", []),
        depth=scan_config_dict.get("depth"),
    )

    app.state.scan_in_progress = True
    logger.info("Starting initial scan: %s", scan_config_dict)

    try:
        loop = asyncio.get_event_loop()
        scan_result = await loop.run_in_executor(
            None, run_scoped_scan, extractor, config
        )

        existing_manual_edges = None
        if app.state.graph is not None:
            existing_manual_edges = [
                e for e in app.state.graph.edges if e.edge_type == "manual"
            ]

        graph = await loop.run_in_executor(
            None,
            build_graph,
            scan_result,
            config,
            app.state.project_id,
            existing_manual_edges,
        )

        app.state.graph = graph
        save_graph(graph, app.state.data_dir, app.state.project_id)

        logger.info(
            "Initial scan complete: %d nodes, %d edges",
            len(graph.nodes),
            len(graph.edges),
        )
    except Exception:
        logger.exception("Initial scan failed")
    finally:
        app.state.scan_in_progress = False
