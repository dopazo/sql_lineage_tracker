"""FastAPI application for the lineage tracker."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from lineage_tracker.extractor import BigQueryExtractor

logger = logging.getLogger(__name__)


def create_app(
    project_id: str,
    data_dir: Path,
    no_scan: bool = False,
    initial_scan_config: dict[str, Any] | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(title="SQL Lineage Tracker", version="0.1.0")

    app.state.project_id = project_id
    app.state.data_dir = data_dir
    app.state.no_scan = no_scan
    app.state.initial_scan_config = initial_scan_config

    # Initialize BigQuery extractor (lazy — only used when not in no_scan mode)
    extractor: BigQueryExtractor | None = None
    if not no_scan:
        try:
            extractor = BigQueryExtractor(project_id)
        except Exception:
            logger.exception("Failed to initialize BigQuery client")

    app.state.extractor = extractor

    @app.get("/api/health")
    async def health() -> dict:
        bq_connected = extractor is not None
        return {
            "status": "ok",
            "project_id": project_id,
            "no_scan": no_scan,
            "bigquery_connected": bq_connected,
        }

    @app.get("/api/graph")
    async def get_graph() -> JSONResponse:
        # Placeholder — will load from persistence in later phases
        return JSONResponse(content={"metadata": None, "nodes": {}, "edges": []})

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
