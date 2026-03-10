"""FastAPI application for the lineage tracker."""

from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse


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

    @app.get("/api/health")
    async def health() -> dict:
        return {
            "status": "ok",
            "project_id": project_id,
            "no_scan": no_scan,
        }

    @app.get("/api/graph")
    async def get_graph() -> JSONResponse:
        # Placeholder — will load from persistence in later phases
        return JSONResponse(content={"metadata": None, "nodes": {}, "edges": []})

    return app
