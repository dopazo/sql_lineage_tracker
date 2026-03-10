"""Tests for the FastAPI server endpoints."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lineage_tracker.models import (
    ColumnInfo,
    ColumnMapping,
    GraphMetadata,
    LineageEdge,
    LineageGraph,
    LineageNode,
    ScanConfig,
    ScanStats,
)
from lineage_tracker.persistence import save_graph
from lineage_tracker.server import create_app


def _make_sample_graph() -> LineageGraph:
    return LineageGraph(
        metadata=GraphMetadata(
            project_id="test-project",
            generated_at="2026-03-09T10:30:00Z",
            scan_config=ScanConfig(target="staging.view1", datasets=["staging"]),
            scan_stats=ScanStats(total_nodes=1, total_edges=0),
        ),
        nodes={
            "staging.view1": LineageNode(
                id="staging.view1",
                type="view",
                dataset="staging",
                name="view1",
                columns=[
                    ColumnInfo(name="col1", data_type="STRING"),
                ],
                source="bigquery_view",
                sql="SELECT col1 FROM raw.t1",
            ),
        },
        edges=[],
    )


class TestHealthEndpoint:
    def test_health_no_scan(self, tmp_path: Path):
        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,
        )
        client = TestClient(app)
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["project_id"] == "test-project"
        assert data["no_scan"] is True
        assert data["bigquery_connected"] is False
        assert data["has_graph"] is False

    def test_health_with_cached_graph(self, tmp_path: Path):
        # Save a graph to disk first
        graph = _make_sample_graph()
        save_graph(graph, tmp_path, "test-project")

        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,
        )
        client = TestClient(app)
        resp = client.get("/api/health")
        data = resp.json()
        assert data["has_graph"] is True


class TestGraphEndpoint:
    def test_no_graph(self, tmp_path: Path):
        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,
        )
        client = TestClient(app)
        resp = client.get("/api/graph")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metadata"] is None
        assert data["nodes"] == {}
        assert data["edges"] == []

    def test_with_cached_graph(self, tmp_path: Path):
        graph = _make_sample_graph()
        save_graph(graph, tmp_path, "test-project")

        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,
        )
        client = TestClient(app)
        resp = client.get("/api/graph")
        assert resp.status_code == 200
        data = resp.json()

        assert data["metadata"]["project_id"] == "test-project"
        assert "staging.view1" in data["nodes"]
        node = data["nodes"]["staging.view1"]
        assert node["type"] == "view"
        assert node["name"] == "view1"
        assert len(node["columns"]) == 1
        assert node["columns"][0]["name"] == "col1"


class TestDatasetsEndpoint:
    def test_no_bigquery(self, tmp_path: Path):
        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,
        )
        client = TestClient(app)
        resp = client.get("/api/datasets")
        assert resp.status_code == 503

    def test_tables_no_bigquery(self, tmp_path: Path):
        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,
        )
        client = TestClient(app)
        resp = client.get("/api/datasets/staging/tables")
        assert resp.status_code == 503
