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


class TestScanEndpoint:
    def test_scan_rejected_when_no_scan_mode(self, tmp_path: Path):
        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,
        )
        client = TestClient(app)
        resp = client.post("/api/scan", json={"target": "staging.view1"})
        assert resp.status_code == 400
        assert "disabled" in resp.json()["error"]

    def test_scan_rejected_when_no_bigquery(self, tmp_path: Path):
        # no_scan=False but BigQuery init will fail (no credentials)
        # We manually set extractor to None to simulate
        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,  # easiest way to get extractor=None
        )
        # Override no_scan to False so the check passes the first guard
        app.state.no_scan = False
        client = TestClient(app)
        resp = client.post("/api/scan", json={"target": "staging.view1"})
        assert resp.status_code == 503
        assert "BigQuery" in resp.json()["error"]

    def test_scan_rejected_when_already_in_progress(self, tmp_path: Path):
        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,
        )
        app.state.no_scan = False
        # Fake an extractor so it passes the None check
        app.state.extractor = "fake"
        app.state.scan_in_progress = True
        client = TestClient(app)
        resp = client.post("/api/scan", json={"target": "staging.view1"})
        assert resp.status_code == 409
        assert "already in progress" in resp.json()["error"]


class TestScanEventsEndpoint:
    def test_sse_endpoint_registered(self, tmp_path: Path):
        """Verify the SSE endpoint route exists on the app."""
        app = create_app(
            project_id="test-project",
            data_dir=tmp_path,
            no_scan=True,
        )
        routes = {r.path for r in app.routes}
        assert "/api/scan/events" in routes


class TestScanEventBus:
    def test_publish_and_subscribe(self):
        import asyncio
        import json

        from lineage_tracker.server import ScanEventBus

        bus = ScanEventBus()
        loop = asyncio.new_event_loop()
        bus.bind_loop(loop)

        q = bus.subscribe()
        bus.publish("scan_start", "Starting scan")

        # Process the call_soon_threadsafe callbacks
        loop.run_until_complete(asyncio.sleep(0))

        assert not q.empty()
        event = q.get_nowait()
        assert event["event"] == "scan_start"
        data = json.loads(event["data"])
        assert data["type"] == "scan_start"
        assert data["message"] == "Starting scan"
        assert "timestamp" in data

        loop.close()

    def test_finish_sends_none(self):
        import asyncio

        from lineage_tracker.server import ScanEventBus

        bus = ScanEventBus()
        q = bus.subscribe()
        bus.finish()

        assert not q.empty()
        event = q.get_nowait()
        assert event is None

    def test_reset_clears_state(self):
        import asyncio

        from lineage_tracker.server import ScanEventBus

        bus = ScanEventBus()
        q = bus.subscribe()
        bus.publish("test", "msg")
        bus.reset()

        # Old subscriber should get None (finish signal from reset)
        # New subscriber should get no history
        q2 = bus.subscribe()
        assert q2.empty()

    def test_late_subscriber_gets_history(self):
        import asyncio
        import json

        from lineage_tracker.server import ScanEventBus

        bus = ScanEventBus()
        # Publish before subscribing (no loop needed — no subscribers)
        bus.publish("event1", "first")
        bus.publish("event2", "second")

        q = bus.subscribe()
        # Should receive history
        assert not q.empty()
        e1 = q.get_nowait()
        e2 = q.get_nowait()
        assert e1["event"] == "event1"
        assert e2["event"] == "event2"
