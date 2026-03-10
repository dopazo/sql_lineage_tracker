"""Tests for the persistence module."""

from __future__ import annotations

from pathlib import Path

import pytest

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
from lineage_tracker.persistence import (
    dict_to_graph,
    graph_to_dict,
    load_graph,
    save_graph,
)


def _make_sample_graph() -> LineageGraph:
    """Create a sample graph for testing."""
    return LineageGraph(
        metadata=GraphMetadata(
            project_id="test-project",
            generated_at="2026-03-09T10:30:00Z",
            description="Test graph",
            scan_config=ScanConfig(
                target="analytics.monthly_revenue",
                datasets=["staging", "raw_data"],
                depth=3,
            ),
            scan_stats=ScanStats(
                total_nodes=2,
                total_edges=1,
                nodes_by_type={"table": 1, "view": 1},
                orphan_nodes=0,
                terminal_nodes=1,
                truncated_nodes=0,
                parse_errors=0,
            ),
        ),
        nodes={
            "raw_data.orders": LineageNode(
                id="raw_data.orders",
                type="table",
                dataset="raw_data",
                name="orders",
                columns=[
                    ColumnInfo(name="order_id", data_type="STRING", lineage_status="resolved"),
                    ColumnInfo(name="amount", data_type="FLOAT64", lineage_status="resolved"),
                ],
                source="ingestion",
                sql=None,
            ),
            "staging.orders_clean": LineageNode(
                id="staging.orders_clean",
                type="view",
                dataset="staging",
                name="orders_clean",
                columns=[
                    ColumnInfo(name="id_pedido", data_type="STRING", lineage_status="resolved"),
                    ColumnInfo(name="revenue", data_type="FLOAT64", lineage_status="resolved"),
                ],
                source="bigquery_view",
                sql="SELECT order_id AS id_pedido, amount AS revenue FROM `raw_data.orders`",
            ),
        },
        edges=[
            LineageEdge(
                id="edge_raw_data.orders__staging.orders_clean",
                source_node="raw_data.orders",
                target_node="staging.orders_clean",
                edge_type="automatic",
                column_mappings=[
                    ColumnMapping(
                        source_columns=["order_id"],
                        target_column="id_pedido",
                        transformation="rename",
                    ),
                    ColumnMapping(
                        source_columns=["amount"],
                        target_column="revenue",
                        transformation="rename",
                    ),
                ],
            ),
        ],
    )


class TestGraphSerialization:
    def test_roundtrip(self):
        """graph_to_dict -> dict_to_graph should preserve all data."""
        graph = _make_sample_graph()
        data = graph_to_dict(graph)
        restored = dict_to_graph(data)

        # Metadata
        assert restored.metadata.project_id == "test-project"
        assert restored.metadata.generated_at == "2026-03-09T10:30:00Z"
        assert restored.metadata.description == "Test graph"
        assert restored.metadata.scan_config.target == "analytics.monthly_revenue"
        assert restored.metadata.scan_config.datasets == ["staging", "raw_data"]
        assert restored.metadata.scan_config.depth == 3
        assert restored.metadata.scan_stats.total_nodes == 2
        assert restored.metadata.scan_stats.total_edges == 1
        assert restored.metadata.scan_stats.nodes_by_type == {"table": 1, "view": 1}

        # Nodes
        assert len(restored.nodes) == 2
        assert "raw_data.orders" in restored.nodes
        assert "staging.orders_clean" in restored.nodes

        orders = restored.nodes["raw_data.orders"]
        assert orders.type == "table"
        assert orders.dataset == "raw_data"
        assert orders.source == "ingestion"
        assert orders.sql is None
        assert len(orders.columns) == 2
        assert orders.columns[0].name == "order_id"
        assert orders.columns[0].data_type == "STRING"

        clean = restored.nodes["staging.orders_clean"]
        assert clean.type == "view"
        assert clean.source == "bigquery_view"
        assert clean.sql is not None
        assert "order_id AS id_pedido" in clean.sql

        # Edges
        assert len(restored.edges) == 1
        edge = restored.edges[0]
        assert edge.source_node == "raw_data.orders"
        assert edge.target_node == "staging.orders_clean"
        assert edge.edge_type == "automatic"
        assert len(edge.column_mappings) == 2
        assert edge.column_mappings[0].target_column == "id_pedido"
        assert edge.column_mappings[0].transformation == "rename"

    def test_dict_format_matches_spec(self):
        """The dict format should match the JSON structure from the spec."""
        graph = _make_sample_graph()
        data = graph_to_dict(graph)

        # Top-level keys
        assert set(data.keys()) == {"metadata", "nodes", "edges"}

        # Metadata structure
        meta = data["metadata"]
        assert "project_id" in meta
        assert "generated_at" in meta
        assert "scan_config" in meta
        assert "scan_stats" in meta

        # Node structure (should not include 'id' key — it's the dict key)
        node = data["nodes"]["raw_data.orders"]
        assert "type" in node
        assert "dataset" in node
        assert "name" in node
        assert "columns" in node
        assert "source" in node
        assert "sql" in node
        assert "status" in node

        # Edge structure
        edge = data["edges"][0]
        assert "id" in edge
        assert "source_node" in edge
        assert "target_node" in edge
        assert "edge_type" in edge
        assert "column_mappings" in edge

    def test_empty_graph(self):
        """An empty graph should serialize/deserialize correctly."""
        graph = LineageGraph(
            metadata=GraphMetadata(
                project_id="empty-project",
                generated_at="2026-01-01T00:00:00Z",
            ),
        )
        data = graph_to_dict(graph)
        restored = dict_to_graph(data)

        assert restored.metadata.project_id == "empty-project"
        assert len(restored.nodes) == 0
        assert len(restored.edges) == 0


class TestDiskPersistence:
    def test_save_and_load(self, tmp_path: Path):
        graph = _make_sample_graph()
        save_graph(graph, tmp_path, "test-project")

        loaded = load_graph(tmp_path, "test-project")
        assert loaded is not None
        assert len(loaded.nodes) == 2
        assert len(loaded.edges) == 1
        assert loaded.metadata.project_id == "test-project"

    def test_load_nonexistent(self, tmp_path: Path):
        loaded = load_graph(tmp_path, "nonexistent")
        assert loaded is None

    def test_save_creates_directory(self, tmp_path: Path):
        graph = _make_sample_graph()
        path = save_graph(graph, tmp_path, "new-project")

        assert path.exists()
        assert path.parent.name == "new-project"

    def test_save_overwrites(self, tmp_path: Path):
        graph1 = _make_sample_graph()
        save_graph(graph1, tmp_path, "test-project")

        # Modify and save again
        graph2 = _make_sample_graph()
        graph2.metadata.description = "Updated"
        save_graph(graph2, tmp_path, "test-project")

        loaded = load_graph(tmp_path, "test-project")
        assert loaded is not None
        assert loaded.metadata.description == "Updated"
