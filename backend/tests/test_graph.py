"""Unit tests for the graph module.

Tests topological sorting, graph building, hole detection,
column lineage status marking, and statistics computation.
"""

import pytest

from lineage_tracker.graph import build_graph, topological_sort
from lineage_tracker.models import (
    ColumnInfo,
    LineageEdge,
    LineageNode,
    ColumnMapping,
    ScanConfig,
)
from lineage_tracker.scanner import ScanResult


def _make_table(node_id: str, columns: list[str] | None = None) -> LineageNode:
    """Helper to create a base table node."""
    dataset, name = node_id.split(".")
    cols = [ColumnInfo(name=c, data_type="STRING") for c in (columns or [])]
    return LineageNode(
        id=node_id,
        type="table",
        dataset=dataset,
        name=name,
        columns=cols,
        source="ingestion",
    )


def _make_view(
    node_id: str,
    sql: str,
    columns: list[str] | None = None,
) -> LineageNode:
    """Helper to create a view node."""
    dataset, name = node_id.split(".")
    cols = [ColumnInfo(name=c, data_type="STRING") for c in (columns or [])]
    return LineageNode(
        id=node_id,
        type="view",
        dataset=dataset,
        name=name,
        columns=cols,
        source="bigquery_view",
        sql=sql,
    )


class TestTopologicalSort:
    """Tests for topological_sort()."""

    def test_base_tables_first(self):
        """Base tables (no SQL) should come before views."""
        nodes = {
            "ds.view_a": _make_view(
                "ds.view_a",
                "SELECT col FROM ds.table_b",
                ["col"],
            ),
            "ds.table_b": _make_table("ds.table_b", ["col"]),
        }
        order = topological_sort(nodes)
        assert order.index("ds.table_b") < order.index("ds.view_a")

    def test_chain_order(self):
        """A -> B -> C should sort as A, B, C."""
        nodes = {
            "ds.c": _make_view("ds.c", "SELECT x FROM ds.b", ["x"]),
            "ds.b": _make_view("ds.b", "SELECT x FROM ds.a", ["x"]),
            "ds.a": _make_table("ds.a", ["x"]),
        }
        order = topological_sort(nodes)
        assert order.index("ds.a") < order.index("ds.b")
        assert order.index("ds.b") < order.index("ds.c")

    def test_diamond_dependency(self):
        """Diamond: A depends on B and C, both depend on D."""
        nodes = {
            "ds.a": _make_view(
                "ds.a",
                "SELECT x FROM ds.b JOIN ds.c ON TRUE",
                ["x"],
            ),
            "ds.b": _make_view("ds.b", "SELECT x FROM ds.d", ["x"]),
            "ds.c": _make_view("ds.c", "SELECT x FROM ds.d", ["x"]),
            "ds.d": _make_table("ds.d", ["x"]),
        }
        order = topological_sort(nodes)
        assert order.index("ds.d") < order.index("ds.b")
        assert order.index("ds.d") < order.index("ds.c")
        assert order.index("ds.b") < order.index("ds.a")
        assert order.index("ds.c") < order.index("ds.a")

    def test_multiple_independent_chains(self):
        """Independent chains should both be sorted correctly."""
        nodes = {
            "ds.v1": _make_view("ds.v1", "SELECT x FROM ds.t1", ["x"]),
            "ds.t1": _make_table("ds.t1", ["x"]),
            "ds.v2": _make_view("ds.v2", "SELECT y FROM ds.t2", ["y"]),
            "ds.t2": _make_table("ds.t2", ["y"]),
        }
        order = topological_sort(nodes)
        assert order.index("ds.t1") < order.index("ds.v1")
        assert order.index("ds.t2") < order.index("ds.v2")

    def test_cycle_does_not_crash(self):
        """Cycles should not cause infinite loop; all nodes included."""
        nodes = {
            "ds.a": _make_view("ds.a", "SELECT x FROM ds.b", ["x"]),
            "ds.b": _make_view("ds.b", "SELECT x FROM ds.a", ["x"]),
        }
        order = topological_sort(nodes)
        assert set(order) == {"ds.a", "ds.b"}

    def test_empty_nodes(self):
        """Empty dict should return empty list."""
        assert topological_sort({}) == []

    def test_single_node(self):
        """Single node returns that node."""
        nodes = {"ds.t": _make_table("ds.t", ["x"])}
        assert topological_sort(nodes) == ["ds.t"]

    def test_cross_dataset_reference(self):
        """Views referencing other datasets are handled."""
        nodes = {
            "analytics.report": _make_view(
                "analytics.report",
                "SELECT x FROM staging.clean",
                ["x"],
            ),
            "staging.clean": _make_table("staging.clean", ["x"]),
        }
        order = topological_sort(nodes)
        assert order.index("staging.clean") < order.index("analytics.report")


class TestBuildGraph:
    """Tests for build_graph()."""

    def test_simple_chain(self):
        """Build graph for table -> view chain."""
        scan_result = ScanResult(
            nodes={
                "ds.orders": _make_table("ds.orders", ["order_id", "amount"]),
                "ds.clean": _make_view(
                    "ds.clean",
                    "SELECT order_id AS id, amount AS revenue FROM ds.orders",
                    ["id", "revenue"],
                ),
            }
        )
        config = ScanConfig(target="ds.clean", datasets=["ds"])
        graph = build_graph(scan_result, config, "test-project")

        assert len(graph.nodes) == 2
        assert len(graph.edges) >= 1
        assert graph.metadata.project_id == "test-project"
        assert graph.metadata.scan_config == config

        # Check that there's an edge from orders to clean
        edge = next(
            (e for e in graph.edges if e.source_node == "ds.orders"),
            None,
        )
        assert edge is not None
        assert edge.target_node == "ds.clean"

    def test_stats_computed(self):
        """Stats should be populated."""
        scan_result = ScanResult(
            nodes={
                "ds.t1": _make_table("ds.t1", ["x"]),
                "ds.t2": _make_table("ds.t2", ["y"]),
            }
        )
        graph = build_graph(scan_result, ScanConfig(), "proj")

        stats = graph.metadata.scan_stats
        assert stats.total_nodes == 2
        assert stats.total_edges == 0
        # Two disconnected base tables = 2 orphans
        assert stats.orphan_nodes == 2

    def test_truncated_nodes_counted(self):
        """Truncated nodes should be counted in stats."""
        nodes = {
            "ds.t": _make_table("ds.t", ["x"]),
        }
        nodes["ds.t"].status = "truncated"
        nodes["ds.t"].status_message = "Depth limit"

        scan_result = ScanResult(nodes=nodes)
        graph = build_graph(scan_result, ScanConfig(), "proj")
        assert graph.metadata.scan_stats.truncated_nodes == 1

    def test_manual_edges_preserved(self):
        """Manual edges should be preserved if nodes still exist."""
        scan_result = ScanResult(
            nodes={
                "ds.a": _make_table("ds.a", ["x"]),
                "ds.b": _make_table("ds.b", ["y"]),
            }
        )
        manual = [
            LineageEdge(
                id="manual_1",
                source_node="ds.a",
                target_node="ds.b",
                edge_type="manual",
                column_mappings=[
                    ColumnMapping(
                        source_columns=["x"],
                        target_column="y",
                        transformation="external",
                    )
                ],
            )
        ]

        graph = build_graph(scan_result, ScanConfig(), "proj", existing_manual_edges=manual)
        assert any(e.id == "manual_1" for e in graph.edges)

    def test_manual_edges_dropped_if_nodes_missing(self):
        """Manual edges referencing deleted nodes should be dropped."""
        scan_result = ScanResult(
            nodes={
                "ds.a": _make_table("ds.a", ["x"]),
            }
        )
        manual = [
            LineageEdge(
                id="manual_1",
                source_node="ds.a",
                target_node="ds.deleted",
                edge_type="manual",
            )
        ]

        graph = build_graph(scan_result, ScanConfig(), "proj", existing_manual_edges=manual)
        assert not any(e.id == "manual_1" for e in graph.edges)

    def test_column_lineage_status_marked(self):
        """Columns should be marked as resolved or unknown after parsing."""
        scan_result = ScanResult(
            nodes={
                "ds.src": _make_table("ds.src", ["a", "b"]),
                "ds.v": _make_view(
                    "ds.v",
                    "SELECT a FROM ds.src",
                    ["a"],
                ),
            }
        )
        graph = build_graph(scan_result, ScanConfig(), "proj")
        view_node = graph.nodes["ds.v"]

        # Column 'a' should be resolved (it's selected from src)
        a_col = next(c for c in view_node.columns if c.name == "a")
        assert a_col.lineage_status == "resolved"

    def test_parse_error_sets_warning(self):
        """Views with unparseable SQL should get warning status."""
        scan_result = ScanResult(
            nodes={
                "ds.bad": _make_view(
                    "ds.bad",
                    "THIS IS NOT VALID SQL AT ALL @@@ !!!",
                    ["x"],
                ),
            }
        )
        graph = build_graph(scan_result, ScanConfig(), "proj")

        # The node should still exist
        assert "ds.bad" in graph.nodes
        # Stats should count the parse error
        assert graph.metadata.scan_stats.parse_errors >= 1

    def test_metadata_timestamp(self):
        """Graph metadata should have a generated_at timestamp."""
        scan_result = ScanResult(nodes={})
        graph = build_graph(scan_result, ScanConfig(), "proj")
        assert graph.metadata.generated_at is not None
        assert "T" in graph.metadata.generated_at  # ISO format

    def test_terminal_nodes_detected(self):
        """Nodes with incoming but no outgoing edges = terminal."""
        scan_result = ScanResult(
            nodes={
                "ds.src": _make_table("ds.src", ["x"]),
                "ds.v": _make_view(
                    "ds.v",
                    "SELECT x FROM ds.src",
                    ["x"],
                ),
            }
        )
        graph = build_graph(scan_result, ScanConfig(), "proj")
        # ds.v has an incoming edge (from ds.src) but no outgoing
        assert graph.metadata.scan_stats.terminal_nodes == 1
