"""Tests for LEFT JOIN lineage parsing.

View tested:
  - analytics.product_sales: LEFT JOIN between products_clean and transactions_clean,
    with COUNT(DISTINCT), SUM and MAX aggregations on the joined table.
"""

import pytest

from lineage_tracker.parser import parse_view_lineage


def _get_mapping(edges, source_node, target_column):
    """Helper: find a specific column mapping from edges."""
    for edge in edges:
        if edge.source_node == source_node:
            for m in edge.column_mappings:
                if m.target_column == target_column:
                    return m
    return None


def _get_edge(edges, source_node):
    """Helper: find edge by source node."""
    for edge in edges:
        if edge.source_node == source_node:
            return edge
    return None


class TestProductSales:
    """analytics.product_sales: LEFT JOIN with aggregations on the right-hand table."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "analytics.product_sales",
            view_sql["analytics.product_sales"],
            schemas,
        )

    # ── Graph structure ───────────────────────────────────────────────────────

    def test_two_source_edges(self):
        """LEFT JOIN produces one edge per source table."""
        assert len(self.edges) == 2

    def test_source_nodes(self):
        source_nodes = {e.source_node for e in self.edges}
        assert source_nodes == {"staging.products_clean", "staging.transactions_clean"}

    def test_target_node(self):
        for edge in self.edges:
            assert edge.target_node == "analytics.product_sales"

    # ── Columns from the left table (staging.products_clean) ─────────────────

    def test_product_id_direct(self):
        m = _get_mapping(self.edges, "staging.products_clean", "product_id")
        assert m is not None
        assert m.source_columns == ["product_id"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_product_name_direct(self):
        m = _get_mapping(self.edges, "staging.products_clean", "product_name")
        assert m is not None
        assert m.source_columns == ["product_name"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_category_direct(self):
        m = _get_mapping(self.edges, "staging.products_clean", "category")
        assert m is not None
        assert m.source_columns == ["category"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_unit_price_direct(self):
        m = _get_mapping(self.edges, "staging.products_clean", "unit_price")
        assert m is not None
        assert m.source_columns == ["unit_price"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_left_table_has_four_mappings(self):
        """products_clean contributes exactly the 4 GROUP BY key columns."""
        edge = _get_edge(self.edges, "staging.products_clean")
        assert edge is not None
        target_cols = {m.target_column for m in edge.column_mappings}
        assert target_cols == {"product_id", "product_name", "category", "unit_price"}

    # ── Aggregated columns from the right table (staging.transactions_clean) ──

    def test_total_units_sold_sum_aggregation(self):
        m = _get_mapping(self.edges, "staging.transactions_clean", "total_units_sold")
        assert m is not None
        assert m.source_columns == ["quantity"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "SUM" in m.expression

    def test_total_transactions_count_distinct(self):
        m = _get_mapping(self.edges, "staging.transactions_clean", "total_transactions")
        assert m is not None
        assert m.source_columns == ["transaction_id"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "COUNT" in m.expression
        assert "DISTINCT" in m.expression

    def test_channels_used_count_distinct(self):
        m = _get_mapping(self.edges, "staging.transactions_clean", "channels_used")
        assert m is not None
        assert m.source_columns == ["channel"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "COUNT" in m.expression
        assert "DISTINCT" in m.expression

    def test_last_sale_at_max_aggregation(self):
        m = _get_mapping(self.edges, "staging.transactions_clean", "last_sale_at")
        assert m is not None
        assert m.source_columns == ["sold_at"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "MAX" in m.expression

    def test_right_table_has_four_aggregated_mappings(self):
        """transactions_clean contributes exactly the 4 aggregated columns."""
        edge = _get_edge(self.edges, "staging.transactions_clean")
        assert edge is not None
        target_cols = {m.target_column for m in edge.column_mappings}
        assert target_cols == {
            "total_units_sold",
            "total_transactions",
            "channels_used",
            "last_sale_at",
        }

    def test_no_aggregation_on_left_table_columns(self):
        """Columns coming from the left table (GROUP BY keys) are never aggregations."""
        edge = _get_edge(self.edges, "staging.products_clean")
        assert edge is not None
        for m in edge.column_mappings:
            assert m.transformation != "aggregation", (
                f"{m.target_column} should not be aggregation on the left table"
            )

    def test_all_right_table_columns_are_aggregations(self):
        """Every column from the right (nullable) table is an aggregation."""
        edge = _get_edge(self.edges, "staging.transactions_clean")
        assert edge is not None
        for m in edge.column_mappings:
            assert m.transformation == "aggregation", (
                f"{m.target_column} expected aggregation, got {m.transformation!r}"
            )