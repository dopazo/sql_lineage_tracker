"""Tests for aggregation lineage parsing.

View tested:
  - analytics.monthly_revenue: FORMAT_TIMESTAMP, SUM, COUNT, GROUP BY
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


class TestMonthlyRevenue:
    """analytics.monthly_revenue: expressions + aggregations."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "analytics.monthly_revenue",
            view_sql["analytics.monthly_revenue"],
            schemas,
        )

    def test_single_source_edge(self):
        assert len(self.edges) == 1
        assert self.edges[0].source_node == "staging.orders_with_customer"

    def test_month_expression(self):
        m = _get_mapping(self.edges, "staging.orders_with_customer", "month")
        assert m is not None
        assert m.source_columns == ["created_at"]
        assert m.transformation == "expression"
        assert m.expression is not None
        assert "FORMAT_TIMESTAMP" in m.expression

    def test_pais_direct(self):
        m = _get_mapping(self.edges, "staging.orders_with_customer", "pais")
        assert m is not None
        assert m.source_columns == ["pais"]
        assert m.transformation == "direct"

    def test_total_revenue_aggregation(self):
        m = _get_mapping(
            self.edges, "staging.orders_with_customer", "total_revenue"
        )
        assert m is not None
        assert m.source_columns == ["revenue"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "SUM" in m.expression

    def test_order_count_aggregation(self):
        m = _get_mapping(
            self.edges, "staging.orders_with_customer", "order_count"
        )
        assert m is not None
        assert m.source_columns == ["*"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "COUNT" in m.expression
