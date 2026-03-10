"""Tests for CTE lineage parsing.

View tested:
  - analytics.customer_summary: CTE with aggregation + arithmetic expression
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


class TestCustomerSummary:
    """analytics.customer_summary: CTE -> final SELECT with derived column."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "analytics.customer_summary",
            view_sql["analytics.customer_summary"],
            schemas,
        )

    def test_single_source_edge(self):
        # All columns trace back to staging.orders_with_customer
        assert len(self.edges) == 1
        assert self.edges[0].source_node == "staging.orders_with_customer"

    def test_nombre_cliente_through_cte(self):
        m = _get_mapping(
            self.edges, "staging.orders_with_customer", "nombre_cliente"
        )
        assert m is not None
        assert m.source_columns == ["nombre_cliente"]
        assert m.transformation == "direct"

    def test_pais_through_cte(self):
        m = _get_mapping(self.edges, "staging.orders_with_customer", "pais")
        assert m is not None
        assert m.source_columns == ["pais"]
        assert m.transformation == "direct"

    def test_total_orders_aggregation(self):
        m = _get_mapping(
            self.edges, "staging.orders_with_customer", "total_orders"
        )
        assert m is not None
        assert m.transformation == "aggregation"
        assert m.source_columns == ["*"]
        assert m.expression is not None
        assert "COUNT" in m.expression

    def test_total_spent_aggregation(self):
        m = _get_mapping(
            self.edges, "staging.orders_with_customer", "total_spent"
        )
        assert m is not None
        assert m.transformation == "aggregation"
        assert m.source_columns == ["revenue"]
        assert m.expression is not None
        assert "SUM" in m.expression

    def test_avg_order_value_expression(self):
        m = _get_mapping(
            self.edges, "staging.orders_with_customer", "avg_order_value"
        )
        assert m is not None
        assert m.transformation == "expression"
        assert m.expression is not None
        assert "ROUND" in m.expression

    def test_source_is_orders_with_customer(self):
        # All columns ultimately come from staging.orders_with_customer
        source_nodes = {e.source_node for e in self.edges}
        assert source_nodes == {"staging.orders_with_customer"}
