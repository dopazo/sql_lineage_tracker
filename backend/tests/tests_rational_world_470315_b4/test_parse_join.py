"""Tests for JOIN lineage parsing.

View tested:
  - staging.orders_with_customer: JOIN between orders_clean and customers_clean
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


class TestOrdersWithCustomer:
    """staging.orders_with_customer: columns from two joined views."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "staging.orders_with_customer",
            view_sql["staging.orders_with_customer"],
            schemas,
        )

    def test_id_pedido_from_orders_clean(self):
        m = _get_mapping(self.edges, "staging.orders_clean", "id_pedido")
        assert m is not None
        assert m.source_columns == ["id_pedido"]
        assert m.transformation == "direct"

    def test_revenue_from_orders_clean(self):
        m = _get_mapping(self.edges, "staging.orders_clean", "revenue")
        assert m is not None
        assert m.source_columns == ["revenue"]
        assert m.transformation == "direct"

    def test_created_at_from_orders_clean(self):
        m = _get_mapping(self.edges, "staging.orders_clean", "created_at")
        assert m is not None
        assert m.source_columns == ["created_at"]
        assert m.transformation == "direct"

    def test_nombre_as_nombre_cliente(self):
        m = _get_mapping(self.edges, "staging.customers_clean", "nombre_cliente")
        assert m is not None
        assert m.source_columns == ["nombre"]
        assert m.transformation == "rename"

    def test_pais_from_customers_clean(self):
        m = _get_mapping(self.edges, "staging.customers_clean", "pais")
        assert m is not None
        assert m.source_columns == ["pais"]
        assert m.transformation == "direct"

    def test_edge_from_orders_clean_exists(self):
        edge = _get_edge(self.edges, "staging.orders_clean")
        assert edge is not None
        assert edge.target_node == "staging.orders_with_customer"
        assert len(edge.column_mappings) == 3  # id_pedido, revenue, created_at

    def test_edge_from_customers_clean_exists(self):
        edge = _get_edge(self.edges, "staging.customers_clean")
        assert edge is not None
        assert edge.target_node == "staging.orders_with_customer"
        assert len(edge.column_mappings) == 2  # nombre_cliente, pais
