"""Tests for rename/alias lineage parsing.

Views tested:
  - staging.orders_clean: SELECT col AS alias FROM table
  - staging.customers_clean: SELECT col AS alias, UPPER(col) AS alias
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


class TestOrdersClean:
    """staging.orders_clean: simple renames + direct pass-through."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "staging.orders_clean", view_sql["staging.orders_clean"], schemas
        )

    def test_single_edge_from_raw_data_orders(self):
        assert len(self.edges) == 1
        assert self.edges[0].source_node == "raw_data.orders"
        assert self.edges[0].target_node == "staging.orders_clean"

    def test_order_id_renamed_to_id_pedido(self):
        m = _get_mapping(self.edges, "raw_data.orders", "id_pedido")
        assert m is not None
        assert m.source_columns == ["order_id"]
        assert m.transformation == "rename"
        assert m.expression is None

    def test_customer_id_renamed_to_id_cliente(self):
        m = _get_mapping(self.edges, "raw_data.orders", "id_cliente")
        assert m is not None
        assert m.source_columns == ["customer_id"]
        assert m.transformation == "rename"

    def test_amount_renamed_to_revenue(self):
        m = _get_mapping(self.edges, "raw_data.orders", "revenue")
        assert m is not None
        assert m.source_columns == ["amount"]
        assert m.transformation == "rename"

    def test_created_at_direct(self):
        m = _get_mapping(self.edges, "raw_data.orders", "created_at")
        assert m is not None
        assert m.source_columns == ["created_at"]
        assert m.transformation == "direct"
        assert m.expression is None


class TestCustomersClean:
    """staging.customers_clean: renames + expression (UPPER)."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "staging.customers_clean",
            view_sql["staging.customers_clean"],
            schemas,
        )

    def test_customer_id_renamed_to_id_cliente(self):
        m = _get_mapping(self.edges, "raw_data.customers", "id_cliente")
        assert m is not None
        assert m.source_columns == ["customer_id"]
        assert m.transformation == "rename"

    def test_name_renamed_to_nombre(self):
        m = _get_mapping(self.edges, "raw_data.customers", "nombre")
        assert m is not None
        assert m.source_columns == ["name"]
        assert m.transformation == "rename"

    def test_country_expression_upper(self):
        m = _get_mapping(self.edges, "raw_data.customers", "pais")
        assert m is not None
        assert m.source_columns == ["country"]
        assert m.transformation == "expression"
        assert m.expression is not None
        assert "UPPER" in m.expression

    def test_registered_at_renamed_to_fecha_registro(self):
        m = _get_mapping(self.edges, "raw_data.customers", "fecha_registro")
        assert m is not None
        assert m.source_columns == ["registered_at"]
        assert m.transformation == "rename"
