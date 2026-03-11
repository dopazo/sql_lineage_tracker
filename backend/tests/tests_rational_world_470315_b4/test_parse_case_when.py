"""Tests for CASE WHEN, LOWER(), UPPER(), COALESCE and IF() lineage parsing.

Views tested:
  - staging.products_clean:      CASE WHEN, LOWER(), UPPER(), WHERE filter
  - staging.transactions_clean:  COALESCE(), IF(), boolean derived column
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


# ──────────────────────────────────────────────────────────────────────────────
# staging.products_clean
# ──────────────────────────────────────────────────────────────────────────────

class TestProductsClean:
    """staging.products_clean: renames, scalar functions and CASE WHEN."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "staging.products_clean",
            view_sql["staging.products_clean"],
            schemas,
        )

    # ── estructura del grafo ──────────────────────────────────────────────────

    def test_single_edge_from_raw_data_products(self):
        """Solo debe haber un arco, desde raw_data.products."""
        assert len(self.edges) == 1
        assert self.edges[0].source_node == "raw_data.products"
        assert self.edges[0].target_node == "staging.products_clean"

    def test_all_output_columns_mapped(self):
        """Todas las columnas de salida deben estar mapeadas."""
        edge = _get_edge(self.edges, "raw_data.products")
        mapped = {m.target_column for m in edge.column_mappings}
        assert mapped == {"product_id", "product_name", "category", "unit_price", "status"}

    # ── columnas directas ─────────────────────────────────────────────────────

    def test_product_id_direct(self):
        """product_id se pasa sin transformación."""
        m = _get_mapping(self.edges, "raw_data.products", "product_id")
        assert m is not None
        assert m.source_columns == ["product_id"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_unit_price_direct(self):
        """unit_price se pasa sin transformación."""
        m = _get_mapping(self.edges, "raw_data.products", "unit_price")
        assert m is not None
        assert m.source_columns == ["unit_price"]
        assert m.transformation == "direct"
        assert m.expression is None

    # ── expresiones escalares ─────────────────────────────────────────────────

    def test_product_name_expression_lower(self):
        """product_name = LOWER(name) debe ser expression con fuente 'name'."""
        m = _get_mapping(self.edges, "raw_data.products", "product_name")
        assert m is not None
        assert m.source_columns == ["name"]
        assert m.transformation == "expression"
        assert m.expression is not None
        assert "LOWER" in m.expression

    def test_category_expression_upper(self):
        """category = UPPER(category) debe ser expression con fuente 'category'."""
        m = _get_mapping(self.edges, "raw_data.products", "category")
        assert m is not None
        assert m.source_columns == ["category"]
        assert m.transformation == "expression"
        assert m.expression is not None
        assert "UPPER" in m.expression

    # ── CASE WHEN ─────────────────────────────────────────────────────────────

    def test_status_case_when_expression(self):
        """status = CASE WHEN active THEN ... debe ser expression con fuente 'active'."""
        m = _get_mapping(self.edges, "raw_data.products", "status")
        assert m is not None
        assert m.source_columns == ["active"]
        assert m.transformation == "expression"

    def test_status_expression_contains_case(self):
        """La expresión de status debe incluir la palabra CASE."""
        m = _get_mapping(self.edges, "raw_data.products", "status")
        assert m.expression is not None
        assert "CASE" in m.expression

    def test_status_expression_contains_both_branches(self):
        """La expresión de status debe incluir ambas ramas del CASE."""
        m = _get_mapping(self.edges, "raw_data.products", "status")
        assert "active" in m.expression
        assert "discontinued" in m.expression


# ──────────────────────────────────────────────────────────────────────────────
# staging.transactions_clean
# ──────────────────────────────────────────────────────────────────────────────

class TestTransactionsClean:
    """staging.transactions_clean: COALESCE(), IF() y columna booleana derivada."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "staging.transactions_clean",
            view_sql["staging.transactions_clean"],
            schemas,
        )

    # ── estructura del grafo ──────────────────────────────────────────────────

    def test_single_edge_from_raw_data_transactions(self):
        """Solo debe haber un arco, desde raw_data.transactions."""
        assert len(self.edges) == 1
        assert self.edges[0].source_node == "raw_data.transactions"
        assert self.edges[0].target_node == "staging.transactions_clean"

    def test_all_output_columns_mapped(self):
        """Todas las columnas de salida deben estar mapeadas."""
        edge = _get_edge(self.edges, "raw_data.transactions")
        mapped = {m.target_column for m in edge.column_mappings}
        assert mapped == {
            "transaction_id", "product_id", "quantity", "sold_at", "channel", "is_online"
        }

    # ── columnas directas ─────────────────────────────────────────────────────

    def test_transaction_id_direct(self):
        m = _get_mapping(self.edges, "raw_data.transactions", "transaction_id")
        assert m is not None
        assert m.source_columns == ["transaction_id"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_product_id_direct(self):
        m = _get_mapping(self.edges, "raw_data.transactions", "product_id")
        assert m is not None
        assert m.source_columns == ["product_id"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_quantity_direct(self):
        m = _get_mapping(self.edges, "raw_data.transactions", "quantity")
        assert m is not None
        assert m.source_columns == ["quantity"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_sold_at_direct(self):
        m = _get_mapping(self.edges, "raw_data.transactions", "sold_at")
        assert m is not None
        assert m.source_columns == ["sold_at"]
        assert m.transformation == "direct"
        assert m.expression is None

    # ── COALESCE ─────────────────────────────────────────────────────────────

    def test_channel_coalesce_expression(self):
        """channel = COALESCE(channel, 'unknown') debe ser expression."""
        m = _get_mapping(self.edges, "raw_data.transactions", "channel")
        assert m is not None
        assert m.source_columns == ["channel"]
        assert m.transformation == "expression"

    def test_channel_expression_contains_coalesce(self):
        """La expresión de channel debe contener COALESCE."""
        m = _get_mapping(self.edges, "raw_data.transactions", "channel")
        assert m.expression is not None
        assert "COALESCE" in m.expression

    # ── IF() ──────────────────────────────────────────────────────────────────

    def test_is_online_if_expression(self):
        """is_online = IF(channel = 'online', ...) debe ser expression con fuente 'channel'."""
        m = _get_mapping(self.edges, "raw_data.transactions", "is_online")
        assert m is not None
        assert m.source_columns == ["channel"]
        assert m.transformation == "expression"

    def test_is_online_expression_contains_if(self):
        """La expresión de is_online debe contener IF."""
        m = _get_mapping(self.edges, "raw_data.transactions", "is_online")
        assert m.expression is not None
        assert "IF" in m.expression

    def test_is_online_and_channel_share_same_source_column(self):
        """Tanto channel como is_online derivan de la columna 'channel' de la tabla raw."""
        m_channel = _get_mapping(self.edges, "raw_data.transactions", "channel")
        m_is_online = _get_mapping(self.edges, "raw_data.transactions", "is_online")
        assert m_channel.source_columns == ["channel"]
        assert m_is_online.source_columns == ["channel"]