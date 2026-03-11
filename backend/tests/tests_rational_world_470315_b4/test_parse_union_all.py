"""Tests for UNION ALL lineage parsing.

View tested:
  - staging.all_revenue: UNION ALL de staging.orders_clean + staging.transactions_clean

Comportamiento del parser con UNION ALL:
  - Genera 2 edges, uno por rama del UNION ALL.
  - Las columnas se trazan por separado a cada fuente.
  - Las columnas con valor literal (ej. 'order') quedan como [unknown] en su rama.
  - El campo 'event_type' solo aparece en el edge de orders_clean (primera rama).
"""

import pytest

from lineage_tracker.parser import parse_view_lineage


def _get_mapping(edges, source_node, target_column):
    """Helper: busca un ColumnMapping por source_node y target_column."""
    for edge in edges:
        if edge.source_node == source_node:
            for m in edge.column_mappings:
                if m.target_column == target_column:
                    return m
    return None


def _get_edge(edges, source_node):
    """Helper: busca un LineageEdge por source_node."""
    for edge in edges:
        if edge.source_node == source_node:
            return edge
    return None


class TestAllRevenue:
    """staging.all_revenue: UNION ALL entre dos fuentes independientes."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "staging.all_revenue",
            view_sql["staging.all_revenue"],
            schemas,
        )

    # ── Estructura del grafo ───────────────────────────────────────────────────

    def test_dos_edges_uno_por_rama(self):
        """Cada rama del UNION ALL genera un edge independiente."""
        assert len(self.edges) == 2

    def test_edge_desde_orders_clean_existe(self):
        edge = _get_edge(self.edges, "staging.orders_clean")
        assert edge is not None
        assert edge.target_node == "staging.all_revenue"

    def test_edge_desde_transactions_clean_existe(self):
        edge = _get_edge(self.edges, "staging.transactions_clean")
        assert edge is not None
        assert edge.target_node == "staging.all_revenue"

    def test_ambos_edges_son_automaticos(self):
        for edge in self.edges:
            assert edge.edge_type == "automatic"

    # ── Rama A: staging.orders_clean ──────────────────────────────────────────

    def test_event_id_desde_orders_clean_es_rename_de_id_pedido(self):
        m = _get_mapping(self.edges, "staging.orders_clean", "event_id")
        assert m is not None
        assert m.source_columns == ["id_pedido"]
        assert m.transformation == "rename"

    def test_event_at_desde_orders_clean_es_rename_de_created_at(self):
        m = _get_mapping(self.edges, "staging.orders_clean", "event_at")
        assert m is not None
        assert m.source_columns == ["created_at"]
        assert m.transformation == "rename"

    def test_amount_desde_orders_clean_traza_a_revenue(self):
        """amount viene de revenue en la rama de orders_clean."""
        m = _get_mapping(self.edges, "staging.orders_clean", "amount")
        assert m is not None
        assert m.source_columns == ["revenue"]

    def test_event_type_en_orders_clean_es_unknown_por_ser_literal(self):
        """'order' es un literal sin columna fuente: el parser lo marca como unknown."""
        m = _get_mapping(self.edges, "staging.orders_clean", "event_type")
        assert m is not None
        assert m.transformation == "unknown"
        assert m.source_columns == []

    # ── Rama B: staging.transactions_clean ────────────────────────────────────

    def test_event_id_desde_transactions_clean_es_rename_de_transaction_id(self):
        m = _get_mapping(self.edges, "staging.transactions_clean", "event_id")
        assert m is not None
        assert m.source_columns == ["transaction_id"]
        assert m.transformation == "rename"

    def test_event_at_desde_transactions_clean_es_rename_de_sold_at(self):
        m = _get_mapping(self.edges, "staging.transactions_clean", "event_at")
        assert m is not None
        assert m.source_columns == ["sold_at"]
        assert m.transformation == "rename"

    def test_amount_desde_transactions_clean_traza_a_quantity(self):
        """amount viene de quantity (CAST a FLOAT64) en la rama de transactions_clean."""
        m = _get_mapping(self.edges, "staging.transactions_clean", "amount")
        assert m is not None
        assert m.source_columns == ["quantity"]
        assert m.transformation == "expression"
        assert m.expression is not None
        assert "CAST" in m.expression

    # ── Verificación de columnas por nombre en cada edge ──────────────────────

    def test_edge_orders_clean_contiene_event_id(self):
        edge = _get_edge(self.edges, "staging.orders_clean")
        target_cols = [m.target_column for m in edge.column_mappings]
        assert "event_id" in target_cols

    def test_edge_orders_clean_contiene_event_at(self):
        edge = _get_edge(self.edges, "staging.orders_clean")
        target_cols = [m.target_column for m in edge.column_mappings]
        assert "event_at" in target_cols

    def test_edge_transactions_clean_no_contiene_event_type_literal(self):
        """El literal 'transaction' no genera un mapping en la segunda rama."""
        edge = _get_edge(self.edges, "staging.transactions_clean")
        target_cols = [m.target_column for m in edge.column_mappings]
        assert "event_type" not in target_cols

    def test_union_all_columnas_mismas_en_ambas_ramas(self):
        """Las columnas trazables (sin literales) deben aparecer en ambas ramas."""
        orders_cols = {
            m.target_column
            for m in _get_edge(self.edges, "staging.orders_clean").column_mappings
            if m.transformation != "unknown"
        }
        tx_cols = {
            m.target_column
            for m in _get_edge(self.edges, "staging.transactions_clean").column_mappings
        }
        # event_id, amount y event_at deben existir en ambas ramas
        assert orders_cols >= {"event_id", "amount", "event_at"}
        assert tx_cols >= {"event_id", "amount", "event_at"}