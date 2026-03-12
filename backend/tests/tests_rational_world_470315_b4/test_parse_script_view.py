"""Tests for parsing a view that contains a commented-out CREATE TABLE statement.

View tested:
  - staging.script_crear_resumen: has a leading SQL comment
    `-- create or replace table ...` followed by a SELECT from
    analytics.customer_summary.  The parser must ignore the comment
    and correctly trace lineage from customer_summary.

Manual-edge context:
  This view was used as a one-off script to populate
  analytics.resumen_creado.  The connection between the view and
  that table is a manual edge (not testable via parse_view_lineage).
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


class TestScriptCrearResumen:
    """staging.script_crear_resumen: view with leading -- comment."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "staging.script_crear_resumen",
            view_sql["staging.script_crear_resumen"],
            schemas,
        )

    # ── Graph structure ───────────────────────────────────────────────────────

    def test_single_edge_from_customer_summary(self):
        """Parser should ignore the comment and find one source: customer_summary."""
        assert len(self.edges) == 1
        assert self.edges[0].source_node == "analytics.customer_summary"
        assert self.edges[0].target_node == "staging.script_crear_resumen"

    def test_edge_is_automatic(self):
        assert self.edges[0].edge_type == "automatic"

    def test_four_columns_mapped(self):
        edge = self.edges[0]
        target_cols = {m.target_column for m in edge.column_mappings}
        assert target_cols == {
            "nombre_cliente",
            "pais",
            "total_orders",
            "total_spent",
        }

    # ── Individual column mappings ────────────────────────────────────────────

    def test_nombre_cliente_direct(self):
        m = _get_mapping(self.edges, "analytics.customer_summary", "nombre_cliente")
        assert m is not None
        assert m.source_columns == ["nombre_cliente"]
        assert m.transformation == "direct"

    def test_pais_direct(self):
        m = _get_mapping(self.edges, "analytics.customer_summary", "pais")
        assert m is not None
        assert m.source_columns == ["pais"]
        assert m.transformation == "direct"

    def test_total_orders_direct(self):
        m = _get_mapping(self.edges, "analytics.customer_summary", "total_orders")
        assert m is not None
        assert m.source_columns == ["total_orders"]
        assert m.transformation == "direct"

    def test_total_spent_direct(self):
        m = _get_mapping(self.edges, "analytics.customer_summary", "total_spent")
        assert m is not None
        assert m.source_columns == ["total_spent"]
        assert m.transformation == "direct"
