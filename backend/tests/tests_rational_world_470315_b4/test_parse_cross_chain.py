"""Tests for cross-chain convergence lineage parsing.

View tested:
  - reporting.executive_dashboard: LEFT JOIN between analytics.monthly_revenue
    (Cadena A) and analytics.product_sales (Cadena B), with a derived column
    that combines sources from both chains via ROUND / NULLIF.
"""

import pytest

from lineage_tracker.parser import parse_view_lineage


def _get_mapping(edges, source_node, target_column):
    """Helper: find a specific column mapping by source node and target column."""
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


class TestExecutiveDashboard:
    """reporting.executive_dashboard: convergence of two independent chains."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "reporting.executive_dashboard",
            view_sql["reporting.executive_dashboard"],
            schemas,
        )

    # ── Graph structure ───────────────────────────────────────────────────────

    def test_two_source_edges(self):
        """View joins two independent chains — must produce exactly 2 edges."""
        assert len(self.edges) == 2

    def test_source_nodes_are_both_chains(self):
        source_nodes = {e.source_node for e in self.edges}
        assert source_nodes == {
            "analytics.monthly_revenue",
            "analytics.product_sales",
        }

    def test_both_edges_target_executive_dashboard(self):
        for edge in self.edges:
            assert edge.target_node == "reporting.executive_dashboard"

    def test_both_edges_are_automatic(self):
        for edge in self.edges:
            assert edge.edge_type == "automatic"

    # ── Columns from Cadena A (analytics.monthly_revenue) ────────────────────

    def test_month_direct_from_monthly_revenue(self):
        m = _get_mapping(self.edges, "analytics.monthly_revenue", "month")
        assert m is not None
        assert m.source_columns == ["month"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_pais_direct_from_monthly_revenue(self):
        m = _get_mapping(self.edges, "analytics.monthly_revenue", "pais")
        assert m is not None
        assert m.source_columns == ["pais"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_total_revenue_direct_from_monthly_revenue(self):
        m = _get_mapping(self.edges, "analytics.monthly_revenue", "total_revenue")
        assert m is not None
        assert m.source_columns == ["total_revenue"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_order_count_direct_from_monthly_revenue(self):
        m = _get_mapping(self.edges, "analytics.monthly_revenue", "order_count")
        assert m is not None
        assert m.source_columns == ["order_count"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_edge_from_monthly_revenue_column_count(self):
        """Edge from monthly_revenue carries 4 direct columns + revenue_per_unit."""
        edge = _get_edge(self.edges, "analytics.monthly_revenue")
        assert edge is not None
        target_cols = {m.target_column for m in edge.column_mappings}
        assert target_cols == {
            "month",
            "pais",
            "total_revenue",
            "order_count",
            "revenue_per_unit",
        }

    # ── Columns from Cadena B (analytics.product_sales) ──────────────────────

    def test_total_units_sold_direct_from_product_sales(self):
        m = _get_mapping(self.edges, "analytics.product_sales", "total_units_sold")
        assert m is not None
        assert m.source_columns == ["total_units_sold"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_total_transactions_direct_from_product_sales(self):
        m = _get_mapping(self.edges, "analytics.product_sales", "total_transactions")
        assert m is not None
        assert m.source_columns == ["total_transactions"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_edge_from_product_sales_column_count(self):
        """Edge from product_sales carries 2 direct columns + revenue_per_unit."""
        edge = _get_edge(self.edges, "analytics.product_sales")
        assert edge is not None
        target_cols = {m.target_column for m in edge.column_mappings}
        assert target_cols == {
            "total_units_sold",
            "total_transactions",
            "revenue_per_unit",
        }

    # ── Cross-chain derived column: revenue_per_unit ──────────────────────────

    def test_revenue_per_unit_is_expression_from_monthly_revenue(self):
        """revenue_per_unit traces total_revenue contribution from Cadena A."""
        m = _get_mapping(self.edges, "analytics.monthly_revenue", "revenue_per_unit")
        assert m is not None
        assert m.transformation == "expression"
        assert m.source_columns == ["total_revenue"]
        assert m.expression is not None

    def test_revenue_per_unit_is_expression_from_product_sales(self):
        """revenue_per_unit traces total_units_sold contribution from Cadena B."""
        m = _get_mapping(self.edges, "analytics.product_sales", "revenue_per_unit")
        assert m is not None
        assert m.transformation == "expression"
        assert m.source_columns == ["total_units_sold"]
        assert m.expression is not None

    def test_revenue_per_unit_expression_contains_round(self):
        """Expression must include ROUND (outer function)."""
        m = _get_mapping(self.edges, "analytics.monthly_revenue", "revenue_per_unit")
        assert m is not None
        assert "ROUND" in m.expression

    def test_revenue_per_unit_expression_contains_nullif(self):
        """Expression must include NULLIF to guard against division by zero."""
        m = _get_mapping(self.edges, "analytics.monthly_revenue", "revenue_per_unit")
        assert m is not None
        assert "NULLIF" in m.expression

    def test_revenue_per_unit_appears_in_both_edges(self):
        """Derived column that uses sources from two chains must appear in both edges."""
        m_a = _get_mapping(self.edges, "analytics.monthly_revenue", "revenue_per_unit")
        m_b = _get_mapping(self.edges, "analytics.product_sales", "revenue_per_unit")
        assert m_a is not None, "revenue_per_unit missing from monthly_revenue edge"
        assert m_b is not None, "revenue_per_unit missing from product_sales edge"