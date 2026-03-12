"""Tests for window function lineage parsing.

View tested:
  - analytics.customer_ranking: ROW_NUMBER, RANK, LAG with OVER (PARTITION BY … ORDER BY …)

Parser behaviour observed for window functions:
  - Pass-through columns (no OVER clause) → "direct"
  - ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) → "expression"
    source_columns includes the columns referenced in PARTITION BY and ORDER BY
  - RANK()  OVER (ORDER BY y)                     → "expression"
  - LAG(col) OVER (PARTITION BY x ORDER BY y)     → "expression"
    source_columns includes both the LAG argument and the PARTITION BY / ORDER BY columns
"""

import pytest

from lineage_tracker.parser import parse_view_lineage


def _get_mapping(edges, source_node, target_column):
    """Return the ColumnMapping for a specific (source_node, target_column) pair."""
    for edge in edges:
        if edge.source_node == source_node:
            for m in edge.column_mappings:
                if m.target_column == target_column:
                    return m
    return None


class TestCustomerRanking:
    """analytics.customer_ranking — window functions on top of analytics.customer_summary."""

    SOURCE = "analytics.customer_summary"

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "analytics.customer_ranking",
            view_sql["analytics.customer_ranking"],
            schemas,
        )

    # ── Graph structure ────────────────────────────────────────────────────────

    def test_single_edge(self):
        """Only one upstream source: analytics.customer_summary."""
        assert len(self.edges) == 1

    def test_source_node(self):
        assert self.edges[0].source_node == self.SOURCE

    def test_target_node(self):
        assert self.edges[0].target_node == "analytics.customer_ranking"

    def test_all_output_columns_mapped(self):
        """Every output column must have exactly one mapping."""
        expected = {
            "nombre_cliente", "pais", "total_spent", "total_orders",
            "rank_in_country", "global_rank", "prev_customer_spent",
        }
        mapped = {m.target_column for m in self.edges[0].column_mappings}
        assert mapped == expected

    # ── Pass-through columns ───────────────────────────────────────────────────

    def test_nombre_cliente_direct(self):
        m = _get_mapping(self.edges, self.SOURCE, "nombre_cliente")
        assert m is not None
        assert m.source_columns == ["nombre_cliente"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_pais_direct(self):
        m = _get_mapping(self.edges, self.SOURCE, "pais")
        assert m is not None
        assert m.source_columns == ["pais"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_total_spent_direct(self):
        m = _get_mapping(self.edges, self.SOURCE, "total_spent")
        assert m is not None
        assert m.source_columns == ["total_spent"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_total_orders_direct(self):
        m = _get_mapping(self.edges, self.SOURCE, "total_orders")
        assert m is not None
        assert m.source_columns == ["total_orders"]
        assert m.transformation == "direct"
        assert m.expression is None

    # ── ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) → expression ────────────

    def test_rank_in_country_is_expression(self):
        """ROW_NUMBER() with PARTITION BY is classified as 'expression'."""
        m = _get_mapping(self.edges, self.SOURCE, "rank_in_country")
        assert m is not None
        assert m.transformation == "expression"

    def test_rank_in_country_expression_contains_row_number(self):
        m = _get_mapping(self.edges, self.SOURCE, "rank_in_country")
        assert m.expression is not None
        assert "ROW_NUMBER" in m.expression

    def test_rank_in_country_expression_contains_partition_by(self):
        m = _get_mapping(self.edges, self.SOURCE, "rank_in_country")
        assert m.expression is not None
        assert "PARTITION BY" in m.expression

    def test_rank_in_country_sources_include_partition_and_order_cols(self):
        """Source columns for a windowed rank include both PARTITION BY and ORDER BY columns."""
        m = _get_mapping(self.edges, self.SOURCE, "rank_in_country")
        assert m is not None
        assert "pais" in m.source_columns
        assert "total_spent" in m.source_columns

    # ── RANK() OVER (ORDER BY …) → expression ─────────────────────────────────

    def test_global_rank_is_expression(self):
        """RANK() OVER is a window function, classified as 'expression'."""
        m = _get_mapping(self.edges, self.SOURCE, "global_rank")
        assert m is not None
        assert m.transformation == "expression"

    def test_global_rank_expression_contains_rank(self):
        m = _get_mapping(self.edges, self.SOURCE, "global_rank")
        assert m.expression is not None
        assert "RANK" in m.expression

    def test_global_rank_expression_contains_order_by(self):
        m = _get_mapping(self.edges, self.SOURCE, "global_rank")
        assert m.expression is not None
        assert "ORDER BY" in m.expression

    def test_global_rank_source_is_order_col(self):
        """RANK() ORDER BY total_spent — source column should be total_spent."""
        m = _get_mapping(self.edges, self.SOURCE, "global_rank")
        assert m is not None
        assert "total_spent" in m.source_columns

    # ── LAG(col) OVER (PARTITION BY … ORDER BY …) → expression ────────────────

    def test_prev_customer_spent_is_expression(self):
        """LAG() OVER is a window function, classified as 'expression'."""
        m = _get_mapping(self.edges, self.SOURCE, "prev_customer_spent")
        assert m is not None
        assert m.transformation == "expression"

    def test_prev_customer_spent_expression_contains_lag(self):
        m = _get_mapping(self.edges, self.SOURCE, "prev_customer_spent")
        assert m.expression is not None
        assert "LAG" in m.expression

    def test_prev_customer_spent_source_includes_lag_argument(self):
        """LAG(total_spent) — total_spent must be among the source columns."""
        m = _get_mapping(self.edges, self.SOURCE, "prev_customer_spent")
        assert m is not None
        assert "total_spent" in m.source_columns

    def test_prev_customer_spent_source_includes_partition_col(self):
        """PARTITION BY pais — pais must also be among the source columns."""
        m = _get_mapping(self.edges, self.SOURCE, "prev_customer_spent")
        assert m is not None
        assert "pais" in m.source_columns