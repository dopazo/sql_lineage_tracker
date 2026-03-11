"""Tests for multiple chained CTEs lineage parsing.

View tested:
  - analytics.user_funnel: two CTEs (session_stats → user_segments) consuming
    staging.sessions. Verifies that the parser traces lineage correctly through
    two levels of CTEs and classifies aggregations and CASE WHEN expressions.

Parser behaviour confirmed by diagnostic run:
  - All columns ultimately resolve to a single edge from staging.sessions.
  - GROUP BY key columns (user_id) → "direct".
  - COUNT(DISTINCT), SUM, MAX inside session_stats CTE → "aggregation".
  - CASE WHEN in user_segments that references a CTE column (pages_visited,
    which itself is COUNT(DISTINCT page)) → "expression" at the top level,
    but the traced leaf is the original raw source column (page).
"""

import pytest

from lineage_tracker.parser import parse_view_lineage


def _get_mapping(edges, source_node, target_column):
    """Return the ColumnMapping for a specific target column and source node."""
    for edge in edges:
        if edge.source_node == source_node:
            for m in edge.column_mappings:
                if m.target_column == target_column:
                    return m
    return None


SOURCE = "staging.sessions"
VIEW = "analytics.user_funnel"


class TestUserFunnelStructure:
    """High-level structural assertions about the produced edges."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(VIEW, view_sql[VIEW], schemas)

    def test_single_edge(self):
        """Both CTEs resolve to a single ultimate source: staging.sessions."""
        assert len(self.edges) == 1

    def test_source_is_staging_sessions(self):
        assert self.edges[0].source_node == SOURCE

    def test_target_is_user_funnel(self):
        assert self.edges[0].target_node == VIEW

    def test_all_output_columns_have_a_mapping(self):
        """Every column declared in the schema must appear in the edge."""
        mapped = {m.target_column for m in self.edges[0].column_mappings}
        expected = {"user_id", "pages_visited", "total_time_spent", "last_seen", "segment"}
        assert expected.issubset(mapped)


class TestUserFunnelDirectColumns:
    """Columns that pass through both CTEs without transformation."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(VIEW, view_sql[VIEW], schemas)

    def test_user_id_is_direct(self):
        """user_id is a GROUP BY key in session_stats; passes through unchanged."""
        m = _get_mapping(self.edges, SOURCE, "user_id")
        assert m is not None
        assert m.source_columns == ["user_id"]
        assert m.transformation == "direct"
        assert m.expression is None


class TestUserFunnelAggregations:
    """Columns computed by aggregate functions inside the first CTE."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(VIEW, view_sql[VIEW], schemas)

    def test_pages_visited_is_aggregation(self):
        """COUNT(DISTINCT page) in session_stats → aggregation, source = page."""
        m = _get_mapping(self.edges, SOURCE, "pages_visited")
        assert m is not None
        assert m.source_columns == ["page"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "COUNT" in m.expression

    def test_pages_visited_expression_has_distinct(self):
        m = _get_mapping(self.edges, SOURCE, "pages_visited")
        assert "DISTINCT" in m.expression

    def test_total_time_spent_is_aggregation(self):
        """SUM(duration_seconds) → aggregation, source = duration_seconds."""
        m = _get_mapping(self.edges, SOURCE, "total_time_spent")
        assert m is not None
        assert m.source_columns == ["duration_seconds"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "SUM" in m.expression

    def test_last_seen_is_aggregation(self):
        """MAX(session_start) → aggregation, source = session_start."""
        m = _get_mapping(self.edges, SOURCE, "last_seen")
        assert m is not None
        assert m.source_columns == ["session_start"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "MAX" in m.expression


class TestUserFunnelCaseWhen:
    """The segment column is a CASE WHEN in the second CTE that references
    pages_visited, which is itself COUNT(DISTINCT page) from the first CTE.
    The parser traces the leaf all the way back to the raw source column (page)
    and classifies the top-level transformation as "expression"."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(VIEW, view_sql[VIEW], schemas)

    def test_segment_is_expression(self):
        m = _get_mapping(self.edges, SOURCE, "segment")
        assert m is not None
        assert m.transformation == "expression"

    def test_segment_traces_to_page(self):
        """The CASE WHEN depends on pages_visited which comes from page."""
        m = _get_mapping(self.edges, SOURCE, "segment")
        assert m is not None
        assert "page" in m.source_columns

    def test_segment_expression_contains_case(self):
        m = _get_mapping(self.edges, SOURCE, "segment")
        assert m.expression is not None
        assert "CASE" in m.expression

    def test_segment_expression_contains_all_branches(self):
        """All three segment values must be present in the expression string."""
        m = _get_mapping(self.edges, SOURCE, "segment")
        assert "power_user" in m.expression
        assert "regular" in m.expression
        assert "casual" in m.expression