"""Tests for subquery inline in FROM and deep aggregation chain.

Views tested:
  - staging.raw_sessions: GROUP BY aggregation (MIN, MAX, COUNT) from raw table
  - staging.sessions:     subquery inline in FROM clause + TIMESTAMP_DIFF expression
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


# ──────────────────────────────────────────────────────────────────────────────
# staging.raw_sessions
# Nivel 1 de la cadena profunda: GROUP BY directo sobre tabla raw
# Patrones: MIN, MAX, COUNT(*), GROUP BY keys como direct
# ──────────────────────────────────────────────────────────────────────────────

class TestRawSessions:
    """staging.raw_sessions: aggregation from raw_data.events."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "staging.raw_sessions",
            view_sql["staging.raw_sessions"],
            schemas,
        )

    def test_single_edge_from_raw_events(self):
        assert len(self.edges) == 1
        assert self.edges[0].source_node == "raw_data.events"
        assert self.edges[0].target_node == "staging.raw_sessions"

    def test_all_output_columns_mapped(self):
        edge = _get_edge(self.edges, "raw_data.events")
        mapped_targets = {m.target_column for m in edge.column_mappings}
        assert mapped_targets == {"user_id", "page", "session_start", "session_end", "event_count"}

    # GROUP BY keys: deben aparecer como direct
    def test_user_id_group_key_is_direct(self):
        m = _get_mapping(self.edges, "raw_data.events", "user_id")
        assert m is not None
        assert m.source_columns == ["user_id"]
        assert m.transformation == "direct"
        assert m.expression is None

    def test_page_group_key_is_direct(self):
        m = _get_mapping(self.edges, "raw_data.events", "page")
        assert m is not None
        assert m.source_columns == ["page"]
        assert m.transformation == "direct"
        assert m.expression is None

    # Columnas derivadas por agregación
    def test_session_start_is_min_aggregation(self):
        m = _get_mapping(self.edges, "raw_data.events", "session_start")
        assert m is not None
        assert m.source_columns == ["event_at"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "MIN" in m.expression

    def test_session_end_is_max_aggregation(self):
        m = _get_mapping(self.edges, "raw_data.events", "session_end")
        assert m is not None
        assert m.source_columns == ["event_at"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "MAX" in m.expression

    def test_session_start_and_end_share_source_column(self):
        """Both MIN and MAX aggregate the same source column: event_at."""
        m_start = _get_mapping(self.edges, "raw_data.events", "session_start")
        m_end = _get_mapping(self.edges, "raw_data.events", "session_end")
        assert m_start.source_columns == m_end.source_columns == ["event_at"]

    def test_event_count_is_count_star_aggregation(self):
        m = _get_mapping(self.edges, "raw_data.events", "event_count")
        assert m is not None
        assert m.source_columns == ["*"]
        assert m.transformation == "aggregation"
        assert m.expression is not None
        assert "COUNT" in m.expression


# ──────────────────────────────────────────────────────────────────────────────
# staging.sessions
# Nivel 2 de la cadena profunda: subquery inline en FROM + TIMESTAMP_DIFF
# Patrones: parser resuelve la fuente a través del subquery, columna derivada
#           de dos fuentes (session_start, session_end) via expresión
# ──────────────────────────────────────────────────────────────────────────────

class TestSessions:
    """staging.sessions: columns traced through inline subquery in FROM."""

    @pytest.fixture(autouse=True)
    def parse(self, schemas, view_sql):
        self.edges = parse_view_lineage(
            "staging.sessions",
            view_sql["staging.sessions"],
            schemas,
        )

    def test_single_edge_from_raw_sessions(self):
        """El parser resuelve el subquery inline y traza hasta staging.raw_sessions."""
        assert len(self.edges) == 1
        assert self.edges[0].source_node == "staging.raw_sessions"
        assert self.edges[0].target_node == "staging.sessions"

    def test_all_output_columns_mapped(self):
        edge = _get_edge(self.edges, "staging.raw_sessions")
        mapped_targets = {m.target_column for m in edge.column_mappings}
        assert mapped_targets == {
            "user_id", "page", "session_start", "session_end",
            "event_count", "duration_seconds",
        }

    # Pass-through a través del subquery
    def test_user_id_direct_through_subquery(self):
        m = _get_mapping(self.edges, "staging.raw_sessions", "user_id")
        assert m is not None
        assert m.source_columns == ["user_id"]
        assert m.transformation == "direct"

    def test_page_direct_through_subquery(self):
        m = _get_mapping(self.edges, "staging.raw_sessions", "page")
        assert m is not None
        assert m.source_columns == ["page"]
        assert m.transformation == "direct"

    def test_session_start_direct_through_subquery(self):
        m = _get_mapping(self.edges, "staging.raw_sessions", "session_start")
        assert m is not None
        assert m.source_columns == ["session_start"]
        assert m.transformation == "direct"

    def test_session_end_direct_through_subquery(self):
        m = _get_mapping(self.edges, "staging.raw_sessions", "session_end")
        assert m is not None
        assert m.source_columns == ["session_end"]
        assert m.transformation == "direct"

    def test_event_count_direct_through_subquery(self):
        m = _get_mapping(self.edges, "staging.raw_sessions", "event_count")
        assert m is not None
        assert m.source_columns == ["event_count"]
        assert m.transformation == "direct"

    # Columna derivada: TIMESTAMP_DIFF usa dos columnas fuente
    def test_duration_seconds_is_expression(self):
        m = _get_mapping(self.edges, "staging.raw_sessions", "duration_seconds")
        assert m is not None
        assert m.transformation == "expression"
        assert m.expression is not None
        assert "TIMESTAMP_DIFF" in m.expression

    def test_duration_seconds_depends_on_two_source_columns(self):
        """TIMESTAMP_DIFF(session_end, session_start, ...) tiene dos columnas fuente."""
        m = _get_mapping(self.edges, "staging.raw_sessions", "duration_seconds")
        assert m is not None
        assert set(m.source_columns) == {"session_start", "session_end"}

    def test_duration_seconds_expression_references_both_timestamps(self):
        m = _get_mapping(self.edges, "staging.raw_sessions", "duration_seconds")
        assert "session_end" in m.expression
        assert "session_start" in m.expression