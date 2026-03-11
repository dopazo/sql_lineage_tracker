"""Unit tests for the parser module.

Tests cover edge cases, error handling, and core parsing functions
independent of the BigQuery test project fixtures.
"""

import pytest

from lineage_tracker.parser import parse_view_lineage


def _get_mapping(edges, source_node, target_column):
    for edge in edges:
        if edge.source_node == source_node:
            for m in edge.column_mappings:
                if m.target_column == target_column:
                    return m
    return None


def _get_edge(edges, source_node):
    for edge in edges:
        if edge.source_node == source_node:
            return edge
    return None


class TestSimpleSelect:
    """Basic SELECT with no transformations."""

    def test_direct_column(self):
        sql = "SELECT id, name FROM mydb.users"
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "target.view1": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.view1", sql, schemas)
        assert len(edges) == 1
        m = _get_mapping(edges, "mydb.users", "id")
        assert m.transformation == "direct"
        m = _get_mapping(edges, "mydb.users", "name")
        assert m.transformation == "direct"

    def test_alias_rename(self):
        sql = "SELECT id AS user_id FROM mydb.users"
        schemas = {
            "mydb.users": {"id": "INT64"},
            "target.v": {"user_id": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.users", "user_id")
        assert m.transformation == "rename"
        assert m.source_columns == ["id"]


class TestExpressions:
    """Expression-based transformations."""

    def test_function_expression(self):
        sql = "SELECT LOWER(name) AS name_lower FROM mydb.users"
        schemas = {
            "mydb.users": {"name": "STRING"},
            "target.v": {"name_lower": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.users", "name_lower")
        assert m.transformation == "expression"
        assert "LOWER" in m.expression

    def test_arithmetic_expression(self):
        sql = "SELECT price * quantity AS total FROM mydb.orders"
        schemas = {
            "mydb.orders": {"price": "FLOAT64", "quantity": "INT64"},
            "target.v": {"total": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.orders", "total")
        assert m.transformation == "expression"
        assert m.expression is not None


class TestAggregations:
    """Aggregate function detection."""

    def test_sum(self):
        sql = "SELECT SUM(amount) AS total FROM mydb.orders GROUP BY 1"
        schemas = {
            "mydb.orders": {"amount": "FLOAT64", "category": "STRING"},
            "target.v": {"total": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.orders", "total")
        assert m.transformation == "aggregation"
        assert "SUM" in m.expression

    def test_count_star(self):
        sql = "SELECT COUNT(*) AS cnt FROM mydb.orders"
        schemas = {
            "mydb.orders": {"id": "INT64"},
            "target.v": {"cnt": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.orders", "cnt")
        assert m.transformation == "aggregation"
        assert m.source_columns == ["*"]


class TestProjectPrefix:
    """Handling of fully-qualified BigQuery table references."""

    def test_strips_project_prefix(self):
        sql = "SELECT col FROM `my-project-123.mydb.orders`"
        schemas = {
            "mydb.orders": {"col": "STRING"},
            "target.v": {"col": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        assert edges[0].source_node == "mydb.orders"


class TestEdgeCases:
    """Error handling and edge cases."""

    def test_no_schema_returns_empty(self):
        sql = "SELECT id FROM mydb.users"
        schemas = {"mydb.users": {"id": "INT64"}}
        # No schema for the view itself
        edges = parse_view_lineage("target.v", sql, schemas)
        assert edges == []

    def test_invalid_sql_returns_empty(self):
        sql = "THIS IS NOT SQL"
        schemas = {"target.v": {"col": "STRING"}}
        edges = parse_view_lineage("target.v", sql, schemas)
        # Should not crash; may return empty or unknown mappings
        assert isinstance(edges, list)

    def test_edge_metadata(self):
        sql = "SELECT id FROM mydb.users"
        schemas = {
            "mydb.users": {"id": "INT64"},
            "target.v": {"id": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        edge = edges[0]
        assert edge.edge_type == "automatic"
        assert edge.id == "edge_mydb.users__target.v"
        assert edge.source_node == "mydb.users"
        assert edge.target_node == "target.v"


class TestSelectStar:
    """SELECT * expansion with schema."""

    def test_select_star_simple(self):
        """SELECT * expands to all columns from source table."""
        sql = "SELECT * FROM mydb.users"
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING", "email": "STRING"},
            "target.v": {"id": "INT64", "name": "STRING", "email": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        edge = edges[0]
        assert edge.source_node == "mydb.users"
        # All columns should be mapped as direct
        mapped_cols = {m.target_column for m in edge.column_mappings}
        assert mapped_cols == {"id", "name", "email"}
        for m in edge.column_mappings:
            assert m.transformation == "direct"
            assert m.source_columns == [m.target_column]

    def test_select_star_with_alias(self):
        """SELECT t.* FROM table AS t."""
        sql = "SELECT t.* FROM mydb.users AS t"
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "target.v": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        mapped_cols = {m.target_column for m in edges[0].column_mappings}
        assert mapped_cols == {"id", "name"}

    def test_select_star_with_extra_columns(self):
        """SELECT *, UPPER(name) AS name_upper FROM table."""
        sql = "SELECT *, UPPER(name) AS name_upper FROM mydb.users"
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "target.v": {"id": "INT64", "name": "STRING", "name_upper": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        m_id = _get_mapping(edges, "mydb.users", "id")
        assert m_id.transformation == "direct"
        m_upper = _get_mapping(edges, "mydb.users", "name_upper")
        assert m_upper.transformation == "expression"
        assert "UPPER" in m_upper.expression

    def test_select_star_join(self):
        """SELECT * FROM t1 JOIN t2 expands columns from both tables."""
        sql = "SELECT * FROM mydb.orders JOIN mydb.users ON orders.user_id = users.id"
        schemas = {
            "mydb.orders": {"order_id": "INT64", "user_id": "INT64"},
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "target.v": {"order_id": "INT64", "user_id": "INT64", "id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        # Should have edges from both source tables
        source_nodes = {e.source_node for e in edges}
        assert "mydb.orders" in source_nodes
        assert "mydb.users" in source_nodes

    def test_select_star_in_cte(self):
        """SELECT * inside a CTE is expanded correctly."""
        sql = """
        WITH base AS (
            SELECT * FROM mydb.users
        )
        SELECT id, name FROM base
        """
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING", "email": "STRING"},
            "target.v": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        assert edges[0].source_node == "mydb.users"
        mapped_cols = {m.target_column for m in edges[0].column_mappings}
        assert "id" in mapped_cols
        assert "name" in mapped_cols


class TestCreateTableAsSelect:
    """CREATE TABLE AS SELECT support."""

    def test_ctas(self):
        sql = "CREATE TABLE target.output AS SELECT id, name FROM mydb.users"
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "target.output": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.output", sql, schemas)
        assert len(edges) == 1
        m = _get_mapping(edges, "mydb.users", "id")
        assert m is not None
        assert m.transformation == "direct"
