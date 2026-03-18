"""Unit tests for the parser module.

Tests cover edge cases, error handling, and core parsing functions
independent of the BigQuery test project fixtures.
"""

import pytest

from lineage_tracker.parser import (
    _expand_array_agg_struct_star,
    contains_dynamic_sql,
    parse_view_lineage,
)


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


class TestUnionAll:
    """UNION ALL / UNION DISTINCT — columns mapped by position."""

    def test_basic_union_all(self):
        """Each branch produces edges to the same target, mapped by position."""
        sql = "SELECT a, b FROM ds1.t1 UNION ALL SELECT c, d FROM ds2.t2"
        schemas = {
            "ds1.t1": {"a": "INT64", "b": "STRING"},
            "ds2.t2": {"c": "INT64", "d": "STRING"},
            "target.v": {"a": "INT64", "b": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 2

        # First branch: direct mapping
        m = _get_mapping(edges, "ds1.t1", "a")
        assert m.transformation == "direct"
        assert m.source_columns == ["a"]
        m = _get_mapping(edges, "ds1.t1", "b")
        assert m.transformation == "direct"

        # Second branch: positional rename
        m = _get_mapping(edges, "ds2.t2", "a")
        assert m.transformation == "rename"
        assert m.source_columns == ["c"]
        m = _get_mapping(edges, "ds2.t2", "b")
        assert m.transformation == "rename"
        assert m.source_columns == ["d"]

    def test_three_way_union_all(self):
        """Three-branch UNION ALL produces edges from all three sources."""
        sql = (
            "SELECT a FROM ds1.t1 UNION ALL SELECT b FROM ds2.t2 "
            "UNION ALL SELECT c FROM ds3.t3"
        )
        schemas = {
            "ds1.t1": {"a": "INT64"},
            "ds2.t2": {"b": "INT64"},
            "ds3.t3": {"c": "INT64"},
            "target.v": {"a": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        source_nodes = {e.source_node for e in edges}
        assert source_nodes == {"ds1.t1", "ds2.t2", "ds3.t3"}

    def test_union_all_with_expressions(self):
        """Expression attribution is correct per branch."""
        sql = (
            "SELECT UPPER(name) AS label FROM ds1.t1 "
            "UNION ALL SELECT LOWER(title) AS label FROM ds2.t2"
        )
        schemas = {
            "ds1.t1": {"name": "STRING"},
            "ds2.t2": {"title": "STRING"},
            "target.v": {"label": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m1 = _get_mapping(edges, "ds1.t1", "label")
        assert m1.transformation == "expression"
        assert "UPPER" in m1.expression

        m2 = _get_mapping(edges, "ds2.t2", "label")
        assert m2.transformation == "expression"
        assert "LOWER" in m2.expression

    def test_union_all_with_aggregations(self):
        """Aggregation expressions are correctly attributed per branch."""
        sql = (
            "SELECT category, SUM(amount) AS total FROM ds1.sales GROUP BY category "
            "UNION ALL "
            "SELECT category, SUM(revenue) AS total FROM ds2.returns GROUP BY category"
        )
        schemas = {
            "ds1.sales": {"category": "STRING", "amount": "FLOAT64"},
            "ds2.returns": {"category": "STRING", "revenue": "FLOAT64"},
            "target.v": {"category": "STRING", "total": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m1 = _get_mapping(edges, "ds1.sales", "total")
        assert m1.transformation == "aggregation"
        assert "amount" in m1.expression

        m2 = _get_mapping(edges, "ds2.returns", "total")
        assert m2.transformation == "aggregation"
        assert "revenue" in m2.expression

    def test_union_all_select_star(self):
        """SELECT * in UNION ALL branches expands using schemas."""
        sql = "SELECT * FROM ds1.t1 UNION ALL SELECT * FROM ds2.t2"
        schemas = {
            "ds1.t1": {"id": "INT64", "name": "STRING"},
            "ds2.t2": {"id": "INT64", "name": "STRING"},
            "target.v": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 2
        for e in edges:
            mapped_cols = {m.target_column for m in e.column_mappings}
            assert mapped_cols == {"id", "name"}

    def test_union_all_select_star_different_names(self):
        """SELECT * with different column names maps by position."""
        sql = "SELECT * FROM ds1.orders UNION ALL SELECT * FROM ds2.purchases"
        schemas = {
            "ds1.orders": {"order_id": "STRING", "total": "FLOAT64"},
            "ds2.purchases": {"purchase_id": "STRING", "amount": "FLOAT64"},
            "target.v": {"order_id": "STRING", "total": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "ds2.purchases", "order_id")
        assert m.transformation == "rename"
        assert m.source_columns == ["purchase_id"]
        m = _get_mapping(edges, "ds2.purchases", "total")
        assert m.transformation == "rename"
        assert m.source_columns == ["amount"]

    def test_union_distinct(self):
        """UNION DISTINCT (valid BigQuery syntax) works like UNION ALL for lineage."""
        sql = "SELECT a FROM ds1.t1 UNION DISTINCT SELECT b FROM ds2.t2"
        schemas = {
            "ds1.t1": {"a": "INT64"},
            "ds2.t2": {"b": "INT64"},
            "target.v": {"a": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 2
        m = _get_mapping(edges, "ds2.t2", "a")
        assert m.transformation == "rename"
        assert m.source_columns == ["b"]

    def test_union_all_in_cte(self):
        """UNION ALL inside a CTE — outer SELECT traces through both branches."""
        sql = """
        WITH combined AS (
            SELECT id, name FROM ds1.t1
            UNION ALL
            SELECT user_id AS id, username AS name FROM ds2.t2
        )
        SELECT id, UPPER(name) AS name_upper FROM combined
        """
        schemas = {
            "ds1.t1": {"id": "INT64", "name": "STRING"},
            "ds2.t2": {"user_id": "INT64", "username": "STRING"},
            "target.v": {"id": "INT64", "name_upper": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 2
        # Both sources traced through CTE
        source_nodes = {e.source_node for e in edges}
        assert source_nodes == {"ds1.t1", "ds2.t2"}

    def test_union_all_referencing_ctes(self):
        """UNION ALL at top level where each branch references a CTE."""
        sql = """
        WITH cte1 AS (
            SELECT id, name FROM ds1.t1
        ), cte2 AS (
            SELECT user_id AS id, username AS name FROM ds2.t2
        )
        SELECT id, name FROM cte1
        UNION ALL
        SELECT id, name FROM cte2
        """
        schemas = {
            "ds1.t1": {"id": "INT64", "name": "STRING"},
            "ds2.t2": {"user_id": "INT64", "username": "STRING"},
            "target.v": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        source_nodes = {e.source_node for e in edges}
        assert "ds1.t1" in source_nodes
        assert "ds2.t2" in source_nodes

    def test_union_all_same_source(self):
        """Both UNION branches reference the same table with different filters."""
        sql = (
            "SELECT id, name FROM ds1.t1 WHERE active = TRUE "
            "UNION ALL "
            "SELECT id, name FROM ds1.t1 WHERE archived = TRUE"
        )
        schemas = {
            "ds1.t1": {"id": "INT64", "name": "STRING", "active": "BOOL", "archived": "BOOL"},
            "target.v": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        # Single edge to the same source, last branch wins for mappings
        assert len(edges) == 1
        assert edges[0].source_node == "ds1.t1"
        mapped_cols = {m.target_column for m in edges[0].column_mappings}
        assert mapped_cols == {"id", "name"}


class TestSubqueries:
    """Subquery support — derived tables, scalar subqueries, WHERE IN, nested."""

    def test_subquery_in_from(self):
        """Derived table in FROM: traces through to base table."""
        sql = "SELECT id, name_upper FROM (SELECT id, UPPER(name) AS name_upper FROM mydb.users) sub"
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "target.v": {"id": "INT64", "name_upper": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        assert edges[0].source_node == "mydb.users"
        m = _get_mapping(edges, "mydb.users", "id")
        assert m.transformation == "direct"
        m = _get_mapping(edges, "mydb.users", "name_upper")
        assert m.transformation == "expression"
        assert "UPPER" in m.expression

    def test_scalar_subquery_in_select(self):
        """Correlated scalar subquery in SELECT list."""
        sql = (
            "SELECT id, "
            "(SELECT MAX(amount) FROM mydb.orders WHERE orders.user_id = users.id) AS max_order "
            "FROM mydb.users"
        )
        schemas = {
            "mydb.users": {"id": "INT64"},
            "mydb.orders": {"amount": "FLOAT64", "user_id": "INT64"},
            "target.v": {"id": "INT64", "max_order": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m_id = _get_mapping(edges, "mydb.users", "id")
        assert m_id.transformation == "direct"
        m_max = _get_mapping(edges, "mydb.orders", "max_order")
        assert m_max is not None
        assert m_max.source_columns == ["amount"]

    def test_multiple_scalar_subqueries(self):
        """Multiple scalar subqueries in SELECT."""
        sql = (
            "SELECT id, "
            "(SELECT MAX(amount) FROM mydb.orders WHERE orders.user_id = users.id) AS max_order, "
            "(SELECT MIN(amount) FROM mydb.orders WHERE orders.user_id = users.id) AS min_order "
            "FROM mydb.users"
        )
        schemas = {
            "mydb.users": {"id": "INT64"},
            "mydb.orders": {"amount": "FLOAT64", "user_id": "INT64"},
            "target.v": {"id": "INT64", "max_order": "FLOAT64", "min_order": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m_max = _get_mapping(edges, "mydb.orders", "max_order")
        assert m_max is not None
        m_min = _get_mapping(edges, "mydb.orders", "min_order")
        assert m_min is not None

    def test_where_in_subquery(self):
        """WHERE IN subquery: only main table columns appear in lineage."""
        sql = "SELECT id, name FROM mydb.users WHERE id IN (SELECT user_id FROM mydb.active_users)"
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "mydb.active_users": {"user_id": "INT64"},
            "target.v": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        source_nodes = {e.source_node for e in edges}
        assert "mydb.users" in source_nodes
        # active_users is only used for filtering, not column lineage
        assert "mydb.active_users" not in source_nodes
        m = _get_mapping(edges, "mydb.users", "id")
        assert m.transformation == "direct"

    def test_where_not_in_subquery(self):
        """WHERE NOT IN subquery: filter table excluded from lineage."""
        sql = "SELECT id, name FROM mydb.users WHERE id NOT IN (SELECT user_id FROM mydb.blocked)"
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "mydb.blocked": {"user_id": "INT64"},
            "target.v": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        source_nodes = {e.source_node for e in edges}
        assert source_nodes == {"mydb.users"}

    def test_exists_subquery(self):
        """EXISTS subquery: only main table contributes to lineage."""
        sql = (
            "SELECT id, name FROM mydb.users "
            "WHERE EXISTS (SELECT 1 FROM mydb.orders WHERE orders.user_id = users.id)"
        )
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "mydb.orders": {"user_id": "INT64"},
            "target.v": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        source_nodes = {e.source_node for e in edges}
        assert source_nodes == {"mydb.users"}

    def test_nested_subquery(self):
        """Two levels of nested subqueries: traces through to base table."""
        sql = """SELECT a FROM (
            SELECT x AS a FROM (
                SELECT col1 AS x FROM mydb.raw
            ) inner_sq
        ) outer_sq"""
        schemas = {
            "mydb.raw": {"col1": "INT64"},
            "target.v": {"a": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        assert edges[0].source_node == "mydb.raw"
        m = _get_mapping(edges, "mydb.raw", "a")
        assert m.transformation == "rename"
        assert m.source_columns == ["col1"]

    def test_subquery_with_aggregation(self):
        """Subquery containing GROUP BY: aggregation is detected."""
        sql = """SELECT user_id, total_orders FROM (
            SELECT user_id, COUNT(*) AS total_orders
            FROM mydb.orders GROUP BY user_id
        ) summary"""
        schemas = {
            "mydb.orders": {"user_id": "INT64", "order_id": "INT64"},
            "target.v": {"user_id": "INT64", "total_orders": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        m = _get_mapping(edges, "mydb.orders", "total_orders")
        assert m.transformation == "aggregation"

    def test_subquery_in_join(self):
        """Subquery as JOIN operand: both sources contribute edges."""
        sql = """SELECT u.id, u.name, o.total_amount FROM mydb.users u
            JOIN (SELECT user_id, SUM(amount) AS total_amount FROM mydb.orders GROUP BY user_id) o
            ON u.id = o.user_id"""
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "mydb.orders": {"user_id": "INT64", "amount": "FLOAT64"},
            "target.v": {"id": "INT64", "name": "STRING", "total_amount": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        source_nodes = {e.source_node for e in edges}
        assert source_nodes == {"mydb.users", "mydb.orders"}
        m = _get_mapping(edges, "mydb.orders", "total_amount")
        assert m.transformation == "aggregation"
        assert "SUM" in m.expression

    def test_subquery_with_case_when(self):
        """CASE WHEN inside subquery is traced as expression."""
        sql = """SELECT id, category FROM (
            SELECT id, CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS category
            FROM mydb.orders
        ) categorized"""
        schemas = {
            "mydb.orders": {"id": "INT64", "amount": "FLOAT64"},
            "target.v": {"id": "INT64", "category": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.orders", "category")
        assert m.transformation == "expression"
        assert "CASE" in m.expression

    def test_union_inside_subquery(self):
        """UNION ALL inside a derived table: both branches traced."""
        sql = """SELECT id, name FROM (
            SELECT id, name FROM mydb.t1
            UNION ALL
            SELECT user_id AS id, username AS name FROM mydb.t2
        ) combined"""
        schemas = {
            "mydb.t1": {"id": "INT64", "name": "STRING"},
            "mydb.t2": {"user_id": "INT64", "username": "STRING"},
            "target.v": {"id": "INT64", "name": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        source_nodes = {e.source_node for e in edges}
        assert source_nodes == {"mydb.t1", "mydb.t2"}

    def test_subquery_referencing_cte(self):
        """Subquery in FROM that references a CTE."""
        sql = """WITH base AS (
            SELECT id, name FROM mydb.users
        )
        SELECT id, name_upper FROM (
            SELECT id, UPPER(name) AS name_upper FROM base
        ) sub"""
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "target.v": {"id": "INT64", "name_upper": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        assert edges[0].source_node == "mydb.users"

    def test_cte_with_subquery_inside(self):
        """CTE defined using a subquery."""
        sql = """WITH summary AS (
            SELECT user_id, total FROM (
                SELECT user_id, SUM(amount) AS total FROM mydb.orders GROUP BY user_id
            ) agg
        )
        SELECT user_id, total FROM summary"""
        schemas = {
            "mydb.orders": {"user_id": "INT64", "amount": "FLOAT64"},
            "target.v": {"user_id": "INT64", "total": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        assert edges[0].source_node == "mydb.orders"
        m = _get_mapping(edges, "mydb.orders", "total")
        assert m.transformation == "aggregation"

    def test_subquery_without_alias(self):
        """Subquery in FROM without explicit alias (BigQuery allows this)."""
        sql = "SELECT x FROM (SELECT id AS x FROM mydb.users)"
        schemas = {
            "mydb.users": {"id": "INT64"},
            "target.v": {"x": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        m = _get_mapping(edges, "mydb.users", "x")
        assert m.transformation == "rename"
        assert m.source_columns == ["id"]


class TestWindowFunctions:
    """Window functions (OVER clause) — classified as expression, not aggregation."""

    def test_row_number(self):
        """ROW_NUMBER() OVER (...) is an expression, not aggregation."""
        sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM mydb.users"
        schemas = {
            "mydb.users": {"id": "INT64"},
            "target.v": {"id": "INT64", "rn": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        m = _get_mapping(edges, "mydb.users", "rn")
        assert m is not None
        assert m.transformation == "expression"
        assert "ROW_NUMBER" in m.expression
        assert "OVER" in m.expression

    def test_sum_over_partition(self):
        """SUM(x) OVER (...) should be expression, NOT aggregation."""
        sql = (
            "SELECT user_id, amount, "
            "SUM(amount) OVER (PARTITION BY user_id) AS running_total "
            "FROM mydb.orders"
        )
        schemas = {
            "mydb.orders": {"user_id": "INT64", "amount": "FLOAT64"},
            "target.v": {"user_id": "INT64", "amount": "FLOAT64", "running_total": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.orders", "running_total")
        assert m is not None
        assert m.transformation == "expression"
        assert "SUM" in m.expression
        assert "OVER" in m.expression

    def test_rank_and_dense_rank(self):
        """RANK() and DENSE_RANK() window functions."""
        sql = (
            "SELECT id, "
            "RANK() OVER (ORDER BY score DESC) AS rnk, "
            "DENSE_RANK() OVER (ORDER BY score DESC) AS drnk "
            "FROM mydb.scores"
        )
        schemas = {
            "mydb.scores": {"id": "INT64", "score": "FLOAT64"},
            "target.v": {"id": "INT64", "rnk": "INT64", "drnk": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m_rnk = _get_mapping(edges, "mydb.scores", "rnk")
        assert m_rnk is not None
        assert m_rnk.transformation == "expression"
        assert "RANK" in m_rnk.expression

        m_drnk = _get_mapping(edges, "mydb.scores", "drnk")
        assert m_drnk is not None
        assert m_drnk.transformation == "expression"
        assert "DENSE_RANK" in m_drnk.expression

    def test_lag_lead(self):
        """LAG/LEAD window functions track source column."""
        sql = (
            "SELECT date, price, "
            "LAG(price, 1) OVER (ORDER BY date) AS prev_price "
            "FROM mydb.stocks"
        )
        schemas = {
            "mydb.stocks": {"date": "DATE", "price": "FLOAT64"},
            "target.v": {"date": "DATE", "price": "FLOAT64", "prev_price": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.stocks", "prev_price")
        assert m is not None
        assert m.transformation == "expression"
        assert "LAG" in m.expression

    def test_count_over(self):
        """COUNT(*) OVER (...) is expression, not aggregation."""
        sql = (
            "SELECT id, "
            "COUNT(*) OVER (PARTITION BY category) AS category_count "
            "FROM mydb.products"
        )
        schemas = {
            "mydb.products": {"id": "INT64", "category": "STRING"},
            "target.v": {"id": "INT64", "category_count": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.products", "category_count")
        assert m is not None
        assert m.transformation == "expression"
        assert "OVER" in m.expression

    def test_window_alongside_regular_aggregation(self):
        """Query with GROUP BY aggregation AND window function over the result."""
        sql = (
            "SELECT category, SUM(amount) AS total, "
            "RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk "
            "FROM mydb.sales GROUP BY category"
        )
        schemas = {
            "mydb.sales": {"category": "STRING", "amount": "FLOAT64"},
            "target.v": {"category": "STRING", "total": "FLOAT64", "rnk": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m_total = _get_mapping(edges, "mydb.sales", "total")
        assert m_total is not None
        assert m_total.transformation == "aggregation"

        m_rnk = _get_mapping(edges, "mydb.sales", "rnk")
        assert m_rnk is not None
        assert m_rnk.transformation == "expression"
        assert "RANK" in m_rnk.expression

    def test_window_function_in_cte(self):
        """Window function inside a CTE, passed through to outer query."""
        sql = """
        WITH ranked AS (
            SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) AS rn
            FROM mydb.users
        )
        SELECT id, name, rn FROM ranked WHERE rn = 1
        """
        schemas = {
            "mydb.users": {"id": "INT64", "name": "STRING"},
            "target.v": {"id": "INT64", "name": "STRING", "rn": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        assert edges[0].source_node == "mydb.users"
        m = _get_mapping(edges, "mydb.users", "rn")
        assert m is not None
        assert m.transformation == "expression"
        assert "ROW_NUMBER" in m.expression

    def test_window_function_in_subquery(self):
        """Window function inside a subquery."""
        sql = """SELECT id, rn FROM (
            SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS rn
            FROM mydb.users
        ) ranked WHERE rn <= 10"""
        schemas = {
            "mydb.users": {"id": "INT64"},
            "target.v": {"id": "INT64", "rn": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        m = _get_mapping(edges, "mydb.users", "rn")
        assert m is not None
        assert m.transformation == "expression"

    def test_window_with_expression_arg(self):
        """Window function with an expression as argument: SUM(price * qty) OVER (...)."""
        sql = (
            "SELECT order_id, "
            "SUM(price * quantity) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total "
            "FROM mydb.order_items"
        )
        schemas = {
            "mydb.order_items": {
                "order_id": "INT64", "price": "FLOAT64",
                "quantity": "INT64", "customer_id": "INT64", "order_date": "DATE",
            },
            "target.v": {"order_id": "INT64", "running_total": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.order_items", "running_total")
        assert m is not None
        assert m.transformation == "expression"
        assert "OVER" in m.expression

    def test_multiple_window_functions(self):
        """Multiple window functions in the same SELECT."""
        sql = (
            "SELECT id, "
            "ROW_NUMBER() OVER (ORDER BY id) AS rn, "
            "LAG(score) OVER (ORDER BY id) AS prev_score, "
            "AVG(score) OVER (PARTITION BY category) AS avg_score "
            "FROM mydb.items"
        )
        schemas = {
            "mydb.items": {"id": "INT64", "score": "FLOAT64", "category": "STRING"},
            "target.v": {"id": "INT64", "rn": "INT64", "prev_score": "FLOAT64", "avg_score": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        for col in ("rn", "prev_score", "avg_score"):
            m = _get_mapping(edges, "mydb.items", col)
            assert m is not None, f"Missing mapping for {col}"
            assert m.transformation == "expression", f"{col} should be expression, got {m.transformation}"
            assert "OVER" in m.expression, f"{col} expression should contain OVER"


class TestUnnest:
    """UNNEST — flattening arrays in BigQuery."""

    def test_simple_unnest(self):
        """Basic UNNEST: flatten array column into rows."""
        sql = "SELECT id, element FROM mydb.users, UNNEST(tags) AS element"
        schemas = {
            "mydb.users": {"id": "INT64", "tags": "ARRAY<STRING>"},
            "target.v": {"id": "INT64", "element": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        m = _get_mapping(edges, "mydb.users", "id")
        assert m.transformation == "direct"
        m = _get_mapping(edges, "mydb.users", "element")
        assert m.source_columns == ["tags"]
        assert m.transformation == "expression"
        assert "UNNEST" in m.expression

    def test_cross_join_unnest(self):
        """Explicit CROSS JOIN UNNEST syntax."""
        sql = "SELECT id, tag FROM mydb.users CROSS JOIN UNNEST(tags) AS tag"
        schemas = {
            "mydb.users": {"id": "INT64", "tags": "ARRAY<STRING>"},
            "target.v": {"id": "INT64", "tag": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.users", "tag")
        assert m.source_columns == ["tags"]
        assert m.transformation == "expression"
        assert "UNNEST" in m.expression

    def test_unnest_struct_fields(self):
        """UNNEST of struct array, accessing struct fields."""
        sql = "SELECT id, e.name, e.value FROM mydb.events, UNNEST(properties) AS e"
        schemas = {
            "mydb.events": {
                "id": "INT64",
                "properties": "ARRAY<STRUCT<name STRING, value STRING>>",
            },
            "target.v": {"id": "INT64", "name": "STRING", "value": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        m_name = _get_mapping(edges, "mydb.events", "name")
        assert m_name.source_columns == ["properties"]
        assert m_name.transformation == "expression"
        # Expression should not contain internal _0. prefix
        assert "_0." not in m_name.expression

    def test_unnest_with_aggregation(self):
        """UNNEST followed by aggregation."""
        sql = "SELECT id, COUNT(tag) AS tag_count FROM mydb.users, UNNEST(tags) AS tag GROUP BY id"
        schemas = {
            "mydb.users": {"id": "INT64", "tags": "ARRAY<STRING>"},
            "target.v": {"id": "INT64", "tag_count": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.users", "tag_count")
        assert m.transformation == "aggregation"
        # Expression should not contain internal _0. prefix
        assert "_0." not in m.expression
        assert "COUNT" in m.expression

    def test_multiple_unnest(self):
        """Multiple UNNEST in the same query."""
        sql = "SELECT id, tag, score FROM mydb.users, UNNEST(tags) AS tag, UNNEST(scores) AS score"
        schemas = {
            "mydb.users": {
                "id": "INT64",
                "tags": "ARRAY<STRING>",
                "scores": "ARRAY<FLOAT64>",
            },
            "target.v": {"id": "INT64", "tag": "STRING", "score": "FLOAT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m_tag = _get_mapping(edges, "mydb.users", "tag")
        assert m_tag.source_columns == ["tags"]
        m_score = _get_mapping(edges, "mydb.users", "score")
        assert m_score.source_columns == ["scores"]

    def test_left_join_unnest(self):
        """LEFT JOIN UNNEST preserves rows without array elements."""
        sql = "SELECT u.id, tag FROM mydb.users u LEFT JOIN UNNEST(u.tags) AS tag ON TRUE"
        schemas = {
            "mydb.users": {"id": "INT64", "tags": "ARRAY<STRING>"},
            "target.v": {"id": "INT64", "tag": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.users", "tag")
        assert m.source_columns == ["tags"]
        assert m.transformation == "expression"

    def test_unnest_in_cte(self):
        """UNNEST inside a CTE, consumed by outer query."""
        sql = """
        WITH expanded AS (
            SELECT id, tag FROM mydb.users, UNNEST(tags) AS tag
        )
        SELECT id, UPPER(tag) AS tag_upper FROM expanded
        """
        schemas = {
            "mydb.users": {"id": "INT64", "tags": "ARRAY<STRING>"},
            "target.v": {"id": "INT64", "tag_upper": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        assert edges[0].source_node == "mydb.users"
        m = _get_mapping(edges, "mydb.users", "tag_upper")
        assert m.transformation == "expression"
        assert "UPPER" in m.expression

    def test_unnest_with_offset(self):
        """UNNEST WITH OFFSET produces an index column."""
        sql = "SELECT id, tag, pos FROM mydb.users, UNNEST(tags) AS tag WITH OFFSET AS pos"
        schemas = {
            "mydb.users": {"id": "INT64", "tags": "ARRAY<STRING>"},
            "target.v": {"id": "INT64", "tag": "STRING", "pos": "INT64"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m_tag = _get_mapping(edges, "mydb.users", "tag")
        assert m_tag.source_columns == ["tags"]
        # pos should also be traced (it comes from the UNNEST operation)
        m_pos = _get_mapping(edges, "mydb.users", "pos")
        assert m_pos is not None


class TestStruct:
    """STRUCT — accessing and creating structured types."""

    def test_struct_field_access(self):
        """Accessing fields of a STRUCT column."""
        sql = "SELECT address.city, address.state FROM mydb.users"
        schemas = {
            "mydb.users": {"address": "STRUCT<city STRING, state STRING>"},
            "target.v": {"city": "STRING", "state": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        assert len(edges) == 1
        m_city = _get_mapping(edges, "mydb.users", "city")
        assert m_city.source_columns == ["address"]
        assert m_city.transformation == "expression"
        assert "address" in m_city.expression.lower()

    def test_struct_creation(self):
        """Creating a STRUCT in SELECT."""
        sql = "SELECT STRUCT(a, b) AS s FROM mydb.t1"
        schemas = {
            "mydb.t1": {"a": "INT64", "b": "STRING"},
            "target.v": {"s": "STRUCT<a INT64, b STRING>"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.t1", "s")
        assert m.transformation == "expression"
        assert "STRUCT" in m.expression
        assert "a" in m.source_columns
        assert "b" in m.source_columns

    def test_struct_field_in_expression(self):
        """STRUCT field used in an expression."""
        sql = "SELECT UPPER(address.city) AS city_upper FROM mydb.users"
        schemas = {
            "mydb.users": {"address": "STRUCT<city STRING, state STRING>"},
            "target.v": {"city_upper": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.users", "city_upper")
        assert m.transformation == "expression"
        assert "UPPER" in m.expression
        assert m.source_columns == ["address"]

    def test_nested_struct_access(self):
        """Accessing nested STRUCT fields (address.city.zip_code)."""
        sql = "SELECT address.city.zip_code AS zip FROM mydb.users"
        schemas = {
            "mydb.users": {"address": "STRUCT<city STRUCT<zip_code STRING, name STRING>>"},
            "target.v": {"zip": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.users", "zip")
        assert m.source_columns == ["address"]
        assert m.transformation == "expression"

    def test_struct_field_with_alias(self):
        """STRUCT field access with explicit alias."""
        sql = "SELECT address.city AS user_city FROM mydb.users"
        schemas = {
            "mydb.users": {"address": "STRUCT<city STRING, state STRING>"},
            "target.v": {"user_city": "STRING"},
        }
        edges = parse_view_lineage("target.v", sql, schemas)
        m = _get_mapping(edges, "mydb.users", "user_city")
        assert m.source_columns == ["address"]
        assert m.transformation == "expression"


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


class TestDynamicSQL:
    """Dynamic SQL (EXECUTE IMMEDIATE) detection."""

    def test_execute_immediate_detected(self):
        sql = "EXECUTE IMMEDIATE 'SELECT 1'"
        assert contains_dynamic_sql(sql) is True

    def test_execute_immediate_concat(self):
        sql = """EXECUTE IMMEDIATE CONCAT('SELECT ', col, ' FROM ', tbl)"""
        assert contains_dynamic_sql(sql) is True

    def test_execute_immediate_variable(self):
        sql = """
        DECLARE query STRING;
        SET query = 'SELECT a, b FROM table1';
        EXECUTE IMMEDIATE query;
        """
        assert contains_dynamic_sql(sql) is True

    def test_execute_immediate_case_insensitive(self):
        sql = "execute immediate 'SELECT 1'"
        assert contains_dynamic_sql(sql) is True

    def test_execute_immediate_in_procedure(self):
        sql = """
        CREATE PROCEDURE my_dataset.my_proc()
        BEGIN
          DECLARE tbl STRING DEFAULT 'my_table';
          EXECUTE IMMEDIATE CONCAT('SELECT * FROM ', tbl);
        END;
        """
        assert contains_dynamic_sql(sql) is True

    def test_execute_immediate_with_using(self):
        """EXECUTE IMMEDIATE with USING clause for parameters."""
        sql = """
        EXECUTE IMMEDIATE 'SELECT @col FROM table1'
        USING 'name' AS col;
        """
        assert contains_dynamic_sql(sql) is True

    def test_static_sql_not_detected(self):
        sql = "SELECT id, name FROM mydb.users"
        assert contains_dynamic_sql(sql) is False

    def test_cte_not_detected(self):
        sql = """
        WITH cte AS (SELECT id FROM mydb.users)
        SELECT id FROM cte
        """
        assert contains_dynamic_sql(sql) is False

    def test_execute_in_comment_still_detected(self):
        """String-based detection catches comments too — acceptable trade-off."""
        sql = """
        -- EXECUTE IMMEDIATE 'old code'
        SELECT id FROM mydb.users
        """
        # This is a known limitation: commented-out EXECUTE IMMEDIATE
        # is still detected. This is a conservative choice — better to
        # warn unnecessarily than to miss actual dynamic SQL.
        assert contains_dynamic_sql(sql) is True

    def test_word_execute_alone_not_detected(self):
        """The word 'execute' without 'immediate' is not flagged."""
        sql = "SELECT execute_count FROM mydb.stats"
        assert contains_dynamic_sql(sql) is False


class TestExpandArrayAggStructStar:
    """Tests for _expand_array_agg_struct_star() SQL rewrite."""

    def test_basic_pattern_with_order_and_limit(self):
        """Full pattern: ARRAY_AGG(STRUCT(...) ORDER BY ... LIMIT 1)[OFFSET(0)].*"""
        sql = (
            "SELECT numero_sg, "
            "ARRAY_AGG(STRUCT(causa, resolucion) ORDER BY fecha LIMIT 1)"
            "[OFFSET(0)].* "
            "FROM ds.tbl GROUP BY numero_sg"
        )
        result = _expand_array_agg_struct_star(sql)
        assert "STRUCT" not in result
        assert ".*" not in result.replace("ds.*", "")  # don't match table wildcards
        assert "as causa" in result.lower()
        assert "as resolucion" in result.lower()
        assert "ARRAY_AGG(causa" in result or "ARRAY_AGG(`causa`" in result

    def test_with_alias_in_struct(self):
        """Fields with aliases: STRUCT(expr AS name)."""
        sql = (
            "SELECT "
            "ARRAY_AGG(STRUCT(a, CASE WHEN x THEN y ELSE z END AS b) "
            "ORDER BY a LIMIT 1)[OFFSET(0)].* "
            "FROM ds.tbl GROUP BY a"
        )
        result = _expand_array_agg_struct_star(sql)
        assert "as b" in result.lower() or "as `b`" in result.lower()
        assert "case" in result.lower()

    def test_no_order_no_limit(self):
        """Bare ARRAY_AGG(STRUCT(...))[OFFSET(0)].*"""
        sql = (
            "SELECT ARRAY_AGG(STRUCT(a, b))[OFFSET(0)].* "
            "FROM ds.tbl"
        )
        result = _expand_array_agg_struct_star(sql)
        assert "STRUCT" not in result
        assert "as a" in result.lower() or "as `a`" in result.lower()
        assert "as b" in result.lower() or "as `b`" in result.lower()

    def test_order_only_no_limit(self):
        """ARRAY_AGG(STRUCT(...) ORDER BY x)[OFFSET(0)].*"""
        sql = (
            "SELECT ARRAY_AGG(STRUCT(a, b) ORDER BY a)[OFFSET(0)].* "
            "FROM ds.tbl"
        )
        result = _expand_array_agg_struct_star(sql)
        assert "STRUCT" not in result
        assert "ORDER BY" in result.upper()

    def test_mixed_with_regular_columns(self):
        """Regular columns alongside the ARRAY_AGG pattern."""
        sql = (
            "SELECT id, name, "
            "ARRAY_AGG(STRUCT(x, y) ORDER BY x LIMIT 1)[OFFSET(0)].* "
            "FROM ds.tbl GROUP BY id, name"
        )
        result = _expand_array_agg_struct_star(sql)
        # Regular columns preserved
        assert "id" in result.lower()
        assert "name" in result.lower()
        # Struct expanded
        assert "as x" in result.lower() or "as `x`" in result.lower()
        assert "as y" in result.lower() or "as `y`" in result.lower()

    def test_no_pattern_returns_unchanged(self):
        """SQL without the pattern is returned unchanged."""
        sql = "SELECT id, name FROM ds.tbl"
        result = _expand_array_agg_struct_star(sql)
        assert result == sql

    def test_end_to_end_lineage_tracing(self):
        """Full integration: parse_view_lineage traces through expanded struct fields."""
        sql = (
            "SELECT numero_sg, "
            "ARRAY_AGG(STRUCT(causa, resolucion_cliente) "
            "ORDER BY causa LIMIT 1)[OFFSET(0)].* "
            "FROM ET_PREV.FT_CRM_PREV "
            "GROUP BY numero_sg"
        )
        schemas = {
            "ET_PREV.FT_CRM_PREV": {
                "numero_sg": "BIGNUMERIC",
                "causa": "STRING",
                "resolucion_cliente": "STRING",
            },
            "ET_SUBQRY.AG_HOLA_MULTI_SG": {
                "numero_sg": "BIGNUMERIC",
                "causa": "STRING",
                "resolucion_cliente": "STRING",
            },
        }
        edges = parse_view_lineage("ET_SUBQRY.AG_HOLA_MULTI_SG", sql, schemas)
        # All fields should trace back to the source table
        m_sg = _get_mapping(edges, "ET_PREV.FT_CRM_PREV", "numero_sg")
        assert m_sg is not None
        m_causa = _get_mapping(edges, "ET_PREV.FT_CRM_PREV", "causa")
        assert m_causa is not None
        assert "causa" in m_causa.source_columns
        m_res = _get_mapping(edges, "ET_PREV.FT_CRM_PREV", "resolucion_cliente")
        assert m_res is not None
        assert "resolucion_cliente" in m_res.source_columns
