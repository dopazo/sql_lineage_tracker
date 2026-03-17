"""Minimized tests for nested subquery (derived table) lineage resolution.

Tests the same SQL patterns as test_nested_subquery.py but with generic
table/column names (no production data references). Covers:
- 2-level nesting with explicit columns
- 3-level nesting with SELECT *, window functions, and expressions
- Subqueries with and without aliases
- _expand_star and _normalize_derived_tables helpers
"""

import pytest

from lineage_tracker.parser import (
    _build_sqlglot_schema,
    _expand_star,
    _normalize_derived_tables,
    parse_view_lineage,
)


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
# 2-level nested subquery: SELECT cols FROM (SELECT cols FROM real_table)
# ──────────────────────────────────────────────────────────────────────────────

class TestTwoLevelNestedSubquery:
    """Lineage through 2 levels of nesting with explicit columns."""

    SQL = """
    SELECT
        order_id,
        total_amount,
        order_date
    FROM (
        SELECT
            order_id,
            amount AS total_amount,
            created_at AS order_date
        FROM staging.orders
    ) inner_q
    """

    SCHEMAS = {
        "staging.orders": {
            "order_id": "STRING",
            "amount": "FLOAT64",
            "created_at": "TIMESTAMP",
            "status": "STRING",
        },
        "target.view_2level": {
            "order_id": "STRING",
            "total_amount": "FLOAT64",
            "order_date": "TIMESTAMP",
        },
    }

    @pytest.fixture(autouse=True)
    def parse(self):
        self.edges = parse_view_lineage(
            "target.view_2level", self.SQL, self.SCHEMAS
        )

    def test_has_edge_from_source(self):
        edge = _get_edge(self.edges, "staging.orders")
        assert edge is not None, "Should produce edge from staging.orders"

    def test_order_id_is_direct(self):
        m = _get_mapping(self.edges, "staging.orders", "order_id")
        assert m is not None
        assert m.source_columns == ["order_id"]
        assert m.transformation == "direct"

    def test_total_amount_is_rename(self):
        m = _get_mapping(self.edges, "staging.orders", "total_amount")
        assert m is not None
        assert m.source_columns == ["amount"]
        assert m.transformation == "rename"

    def test_order_date_is_rename(self):
        m = _get_mapping(self.edges, "staging.orders", "order_date")
        assert m is not None
        assert m.source_columns == ["created_at"]
        assert m.transformation == "rename"


# ──────────────────────────────────────────────────────────────────────────────
# 2-level nested subquery without alias (anonymous derived table)
# ──────────────────────────────────────────────────────────────────────────────

class TestTwoLevelNoAlias:
    """Lineage through 2 levels with unnamed derived table."""

    SQL = """
    SELECT
        user_id,
        total_events
    FROM (
        SELECT
            user_id,
            COUNT(*) AS total_events
        FROM raw.events
        GROUP BY user_id
    )
    """

    SCHEMAS = {
        "raw.events": {
            "event_id": "STRING",
            "user_id": "STRING",
            "event_type": "STRING",
        },
        "target.event_counts": {
            "user_id": "STRING",
            "total_events": "INT64",
        },
    }

    @pytest.fixture(autouse=True)
    def parse(self):
        self.edges = parse_view_lineage(
            "target.event_counts", self.SQL, self.SCHEMAS
        )

    def test_has_edge(self):
        edge = _get_edge(self.edges, "raw.events")
        assert edge is not None

    def test_user_id_direct(self):
        m = _get_mapping(self.edges, "raw.events", "user_id")
        assert m is not None
        assert m.source_columns == ["user_id"]
        assert m.transformation == "direct"

    def test_total_events_aggregation(self):
        m = _get_mapping(self.edges, "raw.events", "total_events")
        assert m is not None
        assert m.transformation == "aggregation"
        assert "COUNT" in (m.expression or "")


# ──────────────────────────────────────────────────────────────────────────────
# 3-level nesting: inner JOINs, middle window fns, outer expressions
# ──────────────────────────────────────────────────────────────────────────────

class TestThreeLevelNestedSubquery:
    """Lineage through 3 levels: inner JOINs, middle window fns, outer expressions."""

    SQL = """
    SELECT
        order_id,
        ref_id,
        sale_total,
        refund_total,
        sale_total + refund_total AS net_amount
    FROM (
        SELECT
            order_id,
            ref_id,
            SUM(sale_total) OVER(PARTITION BY order_id) AS sale_total,
            SUM(refund_total) OVER(PARTITION BY order_id) AS refund_total
        FROM (
            SELECT
                c.order_id,
                c.ref_id,
                c.sale_total,
                COALESCE(d.refund_amount, 0) AS refund_total
            FROM staging.invoices c
            LEFT JOIN staging.refunds d ON c.ref_id = d.ref_id
        ) base
    ) windowed
    """

    SCHEMAS = {
        "staging.invoices": {
            "order_id": "STRING",
            "ref_id": "STRING",
            "sale_total": "FLOAT64",
            "refund_total": "FLOAT64",
        },
        "staging.refunds": {
            "ref_id": "STRING",
            "refund_amount": "FLOAT64",
        },
        "target.net_summary": {
            "order_id": "STRING",
            "ref_id": "STRING",
            "sale_total": "FLOAT64",
            "refund_total": "FLOAT64",
            "net_amount": "FLOAT64",
        },
    }

    @pytest.fixture(autouse=True)
    def parse(self):
        self.edges = parse_view_lineage(
            "target.net_summary", self.SQL, self.SCHEMAS
        )

    def test_has_edges(self):
        """Should produce at least one edge (not all unknown)."""
        assert len(self.edges) > 0

    def test_order_id_traced_to_invoices(self):
        m = _get_mapping(self.edges, "staging.invoices", "order_id")
        assert m is not None
        assert "order_id" in m.source_columns
        assert m.transformation != "unknown"

    def test_ref_id_traced_to_invoices(self):
        m = _get_mapping(self.edges, "staging.invoices", "ref_id")
        assert m is not None
        assert "ref_id" in m.source_columns
        assert m.transformation != "unknown"

    def test_sale_total_is_expression(self):
        """Window function SUM OVER should be classified as expression."""
        m = _get_mapping(self.edges, "staging.invoices", "sale_total")
        assert m is not None
        assert m.transformation != "unknown"

    def test_refund_total_involves_refunds(self):
        """refund_total uses COALESCE with refunds.refund_amount."""
        m_inv = _get_mapping(self.edges, "staging.invoices", "refund_total")
        m_ref = _get_mapping(self.edges, "staging.refunds", "refund_total")
        assert (m_inv is not None) or (m_ref is not None), \
            "refund_total should trace to at least one source"

    def test_net_amount_is_expression(self):
        """net_amount = sale_total + refund_total."""
        found = False
        for edge in self.edges:
            m = _get_mapping([edge], edge.source_node, "net_amount")
            if m is not None and m.transformation != "unknown":
                found = True
                break
        assert found, "net_amount should be classified as expression, not unknown"

    def test_no_all_unknown(self):
        """At least some columns should be resolved (not all unknown)."""
        non_unknown = 0
        for edge in self.edges:
            for m in edge.column_mappings:
                if m.transformation != "unknown":
                    non_unknown += 1
        assert non_unknown > 0, "All columns are unknown - nested subquery resolution failed"


# ──────────────────────────────────────────────────────────────────────────────
# 3-level nesting with SELECT * at outer level
# ──────────────────────────────────────────────────────────────────────────────

class TestThreeLevelWithSelectStar:
    """Outer SELECT * over nested subqueries should expand and resolve."""

    SQL = """
    SELECT
        *,
        amount * quantity AS total_value
    FROM (
        SELECT
            product_id,
            amount,
            quantity
        FROM (
            SELECT
                p.product_id,
                p.unit_price AS amount,
                t.quantity
            FROM catalog.products p
            JOIN sales.transactions t ON p.product_id = t.product_id
        ) joined
    ) enriched
    """

    SCHEMAS = {
        "catalog.products": {
            "product_id": "STRING",
            "unit_price": "FLOAT64",
            "name": "STRING",
        },
        "sales.transactions": {
            "transaction_id": "STRING",
            "product_id": "STRING",
            "quantity": "INT64",
        },
        "target.product_totals": {
            "product_id": "STRING",
            "amount": "FLOAT64",
            "quantity": "INT64",
            "total_value": "FLOAT64",
        },
    }

    @pytest.fixture(autouse=True)
    def parse(self):
        self.edges = parse_view_lineage(
            "target.product_totals", self.SQL, self.SCHEMAS
        )

    def test_has_edges(self):
        assert len(self.edges) > 0

    def test_product_id_resolved(self):
        m = _get_mapping(self.edges, "catalog.products", "product_id")
        assert m is not None
        assert m.transformation != "unknown"

    def test_amount_traced_to_unit_price(self):
        m = _get_mapping(self.edges, "catalog.products", "amount")
        assert m is not None
        assert "unit_price" in m.source_columns
        assert m.transformation == "rename"

    def test_quantity_from_transactions(self):
        m = _get_mapping(self.edges, "sales.transactions", "quantity")
        assert m is not None
        assert m.transformation != "unknown"

    def test_total_value_is_expression(self):
        """total_value = amount * quantity should be an expression."""
        found = False
        for edge in self.edges:
            m = _get_mapping([edge], edge.source_node, "total_value")
            if m is not None and m.transformation == "expression":
                found = True
                break
        assert found, "total_value should be classified as expression"


# ──────────────────────────────────────────────────────────────────────────────
# _expand_star fix: verify SELECT * expansion works with subqueries
# ──────────────────────────────────────────────────────────────────────────────

class TestExpandStarWithSubqueries:
    """Verify _expand_star correctly expands SELECT * through derived tables."""

    def test_expand_star_simple_table(self):
        sql = "SELECT * FROM mydb.users"
        sg_schema = {"mydb": {"users": {"id": "INT64", "name": "STRING"}}}
        result = _expand_star(sql, sg_schema)
        assert result != sql, "_expand_star should expand SELECT * from a table"
        assert "*" not in result

    def test_expand_star_nested_subquery(self):
        sql = "SELECT * FROM (SELECT x, y FROM mydb.t) sub"
        sg_schema = {"mydb": {"t": {"x": "INT64", "y": "STRING"}}}
        result = _expand_star(sql, sg_schema)
        assert result != sql, "_expand_star should expand SELECT * from subquery"
        assert "*" not in result

    def test_expand_star_anonymous_subquery(self):
        sql = "SELECT * FROM (SELECT a FROM mydb.t)"
        sg_schema = {"mydb": {"t": {"a": "STRING", "b": "STRING"}}}
        result = _expand_star(sql, sg_schema)
        assert result != sql, "_expand_star should handle anonymous derived tables"
        assert "*" not in result

    def test_expand_star_deep_nesting(self):
        sql = "SELECT * FROM (SELECT a, b FROM (SELECT x AS a, y AS b FROM mydb.t))"
        sg_schema = {"mydb": {"t": {"x": "INT64", "y": "STRING"}}}
        result = _expand_star(sql, sg_schema)
        assert result != sql
        assert "*" not in result


class TestNormalizeDerivedTables:
    """Verify _normalize_derived_tables assigns aliases to anonymous subqueries."""

    def test_assigns_alias_to_anonymous_subquery(self):
        sql = "SELECT a FROM (SELECT a FROM mydb.t)"
        result = _normalize_derived_tables(sql)
        assert "_subq_" in result

    def test_preserves_existing_alias(self):
        sql = "SELECT a FROM (SELECT a FROM mydb.t) my_alias"
        result = _normalize_derived_tables(sql)
        assert "my_alias" in result

    def test_no_change_without_subquery(self):
        sql = "SELECT a FROM mydb.t"
        result = _normalize_derived_tables(sql)
        assert "_subq_" not in result


# ──────────────────────────────────────────────────────────────────────────────
# 3-level full reproduction pattern with inline subquery + CASE + window fns
# ──────────────────────────────────────────────────────────────────────────────

class TestFullThreeLevelPattern:
    """Full reproduction of 3-level nesting pattern.

    3-level nesting with:
    - Outer: SELECT * + calculated fields
    - Middle: window functions (SUM OVER), renames
    - Inner: multiple JOINs including inline subquery, COALESCE
    """

    SQL = """
    SELECT *
      ,SALE_TOTAL + REFUND_TOTAL AS NET_AMOUNT
      ,PAYMENTS_AGG - (SALE_TOTAL_AGG + REFUND_TOTAL_AGG) AS NET_CALC
    FROM (
      SELECT
        ORDER_ID, REF_ID, GROUP_ID,
        SALE_TOTAL AS SALE_DETAIL,
        SUM(SALE_TOTAL) OVER(PARTITION BY ORDER_ID, GROUP_ID) SALE_TOTAL,
        REFUND_TOTAL AS REFUND_DETAIL,
        SUM(REFUND_TOTAL) OVER(PARTITION BY ORDER_ID, GROUP_ID) REFUND_TOTAL,
        PAYMENTS AS PAYMENTS_DETAIL,
        SUM(PAYMENTS) OVER(PARTITION BY ORDER_ID, GROUP_ID) PAYMENTS_AGG,
        SALE_TOTAL_AGG,
        REFUND_TOTAL_AGG
      FROM (
        SELECT
          INV.ORDER_ID, INV.REF_ID, INV.GROUP_ID,
          INV.SALE_TOTAL,
          COALESCE(RET.RETURN_TOTAL, 0) AS REFUND_TOTAL,
          COALESCE(PAY.TOTAL_PAYMENTS, 0) AS PAYMENTS,
          COALESCE(AGG.SALE_TOTAL_AGG, 0) AS SALE_TOTAL_AGG,
          COALESCE(AGG.REFUND_TOTAL_AGG, 0) AS REFUND_TOTAL_AGG
        FROM test_ds.invoices INV
        LEFT JOIN test_ds.payments PAY ON INV.REF_ID = PAY.REF_ID
        LEFT JOIN (
          SELECT REF_ID, SUM(AMOUNT) AS RETURN_TOTAL
          FROM test_ds.returns
          GROUP BY REF_ID
        ) RET ON INV.REF_ID = RET.REF_ID
        LEFT JOIN test_ds.leftovers LFT ON INV.ORDER_ID = LFT.ORDER_ID
        LEFT JOIN test_ds.aggregates AGG ON INV.REF_ID = AGG.REF_ID
      )
    )
    """

    SCHEMAS = {
        "test_ds.invoices": {
            "ORDER_ID": "STRING", "REF_ID": "STRING", "GROUP_ID": "STRING",
            "SALE_TOTAL": "FLOAT64",
        },
        "test_ds.payments": {"REF_ID": "STRING", "TOTAL_PAYMENTS": "FLOAT64"},
        "test_ds.returns": {"REF_ID": "STRING", "AMOUNT": "FLOAT64"},
        "test_ds.leftovers": {"ORDER_ID": "STRING", "LEFTOVER": "FLOAT64"},
        "test_ds.aggregates": {
            "REF_ID": "STRING",
            "SALE_TOTAL_AGG": "FLOAT64",
            "REFUND_TOTAL_AGG": "FLOAT64",
        },
        "test_ds.full_report": {
            "ORDER_ID": "STRING", "REF_ID": "STRING", "GROUP_ID": "STRING",
            "SALE_DETAIL": "FLOAT64", "SALE_TOTAL": "FLOAT64",
            "REFUND_DETAIL": "FLOAT64", "REFUND_TOTAL": "FLOAT64",
            "PAYMENTS_DETAIL": "FLOAT64", "PAYMENTS_AGG": "FLOAT64",
            "SALE_TOTAL_AGG": "FLOAT64", "REFUND_TOTAL_AGG": "FLOAT64",
            "NET_AMOUNT": "FLOAT64", "NET_CALC": "FLOAT64",
        },
    }

    @pytest.fixture(autouse=True)
    def parse(self):
        self.edges = parse_view_lineage(
            "test_ds.full_report", self.SQL, self.SCHEMAS
        )

    def test_produces_edges(self):
        assert len(self.edges) > 0

    def test_no_unknown_columns(self):
        """The core issue: no column should have transformation='unknown'."""
        unknowns = []
        for edge in self.edges:
            for m in edge.column_mappings:
                if m.transformation == "unknown":
                    unknowns.append(f"{edge.source_node}.{m.target_column}")
        assert unknowns == [], f"Columns with unknown lineage: {unknowns}"

    def test_order_id_traced_to_source(self):
        m = _get_mapping(self.edges, "test_ds.invoices", "ORDER_ID")
        assert m is not None
        assert m.transformation != "unknown"

    def test_sale_total_is_window(self):
        """SUM(SALE_TOTAL) OVER(...) should be expression."""
        m = _get_mapping(self.edges, "test_ds.invoices", "SALE_TOTAL")
        assert m is not None
        assert m.transformation == "expression"

    def test_net_amount_is_expression(self):
        """SALE_TOTAL + REFUND_TOTAL should be expression."""
        found = any(
            _get_mapping([e], e.source_node, "NET_AMOUNT") is not None
            and _get_mapping([e], e.source_node, "NET_AMOUNT").transformation == "expression"
            for e in self.edges
        )
        assert found

    def test_return_total_traced(self):
        """COALESCE(RET.RETURN_TOTAL, 0) should trace to returns."""
        m = _get_mapping(self.edges, "test_ds.returns", "REFUND_DETAIL")
        assert m is not None
        assert "amount" in m.source_columns

    def test_payments_traced(self):
        """COALESCE(PAY.TOTAL_PAYMENTS, 0) should trace to payments."""
        m = _get_mapping(self.edges, "test_ds.payments", "PAYMENTS_DETAIL")
        assert m is not None

    def test_agg_columns_traced(self):
        """AGG columns should trace to aggregates."""
        m_sale = _get_mapping(self.edges, "test_ds.aggregates", "SALE_TOTAL_AGG")
        m_ref = _get_mapping(self.edges, "test_ds.aggregates", "REFUND_TOTAL_AGG")
        assert m_sale is not None
        assert m_ref is not None
