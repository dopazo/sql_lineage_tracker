"""Minimized reproduction tests for star expansion, nested subqueries, and missing columns.

Tests the same SQL patterns as test_repro_dist_cuad.py but with generic
table/column names (no production data references). Covers:
- SELECT A.* with JOINs (uppercase BigQuery column names)
- Deeply nested subqueries (3 levels) with SELECT *
- Window functions, CASE expressions, COALESCE in nested contexts
- Identifier normalization for BigQuery case-insensitivity
- Missing column tolerance (schema-patching)
"""

import pytest

from lineage_tracker.parser import (
    _build_sqlglot_schema,
    _expand_star,
    _normalize_bq_identifiers,
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


# --------------------------------------------------------------------------
# Core fix verification: _expand_star with BigQuery uppercase + JOINs
# --------------------------------------------------------------------------

class TestExpandStarBigQueryCase:
    """Verify _expand_star works with uppercase BigQuery column names and JOINs."""

    def test_star_with_join_uppercase(self):
        """SELECT A.* with JOIN and uppercase columns should expand."""
        sql = "SELECT A.*, T.VAL FROM ds.t1 AS A LEFT JOIN ds.t2 AS T ON T.ID = A.ID"
        schema = _build_sqlglot_schema({
            "ds.t1": {"ID": "STRING", "NAME": "STRING"},
            "ds.t2": {"ID": "STRING", "VAL": "FLOAT64"},
        })
        result = _expand_star(sql, schema)
        assert "*" not in result, f"Star should be expanded, got: {result}"

    def test_star_with_multiple_joins(self):
        """A.* with multiple LEFT JOINs."""
        sql = """SELECT A.*, COALESCE(T.VAL, 0) AS MY_VAL, H.INFO
        FROM ds.main AS A
        LEFT JOIN ds.secondary AS T ON T.KEY = A.KEY
        LEFT JOIN ds.tertiary AS H ON H.KEY2 = A.KEY"""
        schema = _build_sqlglot_schema({
            "ds.main": {"KEY": "STRING", "COL_A": "STRING", "COL_B": "FLOAT64"},
            "ds.secondary": {"KEY": "STRING", "VAL": "FLOAT64"},
            "ds.tertiary": {"KEY2": "STRING", "INFO": "STRING"},
        })
        result = _expand_star(sql, schema)
        assert "*" not in result
        assert "col_a" in result.lower()
        assert "col_b" in result.lower()

    def test_star_nested_subquery_with_join(self):
        """SELECT * from nested subquery containing JOINs."""
        sql = """SELECT * FROM (
            SELECT A.X, B.Y FROM ds.t1 AS A JOIN ds.t2 AS B ON A.ID = B.ID
        )"""
        schema = _build_sqlglot_schema({
            "ds.t1": {"ID": "STRING", "X": "INT64"},
            "ds.t2": {"ID": "STRING", "Y": "INT64"},
        })
        result = _expand_star(sql, schema)
        assert "*" not in result


class TestNormalizeBqIdentifiers:
    """Verify _normalize_bq_identifiers preserves table names but lowercases columns."""

    def test_preserves_table_names(self):
        sql = "SELECT A.COL FROM MY_DATASET.MY_TABLE AS A"
        result = _normalize_bq_identifiers(sql)
        assert "MY_DATASET" in result, "Dataset name should be preserved"
        assert "MY_TABLE" in result, "Table name should be preserved"

    def test_lowercases_columns(self):
        sql = "SELECT A.MY_COL FROM ds.t AS A"
        result = _normalize_bq_identifiers(sql)
        assert "my_col" in result

    def test_lowercases_aliases(self):
        sql = "SELECT COL AS MY_ALIAS FROM ds.t AS ALIAS_A"
        result = _normalize_bq_identifiers(sql)
        assert "my_alias" in result


# --------------------------------------------------------------------------
# Star expansion + JOINs + window functions + CASE aggregation
# Pattern: SELECT A.* from base_table with JOINs adding computed columns
# --------------------------------------------------------------------------

SQL_STAR_JOINS = """
SELECT
A.*,
SUM(COALESCE(B.SALE_AMOUNT, 0) + COALESCE(A.BASE_PRICE, 0)) OVER(PARTITION BY A.ORDER_ID) TOTAL_AMOUNT_AGG,
SUM(COALESCE(B.REFUND_AMOUNT, 0) + COALESCE(A.DISCOUNT, 0)) OVER(PARTITION BY A.ORDER_ID) TOTAL_REFUND_AGG,
SUM((COALESCE(B.SALE_AMOUNT, 0) + COALESCE(B.REFUND_AMOUNT, 0) + COALESCE(A.BASE_PRICE, 0) + COALESCE(A.DISCOUNT, 0))) OVER(PARTITION BY A.ORDER_ID) AS NET_TOTAL_AGG,
COALESCE(B.SALE_AMOUNT, 0) JOINED_SALE,
COALESCE(B.REFUND_AMOUNT, 0) JOINED_REFUND,
(COALESCE(B.SALE_AMOUNT, 0) + COALESCE(B.REFUND_AMOUNT, 0)) AS NET_JOINED,
COALESCE(B.SALE_QTY, 0) JOINED_SALE_QTY,
COALESCE(B.REFUND_QTY, 0) JOINED_REFUND_QTY,
C.CATEGORY_NAME,
C.TICKET_NUMBER,
C.REASON,
C.REVIEW_DATE,
C.CREATED_DATE,
SUM(CASE WHEN (CATEGORY_NAME IN ("VOID", "REJECTED") AND CATEGORY_NAME NOT LIKE '%PARTIAL%') THEN 1 ELSE 0 END) OVER(PARTITION BY A.ORDER_ID, GROUP_ID) AS CATEGORY_COUNT
FROM test_ds.base_orders AS A
LEFT JOIN test_ds.sales_agg AS B ON B.REF_ID = A.REF_ID
LEFT JOIN test_ds.categories AS C ON C.REF_KEY = A.REF_ID
"""

SCHEMAS_STAR_JOINS = {
    "test_ds.base_orders": {
        "ORDER_ID": "STRING", "REF_ID": "STRING", "GROUP_ID": "STRING",
        "ORDER_TYPE": "STRING", "STATUS": "STRING",
        "BASE_PRICE": "FLOAT64", "DISCOUNT": "FLOAT64",
        "ITEM_COUNT": "INT64", "CHANNEL": "STRING",
        "PURCHASE_DATE": "DATE", "DELIVERY_DATE": "DATE",
        "PAYMENT_METHOD": "STRING", "TOTAL_VALUE": "FLOAT64",
    },
    "test_ds.sales_agg": {
        "REF_ID": "STRING",
        "SALE_AMOUNT": "FLOAT64", "REFUND_AMOUNT": "FLOAT64",
        "SALE_QTY": "INT64", "REFUND_QTY": "INT64",
    },
    "test_ds.categories": {
        "REF_KEY": "STRING",
        "CATEGORY_NAME": "STRING", "TICKET_NUMBER": "STRING",
        "REASON": "STRING", "REVIEW_DATE": "DATE", "CREATED_DATE": "DATE",
    },
    # Output schema
    "test_ds.summary_view": {
        "ORDER_ID": "STRING", "REF_ID": "STRING", "GROUP_ID": "STRING",
        "ORDER_TYPE": "STRING", "STATUS": "STRING",
        "BASE_PRICE": "FLOAT64", "DISCOUNT": "FLOAT64",
        "ITEM_COUNT": "INT64", "CHANNEL": "STRING",
        "PURCHASE_DATE": "DATE", "DELIVERY_DATE": "DATE",
        "PAYMENT_METHOD": "STRING", "TOTAL_VALUE": "FLOAT64",
        # Added by view
        "TOTAL_AMOUNT_AGG": "FLOAT64", "TOTAL_REFUND_AGG": "FLOAT64",
        "NET_TOTAL_AGG": "FLOAT64",
        "JOINED_SALE": "FLOAT64", "JOINED_REFUND": "FLOAT64",
        "NET_JOINED": "FLOAT64",
        "JOINED_SALE_QTY": "INT64", "JOINED_REFUND_QTY": "INT64",
        "CATEGORY_NAME": "STRING", "TICKET_NUMBER": "STRING",
        "REASON": "STRING", "REVIEW_DATE": "DATE", "CREATED_DATE": "DATE",
        "CATEGORY_COUNT": "INT64",
    },
}


# --------------------------------------------------------------------------
# 3-level nesting: SELECT * + expressions over window functions over JOINs
# --------------------------------------------------------------------------

SQL_NESTED_3LEVEL = """SELECT *
,SALE_AMOUNT + REFUND_AMOUNT AS NET_AMOUNT
,TOTAL_PAYMENTS - (SALE_AMOUNT_AGG + REFUND_AMOUNT_AGG) - TOTAL_RETURNS AS NET_CALC
,CASE WHEN ITEM_TYPE IN ('GM','GR') THEN TOTAL_PAYMENTS - (SALE_AMOUNT + REFUND_AMOUNT) - TOTAL_RETURNS
      ELSE TOTAL_PAYMENTS - (TICKET_CREDIT + TICKET_DEBIT) - TOTAL_RETURNS
      END
AS NET_CALC_ALT
FROM(
  SELECT
  ORDER_ID
  ,REF_ID
  ,GROUP_ID
  ,ORDER_TYPE
  ,ITEM_TYPE
  ,ITEM_COUNT
  ,CHANNEL
  ,PURCHASE_DATE
  ,TOTAL_VALUE
  ,STATUS
  ,PAYMENT_METHOD
  ,SALE_AMOUNT_AGG
  ,REFUND_AMOUNT_AGG
  ,NET_TOTAL_AGG
  ,SALE_AMOUNT           AS SALE_DETAIL
  ,REFUND_AMOUNT         AS REFUND_DETAIL
  ,SALE_QTY
  ,REFUND_QTY
  ,TICKET_CREDIT
  ,TICKET_DEBIT
  ,TICKET_NET
  ,RETURN_AMOUNT_A
  ,RETURN_QTY_A
  ,RETURN_DATE_A
  ,RETURN_AMOUNT_B
  ,RETURN_QTY_B
  ,RETURN_DATE_B
  ,CATEGORY_NAME
  ,TICKET_NUMBER
  ,REASON
  ,REVIEW_DATE
  ,CREATED_DATE
  ,CATEGORY_COUNT
  ,TOTAL_PAYMENTS AS PAYMENTS_DETAIL
  ,TOTAL_RETURNS AS RETURNS_DETAIL
  ,LEFTOVER_PAYMENTS
  ,LEFTOVER_RETURNS
  ,CASE
      WHEN ITEM_TYPE = 'GM' THEN MAX(TOTAL_PAYMENTS) OVER(PARTITION BY ORDER_ID, GROUP_ID)
      ELSE TOTAL_PAYMENTS
  END AS TOTAL_PAYMENTS
  ,SUM(SALE_AMOUNT) OVER(PARTITION BY ORDER_ID, GROUP_ID) SALE_AMOUNT
  ,SUM(REFUND_AMOUNT) OVER(PARTITION BY ORDER_ID, GROUP_ID) REFUND_AMOUNT
  ,CASE
      WHEN ITEM_TYPE = 'GM' THEN SUM(TOTAL_RETURNS) OVER(PARTITION BY ORDER_ID, GROUP_ID)
      ELSE TOTAL_RETURNS
  END AS TOTAL_RETURNS
  ,SUM(CASE WHEN SALE_AMOUNT + REFUND_AMOUNT < 0 THEN 1 ELSE 0 END) OVER(PARTITION BY ORDER_ID, GROUP_ID) EXCESS_REFUND
  ,SUM(CASE WHEN STATUS NOT IN ('Rescheduled','Cancelled') AND SALE_AMOUNT = 0 THEN 1 ELSE 0 END) OVER(PARTITION BY ORDER_ID, GROUP_ID) MISSING_SALE
  FROM(
    SELECT
    ORD.ORDER_ID
    ,ORD.REF_ID
    ,ORD.GROUP_ID
    ,ORD.ORDER_TYPE
    ,ORD.ITEM_TYPE
    ,ORD.ITEM_COUNT
    ,ORD.CHANNEL
    ,ORD.PURCHASE_DATE
    ,ORD.TOTAL_VALUE
    ,ORD.STATUS
    ,ORD.PAYMENT_METHOD
    ,ORD.SALE_AMOUNT_AGG
    ,ORD.REFUND_AMOUNT_AGG
    ,ORD.NET_TOTAL_AGG
    ,ORD.SALE_AMOUNT
    ,ORD.REFUND_AMOUNT
    ,ORD.NET_JOINED
    ,ORD.SALE_QTY
    ,ORD.REFUND_QTY
    ,COALESCE(ORD.TICKET_CREDIT, 0) TICKET_CREDIT
    ,ORD.TICKET_DEBIT
    ,ORD.TICKET_NET
    ,CASE
      WHEN ORDER_TYPE = 'SINGLE' THEN COALESCE(ORD.RETURN_AMOUNT_A, 0)
      ELSE COALESCE(RET.RETURN_AMOUNT_A, 0) END RETURN_AMOUNT_A
    ,CASE
      WHEN ORDER_TYPE = 'SINGLE' THEN COALESCE(ORD.RETURN_QTY_A, 0)
      ELSE COALESCE(RET.RETURN_QTY_A, 0) END RETURN_QTY_A
    ,ORD.RETURN_DATE_A
    ,CASE
      WHEN ORDER_TYPE = 'SINGLE' THEN COALESCE(ORD.RETURN_AMOUNT_B, 0)
      ELSE COALESCE(RET.RETURN_AMOUNT_B, 0) END RETURN_AMOUNT_B
    ,CASE
      WHEN ORDER_TYPE = 'SINGLE' THEN COALESCE(ORD.RETURN_QTY_B, 0)
      ELSE COALESCE(RET.RETURN_QTY_B, 0) END RETURN_QTY_B
    ,ORD.RETURN_DATE_B
    ,ORD.CATEGORY_NAME
    ,ORD.TICKET_NUMBER
    ,ORD.REASON
    ,ORD.REVIEW_DATE
    ,ORD.CREATED_DATE
    ,ORD.CATEGORY_COUNT
    ,CASE WHEN ORDER_TYPE = 'SINGLE' THEN COALESCE(ORD.SALE_QTY, 0) + COALESCE(ORD.REFUND_QTY, 0) ELSE PAY.TOTAL_PAYMENTS END TOTAL_PAYMENTS
    ,CASE WHEN ORDER_TYPE = 'SINGLE' THEN COALESCE(ORD.SALE_QTY, 0) + COALESCE(ORD.REFUND_QTY, 0) ELSE COALESCE(PAY.PAYMENTS_REF, 0) END TOTAL_PAYMENTS_REF
    ,CASE WHEN ORDER_TYPE = 'SINGLE' THEN CAST((COALESCE(ORD.RETURN_AMOUNT_A, 0) + COALESCE(ORD.RETURN_AMOUNT_B, 0)) AS INT64) ELSE COALESCE(PAY.RETURNS_TOTAL, 0) END TOTAL_RETURNS
    ,CASE WHEN ORDER_TYPE = 'SINGLE' THEN 0 ELSE LEFTOVERS.LEFTOVER_PAYMENTS END LEFTOVER_PAYMENTS
    ,CASE WHEN ORDER_TYPE = 'SINGLE' THEN 0 ELSE LEFTOVERS.LEFTOVER_RETURNS END LEFTOVER_RETURNS
    FROM test_ds.summary_view ORD
    LEFT JOIN test_ds.payments_result PAY
    ON ORD.REF_ID = PAY.REF_ID
    LEFT JOIN (
      SELECT REF_ID
      ,SUM(CASE WHEN METHOD = 'CARD_A' THEN AMOUNT ELSE 0 END) AS RETURN_AMOUNT_A
      ,SUM(CASE WHEN METHOD = 'CARD_A' THEN 1 ELSE 0 END) AS RETURN_QTY_A
      ,SUM(CASE WHEN METHOD = 'CARD_B' THEN AMOUNT ELSE 0 END) AS RETURN_AMOUNT_B
      ,SUM(CASE WHEN METHOD = 'CARD_B' THEN 1 ELSE 0 END) AS RETURN_QTY_B
      ,SUM(AMOUNT) AS RETURNS_REF
      FROM test_ds.returns_result
      WHERE FLAG = 1
      GROUP BY REF_ID
    ) RET
    ON ORD.REF_ID = RET.REF_ID
    LEFT JOIN test_ds.leftovers_result LEFTOVERS
    ON ORD.ORDER_ID = LEFTOVERS.ORDER_ID
  )
)
"""

SCHEMAS_NESTED_3LEVEL = {
    "test_ds.summary_view": SCHEMAS_STAR_JOINS["test_ds.summary_view"],
    "test_ds.payments_result": {
        "REF_ID": "STRING", "PAYMENTS_REF": "FLOAT64", "TOTAL_PAYMENTS": "FLOAT64",
        "RETURNS_TOTAL": "FLOAT64", "LEFTOVER_REF": "FLOAT64",
    },
    "test_ds.returns_result": {
        "REF_ID": "STRING", "AMOUNT": "FLOAT64", "FLAG": "INT64", "METHOD": "STRING",
    },
    "test_ds.leftovers_result": {
        "ORDER_ID": "STRING", "LEFTOVER_PAYMENTS": "FLOAT64", "LEFTOVER_RETURNS": "FLOAT64",
    },
    # Output schema
    "test_ds.nested_report": {
        "ORDER_ID": "STRING", "REF_ID": "STRING", "GROUP_ID": "STRING",
        "ORDER_TYPE": "STRING", "ITEM_TYPE": "STRING", "ITEM_COUNT": "INT64",
        "CHANNEL": "STRING", "PURCHASE_DATE": "DATE",
        "TOTAL_VALUE": "FLOAT64", "STATUS": "STRING", "PAYMENT_METHOD": "STRING",
        "SALE_AMOUNT_AGG": "FLOAT64", "REFUND_AMOUNT_AGG": "FLOAT64",
        "NET_TOTAL_AGG": "FLOAT64",
        "SALE_DETAIL": "FLOAT64", "REFUND_DETAIL": "FLOAT64",
        "SALE_QTY": "INT64", "REFUND_QTY": "INT64",
        "TICKET_CREDIT": "FLOAT64", "TICKET_DEBIT": "FLOAT64",
        "TICKET_NET": "FLOAT64",
        "RETURN_AMOUNT_A": "FLOAT64", "RETURN_QTY_A": "INT64", "RETURN_DATE_A": "DATE",
        "RETURN_AMOUNT_B": "FLOAT64", "RETURN_QTY_B": "INT64", "RETURN_DATE_B": "DATE",
        "CATEGORY_NAME": "STRING", "TICKET_NUMBER": "STRING",
        "REASON": "STRING", "REVIEW_DATE": "DATE", "CREATED_DATE": "DATE",
        "CATEGORY_COUNT": "INT64",
        "TOTAL_PAYMENTS": "FLOAT64", "TOTAL_RETURNS": "FLOAT64",
        "PAYMENTS_DETAIL": "FLOAT64", "RETURNS_DETAIL": "FLOAT64",
        "LEFTOVER_PAYMENTS": "FLOAT64", "LEFTOVER_RETURNS": "FLOAT64",
        "SALE_AMOUNT": "FLOAT64", "REFUND_AMOUNT": "FLOAT64",
        "EXCESS_REFUND": "INT64", "MISSING_SALE": "INT64",
        "NET_AMOUNT": "FLOAT64", "NET_CALC": "FLOAT64", "NET_CALC_ALT": "FLOAT64",
    },
}


# --------------------------------------------------------------------------
# SELECT A.* with JOINs + window functions + CASE
# --------------------------------------------------------------------------

class TestStarWithJoins:
    """Star expansion from base table with JOINs adding computed columns."""

    @pytest.fixture(autouse=True)
    def parse(self):
        self.edges = parse_view_lineage(
            "test_ds.summary_view", SQL_STAR_JOINS, SCHEMAS_STAR_JOINS
        )

    def test_passthrough_col_traced_to_source(self):
        """ORDER_TYPE from A.* should be traced to base_orders."""
        m = _get_mapping(self.edges, "test_ds.base_orders", "ORDER_TYPE")
        assert m is not None, "ORDER_TYPE should be found"
        assert m.transformation != "unknown"

    def test_joined_col_from_secondary(self):
        """COALESCE(B.SALE_AMOUNT, 0) should trace to sales_agg."""
        m = _get_mapping(self.edges, "test_ds.sales_agg", "JOINED_SALE")
        assert m is not None
        assert m.transformation != "unknown"

    def test_joined_col_from_tertiary(self):
        """C.CATEGORY_NAME should trace to categories."""
        m = _get_mapping(self.edges, "test_ds.categories", "CATEGORY_NAME")
        assert m is not None
        assert m.transformation != "unknown"

    def test_all_columns_resolved(self):
        """All output columns should have non-unknown lineage."""
        output_cols = set(SCHEMAS_STAR_JOINS["test_ds.summary_view"].keys())
        resolved = set()
        for edge in self.edges:
            for m in edge.column_mappings:
                if m.transformation != "unknown":
                    resolved.add(m.target_column)
        missing = output_cols - resolved
        assert not missing, f"Columns with unknown lineage: {missing}"


# --------------------------------------------------------------------------
# 3-level nesting with SELECT *, window functions, JOINs
# --------------------------------------------------------------------------

class TestNestedThreeLevel:
    """3-level nested SELECT with outer SELECT *."""

    @pytest.fixture(autouse=True)
    def parse(self):
        self.edges = parse_view_lineage(
            "test_ds.nested_report", SQL_NESTED_3LEVEL, SCHEMAS_NESTED_3LEVEL
        )

    def test_produces_edges(self):
        assert len(self.edges) > 0

    def test_order_id_traced_to_source(self):
        """ORDER_ID should trace through nested subqueries to summary_view."""
        m = _get_mapping(self.edges, "test_ds.summary_view", "ORDER_ID")
        assert m is not None
        assert m.transformation != "unknown"

    def test_net_amount_is_expression(self):
        """NET_AMOUNT = SALE_AMOUNT + REFUND_AMOUNT."""
        found = any(
            _get_mapping([e], e.source_node, "NET_AMOUNT") is not None
            and _get_mapping([e], e.source_node, "NET_AMOUNT").transformation == "expression"
            for e in self.edges
        )
        assert found, "NET_AMOUNT should be an expression"

    def test_no_unknown_columns(self):
        """No column should have transformation='unknown'."""
        unknowns = []
        for edge in self.edges:
            for m in edge.column_mappings:
                if m.transformation == "unknown":
                    unknowns.append(f"{edge.source_node}.{m.target_column}")
        assert unknowns == [], f"Columns with unknown lineage: {unknowns}"

    def test_all_output_columns_resolved(self):
        """Every output column should appear in at least one edge."""
        output_cols = set(SCHEMAS_NESTED_3LEVEL["test_ds.nested_report"].keys())
        resolved = set()
        for edge in self.edges:
            for m in edge.column_mappings:
                if m.transformation != "unknown":
                    resolved.add(m.target_column)
        missing = output_cols - resolved
        assert not missing, f"Missing columns: {missing}"


# --------------------------------------------------------------------------
# Missing column tolerance
# --------------------------------------------------------------------------

class TestMissingColumnTolerance:
    """Verify that a single missing column doesn't abort all lineage.

    Reproduces the scenario where a source schema doesn't include a column
    referenced in the SQL (e.g. column was renamed in source table).
    Without schema-patching, qualify_columns raises OptimizeError for ALL
    columns. With the fix, only the missing column may be imprecise.
    """

    @pytest.fixture(autouse=True)
    def parse(self):
        # Remove ITEM_COUNT from source schema to simulate missing column
        source_schema_real = {
            k: v
            for k, v in SCHEMAS_STAR_JOINS["test_ds.summary_view"].items()
            if k != "ITEM_COUNT"
        }
        schemas_real = dict(SCHEMAS_NESTED_3LEVEL)
        schemas_real["test_ds.summary_view"] = source_schema_real

        self.edges = parse_view_lineage(
            "test_ds.nested_report", SQL_NESTED_3LEVEL, schemas_real
        )
        self.output_cols = set(SCHEMAS_NESTED_3LEVEL["test_ds.nested_report"].keys())

    def test_produces_edges(self):
        """Should produce edges even with a missing column."""
        assert len(self.edges) > 0

    def test_not_all_unknown(self):
        """Most columns should resolve, not 0/N like before the fix."""
        resolved = set()
        for edge in self.edges:
            for m in edge.column_mappings:
                if m.transformation != "unknown":
                    resolved.add(m.target_column)
        pct = len(resolved) / len(self.output_cols) * 100
        assert pct >= 85, (
            f"Only {len(resolved)}/{len(self.output_cols)} columns resolved ({pct:.0f}%). "
            f"Missing: {self.output_cols - resolved}"
        )

    def test_order_id_still_resolved(self):
        """ORDER_ID should still trace despite missing ITEM_COUNT."""
        m = _get_mapping(self.edges, "test_ds.summary_view", "ORDER_ID")
        assert m is not None
        assert m.transformation != "unknown"

    def test_missing_col_in_output(self):
        """ITEM_COUNT should appear in output (patched as STRING placeholder)."""
        found = False
        for edge in self.edges:
            m = _get_mapping([edge], edge.source_node, "ITEM_COUNT")
            if m is not None:
                found = True
                break
        assert found, "ITEM_COUNT should appear in lineage output"


class TestMissingColumnSimple:
    """Simpler test: single missing column in a straightforward query."""

    SQL = """
    SELECT a.id, a.name, a.ghost_col
    FROM ds.my_table a
    """

    def test_missing_col_does_not_abort_others(self):
        schemas = {
            "ds.my_table": {"id": "STRING", "name": "STRING"},
            "ds.my_view": {"id": "STRING", "name": "STRING", "ghost_col": "STRING"},
        }
        edges = parse_view_lineage("ds.my_view", self.SQL, schemas)
        resolved = set()
        for edge in edges:
            for m in edge.column_mappings:
                if m.transformation != "unknown":
                    resolved.add(m.target_column)
        assert "id" in resolved, "id should resolve despite ghost_col"
        assert "name" in resolved, "name should resolve despite ghost_col"
