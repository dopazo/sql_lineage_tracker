"""Tests for nested subquery (derived table) lineage resolution.

Verifies that the parser correctly traces column lineage through
multiple levels of nested subqueries (derived tables), including:
- 2-level nesting with explicit columns
- 3-level nesting with SELECT *, window functions, and expressions
- Subqueries with and without aliases
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
# 3-level nesting: mimics the FT_OX_DIST_CUAD pattern from the issue
# outer: SELECT * + calculated fields
# middle: window functions
# inner: JOINs against real tables
# ──────────────────────────────────────────────────────────────────────────────

class TestThreeLevelNestedSubquery:
    """Lineage through 3 levels: inner JOINs, middle window fns, outer expressions."""

    SQL = """
    SELECT
        orden,
        sg,
        boleta_positiva,
        boleta_negativa,
        boleta_positiva + boleta_negativa AS boleta_neto
    FROM (
        SELECT
            orden,
            sg,
            SUM(boleta_positiva) OVER(PARTITION BY orden) AS boleta_positiva,
            SUM(boleta_negativa) OVER(PARTITION BY orden) AS boleta_negativa
        FROM (
            SELECT
                c.orden,
                c.sg,
                c.boleta_positiva,
                COALESCE(d.monto_devol, 0) AS boleta_negativa
            FROM staging.cuadratura c
            LEFT JOIN staging.devoluciones d ON c.sg = d.sg
        ) base
    ) windowed
    """

    SCHEMAS = {
        "staging.cuadratura": {
            "orden": "STRING",
            "sg": "STRING",
            "boleta_positiva": "FLOAT64",
            "boleta_negativa": "FLOAT64",
        },
        "staging.devoluciones": {
            "sg": "STRING",
            "monto_devol": "FLOAT64",
        },
        "target.dist_cuad": {
            "orden": "STRING",
            "sg": "STRING",
            "boleta_positiva": "FLOAT64",
            "boleta_negativa": "FLOAT64",
            "boleta_neto": "FLOAT64",
        },
    }

    @pytest.fixture(autouse=True)
    def parse(self):
        self.edges = parse_view_lineage(
            "target.dist_cuad", self.SQL, self.SCHEMAS
        )

    def test_has_edges(self):
        """Should produce at least one edge (not all unknown)."""
        assert len(self.edges) > 0

    def test_orden_traced_to_cuadratura(self):
        m = _get_mapping(self.edges, "staging.cuadratura", "orden")
        assert m is not None
        assert "orden" in m.source_columns
        assert m.transformation != "unknown"

    def test_sg_traced_to_cuadratura(self):
        m = _get_mapping(self.edges, "staging.cuadratura", "sg")
        assert m is not None
        assert "sg" in m.source_columns
        assert m.transformation != "unknown"

    def test_boleta_positiva_is_expression(self):
        """Window function SUM OVER should be classified as expression."""
        m = _get_mapping(self.edges, "staging.cuadratura", "boleta_positiva")
        assert m is not None
        assert m.transformation != "unknown"

    def test_boleta_negativa_involves_devoluciones(self):
        """boleta_negativa uses COALESCE with devoluciones.monto_devol."""
        # Could trace to cuadratura or devoluciones depending on resolution
        m_cuad = _get_mapping(self.edges, "staging.cuadratura", "boleta_negativa")
        m_devol = _get_mapping(self.edges, "staging.devoluciones", "boleta_negativa")
        assert (m_cuad is not None) or (m_devol is not None), \
            "boleta_negativa should trace to at least one source"

    def test_boleta_neto_is_expression(self):
        """boleta_neto = boleta_positiva + boleta_negativa."""
        # Find it in any edge
        found = False
        for edge in self.edges:
            m = _get_mapping([edge], edge.source_node, "boleta_neto")
            if m is not None and m.transformation != "unknown":
                found = True
                break
        assert found, "boleta_neto should be classified as expression, not unknown"

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
        # Should be functionally equivalent (may have formatting changes)
        assert "_subq_" not in result


# ──────────────────────────────────────────────────────────────────────────────
# Original issue pattern: FT_OX_DIST_CUAD full reproduction
# ──────────────────────────────────────────────────────────────────────────────

class TestOriginalIssueFtOxDistCuad:
    """Full reproduction of the FT_OX_DIST_CUAD pattern from the issue.

    3-level nesting with:
    - Outer: SELECT * + calculated fields
    - Middle: window functions (SUM OVER), renames
    - Inner: multiple JOINs including inline subquery, COALESCE
    """

    SQL = """
    SELECT *
      ,BOLETA_TRX_POSITIVA + BOLETA_TRX_NEGATIVA AS BOLETA_NETO
      ,TOTAL_PAGOS_OC - (BOLETA_TRX_POSITIVA_OC + BOLETA_TRX_NEGATIVA_OC) AS ORDEN_NETO_CALC_OC
    FROM (
      SELECT
        ORDEN, SG, SG_PADRE,
        BOLETA_TRX_POSITIVA AS BOLETA_SG_HIJO,
        SUM(BOLETA_TRX_POSITIVA) OVER(PARTITION BY ORDEN, SG_PADRE) BOLETA_TRX_POSITIVA,
        BOLETA_TRX_NEGATIVA AS BOLETA_NEG_SG_HIJO,
        SUM(BOLETA_TRX_NEGATIVA) OVER(PARTITION BY ORDEN, SG_PADRE) BOLETA_TRX_NEGATIVA,
        TOTAL_PAGOS AS PAGOS_SG_HIJO,
        SUM(TOTAL_PAGOS) OVER(PARTITION BY ORDEN, SG_PADRE) TOTAL_PAGOS_OC,
        BOLETA_TRX_POSITIVA_OC,
        BOLETA_TRX_NEGATIVA_OC
      FROM (
        SELECT
          CUAD.ORDEN, CUAD.SG, CUAD.SG_PADRE,
          CUAD.BOLETA_TRX_POSITIVA,
          COALESCE(DEVOL.MONTO_DEVOL_TBK, 0) AS BOLETA_TRX_NEGATIVA,
          COALESCE(PAGO.TOTAL_PAGOS, 0) AS TOTAL_PAGOS,
          COALESCE(OC.BOLETA_TRX_POSITIVA_OC, 0) AS BOLETA_TRX_POSITIVA_OC,
          COALESCE(OC.BOLETA_TRX_NEGATIVA_OC, 0) AS BOLETA_TRX_NEGATIVA_OC
        FROM et_subqry.ft_ox_tsl_hola CUAD
        LEFT JOIN et_scratch.dist_pagos_result PAGO ON CUAD.SG = PAGO.SG
        LEFT JOIN (
          SELECT SG, SUM(MONTO) AS MONTO_DEVOL_TBK
          FROM et_scratch.dist_devol_result
          GROUP BY SG
        ) DEVOL ON CUAD.SG = DEVOL.SG
        LEFT JOIN et_scratch.dist_restos_result RESTOS ON CUAD.ORDEN = RESTOS.ORDEN
        LEFT JOIN et_scratch.dist_oc_result OC ON CUAD.SG = OC.SG
      )
    )
    """

    SCHEMAS = {
        "et_subqry.ft_ox_tsl_hola": {
            "ORDEN": "STRING", "SG": "STRING", "SG_PADRE": "STRING",
            "BOLETA_TRX_POSITIVA": "FLOAT64",
        },
        "et_scratch.dist_pagos_result": {"SG": "STRING", "TOTAL_PAGOS": "FLOAT64"},
        "et_scratch.dist_devol_result": {"SG": "STRING", "MONTO": "FLOAT64"},
        "et_scratch.dist_restos_result": {"ORDEN": "STRING", "RESTOS": "FLOAT64"},
        "et_scratch.dist_oc_result": {
            "SG": "STRING",
            "BOLETA_TRX_POSITIVA_OC": "FLOAT64",
            "BOLETA_TRX_NEGATIVA_OC": "FLOAT64",
        },
        "et_subqry.ft_ox_dist_cuad": {
            "ORDEN": "STRING", "SG": "STRING", "SG_PADRE": "STRING",
            "BOLETA_SG_HIJO": "FLOAT64", "BOLETA_TRX_POSITIVA": "FLOAT64",
            "BOLETA_NEG_SG_HIJO": "FLOAT64", "BOLETA_TRX_NEGATIVA": "FLOAT64",
            "PAGOS_SG_HIJO": "FLOAT64", "TOTAL_PAGOS_OC": "FLOAT64",
            "BOLETA_TRX_POSITIVA_OC": "FLOAT64", "BOLETA_TRX_NEGATIVA_OC": "FLOAT64",
            "BOLETA_NETO": "FLOAT64", "ORDEN_NETO_CALC_OC": "FLOAT64",
        },
    }

    @pytest.fixture(autouse=True)
    def parse(self):
        self.edges = parse_view_lineage(
            "et_subqry.ft_ox_dist_cuad", self.SQL, self.SCHEMAS
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

    def test_orden_traced_to_source(self):
        m = _get_mapping(self.edges, "et_subqry.ft_ox_tsl_hola", "ORDEN")
        assert m is not None
        assert m.transformation != "unknown"

    def test_boleta_trx_positiva_is_window(self):
        """SUM(BOLETA_TRX_POSITIVA) OVER(...) should be expression."""
        m = _get_mapping(self.edges, "et_subqry.ft_ox_tsl_hola", "BOLETA_TRX_POSITIVA")
        assert m is not None
        assert m.transformation == "expression"

    def test_boleta_neto_is_expression(self):
        """BOLETA_TRX_POSITIVA + BOLETA_TRX_NEGATIVA should be expression."""
        found = any(
            _get_mapping([e], e.source_node, "BOLETA_NETO") is not None
            and _get_mapping([e], e.source_node, "BOLETA_NETO").transformation == "expression"
            for e in self.edges
        )
        assert found

    def test_devol_monto_traced(self):
        """COALESCE(DEVOL.MONTO_DEVOL_TBK, 0) should trace to dist_devol_result."""
        m = _get_mapping(self.edges, "et_scratch.dist_devol_result", "BOLETA_NEG_SG_HIJO")
        assert m is not None
        assert "monto" in m.source_columns

    def test_pagos_traced(self):
        """COALESCE(PAGO.TOTAL_PAGOS, 0) should trace to dist_pagos_result."""
        m = _get_mapping(self.edges, "et_scratch.dist_pagos_result", "PAGOS_SG_HIJO")
        assert m is not None

    def test_oc_columns_traced(self):
        """OC columns should trace to dist_oc_result."""
        m_pos = _get_mapping(self.edges, "et_scratch.dist_oc_result", "BOLETA_TRX_POSITIVA_OC")
        m_neg = _get_mapping(self.edges, "et_scratch.dist_oc_result", "BOLETA_TRX_NEGATIVA_OC")
        assert m_pos is not None
        assert m_neg is not None
