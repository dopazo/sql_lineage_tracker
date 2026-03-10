"""Tests for CTE lineage parsing.

View tested:
  - analytics.customer_summary: CTE with aggregation + arithmetic expression
"""

import pytest


@pytest.mark.skip(reason="parser.py not implemented yet")
class TestCustomerSummary:
    """analytics.customer_summary: CTE -> final SELECT with derived column."""

    def test_nombre_cliente_through_cte(self, schemas, view_sql):
        # nombre_cliente passes through CTE unchanged
        # source: staging.orders_with_customer.nombre_cliente
        # transformation: "direct"
        pass

    def test_pais_through_cte(self, schemas, view_sql):
        # pais passes through CTE unchanged
        pass

    def test_total_orders_aggregation(self, schemas, view_sql):
        # COUNT(*) AS total_orders in CTE
        # transformation: "aggregation"
        pass

    def test_total_spent_aggregation(self, schemas, view_sql):
        # SUM(revenue) AS total_spent in CTE
        # transformation: "aggregation"
        # source_columns: ["revenue"]
        pass

    def test_avg_order_value_expression(self, schemas, view_sql):
        # ROUND(total_spent / total_orders, 2) AS avg_order_value
        # transformation: "expression"
        # source_columns should trace back to ["revenue", "*"] or similar
        pass

    def test_source_is_orders_with_customer(self, schemas, view_sql):
        # All columns ultimately come from staging.orders_with_customer
        pass
