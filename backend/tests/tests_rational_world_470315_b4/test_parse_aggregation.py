"""Tests for aggregation lineage parsing.

View tested:
  - analytics.monthly_revenue: FORMAT_TIMESTAMP, SUM, COUNT, GROUP BY
"""

import pytest


@pytest.mark.skip(reason="parser.py not implemented yet")
class TestMonthlyRevenue:
    """analytics.monthly_revenue: expressions + aggregations."""

    def test_month_expression(self, schemas, view_sql):
        # FORMAT_TIMESTAMP("%Y-%m", created_at) AS month
        # transformation: "expression"
        # source_columns: ["created_at"]
        # expression: 'FORMAT_TIMESTAMP("%Y-%m", created_at)'
        pass

    def test_pais_direct(self, schemas, view_sql):
        # pais -> pais, transformation: "direct"
        pass

    def test_total_revenue_aggregation(self, schemas, view_sql):
        # SUM(revenue) AS total_revenue
        # transformation: "aggregation"
        # source_columns: ["revenue"]
        # expression: "SUM(revenue)"
        pass

    def test_order_count_aggregation(self, schemas, view_sql):
        # COUNT(*) AS order_count
        # transformation: "aggregation"
        # source_columns: ["*"]
        # expression: "COUNT(*)"
        pass
