"""Tests for JOIN lineage parsing.

View tested:
  - staging.orders_with_customer: JOIN between orders_clean and customers_clean
"""

import pytest


@pytest.mark.skip(reason="parser.py not implemented yet")
class TestOrdersWithCustomer:
    """staging.orders_with_customer: columns from two joined views."""

    def test_id_pedido_from_orders_clean(self, schemas, view_sql):
        # o.id_pedido -> id_pedido, source: staging.orders_clean
        # transformation: "direct"
        pass

    def test_revenue_from_orders_clean(self, schemas, view_sql):
        # o.revenue -> revenue, source: staging.orders_clean
        pass

    def test_created_at_from_orders_clean(self, schemas, view_sql):
        # o.created_at -> created_at, source: staging.orders_clean
        pass

    def test_nombre_as_nombre_cliente(self, schemas, view_sql):
        # c.nombre AS nombre_cliente, source: staging.customers_clean
        # transformation: "rename"
        pass

    def test_pais_from_customers_clean(self, schemas, view_sql):
        # c.pais -> pais, source: staging.customers_clean
        # transformation: "direct"
        pass

    def test_edge_from_orders_clean_exists(self, schemas, view_sql):
        # Should produce an edge from staging.orders_clean -> staging.orders_with_customer
        pass

    def test_edge_from_customers_clean_exists(self, schemas, view_sql):
        # Should produce an edge from staging.customers_clean -> staging.orders_with_customer
        pass
