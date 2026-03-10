"""Tests for rename/alias lineage parsing.

Views tested:
  - staging.orders_clean: SELECT col AS alias FROM table
  - staging.customers_clean: SELECT col AS alias, UPPER(col) AS alias
"""

import pytest


@pytest.mark.skip(reason="parser.py not implemented yet")
class TestOrdersClean:
    """staging.orders_clean: simple renames + direct pass-through."""

    def test_order_id_renamed_to_id_pedido(self, schemas, view_sql):
        # order_id -> id_pedido should be transformation: "rename"
        pass

    def test_customer_id_renamed_to_id_cliente(self, schemas, view_sql):
        # customer_id -> id_cliente should be transformation: "rename"
        pass

    def test_amount_renamed_to_revenue(self, schemas, view_sql):
        # amount -> revenue should be transformation: "rename"
        pass

    def test_created_at_direct(self, schemas, view_sql):
        # created_at -> created_at should be transformation: "direct"
        pass


@pytest.mark.skip(reason="parser.py not implemented yet")
class TestCustomersClean:
    """staging.customers_clean: renames + expression (UPPER)."""

    def test_customer_id_renamed_to_id_cliente(self, schemas, view_sql):
        pass

    def test_name_renamed_to_nombre(self, schemas, view_sql):
        pass

    def test_country_expression_upper(self, schemas, view_sql):
        # UPPER(country) AS pais should be transformation: "expression"
        # source_columns: ["country"], expression: "UPPER(country)"
        pass

    def test_registered_at_renamed_to_fecha_registro(self, schemas, view_sql):
        pass
