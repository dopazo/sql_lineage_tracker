"""Shared fixtures for rational-world-470315-b4 parsing tests.

These tests use real SQL from the test BigQuery project to verify
that the parser produces correct column-level lineage.
"""

import pytest

# Schemas as they exist in BigQuery (see bq_schema.md)
SCHEMAS = {
    "raw_data.orders": {
        "order_id": "STRING",
        "customer_id": "STRING",
        "amount": "FLOAT64",
        "status": "STRING",
        "created_at": "TIMESTAMP",
    },
    "raw_data.customers": {
        "customer_id": "STRING",
        "name": "STRING",
        "email": "STRING",
        "country": "STRING",
        "registered_at": "TIMESTAMP",
    },
    "staging.orders_clean": {
        "id_pedido": "STRING",
        "id_cliente": "STRING",
        "revenue": "FLOAT64",
        "created_at": "TIMESTAMP",
    },
    "staging.customers_clean": {
        "id_cliente": "STRING",
        "nombre": "STRING",
        "pais": "STRING",
        "fecha_registro": "TIMESTAMP",
    },
    "staging.orders_with_customer": {
        "id_pedido": "STRING",
        "revenue": "FLOAT64",
        "created_at": "TIMESTAMP",
        "nombre_cliente": "STRING",
        "pais": "STRING",
    },
    "analytics.monthly_revenue": {
        "month": "STRING",
        "pais": "STRING",
        "total_revenue": "FLOAT64",
        "order_count": "INT64",
    },
    "analytics.customer_summary": {
        "nombre_cliente": "STRING",
        "pais": "STRING",
        "total_orders": "INT64",
        "total_spent": "FLOAT64",
        "avg_order_value": "FLOAT64",
    },
}

# View SQL definitions exactly as BigQuery stores them
VIEW_SQL = {
    "staging.orders_clean": """SELECT
  order_id AS id_pedido,
  customer_id AS id_cliente,
  amount AS revenue,
  created_at
FROM `rational-world-470315-b4.raw_data.orders`
WHERE status = "completed\"""",
    "staging.customers_clean": """SELECT
  customer_id AS id_cliente,
  name AS nombre,
  UPPER(country) AS pais,
  registered_at AS fecha_registro
FROM `rational-world-470315-b4.raw_data.customers`""",
    "staging.orders_with_customer": """SELECT
  o.id_pedido,
  o.revenue,
  o.created_at,
  c.nombre AS nombre_cliente,
  c.pais
FROM `rational-world-470315-b4.staging.orders_clean` o
JOIN `rational-world-470315-b4.staging.customers_clean` c
  ON o.id_cliente = c.id_cliente""",
    "analytics.monthly_revenue": """SELECT
  FORMAT_TIMESTAMP("%Y-%m", created_at) AS month,
  pais,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS order_count
FROM `rational-world-470315-b4.staging.orders_with_customer`
GROUP BY month, pais""",
    "analytics.customer_summary": """WITH order_stats AS (
  SELECT
    nombre_cliente,
    pais,
    COUNT(*) AS total_orders,
    SUM(revenue) AS total_spent
  FROM `rational-world-470315-b4.staging.orders_with_customer`
  GROUP BY nombre_cliente, pais
)
SELECT
  nombre_cliente,
  pais,
  total_orders,
  total_spent,
  ROUND(total_spent / total_orders, 2) AS avg_order_value
FROM order_stats""",
}


@pytest.fixture
def schemas():
    return SCHEMAS


@pytest.fixture
def view_sql():
    return VIEW_SQL
