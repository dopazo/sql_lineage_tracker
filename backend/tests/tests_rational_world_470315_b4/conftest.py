"""Shared fixtures for rational-world-470315-b4 parsing tests.

These tests use real SQL from the test BigQuery project to verify
that the parser produces correct column-level lineage.
"""

import pytest

# Schemas as they exist in BigQuery (see bq_schema.md)
SCHEMAS = {
    # ── Cadena A (original) ────────────────────────────────────────────────────
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

    # ── Cadena B (productos / transacciones — independiente de A) ──────────────
    "raw_data.products": {
        "product_id": "STRING",
        "name": "STRING",
        "category": "STRING",
        "unit_price": "FLOAT64",
        "active": "BOOL",
    },
    "raw_data.transactions": {
        "transaction_id": "STRING",
        "product_id": "STRING",
        "quantity": "INT64",
        "sold_at": "TIMESTAMP",
        "channel": "STRING",
    },
    "staging.products_clean": {
        "product_id": "STRING",
        "product_name": "STRING",
        "category": "STRING",
        "unit_price": "FLOAT64",
        "status": "STRING",
    },
    "staging.transactions_clean": {
        "transaction_id": "STRING",
        "product_id": "STRING",
        "quantity": "INT64",
        "sold_at": "TIMESTAMP",
        "channel": "STRING",
        "is_online": "BOOL",
    },
    "analytics.product_sales": {
        "product_id": "STRING",
        "product_name": "STRING",
        "category": "STRING",
        "unit_price": "FLOAT64",
        "total_units_sold": "INT64",
        "total_transactions": "INT64",
        "channels_used": "INT64",
        "last_sale_at": "TIMESTAMP",
    },

    # ── UNION ALL (cruza cadenas A y B) ───────────────────────────────────────
    "staging.all_revenue": {
        "event_id": "STRING",
        "amount": "FLOAT64",
        "event_at": "TIMESTAMP",
        "event_type": "STRING",
    },

    # ── Window functions (extiende cadena A) ──────────────────────────────────
    "analytics.customer_ranking": {
        "nombre_cliente": "STRING",
        "pais": "STRING",
        "total_spent": "FLOAT64",
        "total_orders": "INT64",
        "rank_in_country": "INT64",
        "global_rank": "INT64",
        "prev_customer_spent": "FLOAT64",
    },

    # ── Cadena profunda: eventos → sesiones (4 niveles) ───────────────────────
    "raw_data.events": {
        "event_id": "STRING",
        "user_id": "STRING",
        "event_type": "STRING",
        "event_at": "TIMESTAMP",
        "page": "STRING",
    },
    "staging.raw_sessions": {
        "user_id": "STRING",
        "page": "STRING",
        "session_start": "TIMESTAMP",
        "session_end": "TIMESTAMP",
        "event_count": "INT64",
    },
    "staging.sessions": {
        "user_id": "STRING",
        "page": "STRING",
        "session_start": "TIMESTAMP",
        "session_end": "TIMESTAMP",
        "event_count": "INT64",
        "duration_seconds": "INT64",
    },
    "analytics.user_funnel": {
        "user_id": "STRING",
        "pages_visited": "INT64",
        "total_time_spent": "INT64",
        "last_seen": "TIMESTAMP",
        "segment": "STRING",
    },

    # ── Convergencia cross-chain A + B (reporting) ───────────────────────────
    "reporting.executive_dashboard": {
        "month": "STRING",
        "pais": "STRING",
        "total_revenue": "FLOAT64",
        "order_count": "INT64",
        "total_units_sold": "INT64",
        "total_transactions": "INT64",
        "revenue_per_unit": "FLOAT64",
    },
}

# View SQL definitions exactly as BigQuery stores them
VIEW_SQL = {
    # ── Cadena A (original) ────────────────────────────────────────────────────

    # Patrones: Rename (AS), filtro WHERE
    "staging.orders_clean": """SELECT
  order_id AS id_pedido,
  customer_id AS id_cliente,
  amount AS revenue,
  created_at
FROM `rational-world-470315-b4.raw_data.orders`
WHERE status = "completed\"""",

    # Patrones: Rename, función escalar UPPER()
    "staging.customers_clean": """SELECT
  customer_id AS id_cliente,
  name AS nombre,
  UPPER(country) AS pais,
  registered_at AS fecha_registro
FROM `rational-world-470315-b4.raw_data.customers`""",

    # Patrones: INNER JOIN entre dos vistas
    "staging.orders_with_customer": """SELECT
  o.id_pedido,
  o.revenue,
  o.created_at,
  c.nombre AS nombre_cliente,
  c.pais
FROM `rational-world-470315-b4.staging.orders_clean` o
JOIN `rational-world-470315-b4.staging.customers_clean` c
  ON o.id_cliente = c.id_cliente""",

    # Patrones: expresión FORMAT_TIMESTAMP, agregación SUM/COUNT, GROUP BY
    "analytics.monthly_revenue": """SELECT
  FORMAT_TIMESTAMP("%Y-%m", created_at) AS month,
  pais,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS order_count
FROM `rational-world-470315-b4.staging.orders_with_customer`
GROUP BY month, pais""",

    # Patrones: CTE simple, agregación, expresión aritmética ROUND/división
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

    # ── Cadena B (productos / transacciones — independiente de A) ──────────────

    # Patrones: CASE WHEN, LOWER(), UPPER(), filtro WHERE
    "staging.products_clean": """SELECT
  product_id,
  LOWER(name) AS product_name,
  UPPER(category) AS category,
  unit_price,
  CASE WHEN active THEN 'active' ELSE 'discontinued' END AS status
FROM `rational-world-470315-b4.raw_data.products`
WHERE unit_price > 0""",

    # Patrones: COALESCE, IF()
    "staging.transactions_clean": """SELECT
  transaction_id,
  product_id,
  quantity,
  sold_at,
  COALESCE(channel, 'unknown') AS channel,
  IF(channel = 'online', TRUE, FALSE) AS is_online
FROM `rational-world-470315-b4.raw_data.transactions`""",

    # Patrones: LEFT JOIN, COUNT(DISTINCT), MAX, GROUP BY multi-columna
    "analytics.product_sales": """SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.unit_price,
  SUM(t.quantity) AS total_units_sold,
  COUNT(DISTINCT t.transaction_id) AS total_transactions,
  COUNT(DISTINCT t.channel) AS channels_used,
  MAX(t.sold_at) AS last_sale_at
FROM `rational-world-470315-b4.staging.products_clean` p
LEFT JOIN `rational-world-470315-b4.staging.transactions_clean` t
  ON p.product_id = t.product_id
GROUP BY p.product_id, p.product_name, p.category, p.unit_price""",

    # ── UNION ALL (cruza cadenas A y B) ───────────────────────────────────────

    # Patrones: UNION ALL, CAST, literal de columna
    "staging.all_revenue": """SELECT
  id_pedido AS event_id,
  revenue AS amount,
  created_at AS event_at,
  'order' AS event_type
FROM `rational-world-470315-b4.staging.orders_clean`
UNION ALL
SELECT
  transaction_id AS event_id,
  CAST(quantity AS FLOAT64) AS amount,
  sold_at AS event_at,
  'transaction' AS event_type
FROM `rational-world-470315-b4.staging.transactions_clean`""",

    # ── Window functions (extiende cadena A) ──────────────────────────────────

    # Patrones: ROW_NUMBER, RANK, LAG con OVER (PARTITION BY ... ORDER BY ...)
    "analytics.customer_ranking": """SELECT
  nombre_cliente,
  pais,
  total_spent,
  total_orders,
  ROW_NUMBER() OVER (PARTITION BY pais ORDER BY total_spent DESC) AS rank_in_country,
  RANK() OVER (ORDER BY total_spent DESC) AS global_rank,
  LAG(total_spent) OVER (PARTITION BY pais ORDER BY total_spent DESC) AS prev_customer_spent
FROM `rational-world-470315-b4.analytics.customer_summary`""",

    # ── Cadena profunda: eventos → sesiones (4 niveles) ───────────────────────

    # Patrones: agregación MIN/MAX/COUNT desde tabla raw, GROUP BY
    "staging.raw_sessions": """SELECT
  user_id,
  page,
  MIN(event_at) AS session_start,
  MAX(event_at) AS session_end,
  COUNT(*) AS event_count
FROM `rational-world-470315-b4.raw_data.events`
GROUP BY user_id, page""",

    # Patrones: subquery inline en FROM, TIMESTAMP_DIFF
    "staging.sessions": """SELECT
  user_id,
  page,
  session_start,
  session_end,
  event_count,
  TIMESTAMP_DIFF(session_end, session_start, SECOND) AS duration_seconds
FROM (
  SELECT
    user_id,
    page,
    session_start,
    session_end,
    event_count
  FROM `rational-world-470315-b4.staging.raw_sessions`
  WHERE event_count > 1
) filtered_sessions""",

    # Patrones: múltiples CTEs encadenadas, CASE WHEN multi-rama, COUNT(DISTINCT), SUM, MAX
    "analytics.user_funnel": """WITH session_stats AS (
  SELECT
    user_id,
    COUNT(DISTINCT page) AS pages_visited,
    SUM(duration_seconds) AS total_time_spent,
    MAX(session_start) AS last_seen
  FROM `rational-world-470315-b4.staging.sessions`
  GROUP BY user_id
),
user_segments AS (
  SELECT
    user_id,
    pages_visited,
    total_time_spent,
    last_seen,
    CASE
      WHEN pages_visited >= 5 THEN 'power_user'
      WHEN pages_visited >= 2 THEN 'regular'
      ELSE 'casual'
    END AS segment
  FROM session_stats
)
SELECT
  user_id,
  pages_visited,
  total_time_spent,
  last_seen,
  segment
FROM user_segments""",

    # ── Convergencia cross-chain A + B (reporting) ───────────────────────────

    # Patrones: LEFT JOIN entre dos cadenas independientes, NULLIF, ROUND
    "reporting.executive_dashboard": """SELECT
  r.month,
  r.pais,
  r.total_revenue,
  r.order_count,
  p.total_units_sold,
  p.total_transactions,
  ROUND(r.total_revenue / NULLIF(p.total_units_sold, 0), 2) AS revenue_per_unit
FROM `rational-world-470315-b4.analytics.monthly_revenue` r
LEFT JOIN `rational-world-470315-b4.analytics.product_sales` p
  ON r.pais = p.category""",
}


@pytest.fixture
def schemas():
    return SCHEMAS


@pytest.fixture
def view_sql():
    return VIEW_SQL