"""
Script to create all BigQuery test objects for the rational-world-470315-b4 project.

Usage (from backend/ directory):
    uv run python tests/tests_rational_world_470315_b4/create_bq_test_schema.py

Requirements:
    - gcloud auth application-default login  (or GOOGLE_APPLICATION_CREDENTIALS set)
    - google-cloud-bigquery (already in pyproject.toml dependencies)
"""

import sys
from google.cloud import bigquery
from google.cloud.exceptions import Conflict

PROJECT = "rational-world-470315-b4"

client = bigquery.Client(project=PROJECT)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def create_dataset(dataset_id: str) -> None:
    ref = bigquery.Dataset(f"{PROJECT}.{dataset_id}")
    ref.location = "US"
    try:
        client.create_dataset(ref)
        print(f"  [+] dataset creado:  {dataset_id}")
    except Conflict:
        print(f"  [=] dataset existe:  {dataset_id}")


def create_table(dataset_id: str, table_id: str, schema: list[bigquery.SchemaField]) -> None:
    ref = f"{PROJECT}.{dataset_id}.{table_id}"
    table = bigquery.Table(ref, schema=schema)
    try:
        client.create_table(table)
        print(f"  [+] tabla creada:    {dataset_id}.{table_id}")
    except Conflict:
        print(f"  [=] tabla existe:    {dataset_id}.{table_id}")


def create_view(dataset_id: str, view_id: str, sql: str) -> None:
    ref = f"{PROJECT}.{dataset_id}.{view_id}"
    table = bigquery.Table(ref)
    table.view_query = sql
    table.view_use_legacy_sql = False
    try:
        client.create_table(table)
        print(f"  [+] vista creada:    {dataset_id}.{view_id}")
    except Conflict:
        # Vista ya existe — actualizarla con el SQL más reciente
        existing = client.get_table(ref)
        existing.view_query = sql
        client.update_table(existing, ["view_query"])
        print(f"  [~] vista actualiz:  {dataset_id}.{view_id}")


# ──────────────────────────────────────────────────────────────────────────────
# 1. Datasets
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Datasets ─────────────────────────────────────────────────────────────")
for ds in ("raw_data", "staging", "analytics", "reporting"):
    create_dataset(ds)


# ──────────────────────────────────────────────────────────────────────────────
# 2. Tablas raw (Cadena A — originales)
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Tablas raw (Cadena A) ────────────────────────────────────────────────")

create_table("raw_data", "orders", [
    bigquery.SchemaField("order_id",    "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("amount",      "FLOAT64"),
    bigquery.SchemaField("status",      "STRING"),
    bigquery.SchemaField("created_at",  "TIMESTAMP"),
])

create_table("raw_data", "customers", [
    bigquery.SchemaField("customer_id",   "STRING"),
    bigquery.SchemaField("name",          "STRING"),
    bigquery.SchemaField("email",         "STRING"),
    bigquery.SchemaField("country",       "STRING"),
    bigquery.SchemaField("registered_at", "TIMESTAMP"),
])


# ──────────────────────────────────────────────────────────────────────────────
# 3. Tablas raw (Cadena B — nuevas)
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Tablas raw (Cadena B) ────────────────────────────────────────────────")

create_table("raw_data", "products", [
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("name",       "STRING"),
    bigquery.SchemaField("category",   "STRING"),
    bigquery.SchemaField("unit_price", "FLOAT64"),
    bigquery.SchemaField("active",     "BOOL"),
])

create_table("raw_data", "transactions", [
    bigquery.SchemaField("transaction_id", "STRING"),
    bigquery.SchemaField("product_id",     "STRING"),
    bigquery.SchemaField("quantity",       "INT64"),
    bigquery.SchemaField("sold_at",        "TIMESTAMP"),
    bigquery.SchemaField("channel",        "STRING"),
])


# ──────────────────────────────────────────────────────────────────────────────
# 4. Tabla raw (Cadena profunda — eventos)
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Tablas raw (Cadena profunda) ─────────────────────────────────────────")

create_table("raw_data", "events", [
    bigquery.SchemaField("event_id",   "STRING"),
    bigquery.SchemaField("user_id",    "STRING"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("event_at",   "TIMESTAMP"),
    bigquery.SchemaField("page",       "STRING"),
])


# ──────────────────────────────────────────────────────────────────────────────
# 5. Vistas staging (Cadena A — originales)
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Vistas staging (Cadena A) ────────────────────────────────────────────")

create_view("staging", "orders_clean", """\
SELECT
  order_id AS id_pedido,
  customer_id AS id_cliente,
  amount AS revenue,
  created_at
FROM `rational-world-470315-b4.raw_data.orders`
WHERE status = "completed"
""")

create_view("staging", "customers_clean", """\
SELECT
  customer_id AS id_cliente,
  name AS nombre,
  UPPER(country) AS pais,
  registered_at AS fecha_registro
FROM `rational-world-470315-b4.raw_data.customers`
""")

create_view("staging", "orders_with_customer", """\
SELECT
  o.id_pedido,
  o.revenue,
  o.created_at,
  c.nombre AS nombre_cliente,
  c.pais
FROM `rational-world-470315-b4.staging.orders_clean` o
JOIN `rational-world-470315-b4.staging.customers_clean` c
  ON o.id_cliente = c.id_cliente
""")


# ──────────────────────────────────────────────────────────────────────────────
# 6. Vistas analytics (Cadena A — originales)
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Vistas analytics (Cadena A) ──────────────────────────────────────────")

create_view("analytics", "monthly_revenue", """\
SELECT
  FORMAT_TIMESTAMP("%Y-%m", created_at) AS month,
  pais,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS order_count
FROM `rational-world-470315-b4.staging.orders_with_customer`
GROUP BY month, pais
""")

create_view("analytics", "customer_summary", """\
WITH order_stats AS (
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
FROM order_stats
""")


# ──────────────────────────────────────────────────────────────────────────────
# 7. Vistas staging (Cadena B — nuevas)
#    Patrón: CASE WHEN, LOWER/UPPER, COALESCE, IF()
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Vistas staging (Cadena B) ────────────────────────────────────────────")

create_view("staging", "products_clean", """\
SELECT
  product_id,
  LOWER(name) AS product_name,
  UPPER(category) AS category,
  unit_price,
  CASE WHEN active THEN 'active' ELSE 'discontinued' END AS status
FROM `rational-world-470315-b4.raw_data.products`
WHERE unit_price > 0
""")

create_view("staging", "transactions_clean", """\
SELECT
  transaction_id,
  product_id,
  quantity,
  sold_at,
  COALESCE(channel, 'unknown') AS channel,
  IF(channel = 'online', TRUE, FALSE) AS is_online
FROM `rational-world-470315-b4.raw_data.transactions`
""")


# ──────────────────────────────────────────────────────────────────────────────
# 8. Vista analytics (Cadena B)
#    Patrón: LEFT JOIN, COUNT(DISTINCT), MAX, GROUP BY multi-columna
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Vistas analytics (Cadena B) ──────────────────────────────────────────")

create_view("analytics", "product_sales", """\
SELECT
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
GROUP BY p.product_id, p.product_name, p.category, p.unit_price
""")


# ──────────────────────────────────────────────────────────────────────────────
# 9. Vista staging UNION ALL (cruza Cadenas A y B)
#    Patrón: UNION ALL, CAST, literal de columna
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Vistas staging (UNION ALL) ───────────────────────────────────────────")

create_view("staging", "all_revenue", """\
SELECT
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
FROM `rational-world-470315-b4.staging.transactions_clean`
""")


# ──────────────────────────────────────────────────────────────────────────────
# 10. Vista analytics window functions (extiende Cadena A)
#     Patrón: ROW_NUMBER, RANK, LAG con OVER (PARTITION BY ... ORDER BY ...)
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Vistas analytics (window functions) ──────────────────────────────────")

create_view("analytics", "customer_ranking", """\
SELECT
  nombre_cliente,
  pais,
  total_spent,
  total_orders,
  ROW_NUMBER() OVER (PARTITION BY pais ORDER BY total_spent DESC) AS rank_in_country,
  RANK() OVER (ORDER BY total_spent DESC) AS global_rank,
  LAG(total_spent) OVER (PARTITION BY pais ORDER BY total_spent DESC) AS prev_customer_spent
FROM `rational-world-470315-b4.analytics.customer_summary`
""")


# ──────────────────────────────────────────────────────────────────────────────
# 11. Cadena profunda: raw_data.events → 3 capas (niveles 1-3)
#     Patrón: GROUP BY desde raw, subquery inline en FROM, TIMESTAMP_DIFF,
#             múltiples CTEs encadenadas, CASE WHEN multi-rama
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Vistas (cadena profunda: events) ─────────────────────────────────────")

create_view("staging", "raw_sessions", """\
SELECT
  user_id,
  page,
  MIN(event_at) AS session_start,
  MAX(event_at) AS session_end,
  COUNT(*) AS event_count
FROM `rational-world-470315-b4.raw_data.events`
GROUP BY user_id, page
""")

create_view("staging", "sessions", """\
SELECT
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
) filtered_sessions
""")

create_view("analytics", "user_funnel", """\
WITH session_stats AS (
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
FROM user_segments
""")


# ──────────────────────────────────────────────────────────────────────────────
# 12. Vista reporting (convergencia cross-chain A + B)
#     Patrón: LEFT JOIN entre dos cadenas independientes, NULLIF, ROUND
# ──────────────────────────────────────────────────────────────────────────────

print("\n── Vistas reporting (convergencia A + B) ────────────────────────────────")

create_view("reporting", "executive_dashboard", """\
SELECT
  r.month,
  r.pais,
  r.total_revenue,
  r.order_count,
  p.total_units_sold,
  p.total_transactions,
  ROUND(r.total_revenue / NULLIF(p.total_units_sold, 0), 2) AS revenue_per_unit
FROM `rational-world-470315-b4.analytics.monthly_revenue` r
LEFT JOIN `rational-world-470315-b4.analytics.product_sales` p
  ON r.pais = p.category
""")


# ──────────────────────────────────────────────────────────────────────────────
# Resumen final
# ──────────────────────────────────────────────────────────────────────────────

print("""
── Listo ────────────────────────────────────────────────────────────────────

Grafo resultante:

  raw_data.orders ──────→ staging.orders_clean ──────────────────────────────────────────────┐
                               │                                                              ├──→ staging.orders_with_customer ──→ analytics.monthly_revenue ──┐
  raw_data.customers ──→ staging.customers_clean ────────────────────────────────────────────┘      └──→ analytics.customer_summary                            │
                                                                                                            └──→ analytics.customer_ranking                    │
                                                                                                                                                                ├──→ reporting.executive_dashboard
  raw_data.products ──→ staging.products_clean ──────────────────────────────────────────────┐                                                                 │
                                                                                              ├──→ analytics.product_sales ──────────────────────────────────────┘
  raw_data.transactions ──→ staging.transactions_clean ──────────────────────────────────────┘
                                      │
            staging.orders_clean ─────┤
                                      └──→ staging.all_revenue (UNION ALL)

  raw_data.events ──→ staging.raw_sessions ──→ staging.sessions ──→ analytics.user_funnel

Total: 19 nodos, 5 datasets, hasta 4 niveles de profundidad
""")