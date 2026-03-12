# BigQuery Test Schema (local)

Proyecto: `rational-world-470315-b4` (sandbox, sin billing)

## Dependency graph

```
╔══════════════════════════════════════════════════════════════════════════════════════════╗
║  CADENA A — Órdenes / Clientes                                                           ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                          ║
║  raw_data.orders ──────→ staging.orders_clean ──────┐                                    ║
║                                                     ├──→ staging.orders_with_customer ──┬──→ analytics.monthly_revenue ──┐ ║
║  raw_data.customers ──→ staging.customers_clean ────┘                                   └──→ analytics.customer_summary  │ ║
║                                                                                                  └──→ analytics.customer_ranking ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║  CADENA B — Productos / Transacciones (independiente de A)                               ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                          ║
║  raw_data.products ─────→ staging.products_clean ──────┐                                 ║
║                                                         ├──→ analytics.product_sales ──┐  ║
║  raw_data.transactions ─→ staging.transactions_clean ───┘                               │  ║
║                                                                                          │  ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║  UNION ALL (cruza cadenas A y B)                                                         ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                          ║
║  staging.orders_clean ──────┐                                                            ║
║                              ├──→ staging.all_revenue                                    ║
║  staging.transactions_clean ─┘                                                           ║
║                                                                                          ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║  CONVERGENCIA CROSS-CHAIN (reporting)                                                    ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                          ║
║  analytics.monthly_revenue ──┐                                                           ║
║                               ├──→ reporting.executive_dashboard                         ║
║  analytics.product_sales ────┘  (une cadenas A y B)                              ←──────┘ ║
║                                                                                          ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║  CADENA PROFUNDA — Eventos / Sesiones (4 niveles de raw → analytics)                    ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                          ║
║  raw_data.events ──→ staging.raw_sessions ──→ staging.sessions ──→ analytics.user_funnel ║
║                                                                                          ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║  MANUAL EDGES — Procesos sin lineage SQL automático                                     ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                          ║
║  analytics.customer_summary ──→ staging.script_crear_resumen ~~manual~~→ analytics.resumen_creado ║
║                                                                                          ║
║  staging.input_proceso_python ~~manual~~→ staging.output_proceso_python                  ║
║                                                                                          ║
╚══════════════════════════════════════════════════════════════════════════════════════════╝
```

**Total: 24 nodos, 5 datasets, hasta 4 niveles de profundidad + 2 manual edges.**

---

## Schemas

### raw_data.orders (table) — Cadena A

| Column | Type |
|---|---|
| order_id | STRING |
| customer_id | STRING |
| amount | FLOAT64 |
| status | STRING |
| created_at | TIMESTAMP |

### raw_data.customers (table) — Cadena A

| Column | Type |
|---|---|
| customer_id | STRING |
| name | STRING |
| email | STRING |
| country | STRING |
| registered_at | TIMESTAMP |

### staging.orders_clean (view) — Cadena A

```sql
SELECT
  order_id AS id_pedido,
  customer_id AS id_cliente,
  amount AS revenue,
  created_at
FROM `rational-world-470315-b4.raw_data.orders`
WHERE status = "completed"
```

| Column | Type |
|---|---|
| id_pedido | STRING |
| id_cliente | STRING |
| revenue | FLOAT64 |
| created_at | TIMESTAMP |

### staging.customers_clean (view) — Cadena A

```sql
SELECT
  customer_id AS id_cliente,
  name AS nombre,
  UPPER(country) AS pais,
  registered_at AS fecha_registro
FROM `rational-world-470315-b4.raw_data.customers`
```

| Column | Type |
|---|---|
| id_cliente | STRING |
| nombre | STRING |
| pais | STRING |
| fecha_registro | TIMESTAMP |

### staging.orders_with_customer (view) — Cadena A

```sql
SELECT
  o.id_pedido,
  o.revenue,
  o.created_at,
  c.nombre AS nombre_cliente,
  c.pais
FROM `rational-world-470315-b4.staging.orders_clean` o
JOIN `rational-world-470315-b4.staging.customers_clean` c
  ON o.id_cliente = c.id_cliente
```

| Column | Type |
|---|---|
| id_pedido | STRING |
| revenue | FLOAT64 |
| created_at | TIMESTAMP |
| nombre_cliente | STRING |
| pais | STRING |

### analytics.monthly_revenue (view) — Cadena A

```sql
SELECT
  FORMAT_TIMESTAMP("%Y-%m", created_at) AS month,
  pais,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS order_count
FROM `rational-world-470315-b4.staging.orders_with_customer`
GROUP BY month, pais
```

| Column | Type |
|---|---|
| month | STRING |
| pais | STRING |
| total_revenue | FLOAT64 |
| order_count | INT64 |

### analytics.customer_summary (view) — Cadena A

```sql
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
```

| Column | Type |
|---|---|
| nombre_cliente | STRING |
| pais | STRING |
| total_orders | INT64 |
| total_spent | FLOAT64 |
| avg_order_value | FLOAT64 |

### analytics.customer_ranking (view) — Cadena A (extiende customer_summary)

```sql
SELECT
  nombre_cliente,
  pais,
  total_spent,
  total_orders,
  ROW_NUMBER() OVER (PARTITION BY pais ORDER BY total_spent DESC) AS rank_in_country,
  RANK() OVER (ORDER BY total_spent DESC) AS global_rank,
  LAG(total_spent) OVER (PARTITION BY pais ORDER BY total_spent DESC) AS prev_customer_spent
FROM `rational-world-470315-b4.analytics.customer_summary`
```

| Column | Type |
|---|---|
| nombre_cliente | STRING |
| pais | STRING |
| total_spent | FLOAT64 |
| total_orders | INT64 |
| rank_in_country | INT64 |
| global_rank | INT64 |
| prev_customer_spent | FLOAT64 |

---

### raw_data.products (table) — Cadena B

| Column | Type |
|---|---|
| product_id | STRING |
| name | STRING |
| category | STRING |
| unit_price | FLOAT64 |
| active | BOOL |

### raw_data.transactions (table) — Cadena B

| Column | Type |
|---|---|
| transaction_id | STRING |
| product_id | STRING |
| quantity | INT64 |
| sold_at | TIMESTAMP |
| channel | STRING |

### staging.products_clean (view) — Cadena B

```sql
SELECT
  product_id,
  LOWER(name) AS product_name,
  UPPER(category) AS category,
  unit_price,
  CASE WHEN active THEN 'active' ELSE 'discontinued' END AS status
FROM `rational-world-470315-b4.raw_data.products`
WHERE unit_price > 0
```

| Column | Type |
|---|---|
| product_id | STRING |
| product_name | STRING |
| category | STRING |
| unit_price | FLOAT64 |
| status | STRING |

### staging.transactions_clean (view) — Cadena B

```sql
SELECT
  transaction_id,
  product_id,
  quantity,
  sold_at,
  COALESCE(channel, 'unknown') AS channel,
  IF(channel = 'online', TRUE, FALSE) AS is_online
FROM `rational-world-470315-b4.raw_data.transactions`
```

| Column | Type |
|---|---|
| transaction_id | STRING |
| product_id | STRING |
| quantity | INT64 |
| sold_at | TIMESTAMP |
| channel | STRING |
| is_online | BOOL |

### analytics.product_sales (view) — Cadena B

```sql
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
```

| Column | Type |
|---|---|
| product_id | STRING |
| product_name | STRING |
| category | STRING |
| unit_price | FLOAT64 |
| total_units_sold | INT64 |
| total_transactions | INT64 |
| channels_used | INT64 |
| last_sale_at | TIMESTAMP |

---

### staging.all_revenue (view) — UNION ALL (cruza A y B)

```sql
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
```

| Column | Type |
|---|---|
| event_id | STRING |
| amount | FLOAT64 |
| event_at | TIMESTAMP |
| event_type | STRING |

---

### reporting.executive_dashboard (view) — Convergencia cross-chain

```sql
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
```

| Column | Type |
|---|---|
| month | STRING |
| pais | STRING |
| total_revenue | FLOAT64 |
| order_count | INT64 |
| total_units_sold | INT64 |
| total_transactions | INT64 |
| revenue_per_unit | FLOAT64 |

---

### raw_data.events (table) — Cadena profunda

| Column | Type |
|---|---|
| event_id | STRING |
| user_id | STRING |
| event_type | STRING |
| event_at | TIMESTAMP |
| page | STRING |

### staging.raw_sessions (view) — Cadena profunda, nivel 1

```sql
SELECT
  user_id,
  page,
  MIN(event_at) AS session_start,
  MAX(event_at) AS session_end,
  COUNT(*) AS event_count
FROM `rational-world-470315-b4.raw_data.events`
GROUP BY user_id, page
```

| Column | Type |
|---|---|
| user_id | STRING |
| page | STRING |
| session_start | TIMESTAMP |
| session_end | TIMESTAMP |
| event_count | INT64 |

### staging.sessions (view) — Cadena profunda, nivel 2

```sql
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
```

| Column | Type |
|---|---|
| user_id | STRING |
| page | STRING |
| session_start | TIMESTAMP |
| session_end | TIMESTAMP |
| event_count | INT64 |
| duration_seconds | INT64 |

### analytics.user_funnel (view) — Cadena profunda, nivel 3

```sql
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
```

| Column | Type |
|---|---|
| user_id | STRING |
| pages_visited | INT64 |
| total_time_spent | INT64 |
| last_seen | TIMESTAMP |
| segment | STRING |

---

### staging.script_crear_resumen (view) — Manual edge: script → tabla

```sql
-- create or replace table `rational-world-470315-b4.analytics.resumen_creado`
SELECT
  nombre_cliente,
  pais,
  total_orders,
  total_spent
FROM `rational-world-470315-b4.analytics.customer_summary`
```

| Column | Type |
|---|---|
| nombre_cliente | STRING |
| pais | STRING |
| total_orders | INT64 |
| total_spent | FLOAT64 |

### analytics.resumen_creado (table) — Creada manualmente desde script_crear_resumen

| Column | Type |
|---|---|
| nombre_cliente | STRING |
| pais | STRING |
| total_orders | INT64 |
| total_spent | FLOAT64 |

### staging.input_proceso_python (table) — Input para proceso externo Python

| Column | Type |
|---|---|
| user_id | STRING |
| segment | STRING |
| pages_visited | INT64 |
| total_time_spent | INT64 |

### staging.output_proceso_python (table) — Output de proceso externo Python

| Column | Type |
|---|---|
| user_id | STRING |
| segment | STRING |
| score | FLOAT64 |
| recommendation | STRING |

---

## SQL patterns cubiertos

| Vista | Patrones SQL |
|---|---|
| `staging.orders_clean` | Rename (AS), filtro WHERE |
| `staging.customers_clean` | Rename, función escalar UPPER() |
| `staging.orders_with_customer` | INNER JOIN entre dos vistas |
| `analytics.monthly_revenue` | FORMAT_TIMESTAMP, SUM, COUNT, GROUP BY |
| `analytics.customer_summary` | CTE simple, agregación, ROUND, división aritmética |
| `analytics.customer_ranking` | **Window functions**: ROW_NUMBER, RANK, LAG con OVER (PARTITION BY … ORDER BY …) |
| `staging.products_clean` | **CASE WHEN** binario, LOWER(), filtro WHERE |
| `staging.transactions_clean` | **COALESCE**, **IF()**, columna booleana derivada |
| `analytics.product_sales` | **LEFT JOIN**, COUNT(DISTINCT), MAX, GROUP BY multi-columna |
| `staging.all_revenue` | **UNION ALL** (múltiples fuentes → misma vista), CAST |
| `staging.raw_sessions` | Agregación MIN/MAX/COUNT desde tabla raw |
| `staging.sessions` | **Subquery inline en FROM**, TIMESTAMP_DIFF |
| `analytics.user_funnel` | **Múltiples CTEs encadenadas**, CASE WHEN multi-rama, COUNT(DISTINCT), SUM, MAX |
| `reporting.executive_dashboard` | **Convergencia cross-chain** (une cadenas A y B), NULLIF, ROUND |
| `staging.script_crear_resumen` | **Comentario SQL** `-- create or replace table`, SELECT directo |
| `staging.input_proceso_python` / `staging.output_proceso_python` | **Proceso externo** (sin SQL), requiere manual edge |

## Casos de uso del lineage tracker cubiertos

| Caso de uso | Cómo se prueba |
|---|---|
| Filtro por tabla final (target) | Cadenas A y B son 100 % independientes hasta `reporting.executive_dashboard` |
| Límite de profundidad (`--depth`) | Cadena profunda tiene 4 niveles: `raw_data.events → staging.raw_sessions → staging.sessions → analytics.user_funnel` |
| Múltiples datasets en scan | 5 datasets: `raw_data`, `staging`, `analytics`, `reporting` |
| Patrón diamante en el grafo | `reporting.executive_dashboard` converge cadenas A y B |
| Nodo con múltiples fuentes | `staging.orders_with_customer`, `analytics.product_sales`, `staging.all_revenue`, `reporting.executive_dashboard` |
| Parser: columna con origen dual | `staging.all_revenue` (UNION ALL) |
| Parser: columna sin fuente directa | Window functions en `analytics.customer_ranking` |
| Parser: subquery como fuente | `staging.sessions` (FROM subquery inline) |
| Parser: CTEs encadenadas | `analytics.user_funnel` (dos CTEs que se referencian) |
| Manual edge: proceso externo | `staging.input_proceso_python` → `staging.output_proceso_python` (no hay SQL entre ellas) |
| Manual edge: script como vista | `staging.script_crear_resumen` → `analytics.resumen_creado` (vista con comentario CREATE TABLE) |