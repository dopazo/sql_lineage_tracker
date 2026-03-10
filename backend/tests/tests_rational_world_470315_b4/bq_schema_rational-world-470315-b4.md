# BigQuery Test Schema (local)

Proyecto: `rational-world-470315-b4` (sandbox, sin billing)

## Dependency graph

```
raw_data.orders ──────→ staging.orders_clean ──────┐
                                                   ├──→ staging.orders_with_customer ──┬──→ analytics.monthly_revenue
raw_data.customers ──→ staging.customers_clean ────┘                                  └──→ analytics.customer_summary
```

7 nodos, 3 datasets, 3 niveles de profundidad.

## Schemas

### raw_data.orders (table)

| Column | Type |
|---|---|
| order_id | STRING |
| customer_id | STRING |
| amount | FLOAT64 |
| status | STRING |
| created_at | TIMESTAMP |

### raw_data.customers (table)

| Column | Type |
|---|---|
| customer_id | STRING |
| name | STRING |
| email | STRING |
| country | STRING |
| registered_at | TIMESTAMP |

### staging.orders_clean (view)

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

### staging.customers_clean (view)

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

### staging.orders_with_customer (view)

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

### analytics.monthly_revenue (view)

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

### analytics.customer_summary (view)

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

## SQL patterns cubiertos

| Vista | Patrones |
|---|---|
| staging.orders_clean | Rename (AS), filtro (WHERE) |
| staging.customers_clean | Rename, expresión (UPPER()) |
| staging.orders_with_customer | JOIN entre dos vistas |
| analytics.monthly_revenue | Expresión (FORMAT_TIMESTAMP), agregación (SUM, COUNT) |
| analytics.customer_summary | CTE, agregación, expresión aritmética (ROUND, /) |
