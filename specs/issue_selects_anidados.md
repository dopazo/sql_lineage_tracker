# Issue: Lineage "unknown" en vistas con SELECTs anidados (derived tables)

**Estado:** 🟢 Resuelto  
**Severidad:** Alta — afecta a cualquier vista con subqueries anidadas  
**Componente:** `backend/src/lineage_tracker/parser.py`

---

## Descripción del problema

Cuando una vista contiene **subqueries anidadas** (derived tables sin nombre), el parser no logra trazar el lineage de ninguna columna. Todas quedan marcadas como `transformation: "unknown"` y `lineage_status: "unknown"`.

Esto se observó con la vista `ET_SUBQRY.FT_OX_DIST_CUAD`, que consume `ET_SUBQRY.FT_OX_TSL_HOLA` y otras tablas a través de dos niveles de subquery anidada.

## Ejemplo reproducible

### Estructura del SQL afectado

```sql
-- Nivel externo: SELECT * + campos calculados
SELECT *
  ,BOLETA_TRX_POSITIVA + BOLETA_TRX_NEGATIVA AS BOLETA_NETO
  ,TOTAL_PAGOS_OC - (BOLETA_TRX_POSITIVA_OC + ...) AS ORDEN_NETO_CALC_OC
  ,...
FROM (
  -- Nivel medio: columnas explícitas + window functions
  SELECT
    ORDEN, SG, SG_PADRE, ...
    ,SUM(BOLETA_TRX_POSITIVA) OVER(PARTITION BY ORDEN, SG_PADRE) BOLETA_TRX_POSITIVA
    ,SUM(BOLETA_TRX_NEGATIVA) OVER(PARTITION BY ORDEN, SG_PADRE) BOLETA_TRX_NEGATIVA
    ,...
  FROM (
    -- Nivel interno: JOINs reales contra tablas conocidas
    SELECT
      CUAD.ORDEN, CUAD.SG, ...
      ,COALESCE(DEVOL.MONTO_DEVOL_TBK, 0) MONTO_DEVOL_TBK
      ,...
    FROM ET_SUBQRY.FT_OX_TSL_HOLA CUAD
    LEFT JOIN ET_SCRATCH.DIST_PAGOS_RESULT PAGO ON CUAD.SG = PAGO.SG
    LEFT JOIN (...subquery agregada...) DEVOL ON CUAD.SG = DEVOL.SG
    LEFT JOIN ET_SCRATCH.DIST_RESTOS_RESULT RESTOS ON CUAD.ORDEN = RESTOS.ORDEN
  )
)
```

### Resultado esperado

El parser debería trazar, por ejemplo:
- `FT_OX_DIST_CUAD.ORDEN` → `FT_OX_TSL_HOLA.ORDEN` (transformation: `direct`)
- `FT_OX_DIST_CUAD.BOLETA_TRX_POSITIVA` → `FT_OX_TSL_HOLA.BOLETA_TRX_POSITIVA` (transformation: `aggregation`, expression: `SUM(BOLETA_TRX_POSITIVA) OVER(...)`)
- `FT_OX_DIST_CUAD.MONTO_DEVOL_TBK` → `FT_OX_TSL_HOLA.MONTO_DEVOL_TBK` + `DIST_DEVOL_RESULT.MONTO` (transformation: `expression`)

### Resultado actual

Todas las columnas de `FT_OX_DIST_CUAD` quedan con `lineage_status: "unknown"` y `transformation: "unknown"`. No se genera ningún edge con mappings válidos.

---

## Diagnóstico técnico

### Causa raíz

El parser (`parse_view_lineage`) depende de `sqlglot.lineage()` para trazar cada columna. Esta función necesita **schemas** de las tablas referenciadas para resolver las columnas a través de las capas del SQL. El problema es que las subqueries anidadas (derived tables) **no tienen un schema registrado** en el diccionario `schemas` que se le pasa a sqlglot, ya que solo contiene schemas de tablas/vistas reales extraídas de BigQuery.

### Cadena de fallos paso a paso

| Paso | Función | Qué ocurre | Resultado |
|------|---------|-------------|-----------|
| 1 | `_strip_project_prefix` | Elimina prefijos de proyecto correctamente | ✅ OK |
| 2 | `_expand_star` | Intenta expandir `SELECT *` del nivel externo usando `qualify_columns` con schemas conocidos | ❌ Falla silenciosamente porque el `*` referencia una subquery cuyo schema no está registrado. El SQL queda con `*` sin expandir |
| 3 | `lineage(col, sql, schema)` | Para cada columna de salida, intenta trazar hacia atrás | ❌ Lanza excepción o retorna árbol sin hojas (no puede atravesar 2 niveles de subquery sin schema intermedio) |
| 4 | `_collect_leaves_and_exprs` | Busca nodos hoja con `exp.Table` como fuente | ❌ `leaves = []` porque las hojas apuntan a la subquery, no a una tabla real |
| 5 | `_extract_table_id` | Intenta extraer `dataset.table` de la fuente | ❌ Retorna `None` si la fuente es una subquery |
| 6 | Fallback | Cae a `_add_unknown_mapping` o no genera mappings | ❌ `transformation = "unknown"` |
| 7 | `_mark_column_lineage_status` | Revisa si algún mapping tiene `transformation != "unknown"` | ❌ Ninguno lo tiene → todas las columnas = `"unknown"` |

### Factores agravantes

1. **`SELECT *` en el nivel externo:** Sin poder expandir el asterisco, sqlglot no sabe qué columnas trazar.
2. **Window functions en el nivel medio:** Redefinen columnas con el mismo nombre (`SUM(X) OVER(...) AS X`), lo que complica la resolución incluso si sqlglot pudiera entrar a la subquery.
3. **Subquery con JOINs y CASE en el nivel interno:** El nivel más interno tiene múltiples JOINs (incluyendo una subquery inline como DEVOL) con CASE WHEN complejos, lo que añade otra capa de indirección.
4. **Renombramientos con alias:** Columnas como `BOLETA_TRX_POSITIVA AS BOLETA_SG_HIJO` y `TOTAL_PAGOS AS PAGOS_SG_HIJO` en el nivel medio cambian nombres, confundiendo aún más al trazador.

### Archivos involucrados

- `backend/src/lineage_tracker/parser.py` — Lógica principal de parsing (`parse_view_lineage`, `_expand_star`, `_collect_leaves_and_exprs`)
- `backend/src/lineage_tracker/graph.py` — `_mark_column_lineage_status` que marca columnas como unknown
- `backend/src/lineage_tracker/scanner.py` — `extract_table_references` que sí detecta las tablas reales correctamente (no es parte del problema)

---

## Soluciones propuestas

### Opción A: Reescritura de subqueries como CTEs (recomendada)

**Idea:** Antes de llamar a `lineage()`, transformar el SQL reescribiendo cada subquery anidada (derived table) como un CTE con nombre sintético. Esto le da a sqlglot un nombre de "tabla" que puede resolver.

**Ejemplo de transformación:**

```sql
-- Original
SELECT * FROM (SELECT a, b FROM (SELECT x AS a, y AS b FROM real_table) sub1) sub2

-- Reescrito
WITH _derived_0 AS (SELECT x AS a, y AS b FROM real_table),
     _derived_1 AS (SELECT a, b FROM _derived_0)
SELECT * FROM _derived_1
```

**Pros:**
- sqlglot maneja CTEs mucho mejor que derived tables anónimas
- Se puede inferir el schema de cada CTE progresivamente
- La transformación es puramente sintáctica (no cambia la semántica)

**Contras:**
- Complejidad de la reescritura AST (hay que manejar subqueries en JOINs, WHERE, etc.)
- Riesgo de romper SQL edge cases

### Opción B: Inferencia de schema por capas

**Idea:** Parsear el SQL de adentro hacia afuera. Primero resolver el SELECT más interno (que sí referencia tablas reales), inferir qué columnas produce, registrar ese schema como una "tabla virtual", y luego usar ese schema para resolver la capa siguiente.

**Pros:**
- No modifica el SQL original
- Modelo mental claro (capas como una cebolla)

**Contras:**
- Requiere un parser que detecte y extraiga subqueries correctamente
- Más complejo si hay subqueries en JOINs (como la subquery DEVOL del ejemplo)
- Necesita manejar `SELECT *` en capas intermedias

### Opción C: Pre-cálculo de schema de subqueries para sqlglot

**Idea:** Antes de llamar a `lineage()`, recorrer el AST, encontrar todas las subqueries/derived tables, calcular su lista de columnas de salida, y registrarlas en el schema de sqlglot como tablas virtuales con nombres sintéticos.

**Pros:**
- Menor invasividad — solo enriquece el schema, no reescribe el SQL
- Aprovecha la capacidad existente de sqlglot con schemas

**Contras:**
- Calcular el schema de una subquery es en sí mismo un problema de resolución de columnas
- `SELECT *` dentro de subqueries requiere resolución recursiva

---

## Alcance del impacto

Este problema afecta a **cualquier vista** cuyo SQL contenga:
- Subqueries anidadas como fuente en `FROM (...)`
- `SELECT *` que referencia una subquery (no una tabla real)
- Múltiples niveles de anidación

Patrones comunes en BigQuery donde esto ocurre:
- Vistas de "cuadraturas" o reconciliaciones con transformaciones por capas
- Vistas que agregan window functions sobre un SELECT base
- Queries generados por herramientas de BI que anidan subqueries en lugar de usar CTEs

---

## Tareas

- [x] Escribir tests que reproduzcan el fallo con SQL de 2 y 3 niveles de anidación
- [x] Implementar la solución elegida (A, B o C)
- [x] Verificar que el lineage de `FT_OX_DIST_CUAD` se resuelve correctamente
- [x] Verificar que no hay regresiones en los tests existentes
- [x] Actualizar documentación del parser si cambia la arquitectura