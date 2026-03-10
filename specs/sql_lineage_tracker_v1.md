# BigQuery Column-Level SQL Lineage Tracker — Especificacion Tecnica v1

**Estado:** Revisado
**Basado en:** `specs/sql_lineage_tracker_v0.md`
**Fecha:** 2026-03-09

---

## 1. Objetivo

Construir una herramienta CLI que se conecta a un proyecto de Google BigQuery, extrae SQL de vistas y tablas, parsea el linaje de datos a nivel de columna con sqlglot, y levanta un servidor local con una interfaz web interactiva para navegar, filtrar y editar el DAG de linaje.

```bash
# Escaneo acotado (caso principal):
uv run sql-lineage-tracker serve --project mi-proyecto --target analytics.vista_final --dataset staging --dataset raw_data

# Escaneo completo del proyecto:
uv run sql-lineage-tracker serve --project mi-proyecto

# Sin flags de escaneo: abre pantalla de configuracion en el frontend
uv run sql-lineage-tracker serve --project mi-proyecto --no-scan
```

El caso de uso principal es: dado una tabla o vista final, rastrear el linaje completo de todos sus campos hacia atras a traves de la cadena de transformaciones. El usuario limita el escaneo lo mas posible para evitar leer datasets innecesarios.

---

## 2. Contexto

### 2.1 Problema

En proyectos de BigQuery con cadenas largas de vistas, los nombres de columnas cambian, se combinan y se transforman. No existe forma nativa de rastrear la historia completa de un campo a traves de toda la cadena. El problema tiene dos dimensiones:

- **Parsing del linaje a nivel de columna** a traves de transformaciones SQL complejas.
- **Inventario acotado** de las fuentes SQL relevantes para asegurar que no haya huecos en el grafo.

### 2.2 Alternativas evaluadas

| Herramienta | Linaje de columna | Limitacion principal |
|---|---|---|
| dbt | No (solo nivel de modelo) | No muestra transformaciones por campo |
| DataHub / OpenMetadata | Si, parcial | UX de filtrado/navegacion insuficiente; infraestructura pesada |
| Dataplex lineage (GCP) | Si | Interfaz limitada, costo adicional |
| sqllineage (Python) | Si | Parser debil con SQL complejo de BigQuery |

**Decision:** construir con **sqlglot** (parsing SQL robusto con soporte BigQuery) + **React Flow** (visualizacion interactiva de grafos).

### 2.3 Observaciones sobre el documento v0

El v0 es una buena definicion de producto, pero se refinan varios aspectos:

**Se mantiene:**
- sqlglot como parser SQL
- React Flow para visualizacion del DAG
- Estructura del JSON del grafo (seccion 6 del v0)
- Concepto de uniones manuales para procesos externos
- Tipos de transformacion definidos

**Se cambia:**

1. **Modelo de ejecucion:** El v0 presenta tres opciones de integracion Python<>React (file upload, API, filesystem directo). Se reemplaza por un **modelo unificado**: el comando `uv run sql-lineage-tracker serve` levanta un servidor FastAPI que sirve la API y el frontend compilado. No hay file upload como modo principal — el flujo integrado es el estandar.

2. **FastAPI no es opcional:** En el v0 aparece como "Fase 3". Aqui es parte del MVP porque el servidor es el mecanismo de entrega del frontend.

3. **Distribucion como paquete PyPI (seccion 2.3 del v0):** Es vision a futuro. Se elimina de esta spec.

4. **`INFORMATION_SCHEMA.JOBS` para detectar procesos externos:** Datos temporales, costosos y ambiguos. Se mantiene como feature opcional futura, no como parte del flujo principal.

5. **6 fases de desarrollo:** Se consolidan en 3 fases.

6. **Tooling:** Se usa **uv** (no pip) para Python y **bun** (no npm) para frontend.

7. **Escaneo acotado:** El v0 asume escaneo completo del proyecto. Esta spec introduce escaneo dirigido: el usuario especifica una tabla/vista target y los datasets relevantes. El scanner sigue dependencias recursivamente con limite de profundidad por saltos de dataset.

8. **Pantalla de configuracion:** Si no se pasan flags de escaneo y no hay grafo guardado, el frontend muestra una pantalla de setup para configurar el escaneo interactivamente.

---

## 3. Especificacion Tecnica

### 3.1 Arquitectura

```
+-----------------------------------------------------------+
|              uv run sql-lineage-tracker serve              |
|                                                            |
|  +--------------+    +--------------+    +------------+    |
|  |  Extraccion  |--->|   Parsing    |--->|  Grafo     |    |
|  |  BigQuery    |    |   sqlglot    |    |  en memoria|    |
|  +--------------+    +--------------+    +------+-----+    |
|                                                |           |
|                                          +-----v-----+    |
|  +--------------+                        |  FastAPI   |    |
|  |  Frontend    |<---------------------->|  API       |    |
|  |  React (pre- |   HTTP localhost:8050  |  + static  |    |
|  |  compilado)  |                        |  files     |    |
|  +--------------+                        +-----+-----+    |
|                                                |           |
|                                          +-----v-----+    |
|                                          |  Disco     |    |
|                                          |  JSON      |    |
|                                          +-----------+    |
+-----------------------------------------------------------+
```

```
+-----------------------------------------------------------+
|                     Progreso de escaneo                    |
|                                                            |
|  POST /api/scan ──> inicia escaneo asincrono               |
|  GET  /api/scan/events ──> Server-Sent Events              |
|       "Escaneando dataset staging (2/4)"                   |
|       "Parseando vista orders_clean..."                    |
|       "Escaneo completado: 45 nodos, 62 aristas"           |
+-----------------------------------------------------------+
```

El servidor FastAPI:
- Sirve el frontend React pre-compilado como archivos estaticos desde `frontend/dist/`.
- Expone endpoints API para que el frontend consuma el grafo.
- Persiste el estado en disco (`~/.sql-lineage-tracker/{project_id}/graph_data.json`).
- Comunica progreso de escaneo via Server-Sent Events (SSE).

### 3.2 CLI

```bash
# Ejecutar desde la carpeta raiz del proyecto:
uv run sql-lineage-tracker serve --project mi-proyecto-gcp

# Opciones de escaneo
--target dataset.tabla           # Tabla/vista objetivo: trazar linaje hacia atras
--dataset nombre_dataset         # Limitar escaneo a datasets especificos (repetible)
--depth 3                        # Profundidad maxima en saltos de dataset (default: sin limite)
--no-scan                        # No conectar a BigQuery; usar ultimo grafo guardado

# Opciones del servidor
--port 8050                      # Puerto del servidor (default: 8050)
--data-dir ~/.sql-lineage-tracker  # Directorio base de persistencia

# El servidor usa las credenciales configuradas en el entorno:
# - Application Default Credentials (gcloud auth application-default login)
# - O la variable GOOGLE_APPLICATION_CREDENTIALS
```

> **Nota:** El comando se ejecuta siempre desde la carpeta del proyecto, donde `uv` encuentra el `pyproject.toml` para resolver dependencias y el entry point.

**Comportamiento al ejecutar:**
1. Si existe un grafo previo en `{data-dir}/{project_id}/`, lo carga.
2. Si se pasan `--target` y/o `--dataset`: escanea solo lo especificado.
3. Si NO se pasan flags de escaneo ni `--no-scan` y NO hay grafo guardado: levanta servidor y muestra pantalla de configuracion en el frontend.
4. Si NO se pasan flags de escaneo ni `--no-scan` pero SI hay grafo guardado: muestra el grafo anterior. El re-escaneo se configura desde la toolbar.
5. Si `--no-scan` esta presente: usa el ultimo grafo guardado sin conectarse a BigQuery.
6. Levanta FastAPI en el puerto especificado.
7. Abre el navegador automaticamente en `http://localhost:{port}`.

**Modelo de profundidad (--depth):**
- La profundidad se mide en **saltos de dataset**: cada vez que una dependencia cruza a un dataset diferente, cuenta como +1.
- Dependencias dentro del mismo dataset NO cuentan como salto.
- Ejemplo con `--target analytics.vista_final --depth 2`: analytics -> staging (1) -> raw_data (2) -> stop.
- Los nodos en el borde del limite aparecen con un indicador de "truncado" (hay mas niveles no explorados).

### 3.3 Modulo Python — Extraccion y Parsing

#### Dependencias (gestionadas con uv)

```toml
[project]
dependencies = [
    "google-cloud-bigquery>=3.0",
    "sqlglot>=20.0",
    "fastapi>=0.100",
    "uvicorn>=0.20",
    "sse-starlette>=1.0",
]

[project.optional-dependencies]
datatransfer = ["google-cloud-bigquery-datatransfer"]
```

#### Fuentes de datos a inventariar

| Fuente | Como se obtiene | SQL disponible |
|---|---|---|
| **Vistas** | `INFORMATION_SCHEMA.VIEWS` por dataset | Si (`view_definition`) |
| **Tablas base** | `INFORMATION_SCHEMA.TABLES` (`table_type = 'BASE TABLE'`) | No — nodos de origen |
| **Columnas** | `INFORMATION_SCHEMA.COLUMNS` por dataset | N/A — define el schema |
| **Scheduled queries** | API BigQuery Data Transfer o carga manual | Si (query SQL) |
| **Routines** | `INFORMATION_SCHEMA.ROUTINES` por dataset | Si (`routine_definition`) |
| **Procesos externos** | No detectables automaticamente | No — union manual |

#### Flujo de extraccion (escaneo acotado)

```
1. Si hay --target:
   a. Obtener SQL de la vista target via INFORMATION_SCHEMA.VIEWS
   b. Parsear SQL para identificar dependencias directas (tablas/vistas referenciadas)
   c. Para cada dependencia:
      - Si esta en un dataset permitido (--dataset) o no hay filtro: extraer su info
      - Si cruza a otro dataset: verificar profundidad (saltos de dataset)
      - Si la profundidad excede --depth: marcar nodo como truncado y parar
   d. Recursivamente seguir dependencias de cada vista intermedia
   e. Extraer columnas via INFORMATION_SCHEMA.COLUMNS para todos los nodos encontrados

2. Si hay --dataset sin --target:
   a. Para cada dataset especificado:
      - INFORMATION_SCHEMA.TABLES -> registrar tablas base
      - INFORMATION_SCHEMA.VIEWS -> recolectar SQL de vistas
      - INFORMATION_SCHEMA.COLUMNS -> schema de cada tabla/vista
   b. Seguir dependencias fuera de los datasets especificados segun --depth

3. Si no hay flags de escaneo:
   a. Delegar al frontend (pantalla de configuracion)

4. Con las vistas recolectadas:
   a. Ordenar en orden topologico (dependencias resueltas primero)
   b. Para cada vista (en orden topologico):
      - Parsear SQL con sqlglot (dialecto BigQuery)
      - Extraer linaje de columnas
      - Registrar schema resultante para resolucion de vistas dependientes
   c. Construir grafo bidireccional (upstream + downstream)
   d. Detectar huecos (nodos sin origen, nodos sin consumidor)
   e. Guardar en disco
```

#### Manejo de errores durante escaneo

- **Permisos insuficientes en un dataset:** saltar el dataset, loggear en consola, continuar con el resto. El nodo aparece en el grafo con indicador de error.
- **SQL invalido o no parseable:** saltar la vista, loggear en consola. El nodo aparece en el grafo con indicador de warning ("linaje no resuelto").
- **Columnas con linaje desconocido:** la columna aparece en el grafo con un indicador visual de "linaje desconocido", invitando al usuario a crear una union manual. Se cuenta en `parse_errors` de `scan_stats`.
- **Nodos truncados por profundidad:** aparecen con indicador de "hay mas niveles" expandible.
- Ningun error individual detiene el proceso completo.

#### Parsing de linaje con sqlglot

**Enfoque principal:** usar `sqlglot.lineage()` que recibe una columna objetivo, el SQL, y los schemas conocidos, y retorna el arbol de dependencias.

```python
import sqlglot
from sqlglot.lineage import lineage

# Para cada columna de la vista destino:
result = lineage(
    column="revenue",
    sql="SELECT a * b AS revenue FROM source_table",
    dialect="bigquery",
    schema={
        "source_table": {"a": "FLOAT64", "b": "FLOAT64"}
    }
)
```

**Tipos de transformacion a detectar:**

| Tipo | Ejemplo SQL | `source_columns` | `expression` |
|---|---|---|---|
| `direct` | `SELECT col FROM t` | `["col"]` | `null` |
| `rename` | `SELECT col AS nuevo FROM t` | `["col"]` | `null` |
| `expression` | `SELECT UPPER(name) AS name_upper` | `["name"]` | `"UPPER(name)"` |
| `aggregation` | `SELECT COUNT(*) AS total` | `["*"]` o columnas en GROUP BY | `"COUNT(*)"` |
| `unknown` | Columna no resuelta por sqlglot | `[]` | `null` |

**Casos SQL — por prioridad de implementacion:**

1. **Fase 1 (MVP):** SELECT simple, alias/rename, JOIN, CTEs, expresiones basicas, `CREATE TABLE AS SELECT`.
2. **Fase 2:** `SELECT *` (expandir con schema), subconsultas, `UNION ALL`, agregaciones.
3. **Fase 3:** `UNNEST`/`STRUCT`, funciones de ventana, subconsultas correlacionadas, SQL dinamico (marcar como warning).

**Fallback para casos no cubiertos por `sqlglot.lineage()`:**

```python
parsed = sqlglot.parse(sql, dialect="bigquery")[0]
# Recorrer parsed.find_all(sqlglot.exp.Column) para extraer referencias
# Mapear cada columna del SELECT a sus fuentes en FROM/JOIN
# Si no se puede resolver: marcar como transformation: "unknown"
```

### 3.4 API FastAPI — Endpoints

| Metodo | Endpoint | Descripcion |
|---|---|---|
| `GET` | `/api/health` | Estado del servidor y conexion a BigQuery |
| `GET` | `/api/graph` | Grafo completo (nodos + aristas automaticas + manuales) |
| `GET` | `/api/datasets` | Listar datasets disponibles en el proyecto (para pantalla de setup) |
| `GET` | `/api/datasets/{id}/tables` | Listar tablas/vistas de un dataset (para pantalla de setup) |
| `POST` | `/api/scan` | Iniciar escaneo con configuracion (target, datasets, depth). Asincrono |
| `GET` | `/api/scan/events` | Server-Sent Events con progreso del escaneo en curso |
| `POST` | `/api/manual-edge` | Crear arista manual |
| `PUT` | `/api/manual-edge/{edge_id}` | Actualizar arista manual |
| `DELETE` | `/api/manual-edge/{edge_id}` | Eliminar arista manual |
| `GET` | `/api/columns/{dataset}/{table}` | Consultar columnas de una tabla en BigQuery |

**Body de POST /api/scan:**
```json
{
  "target": "analytics.vista_final",
  "datasets": ["staging", "raw_data"],
  "depth": 3
}
```
Todos los campos son opcionales. Si no se envian, escanea el proyecto completo.

**Endpoints estaticos:**
- `GET /` -> Sirve el frontend React compilado (`index.html`)
- `GET /assets/*` -> Archivos estaticos del frontend

**Persistencia:** cada operacion de escritura actualiza `{data_dir}/{project_id}/graph_data.json` en disco. Este archivo contiene el grafo completo: nodos, aristas automaticas y aristas manuales.

### 3.5 App React — Visualizacion

#### Stack tecnico

- **React 18+** (con Vite como bundler, **bun** como runtime/package manager)
- **React Flow** (`@xyflow/react` v12+)
- **Tailwind CSS**
- **TypeScript**

El frontend se pre-compila con `bun run build` y el output queda en `frontend/dist/`. Este directorio se commitea al repo para que usuarios sin bun puedan usar la herramienta. FastAPI sirve los archivos estaticos desde esa ruta.

#### Interfaces TypeScript principales

```typescript
interface LineageNode {
  id: string;                    // "dataset.nombre"
  type: "table" | "view" | "materialized" | "routine";
  dataset: string;
  name: string;
  columns: ColumnInfo[];
  source: "bigquery_view" | "scheduled_query" | "routine"
        | "ingestion" | "external_process" | "unknown";
  sql: string | null;
  description: string | null;
  status: "ok" | "warning" | "error" | "truncated";
  status_message: string | null;
}

interface ColumnInfo {
  name: string;
  data_type: string;
  lineage_status: "resolved" | "unknown";
}

interface LineageEdge {
  id: string;
  source_node: string;
  target_node: string;
  edge_type: "automatic" | "manual";
  description: string | null;
  column_mappings: ColumnMapping[];
}

interface ColumnMapping {
  source_columns: string[];
  target_column: string;
  transformation: "direct" | "rename" | "expression"
                | "aggregation" | "external" | "new_field" | "unknown";
  expression: string | null;
  description: string | null;
}

interface LineageGraph {
  metadata: GraphMetadata;
  nodes: Record<string, LineageNode>;
  edges: LineageEdge[];
}

interface GraphMetadata {
  project_id: string;
  generated_at: string;
  description: string | null;
  scan_config: {
    target: string | null;
    datasets: string[];
    depth: number | null;
  };
  scan_stats: {
    total_nodes: number;
    total_edges: number;
    nodes_by_type: Record<string, number>;
    orphan_nodes: number;
    terminal_nodes: number;
    truncated_nodes: number;
    parse_errors: number;
  };
}

// Para la pantalla de configuracion
interface DatasetInfo {
  id: string;
  table_count: number;
  view_count: number;
}

interface TableInfo {
  name: string;
  type: "table" | "view" | "materialized";
  dataset: string;
}

interface ScanConfig {
  target: string | null;
  datasets: string[];
  depth: number | null;
}
```

#### Componentes principales

| Componente | Responsabilidad |
|---|---|
| `App` | Layout principal, estado global del grafo, comunicacion con API |
| `ScanSetupScreen` | Pantalla de configuracion inicial: datasets, target, profundidad |
| `GraphCanvas` | Wrapper de React Flow, renderizado del DAG |
| `TableNode` | Nodo custom — muestra tabla con columnas expandibles, indicadores de status |
| `SearchBar` | Buscador con autocompletado por campo y por tabla |
| `FilterPanel` | Filtros por dataset, tipo de nodo, profundidad |
| `NodeDetailPanel` | Panel lateral con detalle del nodo seleccionado |
| `EdgeDetailPanel` | Panel lateral con detalle de la arista seleccionada |
| `ManualEdgeModal` | Modal para crear/editar uniones manuales |
| `Toolbar` | Barra superior con botones: re-escanear, exportar, filtros |
| `ScanProgressBar` | Barra/panel de progreso del escaneo (consume SSE) |

### 3.6 Estructura del JSON del grafo

```json
{
  "metadata": {
    "project_id": "mi-proyecto-gcp",
    "generated_at": "2026-03-09T10:30:00Z",
    "description": null,
    "scan_config": {
      "target": "analytics.monthly_revenue",
      "datasets": ["staging", "raw_data"],
      "depth": 3
    },
    "scan_stats": {
      "total_nodes": 45,
      "total_edges": 62,
      "nodes_by_type": { "table": 15, "view": 28, "materialized": 2 },
      "orphan_nodes": 3,
      "terminal_nodes": 8,
      "truncated_nodes": 1,
      "parse_errors": 1
    }
  },
  "nodes": {
    "raw_data.orders": {
      "type": "table",
      "dataset": "raw_data",
      "name": "orders",
      "columns": [
        { "name": "order_id", "data_type": "STRING", "lineage_status": "resolved" },
        { "name": "amount", "data_type": "FLOAT64", "lineage_status": "resolved" },
        { "name": "created_at", "data_type": "TIMESTAMP", "lineage_status": "resolved" }
      ],
      "source": "ingestion",
      "sql": null,
      "description": null,
      "status": "ok",
      "status_message": null
    },
    "staging.orders_clean": {
      "type": "view",
      "dataset": "staging",
      "name": "orders_clean",
      "columns": [
        { "name": "id_pedido", "data_type": "STRING", "lineage_status": "resolved" },
        { "name": "revenue", "data_type": "FLOAT64", "lineage_status": "resolved" }
      ],
      "source": "bigquery_view",
      "sql": "SELECT order_id AS id_pedido, amount AS revenue FROM `raw_data.orders`",
      "description": null,
      "status": "ok",
      "status_message": null
    }
  },
  "edges": [
    {
      "id": "edge_raw_data.orders__staging.orders_clean",
      "source_node": "raw_data.orders",
      "target_node": "staging.orders_clean",
      "edge_type": "automatic",
      "description": null,
      "column_mappings": [
        {
          "source_columns": ["order_id"],
          "target_column": "id_pedido",
          "transformation": "rename",
          "expression": null,
          "description": null
        },
        {
          "source_columns": ["amount"],
          "target_column": "revenue",
          "transformation": "rename",
          "expression": null,
          "description": null
        }
      ]
    }
  ]
}
```

---

## 4. Flujo / Integracion

### 4.1 Primera ejecucion con flags CLI

```
1. Usuario ejecuta: uv run sql-lineage-tracker serve --project mi-proyecto --target analytics.vista_final --dataset staging --dataset raw_data --depth 2
2. CLI verifica credenciales GCP (ADC o GOOGLE_APPLICATION_CREDENTIALS)
3. Obtiene SQL de analytics.vista_final
4. Identifica dependencias, sigue recursivamente dentro de staging y raw_data
5. Respeta limite de profundidad (2 saltos de dataset)
6. Para nodos en el borde: marca como truncados
7. Parsea linaje con sqlglot para cada vista encontrada
8. Construye grafo en memoria
9. Detecta huecos, genera estadisticas
10. Guarda graph_data.json en ~/.sql-lineage-tracker/mi-proyecto/
11. Levanta FastAPI en localhost:8050
12. Abre navegador automaticamente
13. Frontend carga grafo via GET /api/graph
14. Renderiza DAG interactivo
```

### 4.2 Primera ejecucion sin flags (pantalla de setup)

```
1. Usuario ejecuta: uv run sql-lineage-tracker serve --project mi-proyecto
2. CLI verifica credenciales GCP
3. No hay grafo guardado ni flags de escaneo
4. Levanta FastAPI en localhost:8050
5. Abre navegador automaticamente
6. Frontend detecta que no hay grafo -> muestra pantalla de configuracion
7. Pantalla de setup carga datasets via GET /api/datasets
8. Usuario selecciona datasets, opcionalmente una tabla target, y profundidad
9. Usuario hace clic en "Escanear"
10. Frontend envia POST /api/scan con la configuracion
11. Frontend muestra progreso via SSE (GET /api/scan/events)
12. Al completar: frontend carga grafo y renderiza DAG
```

### 4.3 Ejecuciones posteriores

```
1. Usuario ejecuta: uv run sql-lineage-tracker serve --project mi-proyecto
2. Existe grafo guardado -> lo carga y muestra directamente
3. No re-escanea automaticamente
4. El usuario puede re-escanear desde la toolbar (configurable)
```

### 4.4 Re-escaneo desde la interfaz

```
1. Usuario hace clic en "Re-escanear" en la toolbar
2. Se abre un mini-panel de configuracion (target, datasets, depth) pre-rellenado con la config del ultimo escaneo
3. Frontend envia POST /api/scan
4. Backend ejecuta ciclo de extraccion y parsing
5. Progreso comunicado via SSE
6. Aristas automaticas se regeneran desde cero; aristas manuales se preservan
7. Si algun nodo referenciado por una arista manual fue eliminado, se notifica
8. Frontend recarga el grafo actualizado
```

### 4.5 Modo offline

```
1. Usuario ejecuta: uv run sql-lineage-tracker serve --no-scan
2. Carga grafo previo desde disco
3. No se conecta a BigQuery
4. Levanta servidor y abre navegador
5. El boton "Re-escanear" en la interfaz queda deshabilitado
```

### 4.6 Busqueda por campo

1. Usuario escribe nombre de campo en el buscador (ej: `order_id`).
2. Dropdown muestra coincidencias agrupadas por tabla: `order_id — raw_data.orders`, `id_pedido — staging.orders_clean`.
3. Usuario selecciona `order_id — raw_data.orders`.
4. **Algoritmo de seguimiento:**
   - Desde el nodo `raw_data.orders`, columna `order_id`.
   - Buscar en aristas downstream donde `source_columns` contenga `order_id`.
   - Si encuentra mapping (ej: `order_id` -> `id_pedido` en `staging.orders_clean`), agregar `id_pedido` a la cadena.
   - Continuar buscando aristas donde `source_columns` contenga `id_pedido`.
   - Repetir hasta agotar aristas.
   - Hacer lo mismo upstream si corresponde.
5. DAG se filtra: solo nodos y aristas de la cadena. Los demas se atenuan.
6. En cada nodo visible, se resalta la columna relevante.

### 4.7 Union manual

1. Usuario hace clic derecho en nodo -> "Agregar nodo anterior" / "Agregar nodo sucesor".
2. Modal muestra:
   - Campo para tabla origen/destino (autocompletado con nodos existentes + opcion de crear nuevo).
   - Si tabla existe: columnas se cargan del grafo.
   - Si es nueva: el backend consulta `GET /api/columns/{dataset}/{table}` en BigQuery. Si no existe en BigQuery, usuario define columnas manualmente.
3. Tabla de mapeo: columnas fijas del nodo actual <> dropdown para columna de origen/destino.
4. Default: matching por mismo nombre.
5. Usuario selecciona tipo de transformacion, agrega descripcion.
6. Al confirmar: `POST /api/manual-edge` -> arista creada, DAG actualizado.

---

## 5. Ejemplos Concretos

### 5.1 Cadena de linaje simple

**BigQuery tiene:**
- Tabla base `raw_data.orders`: `order_id`, `amount`, `created_at`
- Vista `staging.orders_clean`:
  ```sql
  SELECT order_id AS id_pedido, amount AS revenue, created_at
  FROM `raw_data.orders` WHERE amount > 0
  ```
- Vista `analytics.monthly_revenue`:
  ```sql
  SELECT FORMAT_TIMESTAMP('%Y-%m', created_at) AS month,
         SUM(revenue) AS total_revenue,
         COUNT(*) AS order_count
  FROM `staging.orders_clean` GROUP BY month
  ```

**Comando:**
```bash
uv run sql-lineage-tracker serve --project mi-proyecto --target analytics.monthly_revenue
```

**Grafo resultante:**
```
raw_data.orders --> staging.orders_clean --> analytics.monthly_revenue
```

**Mapeos:**

Edge 1 (`raw_data.orders` -> `staging.orders_clean`):
| source_columns | target_column | transformation |
|---|---|---|
| `order_id` | `id_pedido` | `rename` |
| `amount` | `revenue` | `rename` |
| `created_at` | `created_at` | `direct` |

Edge 2 (`staging.orders_clean` -> `analytics.monthly_revenue`):
| source_columns | target_column | transformation | expression |
|---|---|---|---|
| `created_at` | `month` | `expression` | `FORMAT_TIMESTAMP('%Y-%m', created_at)` |
| `revenue` | `total_revenue` | `aggregation` | `SUM(revenue)` |
| `*` | `order_count` | `aggregation` | `COUNT(*)` |

### 5.2 Busqueda: seguir `order_id` desde `raw_data.orders`

Cadena resultante:
```
raw_data.orders (order_id)
  └──> staging.orders_clean (id_pedido) [rename]
       └──> analytics.monthly_revenue (order_count) [aggregation: COUNT(*)]
```

### 5.3 Union manual — Proceso externo

Script Python toma `staging.orders_clean` y genera `analytics.retention_scores`:

| source_columns | target_column | transformation | description |
|---|---|---|---|
| `id_pedido` | `id_cliente` | `external` | "Join con tabla de clientes" |
| `revenue`, `created_at` | `retention_score` | `external` | "Modelo de retencion" |
| — | `segment` | `new_field` | "Segmento calculado" |
| — | `calculated_at` | `new_field` | "Timestamp de ejecucion" |

### 5.4 Nodo truncado por profundidad

```bash
uv run sql-lineage-tracker serve --project mi-proyecto --target analytics.monthly_revenue --depth 1
```

```
[truncated] raw_data.orders (...)  --> staging.orders_clean --> analytics.monthly_revenue
```

`raw_data.orders` aparece con indicador de truncado porque esta a profundidad 1 (salto de dataset: analytics -> staging) y sus propias dependencias estarian a profundidad 2.

---

## 6. Archivos a Crear

### Modulo Python (`/backend`)

| Archivo | Proposito |
|---|---|
| `backend/pyproject.toml` | Configuracion del proyecto y dependencias (uv) |
| `backend/src/lineage_tracker/__init__.py` | Paquete principal |
| `backend/src/lineage_tracker/cli.py` | CLI con comando `serve` y flags de escaneo (entry point) |
| `backend/src/lineage_tracker/server.py` | App FastAPI, endpoints API, SSE, servir frontend estatico |
| `backend/src/lineage_tracker/extractor.py` | Conexion a BigQuery, consultas a INFORMATION_SCHEMA, escaneo acotado |
| `backend/src/lineage_tracker/parser.py` | Parsing de SQL con sqlglot, extraccion de linaje |
| `backend/src/lineage_tracker/graph.py` | Construccion del grafo, deteccion de huecos, merge |
| `backend/src/lineage_tracker/models.py` | Dataclasses / Pydantic models (Node, Edge, ColumnMapping) |
| `backend/src/lineage_tracker/persistence.py` | Lectura/escritura del JSON en disco (por proyecto) |
| `backend/tests/test_parser.py` | Tests unitarios del modulo de parsing |

### App React (`/frontend`)

| Archivo | Proposito |
|---|---|
| `frontend/package.json` | Dependencias (bun) |
| `frontend/dist/` | Build compilado (commiteado al repo) |
| `frontend/src/App.tsx` | Componente raiz, estado global, routing entre setup y grafo |
| `frontend/src/types/graph.ts` | Interfaces TypeScript del grafo |
| `frontend/src/api/client.ts` | Cliente HTTP + SSE para comunicacion con backend |
| `frontend/src/components/ScanSetupScreen.tsx` | Pantalla de configuracion de escaneo |
| `frontend/src/components/GraphCanvas.tsx` | Wrapper de React Flow |
| `frontend/src/components/TableNode.tsx` | Nodo custom con columnas expandibles e indicadores de status |
| `frontend/src/components/SearchBar.tsx` | Buscador con autocompletado |
| `frontend/src/components/FilterPanel.tsx` | Filtros (dataset, tipo, profundidad) |
| `frontend/src/components/NodeDetailPanel.tsx` | Panel lateral de detalle de nodo |
| `frontend/src/components/EdgeDetailPanel.tsx` | Panel lateral de detalle de arista |
| `frontend/src/components/ManualEdgeModal.tsx` | Modal de union manual |
| `frontend/src/components/Toolbar.tsx` | Barra superior (re-escanear, exportar) |
| `frontend/src/components/ScanProgressBar.tsx` | Barra de progreso de escaneo (SSE) |
| `frontend/src/hooks/useLineageGraph.ts` | Hook para estado del grafo y comunicacion con API |
| `frontend/src/hooks/useColumnSearch.ts` | Hook para busqueda y seguimiento de campos |
| `frontend/src/hooks/useScanProgress.ts` | Hook para consumir SSE de progreso |
| `frontend/src/utils/graphLayout.ts` | Layout automatico (dagre) |
| `frontend/src/utils/lineageTraversal.ts` | Algoritmo de seguimiento de linaje por columna |

---

## 7. Fases de Desarrollo

### Fase 1 — MVP: Extraccion acotada + Servidor + Visualizacion basica

**Objetivo:** `uv run sql-lineage-tracker serve --project X --target dataset.tabla` funciona end-to-end.

**Backend (Python/uv):**
- [x] Setup del proyecto con `uv` y `pyproject.toml`
- [x] CLI entry point: `sql-lineage-tracker serve --project <id> [--target] [--dataset] [--depth] [--port] [--data-dir] [--no-scan]` (se ejecuta via `uv run`)
- [x] Conexion a BigQuery: listar datasets, extraer tablas base, vistas, columnas
- [x] Escaneo acotado: seguir dependencias desde --target con limite de profundidad por saltos de dataset
- [x] Filtrado por datasets especificos (--dataset)
- [x] Parsing de linaje con sqlglot: SELECT, alias/rename, JOIN, CTE, expresiones basicas
- [x] Ordenamiento topologico de vistas
- [x] Construccion del grafo y deteccion de huecos
- [x] Columnas con linaje no resuelto: marcar como `unknown` con indicador visual
- [x] Nodos truncados por profundidad: marcar con status `truncated`
- [x] Nodos con errores de permisos/parsing: marcar con status `warning`/`error`
- [x] FastAPI: `GET /api/graph`, `GET /api/health`, `GET /api/datasets`, `GET /api/datasets/{id}/tables`
- [x] `POST /api/scan` (asincrono) + `GET /api/scan/events` (SSE para progreso)
- [ ] Servir frontend compilado desde `frontend/dist/`
- [x] Persistencia del grafo en disco: `~/.sql-lineage-tracker/{project_id}/graph_data.json`
- [x] Abrir navegador automaticamente al iniciar
- [x] Re-escaneo: regenerar aristas automaticas desde cero, preservar aristas manuales existentes
- [ ] Reporte de estadisticas y errores en consola al escanear
- [x] Tests unitarios del modulo de parsing (sqlglot)

**Frontend (React/bun):**
- [ ] Setup con Vite + React + TypeScript + Tailwind + React Flow
- [ ] Build compilado commiteado en `frontend/dist/`
- [ ] Pantalla de configuracion de escaneo (datasets, target, profundidad) cuando no hay grafo
- [ ] Cliente HTTP + SSE para comunicacion con API
- [ ] Barra de progreso de escaneo (consume SSE)
- [ ] Renderizado del DAG con layout automatico left-to-right (dagre), sin persistencia de posiciones
- [ ] Nodos custom: nombre de tabla, dataset, tipo, indicadores de status (ok, warning, error, truncated)
- [ ] Columnas con indicador de linaje resuelto/desconocido
- [ ] Columnas visibles al expandir/colapsar nodo
- [ ] Buscador con autocompletado por campo y por tabla
- [ ] Filtrado del DAG por campo seleccionado (seguimiento de transformaciones/renames)
- [ ] Panel de detalle de nodo (nombre, tipo, columnas, SQL, upstream/downstream)
- [ ] Panel de detalle de arista (mapeos de columnas)
- [ ] Distincion visual por tipo de nodo
- [ ] Indicador visual de huecos y nodos truncados
- [ ] Boton "Re-escanear" en toolbar con mini-panel de configuracion
- [ ] Exportacion del grafo como JSON descargable

### Fase 2 — Uniones manuales + Filtros avanzados

**Objetivo:** Cerrar huecos del grafo manualmente y mejorar navegacion.

- [ ] Endpoints CRUD de aristas manuales (`POST/PUT/DELETE /api/manual-edge`)
- [ ] `GET /api/columns/{dataset}/{table}` — consultar columnas en BigQuery
- [ ] Modal de creacion de union manual (upstream y downstream)
- [ ] Autocompletado de tablas existentes en el modal
- [ ] Tabla de mapeo con defaults por mismo nombre
- [ ] Soporte para tipos: direct, rename, external, new_field, expression (N a 1)
- [ ] Edicion y eliminacion de aristas manuales
- [ ] Aristas manuales visualmente distintas (linea punteada)
- [ ] Sugerencia de union manual al hacer clic en indicador de hueco
- [ ] Filtro por dataset
- [ ] Filtro por tipo de nodo
- [ ] Filtro por tipo de arista (automatica / manual)
- [ ] Filtro por profundidad (niveles upstream/downstream)
- [ ] Drag and drop de nodos con persistencia de posiciones

### Fase 3 — Cobertura SQL extendida + Polish

**Objetivo:** Ampliar cobertura de SQL complejo y pulir la experiencia.

**Parsing extendido:**
- [ ] `SELECT *` con expansion por schema
- [ ] `UNION ALL` — columnas por posicion
- [ ] Subconsultas (subqueries)
- [ ] Funciones de ventana (window functions)
- [ ] `UNNEST` y `STRUCT`
- [ ] Scheduled queries via Data Transfer API
- [ ] Routines (`INFORMATION_SCHEMA.ROUTINES`)
- [ ] SQL dinamico — marcar como warning

**UX:**
- [ ] Syntax highlighting del SQL en panel de detalle
- [ ] Menu de clic derecho en nodos
- [ ] Vista compacta para grafos grandes
- [ ] Notificaciones de aristas manuales obsoletas al re-escanear
- [ ] Busqueda por tabla (mostrar todas sus conexiones sin filtrar por campo)
- [ ] Expansion de nodos truncados (scan parcial adicional)

---

## 8. Dependencias

### Prerrequisitos

- **Python 3.13+** con **uv** instalado
- **bun** instalado (solo para desarrollo del frontend; no necesario para usar la herramienta gracias al build pre-incluido)
- **Credenciales de GCP** con permisos de lectura sobre el proyecto BigQuery:
  - `bigquery.datasets.list`
  - `bigquery.tables.list`
  - `bigquery.tables.get`
  - Acceso a `INFORMATION_SCHEMA` de cada dataset
- **Application Default Credentials** o `GOOGLE_APPLICATION_CREDENTIALS`

### Bibliotecas Python

| Paquete | Version minima | Proposito |
|---|---|---|
| `google-cloud-bigquery` | 3.0 | Conexion a BigQuery |
| `sqlglot` | 20.0 | Parsing SQL y extraccion de linaje |
| `fastapi` | 0.100 | API HTTP |
| `uvicorn` | 0.20 | Servidor ASGI |
| `sse-starlette` | 1.0 | Server-Sent Events para progreso de escaneo |
| `google-cloud-bigquery-datatransfer` | — | Opcional: scheduled queries (Fase 3) |

### Bibliotecas JavaScript/TypeScript

| Paquete | Proposito |
|---|---|
| `react` + `react-dom` | Framework UI |
| `@xyflow/react` | React Flow v12+ para DAG |
| `tailwindcss` | Estilos |
| `@dagrejs/dagre` | Layout automatico del grafo |

---

## 9. Criterios de Aceptacion

### MVP (Fase 1 — Must Have)

**Backend:**
- [ ] `uv run sql-lineage-tracker serve --project X --target dataset.tabla` escanea hacia atras, parsea y levanta servidor
- [ ] `--dataset` limita el escaneo a datasets especificos
- [ ] `--depth N` limita la profundidad por saltos de dataset
- [ ] Sin flags de escaneo y sin grafo guardado: levanta servidor con pantalla de setup
- [ ] Con grafo guardado: levanta servidor y muestra grafo anterior
- [ ] `--no-scan` levanta servidor sin conectarse a BigQuery
- [ ] Parsea linaje a nivel de columna para SELECT, alias, JOIN y CTE correctamente
- [ ] Columnas no resueltas aparecen con indicador visual (no se omiten)
- [ ] Nodos con errores de permisos/parsing aparecen con indicador de error/warning
- [ ] Nodos truncados por profundidad aparecen con indicador
- [ ] Genera JSON con la estructura definida y lo persiste en disco por proyecto
- [ ] Detecta y reporta nodos huerfanos
- [ ] Errores de parsing no crashean el proceso (continua con las demas vistas)
- [ ] Re-escaneo regenera aristas automaticas y preserva aristas manuales
- [ ] Progreso del escaneo comunicado via SSE
- [ ] Tests unitarios del parser cubren casos basicos (SELECT, JOIN, CTE, alias)

**Frontend:**
- [ ] Pantalla de configuracion de escaneo funcional (datasets, target, profundidad)
- [ ] Barra de progreso de escaneo con mensajes en tiempo real
- [ ] Renderiza DAG navegable desde datos de la API
- [ ] Indicadores visuales de status en nodos (ok, warning, error, truncated)
- [ ] Indicadores de linaje resuelto/desconocido en columnas
- [ ] Buscador encuentra campos por nombre con autocompletado
- [ ] Filtrado por campo muestra cadena completa siguiendo renames
- [ ] Panel de detalle de nodo muestra columnas, tipo, SQL
- [ ] Panel de detalle de arista muestra mapeos de columnas
- [ ] Indicadores visuales diferencian tipos de nodo y nodos huerfanos
- [ ] Boton re-escanear funciona con mini-panel de configuracion
- [ ] Exportar grafo como JSON descargable

### Nice to Have (Fases 2-3)

- [ ] Uniones manuales con modal de mapeo
- [ ] Filtros por dataset, tipo, profundidad
- [ ] Drag and drop con persistencia de posiciones
- [ ] Soporte para `SELECT *`, `UNION ALL`, funciones de ventana, UNNEST/STRUCT
- [ ] Syntax highlighting del SQL
- [ ] Vista compacta para grafos grandes
- [ ] Scheduled queries via Data Transfer API
- [ ] Expansion interactiva de nodos truncados

---

## 10. Consideraciones No-Funcionales

### Rendimiento
- **Escaneo acotado:** al limitar por target y datasets, el escaneo es significativamente mas rapido que un escaneo completo. Proyectos medianos (50-200 tablas/vistas) en el scope deberian completar en segundos a pocos minutos.
- **Consultas a `INFORMATION_SCHEMA`** no consumen bytes procesados en BigQuery.
- **Renderizado React:** React Flow maneja ~500 nodos con fluidez. Los filtros son esenciales para grafos mayores.
- **Layout:** dagre recalcula el layout cada vez que se carga el grafo. Sin persistencia de posiciones en el MVP.

### Seguridad
- Credenciales de GCP nunca se almacenan en el JSON ni se exponen al frontend.
- El servidor es solo para uso local (herramienta personal); no se expone a internet.
- El SQL de las vistas se incluye en el JSON para visualizacion — el usuario debe considerar si esto es aceptable segun sus politicas.

### Mantenibilidad
- Backend y frontend son proyectos independientes que se comunican via API REST + SSE.
- El frontend se pre-compila y se commitea en `frontend/dist/`. Los usuarios solo necesitan Python/uv para ejecutar la herramienta.
- El formato del JSON esta implicitamente versionado por `metadata.generated_at`.
- Errores de parsing no bloquean la generacion del grafo — se marcan como warnings.

### Testing
- Tests unitarios del modulo de parsing (sqlglot) son la prioridad del MVP.
- Verificar que SQLs conocidos generan los column mappings correctos.
- Casos de test: SELECT simple, alias/rename, JOIN con multiples tablas, CTE, expresiones basicas.
