# SQL Lineage Tracker

A CLI tool for BigQuery that extracts SQL from views and tables, parses **column-level data lineage** using [sqlglot](https://github.com/tobymao/sqlglot), and provides an interactive web interface to navigate the lineage DAG.

Given a target table or view, SQL Lineage Tracker traces the complete lineage of all its fields backward through the transformation chain. You can limit scanning to specific datasets and control scanning depth by dataset hops.

## Features

- **Column-level lineage** — tracks how individual columns transform across views, not just table-level dependencies
- **Transformation classification** — categorizes mappings as `direct`, `rename`, `expression`, `aggregation`, `new_field`, or `unknown`
- **SQL support** — handles SELECT, JOINs, CTEs, aliases, aggregate functions, and expressions
- **Scoped scanning** — limit queries to specific datasets and control depth via dataset hops
- **Topological sorting** — views are parsed in dependency order using Kahn's algorithm
- **Real-time progress** — scan progress streamed to the browser via Server-Sent Events (SSE)
- **Disk persistence** — graphs are cached as JSON per project for offline access
- **Manual edges** — support for external processes where lineage can't be auto-detected
- **Error resilience** — individual parsing or permission errors don't halt the scan

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.13+ |
| Package manager | [uv](https://docs.astral.sh/uv/) |
| Web framework | FastAPI + Uvicorn |
| SQL parsing | sqlglot (BigQuery dialect) |
| BigQuery client | google-cloud-bigquery |
| SSE | sse-starlette |
| Testing | pytest, pytest-asyncio, httpx |
| Frontend | React 19, TypeScript, React Flow v12 (@xyflow/react), Tailwind CSS v4, Vite v7, bun |

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- GCP credentials configured ([Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc) or `GOOGLE_APPLICATION_CREDENTIALS`)
- BigQuery project with read permissions on `INFORMATION_SCHEMA`

### Required GCP permissions

- `bigquery.datasets.list`
- `bigquery.tables.list`
- `bigquery.tables.get`
- Access to `INFORMATION_SCHEMA` in target datasets

## Installation

```bash
cd backend
uv sync
```

The repo includes a pre-compiled frontend build (`frontend/dist/`) so the web UI works out of the box with just Python — no need to install Node/bun or build the frontend yourself.

## Usage

```bash
uv run sql-lineage-tracker serve [OPTIONS]
```

### Options

| Flag | Description |
|------|-------------|
| `--project` (required) | GCP project ID |
| `--target <dataset>.<table>` | Trace lineage backward from this target |
| `--dataset <name>` | Limit scan to specific dataset(s) (repeatable) |
| `--depth <N>` | Max depth in dataset hops (default: unlimited) |
| `--port <N>` | Server port (default: 8050) |
| `--data-dir <path>` | Persistence directory (default: `~/.sql-lineage-tracker`) |
| `--no-scan` | Skip BigQuery, use cached graph |

### Examples

Scan a specific target, limited to two datasets:

```bash
uv run sql-lineage-tracker serve \
  --project my-gcp-project \
  --target analytics.monthly_report \
  --dataset analytics \
  --dataset raw_data
```

Start the server with a previously cached graph (no BigQuery connection needed):

```bash
uv run sql-lineage-tracker serve \
  --project my-gcp-project \
  --no-scan
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Server health check |
| `GET` | `/api/graph` | Complete lineage graph |
| `GET` | `/api/datasets` | Available datasets |
| `GET` | `/api/datasets/{id}/tables` | Tables in a dataset |
| `POST` | `/api/scan` | Initiate async scan |
| `GET` | `/api/scan/events` | SSE stream for scan progress |
| `POST` | `/api/expand` | Expand a truncated node |
| `GET` | `/api/columns/{dataset}/{table}` | Query BigQuery columns |
| `POST` | `/api/manual-edge` | Create a manual edge |
| `PUT` | `/api/manual-edge/{id}` | Update a manual edge |
| `DELETE` | `/api/manual-edge/{id}` | Delete a manual edge |

## Project Structure

```
sql_lineage_tracker/
├── backend/
│   ├── pyproject.toml
│   ├── src/lineage_tracker/
│   │   ├── cli.py            # CLI entry point
│   │   ├── server.py         # FastAPI app & SSE
│   │   ├── extractor.py      # BigQuery metadata extraction
│   │   ├── parser.py         # SQL lineage parsing (sqlglot)
│   │   ├── graph.py          # DAG construction & topological sort
│   │   ├── scanner.py        # Scoped scanning with depth limits
│   │   ├── persistence.py    # JSON serialization
│   │   └── models.py         # Data models (dataclasses)
│   └── tests/
├── frontend/
│   ├── package.json
│   ├── vite.config.ts
│   └── src/
│       ├── App.tsx
│       ├── api/              # API client
│       ├── components/       # React components
│       ├── hooks/            # Custom hooks
│       ├── types/            # TypeScript types
│       └── utils/            # Utilities
├── specs/
│   └── sql_lineage_tracker_v1.md  # Technical specification
└── README.md
```

## Testing

```bash
cd backend
uv run pytest
```

## How It Works

1. **Extract** — queries BigQuery `INFORMATION_SCHEMA` for table/view metadata and SQL definitions
2. **Sort** — topologically sorts views so dependencies are parsed before dependents
3. **Parse** — uses sqlglot to trace column-level lineage through each view's SQL
4. **Build** — constructs a bidirectional DAG with column mappings and transformation types
5. **Detect** — identifies anomalies: orphan nodes, terminal nodes, and depth-truncated nodes
6. **Persist** — caches the graph to disk as JSON for offline use
7. **Serve** — exposes the graph via FastAPI with SSE for real-time scan progress

## License

Private project — all rights reserved.
