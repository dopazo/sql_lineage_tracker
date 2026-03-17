# CLAUDE.md

This file provides guidance when working with code in this repository.

## Project Overview

**SQL Lineage Tracker** is a CLI tool for BigQuery that extracts SQL from views/tables, parses column-level data lineage using sqlglot, and provides an interactive web interface to navigate the lineage DAG (directed acyclic graph).

**Primary Use Case:** Given a target table/view, trace the complete lineage of all its fields backward through the transformation chain, with ability to limit scanning to specific datasets and profundity.

## Architecture Overview

The system has three main components that communicate via HTTP + SSE:

```
CLI Entry Point (uv run sql-lineage-tracker serve)
    ↓
Backend (Python/FastAPI)
    ├── BigQuery Extractor: Queries INFORMATION_SCHEMA for tables, views, columns
    ├── SQL Parser: Uses sqlglot for column-level lineage parsing
    ├── Graph Builder: Constructs bidirectional DAG, detects holes, merges manual edges
    └── FastAPI Server: Exposes REST API + SSE, serves pre-compiled React frontend
    ↓
Frontend (React/TypeScript)
    ├── Setup Screen: Configure scan scope (datasets, target, depth)
    ├── Graph Canvas: Interactive DAG visualization with React Flow
    ├── Detail Panels: Show node/edge metadata with column mappings
    └── Search: Track column transformations across views
```

## Key Architectural Decisions

1. **Unified Execution Model:** `serve` command launches everything (BigQuery scanning, FastAPI server, opens browser). No separate file upload step.

2. **Scoped Scanning:** Users specify target table and relevant datasets to avoid unnecessary BigQuery reads. Depth limit prevents infinite traversal across datasets.

3. **Asymmetric Lineage:** Sqlglot's `lineage()` function traces column dependencies. Node "truncated" status indicates depth limit reached, not full picture explored.

4. **Manual Edge Support:** Users can add/edit edges for external processes, resolving gaps where lineage detection fails.

5. **Persistence:** Graph saved as JSON per project (`~/.sql-lineage-tracker/{project_id}/graph_data.json`). Frontend is pre-compiled and served as static files.

6. **Tooling Choices:**
   - **Backend:** Python 3.13+ with `uv` package manager
   - **Frontend:** React 18+ with `bun`, Vite, Tailwind, TypeScript
   - **SQL Parsing:** sqlglot (BigQuery dialect) with custom fallback for complex cases
   - **UI Components:** React Flow v12+ for DAG visualization, Tailwind for styling

## CLI Interface

```bash
# Core command (always from project root where uv finds pyproject.toml)
uv run sql-lineage-tracker serve --project <gcp-project-id>

# Scanning options (all optional)
--target <dataset>.<table>     # Trace lineage backward from this target
--dataset <name>               # Limit scan to specific dataset (repeatable)
--depth <N>                    # Max depth (dataset hops), default: unlimited
--no-scan                      # Skip BigQuery connection, use cached graph
--port <N>                     # Server port, default: 8050
--data-dir <path>              # Persistence directory, default: ~/.sql-lineage-tracker
```

**Behavior:**
- With flags: perform scoped scan immediately
- Without flags + no cached graph: show setup screen in frontend
- Without flags + cached graph: display previous graph, allow re-scan from toolbar
- Opens browser automatically at `http://localhost:<port>`

## Key API Endpoints

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/health` | Server status |
| GET | `/api/graph` | Complete lineage graph (nodes + edges) |
| GET | `/api/datasets` | List available BigQuery datasets |
| POST | `/api/scan` | Initiate async scan (body: target, datasets, depth) |
| GET | `/api/scan/events` | Server-Sent Events with scan progress |
| POST/PUT/DELETE | `/api/manual-edge/*` | CRUD for user-defined edges |
| GET | `/api/columns/{dataset}/{table}` | Query columns from BigQuery |


## Common Development Tasks

### Running Backend Locally
```bash
cd backend
uv sync                                    # Install dependencies
uv run python -m lineage_tracker.cli serve --project test-project --no-scan
```

### Running Frontend Locally
```bash
cd frontend
bun install
bun run dev  # Vite dev server on http://localhost:5173
```

### Building Frontend for Distribution
```bash
cd frontend
bun run build  # Creates dist/
# dist/ folder should be committed to repo
```

## IMPORTANT
If you complete a task given from a spec, make sure to also mark the task as complete in the spec itself
