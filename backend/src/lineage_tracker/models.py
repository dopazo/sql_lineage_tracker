"""Data models for the lineage tracker."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

ProgressCallback = Callable[[str, str | None], None]
"""Callable(event_type, message) used to report scan/build progress."""

_NOOP_PROGRESS: ProgressCallback = lambda _event, _msg=None: None


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    lineage_status: str = "resolved"  # "resolved" | "unknown"


@dataclass
class LineageNode:
    id: str  # "dataset.name"
    type: str  # "table" | "view" | "materialized" | "routine"
    dataset: str
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    source: str = "unknown"  # "bigquery_view" | "scheduled_query" | "routine" | "ingestion" | "external_process" | "unknown"
    sql: str | None = None
    description: str | None = None
    status: str = "ok"  # "ok" | "warning" | "error" | "truncated"
    status_message: str | None = None


@dataclass
class ColumnMapping:
    source_columns: list[str]
    target_column: str
    transformation: str = "unknown"  # "direct" | "rename" | "expression" | "aggregation" | "literal" | "external" | "new_field" | "unknown"
    expression: str | None = None
    description: str | None = None


@dataclass
class LineageEdge:
    id: str
    source_node: str
    target_node: str
    edge_type: str = "automatic"  # "automatic" | "manual"
    description: str | None = None
    column_mappings: list[ColumnMapping] = field(default_factory=list)


@dataclass
class ScanConfig:
    target: str | None = None
    datasets: list[str] = field(default_factory=list)
    depth: int | None = None


@dataclass
class ScanStats:
    total_nodes: int = 0
    total_edges: int = 0
    nodes_by_type: dict[str, int] = field(default_factory=dict)
    orphan_nodes: int = 0
    terminal_nodes: int = 0
    truncated_nodes: int = 0
    parse_errors: int = 0


@dataclass
class GraphMetadata:
    project_id: str
    generated_at: str
    description: str | None = None
    scan_config: ScanConfig = field(default_factory=ScanConfig)
    scan_stats: ScanStats = field(default_factory=ScanStats)


@dataclass
class LineageGraph:
    metadata: GraphMetadata
    nodes: dict[str, LineageNode] = field(default_factory=dict)
    edges: list[LineageEdge] = field(default_factory=list)
    prune_points: list[str] = field(default_factory=list)


@dataclass
class DatasetInfo:
    id: str
    table_count: int = 0
    view_count: int = 0


@dataclass
class TableInfo:
    name: str
    type: str  # "table" | "view" | "materialized"
    dataset: str
