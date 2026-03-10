export interface LineageNode {
  id: string;
  type: "table" | "view" | "materialized" | "routine";
  dataset: string;
  name: string;
  columns: ColumnInfo[];
  source:
    | "bigquery_view"
    | "scheduled_query"
    | "routine"
    | "ingestion"
    | "external_process"
    | "unknown";
  sql: string | null;
  description: string | null;
  status: "ok" | "warning" | "error" | "truncated";
  status_message: string | null;
}

export interface ColumnInfo {
  name: string;
  data_type: string;
  lineage_status: "resolved" | "unknown";
}

export interface LineageEdge {
  id: string;
  source_node: string;
  target_node: string;
  edge_type: "automatic" | "manual";
  description: string | null;
  column_mappings: ColumnMapping[];
}

export interface ColumnMapping {
  source_columns: string[];
  target_column: string;
  transformation:
    | "direct"
    | "rename"
    | "expression"
    | "aggregation"
    | "external"
    | "new_field"
    | "unknown";
  expression: string | null;
  description: string | null;
}

export interface LineageGraph {
  metadata: GraphMetadata;
  nodes: Record<string, LineageNode>;
  edges: LineageEdge[];
}

export interface GraphMetadata {
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

export interface DatasetInfo {
  id: string;
  table_count: number;
  view_count: number;
}

export interface TableInfo {
  name: string;
  type: "table" | "view" | "materialized";
  dataset: string;
}

export interface ScanConfig {
  target: string | null;
  datasets: string[];
  depth: number | null;
}

export interface ScanEvent {
  type: "progress" | "complete" | "error";
  message: string;
  data?: Record<string, unknown>;
}

export interface GraphFilters {
  datasets: Set<string>;
  nodeTypes: Set<LineageNode["type"]>;
  edgeTypes: Set<LineageEdge["edge_type"]>;
  maxDepth: number | null; // null = no limit
}

export interface HealthStatus {
  status: string;
  project_id: string;
  has_graph: boolean;
  bigquery_connected: boolean;
}

export interface ManualEdgeRequest {
  source_node: string;
  target_node: string;
  description?: string;
  column_mappings: {
    source_columns: string[];
    target_column: string;
    transformation: ColumnMapping["transformation"];
    expression?: string;
    description?: string;
  }[];
}
