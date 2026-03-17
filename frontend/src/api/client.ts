import type {
  LineageGraph,
  LineageEdge,
  DatasetInfo,
  TableInfo,
  ScanConfig,
  ScanEvent,
  HealthStatus,
  ManualEdgeRequest,
  ColumnInfo,
} from "../types/graph";

const BASE_URL = "/api";

async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${url}`, init);
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`API error ${res.status}: ${body}`);
  }
  return res.json();
}

export function getHealth(): Promise<HealthStatus> {
  return fetchJSON("/health");
}

export function getGraph(): Promise<LineageGraph> {
  return fetchJSON("/graph");
}

export function getDatasets(): Promise<DatasetInfo[]> {
  return fetchJSON("/datasets");
}

export function getDatasetTables(datasetId: string): Promise<TableInfo[]> {
  return fetchJSON(`/datasets/${encodeURIComponent(datasetId)}/tables`);
}

export function startScan(config: ScanConfig): Promise<{ status: string }> {
  return fetchJSON("/scan", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
}

export function subscribeScanEvents(
  onEvent: (event: ScanEvent) => void,
  onError?: () => void
): () => void {
  const source = new EventSource(`${BASE_URL}/scan/events`);
  let completed = false;

  source.onmessage = (e) => {
    try {
      const event: ScanEvent = JSON.parse(e.data);
      if (event.type === "complete" || event.type === "error") {
        completed = true;
      }
      onEvent(event);
    } catch {
      onEvent({ type: "progress", message: e.data });
    }
  };

  source.onerror = () => {
    source.close();
    // Only report error if we never got a complete/error event
    if (!completed) {
      onError?.();
    }
  };

  return () => source.close();
}

export function getColumns(
  datasetId: string,
  tableName: string
): Promise<ColumnInfo[]> {
  return fetchJSON(
    `/columns/${encodeURIComponent(datasetId)}/${encodeURIComponent(tableName)}`
  );
}

export function createManualEdge(
  edge: ManualEdgeRequest
): Promise<LineageEdge> {
  return fetchJSON("/manual-edge", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(edge),
  });
}

export function updateManualEdge(
  edgeId: string,
  edge: Partial<ManualEdgeRequest>
): Promise<LineageEdge> {
  return fetchJSON(`/manual-edge/${encodeURIComponent(edgeId)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(edge),
  });
}

export function deleteManualEdge(
  edgeId: string
): Promise<{ status: string; id: string }> {
  return fetchJSON(`/manual-edge/${encodeURIComponent(edgeId)}`, {
    method: "DELETE",
  });
}

export function expandNode(
  nodeId: string,
  depth?: number
): Promise<{ status: string }> {
  return fetchJSON("/expand", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ node_id: nodeId, depth: depth ?? 1 }),
  });
}

// --- Prune Points ---

export function updatePrunePoints(
  prunePoints: string[]
): Promise<{ status: string; prune_points: string[] }> {
  return fetchJSON("/prune-points", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ prune_points: prunePoints }),
  });
}

// --- Named Scans ---

export interface SavedScanInfo {
  name: string;
  target?: string | null;
  datasets?: string[];
  depth?: number | null;
  total_nodes?: number;
  total_edges?: number;
  generated_at?: string | null;
}

export function listScans(): Promise<SavedScanInfo[]> {
  return fetchJSON("/scans");
}

export function saveScan(
  name: string,
  overwrite: boolean = false
): Promise<{ status: string; name: string; exists?: boolean }> {
  return fetchJSON(`/scans/${encodeURIComponent(name)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ overwrite }),
  });
}

export function loadScan(
  name: string
): Promise<{ status: string; name: string; nodes: number; edges: number }> {
  return fetchJSON(`/scans/${encodeURIComponent(name)}/load`, {
    method: "POST",
  });
}

export function deleteScan(
  name: string
): Promise<{ status: string; name: string }> {
  return fetchJSON(`/scans/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
}

export function exportGraphJSON(graph: LineageGraph): void {
  const blob = new Blob([JSON.stringify(graph, null, 2)], {
    type: "application/json",
  });
  downloadBlob(blob, `lineage_${graph.metadata.project_id}_${new Date().toISOString().slice(0, 10)}.json`);
}

export function exportGraphMermaid(
  graph: LineageGraph,
  detailed: boolean
): void {
  const lines: string[] = ["flowchart LR"];

  // Build node definitions
  for (const [id, node] of Object.entries(graph.nodes)) {
    const safeId = sanitizeMermaidId(id);
    const label = `${node.dataset}.${node.name}`;
    if (detailed && node.columns.length > 0) {
      const cols = node.columns.map((c) => c.name).join("<br/>");
      lines.push(`    ${safeId}["<b>${escapeMermaid(label)}</b><br/>${cols}"]`);
    } else {
      lines.push(`    ${safeId}["${escapeMermaid(label)}"]`);
    }
  }

  // Build edges
  for (const edge of graph.edges) {
    const src = sanitizeMermaidId(edge.source_node);
    const tgt = sanitizeMermaidId(edge.target_node);
    if (edge.edge_type === "manual") {
      lines.push(`    ${src} -.-> ${tgt}`);
    } else {
      lines.push(`    ${src} --> ${tgt}`);
    }
  }

  const content = lines.join("\n") + "\n";
  const blob = new Blob([content], { type: "text/plain" });
  downloadBlob(blob, `lineage_${graph.metadata.project_id}_${new Date().toISOString().slice(0, 10)}.mmd`);
}

function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function sanitizeMermaidId(id: string): string {
  return id.replace(/[^a-zA-Z0-9_]/g, "_");
}

function escapeMermaid(text: string): string {
  return text.replace(/"/g, "&quot;");
}
