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

export function exportGraphJSON(graph: LineageGraph): void {
  const blob = new Blob([JSON.stringify(graph, null, 2)], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `lineage_${graph.metadata.project_id}_${new Date().toISOString().slice(0, 10)}.json`;
  a.click();
  URL.revokeObjectURL(url);
}
