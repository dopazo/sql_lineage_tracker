import type { LineageNode, LineageEdge } from "../types/graph";

interface NodeDetailPanelProps {
  node: LineageNode;
  edges: LineageEdge[];
  onClose: () => void;
}

export function NodeDetailPanel({ node, edges, onClose }: NodeDetailPanelProps) {
  const upstreamEdges = edges.filter((e) => e.target_node === node.id);
  const downstreamEdges = edges.filter((e) => e.source_node === node.id);

  return (
    <div className="w-80 bg-white border-l border-slate-200 overflow-y-auto flex flex-col">
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200">
        <h2 className="text-sm font-semibold text-slate-800">Node Detail</h2>
        <button
          onClick={onClose}
          className="text-slate-400 hover:text-slate-600 text-lg"
        >
          &times;
        </button>
      </div>

      <div className="p-4 space-y-4 flex-1">
        {/* Basic info */}
        <div>
          <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
            Name
          </div>
          <div className="text-sm font-medium">{node.dataset}.{node.name}</div>
        </div>

        <div className="flex gap-4">
          <div>
            <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
              Type
            </div>
            <div className="text-sm">{node.type}</div>
          </div>
          <div>
            <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
              Source
            </div>
            <div className="text-sm">{node.source}</div>
          </div>
          <div>
            <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
              Status
            </div>
            <div className="text-sm">{node.status}</div>
          </div>
        </div>

        {node.status_message && (
          <div className="text-xs text-amber-600 bg-amber-50 px-2 py-1 rounded">
            {node.status_message}
          </div>
        )}

        {/* Columns */}
        <div>
          <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
            Columns ({node.columns.length})
          </div>
          <div className="border border-slate-200 rounded max-h-48 overflow-y-auto">
            {node.columns.map((col) => (
              <div
                key={col.name}
                className="flex items-center gap-2 px-2 py-1 text-xs border-b border-slate-100 last:border-b-0"
              >
                <span
                  className={`w-1.5 h-1.5 rounded-full ${
                    col.lineage_status === "resolved"
                      ? "bg-emerald-400"
                      : "bg-amber-400"
                  }`}
                />
                <span className="flex-1">{col.name}</span>
                <span className="text-slate-400">{col.data_type}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Connections */}
        <div>
          <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
            Upstream ({upstreamEdges.length})
          </div>
          {upstreamEdges.map((e) => (
            <div key={e.id} className="text-xs text-slate-600">
              {e.source_node}
              {e.edge_type === "manual" && (
                <span className="text-purple-500 ml-1">(manual)</span>
              )}
            </div>
          ))}
        </div>

        <div>
          <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
            Downstream ({downstreamEdges.length})
          </div>
          {downstreamEdges.map((e) => (
            <div key={e.id} className="text-xs text-slate-600">
              {e.target_node}
              {e.edge_type === "manual" && (
                <span className="text-purple-500 ml-1">(manual)</span>
              )}
            </div>
          ))}
        </div>

        {/* SQL */}
        {node.sql && (
          <div>
            <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
              SQL
            </div>
            <pre className="text-xs bg-slate-50 p-2 rounded border border-slate-200 overflow-x-auto whitespace-pre-wrap max-h-48">
              {node.sql}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}
