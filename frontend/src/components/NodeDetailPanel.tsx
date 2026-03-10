import type { LineageNode, LineageEdge } from "../types/graph";

interface NodeDetailPanelProps {
  node: LineageNode;
  edges: LineageEdge[];
  onClose: () => void;
}

const STATUS_COLORS: Record<string, string> = {
  ok: "text-emerald-400",
  warning: "text-amber-400",
  error: "text-red-400",
  truncated: "text-blue-400",
};

function EdgeList({ label, edges, dotColor, getLabel }: {
  label: string;
  edges: LineageEdge[];
  dotColor: string;
  getLabel: (e: LineageEdge) => string;
}) {
  return (
    <div>
      <div className="label-dark">
        {label} ({edges.length})
      </div>
      {edges.length === 0 ? (
        <div className="text-xs text-[var(--text-muted)]">None</div>
      ) : (
        <div className="space-y-1">
          {edges.map((e) => (
            <div key={e.id} className="text-xs text-[var(--text-secondary)] font-[var(--font-mono)] flex items-center gap-1.5">
              <span className={`w-1 h-1 rounded-full ${dotColor}`} />
              {getLabel(e)}
              {e.edge_type === "manual" && (
                <span className="text-purple-400 text-[10px]">(manual)</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function NodeDetailPanel({ node, edges, onClose }: NodeDetailPanelProps) {
  const upstreamEdges = edges.filter((e) => e.target_node === node.id);
  const downstreamEdges = edges.filter((e) => e.source_node === node.id);

  return (
    <div className="w-80 glass-elevated border-l border-[var(--border-subtle)] overflow-y-auto flex flex-col animate-fade-in-right">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-[var(--border-subtle)]">
        <h2 className="text-sm font-semibold text-[var(--text-primary)]">
          Node Detail
        </h2>
        <button
          onClick={onClose}
          className="w-6 h-6 rounded-md flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-hover)] transition-colors"
        >
          &times;
        </button>
      </div>

      <div className="p-4 space-y-5 flex-1">
        {/* Name */}
        <div>
          <div className="label-dark">Name</div>
          <div className="text-sm font-medium font-[var(--font-mono)] text-[var(--accent-cyan)]">
            {node.dataset}.{node.name}
          </div>
        </div>

        {/* Meta row */}
        <div className="flex gap-4">
          <div>
            <div className="label-dark">Type</div>
            <div className="text-sm text-[var(--text-primary)]">{node.type}</div>
          </div>
          <div>
            <div className="label-dark">Source</div>
            <div className="text-sm text-[var(--text-primary)]">{node.source}</div>
          </div>
          <div>
            <div className="label-dark">Status</div>
            <div className={`text-sm ${STATUS_COLORS[node.status] ?? "text-blue-400"}`}>
              {node.status}
            </div>
          </div>
        </div>

        {node.status_message && (
          <div className="text-xs text-amber-400 bg-amber-500/10 px-3 py-2 rounded-lg border border-amber-500/20">
            {node.status_message}
          </div>
        )}

        {/* Columns */}
        <div>
          <div className="label-dark">
            Columns ({node.columns.length})
          </div>
          <div className="bg-[var(--bg-deep)] border border-[var(--border-subtle)] rounded-lg max-h-48 overflow-y-auto">
            {node.columns.map((col) => (
              <div
                key={col.name}
                className="flex items-center gap-2 px-3 py-1.5 text-xs border-b border-[var(--border-subtle)] last:border-b-0"
              >
                <span
                  className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                    col.lineage_status === "resolved"
                      ? "bg-emerald-400"
                      : "bg-amber-400"
                  }`}
                />
                <span className="flex-1 font-[var(--font-mono)] text-[var(--text-primary)]">
                  {col.name}
                </span>
                <span className="text-[var(--text-muted)] font-[var(--font-mono)] text-[10px]">
                  {col.data_type}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Connections */}
        <EdgeList
          label="Upstream"
          edges={upstreamEdges}
          dotColor="bg-[var(--accent-cyan)]"
          getLabel={(e) => e.source_node}
        />
        <EdgeList
          label="Downstream"
          edges={downstreamEdges}
          dotColor="bg-[var(--accent-teal)]"
          getLabel={(e) => e.target_node}
        />

        {/* SQL */}
        {node.sql && (
          <div>
            <div className="label-dark">SQL</div>
            <pre className="text-xs text-[var(--text-secondary)] bg-[var(--bg-deep)] p-3 rounded-lg border border-[var(--border-subtle)] overflow-x-auto whitespace-pre-wrap max-h-48 font-[var(--font-mono)] leading-relaxed">
              {node.sql}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}
