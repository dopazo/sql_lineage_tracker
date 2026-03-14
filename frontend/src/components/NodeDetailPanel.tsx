import { useState } from "react";
import type { LineageNode, LineageEdge } from "../types/graph";
import { SqlHighlight } from "./SqlHighlight";
import { SqlModal } from "./SqlModal";

interface NodeDetailPanelProps {
  node: LineageNode;
  edges: LineageEdge[];
  onClose: () => void;
  onAddUpstream?: (nodeId: string) => void;
  onAddDownstream?: (nodeId: string) => void;
  onExpandNode?: (nodeId: string) => void;
  expanding?: boolean;
}

const STATUS_COLORS: Record<string, string> = {
  ok: "text-emerald-400",
  warning: "text-amber-400",
  error: "text-red-400",
  truncated: "text-blue-400",
};

function TruncatedText({ text, className }: { text: string; className?: string }) {
  return (
    <span
      className={`block truncate ${className ?? ""}`}
      title={text}
    >
      {text}
    </span>
  );
}

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
            <div key={e.id} className="text-xs text-[var(--text-secondary)] font-[var(--font-mono)] flex items-center gap-1.5 min-w-0">
              <span className={`w-1 h-1 rounded-full flex-shrink-0 ${dotColor}`} />
              <TruncatedText text={getLabel(e)} className="flex-1 min-w-0" />
              {e.edge_type === "manual" && (
                <span className="text-purple-400 text-[10px] flex-shrink-0">(manual)</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function NodeDetailPanel({ node, edges, onClose, onAddUpstream, onAddDownstream, onExpandNode, expanding }: NodeDetailPanelProps) {
  const upstreamEdges = edges.filter((e) => e.target_node === node.id);
  const downstreamEdges = edges.filter((e) => e.source_node === node.id);
  const [sqlModalOpen, setSqlModalOpen] = useState(false);

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
          <div className="text-xs font-[var(--font-mono)] text-[var(--text-muted)]">
            {node.dataset}.
          </div>
          <TruncatedText
            text={node.name}
            className="text-sm font-medium font-[var(--font-mono)] text-[var(--accent-cyan)]"
          />
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

        {/* Expand truncated node */}
        {node.status === "truncated" && onExpandNode && (
          <button
            onClick={() => onExpandNode(node.id)}
            disabled={expanding}
            className="w-full px-3 py-2.5 text-sm text-blue-400 bg-blue-500/10 hover:bg-blue-500/20 border border-blue-500/30 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
          >
            {expanding ? (
              <>
                <span className="animate-spin text-xs">&#x21BB;</span>
                Expanding...
              </>
            ) : (
              <>
                <span className="text-xs">&#x25B6;</span>
                Expand — scan dependencies
              </>
            )}
          </button>
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
                <TruncatedText
                  text={col.name}
                  className="flex-1 min-w-0 font-[var(--font-mono)] text-[var(--text-primary)]"
                />
                <span className="text-[var(--text-muted)] font-[var(--font-mono)] text-[10px] flex-shrink-0">
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

        {/* Manual Edge Actions */}
        {(onAddUpstream || onAddDownstream) && (
          <div className="flex gap-2">
            {onAddUpstream && (
              <button
                onClick={() => onAddUpstream(node.id)}
                className="btn-ghost text-xs flex-1"
              >
                + Upstream
              </button>
            )}
            {onAddDownstream && (
              <button
                onClick={() => onAddDownstream(node.id)}
                className="btn-ghost text-xs flex-1"
              >
                + Downstream
              </button>
            )}
          </div>
        )}

        {/* SQL */}
        {node.sql && (
          <div>
            <div className="flex items-center justify-between mb-1.5">
              <div className="label-dark !mb-0">SQL</div>
              <button
                onClick={() => setSqlModalOpen(true)}
                className="w-5 h-5 rounded flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-hover)] transition-colors"
                title="Expand SQL"
              >
                <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <polyline points="6 1 1 1 1 6" />
                  <polyline points="10 15 15 15 15 10" />
                  <line x1="1" y1="1" x2="6" y2="6" />
                  <line x1="15" y1="15" x2="10" y2="10" />
                </svg>
              </button>
            </div>
            <SqlHighlight code={node.sql} />
          </div>
        )}
      </div>

      {/* SQL Modal */}
      {sqlModalOpen && node.sql && (
        <SqlModal
          code={node.sql}
          title={`${node.dataset}.${node.name}`}
          onClose={() => setSqlModalOpen(false)}
        />
      )}
    </div>
  );
}
