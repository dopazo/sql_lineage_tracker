import { useState, useRef, useCallback } from "react";
import { createPortal } from "react-dom";
import type { LineageNode, LineageEdge, ColumnMapping } from "../types/graph";
import { SqlHighlight } from "./SqlHighlight";
import { SqlModal } from "./SqlModal";
import { TRANSFORM_STYLES } from "../constants/transforms";

interface NodeDetailPanelProps {
  node: LineageNode;
  edges: LineageEdge[];
  onClose: () => void;
  onAddUpstream?: (nodeId: string) => void;
  onAddDownstream?: (nodeId: string) => void;
  onExpandNode?: (nodeId: string) => void;
  expanding?: boolean;
  onColumnClick?: (nodeId: string, columnName: string) => void;
  highlightedColumns?: string[];
  activeTraceColumn?: string | null;
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

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      }}
      className="ml-auto flex-shrink-0 px-1.5 py-0.5 rounded text-[10px] text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-hover)] transition-colors"
      title="Copy SQL"
    >
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

function ColumnMappingTooltip({
  mappings,
  style,
  onMouseEnter,
  onMouseLeave,
}: {
  mappings: { mapping: ColumnMapping; sourceNode: string }[];
  style: React.CSSProperties;
  onMouseEnter: () => void;
  onMouseLeave: () => void;
}) {
  return (
    <div
      className="fixed z-50 w-64 glass-elevated border border-[var(--border-subtle)] rounded-lg shadow-xl animate-fade-in-right select-text"
      style={style}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className="p-3 space-y-2">
        {mappings.map(({ mapping, sourceNode: src }, i) => (
          <div
            key={i}
            className="bg-[var(--bg-deep)] border border-[var(--border-subtle)] rounded-lg p-3 text-xs"
          >
            <div className="flex items-center gap-2 mb-2 font-[var(--font-mono)]">
              <span className="text-[var(--text-secondary)]">
                {mapping.source_columns.join(", ") || "(none)"}
              </span>
              <svg width="12" height="8" viewBox="0 0 16 10" className="text-[var(--text-muted)] shrink-0">
                <path d="M0 5h13M10 1l4 4-4 4" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
              <span className="font-medium text-[var(--text-primary)]">
                {mapping.target_column}
              </span>
            </div>
            <span
              className={`inline-block px-2 py-0.5 rounded text-[10px] font-semibold font-[var(--font-mono)] uppercase tracking-wider ${
                TRANSFORM_STYLES[mapping.transformation] ??
                TRANSFORM_STYLES.unknown
              }`}
            >
              {mapping.transformation}
            </span>
            {mapping.expression && (
              <div className="mt-2">
                <div className="flex items-center justify-between mb-1">
                  <CopyButton text={mapping.expression} />
                </div>
                <SqlHighlight code={mapping.expression} />
              </div>
            )}
            {mapping.description && (
              <div className="mt-1.5 text-[var(--text-muted)]">
                {mapping.description}
              </div>
            )}
            {mappings.length > 1 && (
              <div className="mt-1.5 text-[10px] text-[var(--text-muted)] font-[var(--font-mono)]">
                from {src}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export function NodeDetailPanel({ node, edges, onClose, onAddUpstream, onAddDownstream, onExpandNode, expanding, onColumnClick, highlightedColumns = [], activeTraceColumn }: NodeDetailPanelProps) {
  const upstreamEdges = edges.filter((e) => e.target_node === node.id);
  const downstreamEdges = edges.filter((e) => e.source_node === node.id);
  const [sqlModalOpen, setSqlModalOpen] = useState(false);
  const [hoveredColumn, setHoveredColumn] = useState<string | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ top: number; right: number } | null>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const openTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const closeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const getColumnMappings = useCallback(
    (columnName: string) => {
      const raw: { mapping: ColumnMapping; sourceNode: string }[] = [];
      for (const edge of upstreamEdges) {
        for (const mapping of edge.column_mappings) {
          if (mapping.target_column === columnName) {
            raw.push({ mapping, sourceNode: edge.source_node });
          }
        }
      }
      // Deduplicate: same target_column + transformation + expression → merge source_columns
      const deduped: { mapping: ColumnMapping; sourceNode: string }[] = [];
      const seen = new Map<string, number>();
      for (const entry of raw) {
        const key = `${entry.mapping.transformation}|${entry.mapping.expression ?? ""}`;
        const idx = seen.get(key);
        if (idx !== undefined) {
          // Merge source columns from duplicate
          const existing = deduped[idx].mapping;
          const merged = new Set([...existing.source_columns, ...entry.mapping.source_columns]);
          deduped[idx] = {
            ...deduped[idx],
            mapping: { ...existing, source_columns: [...merged] },
          };
        } else {
          seen.set(key, deduped.length);
          deduped.push(entry);
        }
      }
      return deduped;
    },
    [upstreamEdges]
  );

  const dismissTooltip = useCallback(() => {
    closeTimerRef.current = setTimeout(() => {
      setHoveredColumn(null);
      setTooltipPos(null);
    }, 150);
  }, []);

  const cancelDismiss = useCallback(() => {
    if (closeTimerRef.current) {
      clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
  }, []);

  const handleColumnMouseEnter = useCallback(
    (columnName: string, e: React.MouseEvent<HTMLDivElement>) => {
      cancelDismiss();
      if (openTimerRef.current) clearTimeout(openTimerRef.current);

      const mappings = getColumnMappings(columnName);
      if (mappings.length === 0) return;

      const rowRect = e.currentTarget.getBoundingClientRect();
      const panelRect = panelRef.current?.getBoundingClientRect();
      if (!panelRect) return;

      openTimerRef.current = setTimeout(() => {
        setHoveredColumn(columnName);
        setTooltipPos({
          top: rowRect.top,
          right: window.innerWidth - panelRect.left + 8,
        });
      }, 300);
    },
    [getColumnMappings, cancelDismiss]
  );

  const handleColumnMouseLeave = useCallback(() => {
    if (openTimerRef.current) {
      clearTimeout(openTimerRef.current);
      openTimerRef.current = null;
    }
    dismissTooltip();
  }, [dismissTooltip]);

  return (
    <div ref={panelRef} className="w-80 glass-elevated border-l border-[var(--border-subtle)] overflow-y-auto flex flex-col animate-fade-in-right">
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
            {node.columns.map((col) => {
              const isHighlighted = highlightedColumns.includes(col.name);
              const isActiveOrigin = activeTraceColumn === col.name;
              return (
                <div
                  key={col.name}
                  onClick={() => onColumnClick?.(node.id, col.name)}
                  onMouseEnter={(e) => handleColumnMouseEnter(col.name, e)}
                  onMouseLeave={handleColumnMouseLeave}
                  className={`flex items-center gap-2 px-3 py-1.5 text-xs border-b border-[var(--border-subtle)] last:border-b-0 transition-colors ${
                    onColumnClick ? "cursor-pointer hover:bg-[var(--bg-hover)]" : ""
                  } ${
                    isActiveOrigin
                      ? "bg-cyan-500/20 border-l-2 border-l-[var(--accent-cyan)]"
                      : isHighlighted
                        ? "bg-cyan-500/10 border-l-2 border-l-[var(--accent-cyan)]/50"
                        : ""
                  }`}
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
              );
            })}
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

      {/* Column mapping tooltip (portaled to body to escape backdrop-filter containing block) */}
      {hoveredColumn && tooltipPos && createPortal(
        <ColumnMappingTooltip
          mappings={getColumnMappings(hoveredColumn)}
          style={{
            top: tooltipPos.top,
            right: tooltipPos.right,
          }}
          onMouseEnter={cancelDismiss}
          onMouseLeave={dismissTooltip}
        />,
        document.body
      )}
    </div>
  );
}
