import type { LineageEdge } from "../types/graph";
import { TRANSFORM_STYLES } from "../constants/transforms";

interface EdgeDetailPanelProps {
  edge: LineageEdge;
  onClose: () => void;
  onEdit?: (edge: LineageEdge) => void;
}

export function EdgeDetailPanel({ edge, onClose, onEdit }: EdgeDetailPanelProps) {
  return (
    <div className="w-80 glass-elevated border-l border-[var(--border-subtle)] overflow-y-auto flex flex-col animate-fade-in-right">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-[var(--border-subtle)]">
        <h2 className="text-sm font-semibold text-[var(--text-primary)]">
          Edge Detail
        </h2>
        <div className="flex items-center gap-1">
          {onEdit && (
            <button
              onClick={() => onEdit(edge)}
              className="px-2 py-1 rounded-md text-xs text-[var(--accent-purple)] hover:bg-[var(--bg-hover)] transition-colors"
            >
              Edit
            </button>
          )}
          <button
            onClick={onClose}
            className="w-6 h-6 rounded-md flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-hover)] transition-colors"
          >
            &times;
          </button>
        </div>
      </div>

      <div className="p-4 space-y-5 flex-1">
        {/* Connection */}
        <div>
          <div className="label-dark">Connection</div>
          <div className="text-sm flex items-center gap-2 font-[var(--font-mono)]">
            <span className="text-[var(--accent-cyan)]">{edge.source_node}</span>
            <svg width="16" height="10" viewBox="0 0 16 10" className="text-[var(--text-muted)] shrink-0">
              <path d="M0 5h13M10 1l4 4-4 4" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
            <span className="text-[var(--accent-teal)]">{edge.target_node}</span>
          </div>
        </div>

        {/* Type */}
        <div>
          <div className="label-dark">Type</div>
          <div className="text-sm text-[var(--text-primary)]">
            {edge.edge_type}
            {edge.description && (
              <span className="text-[var(--text-muted)] ml-2">
                &mdash; {edge.description}
              </span>
            )}
          </div>
        </div>

        {/* Column Mappings */}
        <div>
          <div className="label-dark">
            Column Mappings ({edge.column_mappings.length})
          </div>
          <div className="space-y-2">
            {edge.column_mappings.map((mapping, i) => (
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
                  <code className="block mt-2 text-[11px] text-[var(--text-secondary)] bg-[var(--bg-surface)] px-2 py-1.5 rounded font-[var(--font-mono)]">
                    {mapping.expression}
                  </code>
                )}
                {mapping.description && (
                  <div className="mt-1.5 text-[var(--text-muted)]">
                    {mapping.description}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
