import { useMemo, useState, useRef, useCallback } from "react";
import { createPortal } from "react-dom";
import type { LineageGraph } from "../types/graph";
import {
  findOrigins,
  type OriginEntry,
  type OriginTargetMapping,
} from "../utils/lineageTraversal";
import { TRANSFORM_STYLES } from "../constants/transforms";

interface OriginsModalProps {
  graph: LineageGraph;
  nodeId: string;
  nodeName: string;
  onClose: () => void;
}

type Tab = "tables" | "columns";

/* ── Copy Button ──────────────────────────────────────────── */

function CopyBtn({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1800);
      }}
      className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-all border border-[var(--border-subtle)] hover:border-[var(--border-medium)] hover:bg-[var(--bg-hover)] text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
    >
      {copied ? (
        <>
          <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="var(--accent-emerald)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M3 8.5l3 3 7-7" />
          </svg>
          <span className="text-[var(--accent-emerald)]">Copied</span>
        </>
      ) : (
        <>
          <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <rect x="5" y="5" width="9" height="9" rx="1.5" />
            <path d="M11 5V3.5A1.5 1.5 0 009.5 2h-6A1.5 1.5 0 002 3.5v6A1.5 1.5 0 003.5 11H5" />
          </svg>
          {label}
        </>
      )}
    </button>
  );
}

/* ── Chain Tooltip ─────────────────────────────────────────── */

function ChainTooltip({
  mapping,
  style,
  onMouseEnter,
  onMouseLeave,
}: {
  mapping: OriginTargetMapping;
  style: React.CSSProperties;
  onMouseEnter: () => void;
  onMouseLeave: () => void;
}) {
  return (
    <div
      className="fixed z-[110] w-56 glass-elevated border border-[var(--border-medium)] rounded-lg shadow-xl animate-fade-in select-text"
      style={style}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className="p-3 space-y-0">
        {[...mapping.chain].reverse().map((step, i) => (
          <div key={i}>
            {i > 0 && (
              <div className="flex items-center gap-2 pl-3 py-0.5">
                <svg
                  width="8"
                  height="8"
                  viewBox="0 0 16 16"
                  fill="none"
                  stroke="var(--text-muted)"
                  strokeWidth="2"
                  strokeLinecap="round"
                >
                  <path d="M8 2v12M4 10l4 4 4-4" />
                </svg>
                {step.transformation && (
                  <span
                    className={`px-1.5 py-px rounded text-[9px] font-semibold font-[var(--font-mono)] uppercase tracking-wider leading-tight ${
                      TRANSFORM_STYLES[step.transformation] ?? TRANSFORM_STYLES.unknown
                    }`}
                  >
                    {step.transformation}
                  </span>
                )}
              </div>
            )}
            <div className="px-2 py-1">
              <div className="text-xs font-[var(--font-mono)] text-[var(--text-primary)]">
                {step.columnDisplay}
              </div>
              <div className="text-[10px] font-[var(--font-mono)] text-[var(--text-muted)]">
                {step.nodeName}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

/* ── Target Column Badges (right side of each row) ─────────── */

function TargetBadges({
  mappings,
}: {
  mappings: OriginTargetMapping[];
}) {
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ top: number; left: number } | null>(null);
  const openTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const closeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const cancelDismiss = useCallback(() => {
    if (closeTimerRef.current) {
      clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
  }, []);

  const dismiss = useCallback(() => {
    closeTimerRef.current = setTimeout(() => {
      setHoveredIdx(null);
      setTooltipPos(null);
    }, 150);
  }, []);

  const handleMouseEnter = useCallback(
    (idx: number, e: React.MouseEvent<HTMLSpanElement>) => {
      cancelDismiss();
      if (openTimerRef.current) clearTimeout(openTimerRef.current);
      const rect = e.currentTarget.getBoundingClientRect();
      openTimerRef.current = setTimeout(() => {
        setHoveredIdx(idx);
        setTooltipPos({
          top: rect.bottom + 6,
          left: Math.min(rect.left, window.innerWidth - 240),
        });
      }, 250);
    },
    [cancelDismiss]
  );

  const handleMouseLeave = useCallback(() => {
    if (openTimerRef.current) {
      clearTimeout(openTimerRef.current);
      openTimerRef.current = null;
    }
    dismiss();
  }, [dismiss]);

  return (
    <>
      <div className="flex items-center gap-1 flex-wrap justify-end">
        {mappings.map((m, i) => (
          <span
            key={m.targetColumn}
            onMouseEnter={(e) => handleMouseEnter(i, e)}
            onMouseLeave={handleMouseLeave}
            className="px-1.5 py-0.5 rounded text-[10px] font-[var(--font-mono)] bg-[var(--accent-cyan)]/10 text-[var(--accent-cyan)]/80 cursor-default hover:bg-[var(--accent-cyan)]/20 transition-colors"
          >
            {m.targetColumn}
          </span>
        ))}
      </div>
      {hoveredIdx !== null && tooltipPos && mappings[hoveredIdx]?.chain.length > 1 &&
        createPortal(
          <ChainTooltip
            mapping={mappings[hoveredIdx]}
            style={{ top: tooltipPos.top, left: tooltipPos.left }}
            onMouseEnter={cancelDismiss}
            onMouseLeave={dismiss}
          />,
          document.body
        )}
    </>
  );
}

/* ── Tables View ──────────────────────────────────────────── */

function TablesView({
  tables,
}: {
  tables: { id: string; dataset: string; tableName: string; columnCount: number; targetColumns: string[] }[];
}) {
  return (
    <div className="space-y-1">
      {tables.map((t) => (
        <div
          key={t.id}
          className="flex items-center gap-3 px-4 py-2.5 rounded-lg bg-[var(--bg-deep)] border border-[var(--border-subtle)] hover:border-[var(--border-medium)] transition-colors"
        >
          <span className="w-1.5 h-1.5 rounded-full bg-[var(--accent-teal)] shrink-0" />
          <span className="flex-1 min-w-0 font-[var(--font-mono)] truncate">
            <span className="text-[11px] text-[var(--text-muted)]">{t.dataset}.</span>
            <span className="text-sm font-medium text-[var(--text-primary)]">{t.tableName}</span>
          </span>
          <span className="text-[10px] text-[var(--text-muted)] font-[var(--font-mono)] tabular-nums shrink-0">
            {t.columnCount} col{t.columnCount !== 1 ? "s" : ""}
          </span>
        </div>
      ))}
    </div>
  );
}

/* ── Columns View ─────────────────────────────────────────── */

function ColumnsView({
  grouped,
}: {
  grouped: { id: string; dataset: string; tableName: string; columns: OriginEntry[] }[];
}) {
  return (
    <div className="space-y-3">
      {grouped.map((group) => (
        <div key={group.id}>
          {/* Table header */}
          <div className="flex items-center gap-1 mb-1.5 px-1">
            <span className="w-1.5 h-1.5 rounded-full bg-[var(--accent-teal)] shrink-0 mr-1" />
            <span className="font-[var(--font-mono)]">
              <span className="text-[11px] text-[var(--text-muted)]">{group.dataset}.</span>
              <span className="text-xs font-medium text-[var(--text-secondary)]">{group.tableName}</span>
            </span>
          </div>
          {/* Columns */}
          <div className="bg-[var(--bg-deep)] border border-[var(--border-subtle)] rounded-lg overflow-hidden">
            {group.columns.map((col, i) => (
              <div
                key={`${col.nodeId}-${col.columnName}`}
                className={`flex items-center gap-3 px-4 py-1.5 text-xs ${
                  i < group.columns.length - 1
                    ? "border-b border-[var(--border-subtle)]"
                    : ""
                }`}
              >
                <span className="w-1 h-1 rounded-full bg-[var(--accent-cyan)] shrink-0" />
                <span className="font-[var(--font-mono)] text-[var(--text-primary)] flex-1 min-w-0 truncate">
                  {col.columnDisplay}
                </span>
                <TargetBadges mappings={col.targetMappings} />
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

/* ── Main Modal ───────────────────────────────────────────── */

export function OriginsModal({
  graph,
  nodeId,
  nodeName,
  onClose,
}: OriginsModalProps) {
  const [tab, setTab] = useState<Tab>("tables");

  const origins = useMemo(() => findOrigins(graph, nodeId), [graph, nodeId]);

  // Derive unique tables and grouped columns
  const { tables, grouped } = useMemo(() => {
    const tableMap = new Map<
      string,
      { id: string; dataset: string; tableName: string; columns: OriginEntry[]; targetColumns: Set<string> }
    >();

    for (const o of origins) {
      const key = `${o.dataset}.${o.tableName}`;
      let entry = tableMap.get(key);
      if (!entry) {
        entry = { id: key, dataset: o.dataset, tableName: o.tableName, columns: [], targetColumns: new Set() };
        tableMap.set(key, entry);
      }
      entry.columns.push(o);
      for (const tm of o.targetMappings) {
        entry.targetColumns.add(tm.targetColumn);
      }
    }

    const grouped = [...tableMap.values()].map((g) => ({
      id: g.id,
      dataset: g.dataset,
      tableName: g.tableName,
      columns: g.columns,
    }));

    const tables = [...tableMap.values()].map((g) => ({
      id: g.id,
      dataset: g.dataset,
      tableName: g.tableName,
      columnCount: g.columns.length,
      targetColumns: [...g.targetColumns],
    }));

    return { tables, grouped };
  }, [origins]);

  const tablesText = tables.map((t) => `${t.dataset}.${t.tableName}`).join("\n");
  const columnsText = origins
    .map((o) => `${o.dataset}.${o.tableName}.${o.columnDisplay}`)
    .join("\n");

  return createPortal(
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 z-50 bg-black/50 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="fixed inset-0 z-50 flex items-center justify-center p-8 pointer-events-none">
        <div
          className="glass-elevated rounded-xl w-full max-w-xl max-h-[80vh] flex flex-col animate-fade-in pointer-events-auto"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="px-5 pt-5 pb-0">
            <div className="flex items-start justify-between mb-1">
              <div className="min-w-0 flex-1">
                <h2 className="text-sm font-semibold text-[var(--text-primary)]">
                  Origin Sources
                </h2>
                <div className="text-xs text-[var(--text-muted)] mt-0.5 font-[var(--font-mono)] truncate" title={nodeName}>
                  {nodeName}
                </div>
              </div>

              {/* Stats */}
              <div className="flex items-center gap-3 mr-3 text-[10px] font-[var(--font-mono)] text-[var(--text-muted)] shrink-0 pt-0.5">
                <span>
                  <span className="text-[var(--accent-teal)]">{tables.length}</span>{" "}
                  {tables.length === 1 ? "table" : "tables"}
                </span>
                <span>
                  <span className="text-[var(--accent-cyan)]">{origins.length}</span>{" "}
                  {origins.length === 1 ? "column" : "columns"}
                </span>
              </div>

              <button
                onClick={onClose}
                className="w-6 h-6 rounded-md flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-hover)] transition-colors shrink-0"
              >
                &times;
              </button>
            </div>

            {/* Tab bar + copy */}
            <div className="flex items-center gap-1 mt-4 border-b border-[var(--border-subtle)]">
              <button
                onClick={() => setTab("tables")}
                className={`relative px-4 py-2 text-xs font-medium transition-colors ${
                  tab === "tables"
                    ? "text-[var(--text-primary)]"
                    : "text-[var(--text-muted)] hover:text-[var(--text-secondary)]"
                }`}
              >
                Tables
                {tab === "tables" && (
                  <span className="absolute bottom-0 left-0 right-0 h-[2px] bg-[var(--accent-teal)] rounded-full" />
                )}
              </button>
              <button
                onClick={() => setTab("columns")}
                className={`relative px-4 py-2 text-xs font-medium transition-colors ${
                  tab === "columns"
                    ? "text-[var(--text-primary)]"
                    : "text-[var(--text-muted)] hover:text-[var(--text-secondary)]"
                }`}
              >
                Columns
                {tab === "columns" && (
                  <span className="absolute bottom-0 left-0 right-0 h-[2px] bg-[var(--accent-cyan)] rounded-full" />
                )}
              </button>

              <div className="flex-1" />

              <div className="pb-1.5">
                {tab === "tables" ? (
                  <CopyBtn text={tablesText} label="Copy tables" />
                ) : (
                  <CopyBtn text={columnsText} label="Copy columns" />
                )}
              </div>
            </div>
          </div>

          {/* Content */}
          <div className="flex-1 overflow-y-auto px-5 py-4">
            {origins.length === 0 ? (
              <div className="text-center py-12 text-sm text-[var(--text-muted)]">
                No origin sources found — this node may be a root table.
              </div>
            ) : tab === "tables" ? (
              <TablesView tables={tables} />
            ) : (
              <ColumnsView grouped={grouped} />
            )}
          </div>
        </div>
      </div>
    </>,
    document.body
  );
}
