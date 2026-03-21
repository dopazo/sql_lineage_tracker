import { useMemo, useRef, useEffect } from "react";
import type { LineageGraph } from "../types/graph";
import type { ColumnTraceEntry } from "../utils/lineageTraversal";
import type { TraceOrigin } from "../hooks/useColumnSearch";
import { buildOrderedChain } from "../utils/traceChain";

interface TraceBreadcrumbProps {
  graph: LineageGraph;
  activeTrace: ColumnTraceEntry[];
  traceOrigin: TraceOrigin;
  onStepClick: (nodeId: string) => void;
  onZoomToAll: () => void;
}

export function TraceBreadcrumb({
  graph,
  activeTrace,
  traceOrigin,
  onStepClick,
  onZoomToAll,
}: TraceBreadcrumbProps) {
  const { upstream, origin, downstream } = useMemo(
    () => buildOrderedChain(graph, traceOrigin, activeTrace),
    [graph, traceOrigin, activeTrace]
  );

  const scrollRef = useRef<HTMLDivElement>(null);
  const originRef = useRef<HTMLButtonElement>(null);

  // Auto-scroll to origin step on mount
  useEffect(() => {
    if (originRef.current && scrollRef.current) {
      originRef.current.scrollIntoView({ inline: "center", block: "nearest" });
    }
  }, [origin.nodeId, origin.columnName]);

  const allSteps = [...upstream, origin, ...downstream];
  if (allSteps.length <= 1) return null;

  return (
    <div className="glass border-b border-[var(--border-subtle)] px-4 py-1.5 flex items-center gap-2 animate-fade-in">
      <span className="text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)] font-[var(--font-mono)] shrink-0">
        Trace
      </span>
      <div
        ref={scrollRef}
        className="flex items-center gap-1 overflow-x-auto scrollbar-none"
      >
        {allSteps.map((step, i) => (
          <div key={`${step.nodeId}-${step.columnName}`} className="flex items-center gap-1 shrink-0">
            {i > 0 && (
              <svg
                className="text-[var(--text-muted)] shrink-0 opacity-50"
                width="10"
                height="10"
                viewBox="0 0 16 16"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
              >
                <path d="M6 3l5 5-5 5" />
              </svg>
            )}
            <button
              ref={step.isOrigin ? originRef : undefined}
              onClick={() => onStepClick(step.nodeId)}
              className={`text-[11px] font-[var(--font-mono)] px-2 py-0.5 rounded-md border transition-colors whitespace-nowrap ${
                step.isOrigin
                  ? "bg-cyan-500/15 text-[var(--accent-cyan)] border-cyan-500/30"
                  : "bg-[var(--bg-deep)] text-[var(--text-secondary)] border-[var(--border-subtle)] hover:bg-[var(--bg-hover)] hover:text-[var(--text-primary)]"
              }`}
              title={`${step.nodeName}.${step.columnDisplay}`}
            >
              <span className="opacity-60">
                {step.nodeName.split(".").pop()}.
              </span>
              {step.columnDisplay}
            </button>
          </div>
        ))}
      </div>
      <button
        onClick={onZoomToAll}
        className="shrink-0 ml-1 p-1 rounded-md border border-[var(--border-subtle)] bg-[var(--bg-deep)] text-[var(--text-muted)] hover:bg-[var(--bg-hover)] hover:text-[var(--text-primary)] transition-colors"
        title="Zoom to full trace"
      >
        <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M2 5V2h3" />
          <path d="M14 5V2h-3" />
          <path d="M2 11v3h3" />
          <path d="M14 11v3h-3" />
        </svg>
      </button>
    </div>
  );
}
