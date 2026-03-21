import { useMemo, useRef, useEffect, useState, useCallback } from "react";
import type { LineageGraph } from "../types/graph";
import type { ColumnTraceEntry } from "../utils/lineageTraversal";
import type { TraceOrigin } from "../hooks/useColumnSearch";
import { buildTracePaths, type ChainStep } from "../utils/traceChain";

interface TraceBreadcrumbProps {
  graph: LineageGraph;
  activeTrace: ColumnTraceEntry[];
  traceOrigin: TraceOrigin;
  onStepClick: (nodeId: string) => void;
  onZoomToAll: () => void;
}

function ChevronIcon() {
  return (
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
  );
}

function StepButton({
  step,
  onClick,
  originRef,
}: {
  step: ChainStep;
  onClick: () => void;
  originRef?: React.Ref<HTMLButtonElement>;
}) {
  return (
    <button
      ref={originRef}
      onClick={onClick}
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
  );
}

/** Render a row of steps with chevrons between them, right-aligned */
function StepRow({
  steps,
  onStepClick,
  originRef,
}: {
  steps: ChainStep[];
  onStepClick: (nodeId: string) => void;
  originRef?: React.Ref<HTMLButtonElement>;
}) {
  return (
    <div className="flex items-center gap-1 shrink-0 ml-auto">
      {steps.map((step, i) => (
        <div key={`${step.nodeId}-${step.columnName}`} className="flex items-center gap-1 shrink-0">
          {i > 0 && <ChevronIcon />}
          <StepButton
            step={step}
            onClick={() => onStepClick(step.nodeId)}
            originRef={step.isOrigin ? originRef : undefined}
          />
        </div>
      ))}
    </div>
  );
}

/**
 * Split paths into branches (divergent prefixes) and a shared suffix.
 * Compares from the right side of each path to find the longest common suffix.
 */
function splitPaths(paths: ChainStep[][]): { branches: ChainStep[][]; suffix: ChainStep[] } {
  if (paths.length <= 1) return { branches: paths, suffix: [] };

  const minLen = Math.min(...paths.map((p) => p.length));
  let suffixLen = 0;

  for (let i = 1; i <= minLen; i++) {
    const ref = paths[0][paths[0].length - i];
    const allMatch = paths.every((p) => {
      const step = p[p.length - i];
      return step.nodeId === ref.nodeId && step.columnName === ref.columnName;
    });
    if (allMatch) suffixLen = i;
    else break;
  }

  const suffix = paths[0].slice(paths[0].length - suffixLen);
  const branches = paths.map((p) => p.slice(0, p.length - suffixLen));

  return { branches, suffix };
}

/**
 * Bracket SVG connecting N branches to a single point on the right.
 * Measures the actual branch container height dynamically.
 */
function BranchBracket({ containerRef }: { containerRef: React.RefObject<HTMLDivElement | null> }) {
  const [dims, setDims] = useState<{ heights: number[]; totalHeight: number } | null>(null);

  const measure = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    const rows = Array.from(el.children) as HTMLElement[];
    const heights = rows.map((r) => r.offsetHeight);
    const totalHeight = el.offsetHeight;
    setDims({ heights, totalHeight });
  }, [containerRef]);

  useEffect(() => {
    measure();
    const ro = new ResizeObserver(measure);
    if (containerRef.current) ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, [measure, containerRef]);

  if (!dims || dims.heights.length === 0) return null;

  const { heights, totalHeight } = dims;
  const bracketWidth = 12;
  const midY = totalHeight / 2;

  // Compute the vertical center of each row
  let accum = 0;
  const centers = heights.map((h) => {
    const c = accum + h / 2;
    accum += h;
    return c;
  });

  return (
    <svg
      width={bracketWidth}
      height={totalHeight}
      viewBox={`0 0 ${bracketWidth} ${totalHeight}`}
      className="shrink-0 mx-1.5"
      fill="none"
      stroke="var(--text-muted)"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      style={{ opacity: 0.45 }}
    >
      {/* Horizontal tick from each row center to the vertical bar */}
      {centers.map((y, i) => (
        <line key={i} x1="0" y1={y} x2={bracketWidth * 0.4} y2={y} />
      ))}
      {/* Vertical bar connecting all ticks */}
      <line x1={bracketWidth * 0.4} y1={centers[0]} x2={bracketWidth * 0.4} y2={centers[centers.length - 1]} />
      {/* Horizontal line from bar center to the right (toward suffix) */}
      <line x1={bracketWidth * 0.4} y1={midY} x2={bracketWidth} y2={midY} />
    </svg>
  );
}

export function TraceBreadcrumb({
  graph,
  activeTrace,
  traceOrigin,
  onStepClick,
  onZoomToAll,
}: TraceBreadcrumbProps) {
  const paths = useMemo(
    () => buildTracePaths(graph, traceOrigin, activeTrace),
    [graph, traceOrigin, activeTrace]
  );

  const { branches, suffix } = useMemo(() => splitPaths(paths), [paths]);

  const scrollRef = useRef<HTMLDivElement>(null);
  const originRef = useRef<HTMLButtonElement>(null);
  const branchesRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (originRef.current && scrollRef.current) {
      originRef.current.scrollIntoView({ inline: "center", block: "nearest" });
    }
  }, [traceOrigin.nodeId, traceOrigin.columnName]);

  const totalSteps = paths[0]?.length ?? 0;
  if (paths.length === 0 || totalSteps <= 1) return null;

  const isSinglePath = branches.length <= 1;

  return (
    <div className="glass border-b border-[var(--border-subtle)] px-4 py-1.5 flex items-center gap-2 animate-fade-in">
      <span className="text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)] font-[var(--font-mono)] shrink-0 self-start mt-0.5">
        Trace
      </span>
      <div ref={scrollRef} className="flex items-center overflow-x-auto scrollbar-none min-w-0 flex-1">
        {isSinglePath ? (
          <StepRow
            steps={paths[0]}
            onStepClick={onStepClick}
            originRef={originRef}
          />
        ) : (
          <div className="flex items-center">
            {/* Divergent branches — right-aligned so they stack flush against the bracket */}
            <div ref={branchesRef} className="flex flex-col gap-1 shrink-0">
              {branches.map((branch, i) => (
                <StepRow key={i} steps={branch} onStepClick={onStepClick} />
              ))}
            </div>
            {/* Bracket connecting branches */}
            <BranchBracket containerRef={branchesRef} />
            {/* Shared suffix */}
            {suffix.length > 0 && (
              <div className="flex items-center gap-1 shrink-0">
                {suffix.map((step, i) => (
                  <div key={`${step.nodeId}-${step.columnName}`} className="flex items-center gap-1 shrink-0">
                    {i > 0 && <ChevronIcon />}
                    <StepButton
                      step={step}
                      onClick={() => onStepClick(step.nodeId)}
                      originRef={step.isOrigin ? originRef : undefined}
                    />
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
      <button
        onClick={onZoomToAll}
        className="shrink-0 ml-1 p-1 rounded-md border border-[var(--border-subtle)] bg-[var(--bg-deep)] text-[var(--text-muted)] hover:bg-[var(--bg-hover)] hover:text-[var(--text-primary)] transition-colors self-start mt-0.5"
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
