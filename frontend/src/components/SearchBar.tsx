import { useRef, useState, useCallback, useMemo } from "react";
import type { SearchResult } from "../hooks/useColumnSearch";

interface SearchBarProps {
  query: string;
  onQueryChange: (q: string) => void;
  results: SearchResult[];
  onSelect: (result: SearchResult) => void;
  hasActiveTrace: boolean;
  /** True when the active trace is a column-level trace (not table) */
  hasColumnTrace: boolean;
  onClear: () => void;
  onOpenTraceDetail?: () => void;
}

/* ── Highlight matched characters (fuzzy) ─────────────────── */
function HighlightMatch({
  text,
  indices,
  className,
}: {
  text: string;
  indices?: number[];
  className?: string;
}) {
  if (!indices || indices.length === 0) {
    return <span className={className}>{text}</span>;
  }

  const indexSet = new Set(indices);
  const parts: { text: string; highlight: boolean }[] = [];
  let current = "";
  let currentHighlight = false;

  for (let i = 0; i < text.length; i++) {
    const isMatch = indexSet.has(i);
    if (i === 0) {
      currentHighlight = isMatch;
      current = text[i];
    } else if (isMatch === currentHighlight) {
      current += text[i];
    } else {
      parts.push({ text: current, highlight: currentHighlight });
      current = text[i];
      currentHighlight = isMatch;
    }
  }
  if (current) parts.push({ text: current, highlight: currentHighlight });

  return (
    <span className={className}>
      {parts.map((p, i) =>
        p.highlight ? (
          <span key={i} className="text-[var(--accent-cyan)] font-semibold">
            {p.text}
          </span>
        ) : (
          <span key={i}>{p.text}</span>
        )
      )}
    </span>
  );
}

/* ── Icons ─────────────────────────────────────────────────── */
function SearchIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <circle cx="11" cy="11" r="8" />
      <path d="m21 21-4.3-4.3" />
    </svg>
  );
}

function TableIcon() {
  return (
    <svg
      className="shrink-0 text-[var(--accent-purple)]"
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <rect x="3" y="3" width="18" height="18" rx="2" />
      <path d="M3 9h18" />
      <path d="M3 15h18" />
      <path d="M9 3v18" />
    </svg>
  );
}

function ColumnIcon() {
  return (
    <svg
      className="shrink-0 text-[var(--accent-cyan)]"
      width="13"
      height="13"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M12 2v20" />
      <path d="M2 12h20" />
    </svg>
  );
}

/* ── Main Component ────────────────────────────────────────── */
export function SearchBar({
  query,
  onQueryChange,
  results,
  onSelect,
  hasActiveTrace,
  hasColumnTrace,
  onClear,
  onOpenTraceDetail,
}: SearchBarProps) {
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // Split results into groups
  const { tableResults, columnResults } = useMemo(() => {
    const tableResults: SearchResult[] = [];
    const columnResults: SearchResult[] = [];
    for (const r of results) {
      if (r.isTableResult) tableResults.push(r);
      else columnResults.push(r);
    }
    return { tableResults, columnResults };
  }, [results]);

  // Flat list for keyboard nav (tables first, then columns)
  const flatResults = useMemo(
    () => [...tableResults, ...columnResults],
    [tableResults, columnResults]
  );

  const showDropdown = open && query.length >= 2;

  const scrollActiveIntoView = useCallback(
    (index: number) => {
      if (!listRef.current) return;
      const items = listRef.current.querySelectorAll("[data-result-item]");
      items[index]?.scrollIntoView({ block: "nearest" });
    },
    []
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (!showDropdown || flatResults.length === 0) return;

      if (e.key === "ArrowDown") {
        e.preventDefault();
        const next =
          activeIndex < flatResults.length - 1 ? activeIndex + 1 : 0;
        setActiveIndex(next);
        scrollActiveIntoView(next);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        const prev =
          activeIndex > 0 ? activeIndex - 1 : flatResults.length - 1;
        setActiveIndex(prev);
        scrollActiveIntoView(prev);
      } else if (e.key === "Enter" && activeIndex >= 0) {
        e.preventDefault();
        onSelect(flatResults[activeIndex]);
        setOpen(false);
        setActiveIndex(-1);
      } else if (e.key === "Escape") {
        setOpen(false);
        setActiveIndex(-1);
        inputRef.current?.blur();
      }
    },
    [showDropdown, flatResults, activeIndex, onSelect, scrollActiveIntoView]
  );

  const handleQueryChange = useCallback(
    (value: string) => {
      onQueryChange(value);
      setOpen(true);
      setActiveIndex(-1);
    },
    [onQueryChange]
  );

  // Track position in the flat list for active index
  let flatIndex = 0;

  return (
    <div className="relative">
      <div className="flex items-center gap-1.5">
        <div className="input-dark w-64 py-2 px-3 flex items-center gap-2">
          <SearchIcon className="shrink-0 text-[var(--text-muted)]" />
          <input
            ref={inputRef}
            type="text"
            placeholder="Search tables or columns..."
            value={query}
            onChange={(e) => handleQueryChange(e.target.value)}
            onFocus={() => setOpen(true)}
            onBlur={() => setTimeout(() => setOpen(false), 200)}
            onKeyDown={handleKeyDown}
            className="w-full bg-transparent text-sm text-[var(--text-primary)] outline-none placeholder:text-[var(--text-muted)]"
          />
          {query.length > 0 && (
            <button
              onMouseDown={(e) => {
                e.preventDefault();
                handleQueryChange("");
              }}
              className="shrink-0 text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors"
            >
              <svg
                width="12"
                height="12"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M18 6 6 18" />
                <path d="m6 6 12 12" />
              </svg>
            </button>
          )}
        </div>
        {hasActiveTrace && (
          <>
            {hasColumnTrace && onOpenTraceDetail && (
              <button
                onClick={onOpenTraceDetail}
                className="btn-ghost text-xs py-2 px-3 flex items-center gap-1.5"
                title="View full column trace detail"
              >
                <svg
                  width="12"
                  height="12"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="var(--accent-cyan)"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <path d="M12 2v20" />
                  <circle cx="12" cy="6" r="2" fill="var(--accent-cyan)" />
                  <circle cx="12" cy="12" r="2" fill="var(--accent-cyan)" />
                  <circle cx="12" cy="18" r="2" fill="var(--accent-cyan)" />
                  <path d="M6 6h4M14 6h4M6 12h4M14 12h4M6 18h4M14 18h4" />
                </svg>
                View trace
              </button>
            )}
            <button
              onClick={onClear}
              className="btn-ghost text-xs py-2 px-3 flex items-center gap-1.5"
            >
              <span className="w-1.5 h-1.5 rounded-full bg-[var(--accent-cyan)]" />
              Clear trace
            </button>
          </>
        )}
      </div>

      {showDropdown && (
        <div
          ref={listRef}
          className="absolute top-full left-0 mt-2 min-w-[28rem] max-w-[36rem] glass-elevated rounded-xl z-50 max-h-80 overflow-y-auto animate-fade-in"
        >
          {flatResults.length === 0 ? (
            /* ── Empty state ──────────────────────────────────── */
            <div className="px-5 py-6 text-center">
              <div className="text-[var(--text-muted)] text-sm">
                No results for{" "}
                <span className="font-[var(--font-mono)] text-[var(--text-secondary)]">
                  "{query}"
                </span>
              </div>
              <div className="text-[var(--text-muted)] text-xs mt-1.5 opacity-70">
                Try searching by table name, dataset, or column
              </div>
            </div>
          ) : (
            <>
              {/* ── Tables section ────────────────────────────── */}
              {tableResults.length > 0 && (
                <div>
                  <div className="px-4 pt-3 pb-1.5 flex items-center gap-2">
                    <span className="text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)] font-[var(--font-mono)]">
                      Tables
                    </span>
                    <span className="text-[10px] text-[var(--text-muted)] bg-[var(--bg-deep)] rounded-full px-1.5 py-0.5 font-[var(--font-mono)]">
                      {tableResults.length}
                    </span>
                  </div>
                  {tableResults.map((r, i) => {
                    const idx = flatIndex++;
                    const isActive = idx === activeIndex;
                    return (
                      <button
                        key={`t-${r.nodeId}-${i}`}
                        data-result-item
                        className={`w-full text-left px-4 py-2.5 flex items-center gap-3 text-sm transition-colors
                          ${isActive ? "bg-[var(--bg-hover)]" : "hover:bg-[var(--bg-hover)]"}
                          border-b border-[var(--border-subtle)] last:border-b-0`}
                        onMouseDown={() => {
                          onSelect(r);
                          setOpen(false);
                          setActiveIndex(-1);
                        }}
                        onMouseEnter={() => setActiveIndex(idx)}
                      >
                        <TableIcon />
                        <div className="flex-1 min-w-0">
                          <HighlightMatch
                            text={r.nodeName}
                            indices={r.matchIndices}
                            className="font-medium text-[var(--text-primary)]"
                          />
                        </div>
                        <span className="shrink-0 text-[10px] font-[var(--font-mono)] uppercase px-1.5 py-0.5 rounded bg-[var(--accent-purple)]/10 text-[var(--accent-purple)]">
                          {r.dataType}
                        </span>
                        <span className="shrink-0 text-[10px] text-[var(--text-muted)] opacity-0 group-hover:opacity-100 transition-opacity">
                          Show connections
                        </span>
                      </button>
                    );
                  })}
                </div>
              )}

              {/* ── Columns section ───────────────────────────── */}
              {columnResults.length > 0 && (
                <div>
                  <div className="px-4 pt-3 pb-1.5 flex items-center gap-2">
                    <span className="text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)] font-[var(--font-mono)]">
                      Columns
                    </span>
                    <span className="text-[10px] text-[var(--text-muted)] bg-[var(--bg-deep)] rounded-full px-1.5 py-0.5 font-[var(--font-mono)]">
                      {columnResults.length}
                    </span>
                  </div>
                  {columnResults.map((r, i) => {
                    const idx = flatIndex++;
                    const isActive = idx === activeIndex;
                    return (
                      <button
                        key={`c-${r.nodeId}-${r.columnName}-${i}`}
                        data-result-item
                        className={`w-full text-left px-4 py-2.5 flex items-start gap-3 text-sm transition-colors
                          ${isActive ? "bg-[var(--bg-hover)]" : "hover:bg-[var(--bg-hover)]"}
                          border-b border-[var(--border-subtle)] last:border-b-0`}
                        onMouseDown={() => {
                          onSelect(r);
                          setOpen(false);
                          setActiveIndex(-1);
                        }}
                        onMouseEnter={() => setActiveIndex(idx)}
                      >
                        <ColumnIcon />
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <HighlightMatch
                              text={r.columnName}
                              indices={r.matchIndices}
                              className="font-medium text-[var(--text-primary)] font-[var(--font-mono)]"
                            />
                            <span className="text-[10px] font-[var(--font-mono)] text-[var(--text-muted)] uppercase">
                              {r.dataType}
                            </span>
                          </div>
                          <div className="text-xs text-[var(--text-muted)] mt-0.5">
                            {r.nodeName}
                          </div>
                        </div>
                        <span className="shrink-0 text-[10px] mt-0.5 text-[var(--accent-cyan)]/60 font-[var(--font-mono)]">
                          trace
                        </span>
                      </button>
                    );
                  })}
                </div>
              )}

              {/* ── Keyboard hint ─────────────────────────────── */}
              <div className="px-4 py-2 border-t border-[var(--border-subtle)] flex items-center gap-3 text-[10px] text-[var(--text-muted)]">
                <span className="flex items-center gap-1">
                  <kbd className="px-1 py-0.5 rounded bg-[var(--bg-deep)] border border-[var(--border-subtle)] font-[var(--font-mono)] text-[9px]">
                    &uarr;&darr;
                  </kbd>
                  navigate
                </span>
                <span className="flex items-center gap-1">
                  <kbd className="px-1 py-0.5 rounded bg-[var(--bg-deep)] border border-[var(--border-subtle)] font-[var(--font-mono)] text-[9px]">
                    &crarr;
                  </kbd>
                  select
                </span>
                <span className="flex items-center gap-1">
                  <kbd className="px-1 py-0.5 rounded bg-[var(--bg-deep)] border border-[var(--border-subtle)] font-[var(--font-mono)] text-[9px]">
                    esc
                  </kbd>
                  close
                </span>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
