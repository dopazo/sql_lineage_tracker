import { useRef, useState } from "react";
import type { SearchResult } from "../hooks/useColumnSearch";

interface SearchBarProps {
  query: string;
  onQueryChange: (q: string) => void;
  results: SearchResult[];
  onSelect: (result: SearchResult) => void;
  hasActiveTrace: boolean;
  onClear: () => void;
}

export function SearchBar({
  query,
  onQueryChange,
  results,
  onSelect,
  hasActiveTrace,
  onClear,
}: SearchBarProps) {
  const [open, setOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  return (
    <div className="relative">
      <div className="flex items-center gap-1.5">
        <div className="relative">
          <svg
            className="absolute left-3 top-1/2 -translate-y-1/2 text-[var(--text-muted)]"
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
          <input
            ref={inputRef}
            type="text"
            placeholder="Search columns..."
            value={query}
            onChange={(e) => {
              onQueryChange(e.target.value);
              setOpen(true);
            }}
            onFocus={() => setOpen(true)}
            onBlur={() => setTimeout(() => setOpen(false), 200)}
            className="input-dark w-64 pl-9 py-2"
          />
        </div>
        {hasActiveTrace && (
          <button
            onClick={onClear}
            className="btn-ghost text-xs py-2 px-3 flex items-center gap-1.5"
          >
            <span className="w-1.5 h-1.5 rounded-full bg-[var(--accent-cyan)]" />
            Clear trace
          </button>
        )}
      </div>

      {open && results.length > 0 && (
        <div className="absolute top-full left-0 mt-2 w-96 glass-elevated rounded-xl z-50 max-h-72 overflow-y-auto animate-fade-in">
          {results.map((r, i) => (
            <button
              key={`${r.nodeId}-${r.columnName}-${i}`}
              className="w-full text-left px-4 py-2.5 hover:bg-[var(--bg-hover)] flex items-center gap-2 text-sm transition-colors border-b border-[var(--border-subtle)] last:border-b-0"
              onMouseDown={() => {
                onSelect(r);
                setOpen(false);
              }}
            >
              <span className="font-medium text-[var(--text-primary)] font-[var(--font-mono)]">
                {r.columnName}
              </span>
              <span className="text-[var(--text-muted)] text-xs font-[var(--font-mono)]">
                {r.dataType}
              </span>
              <span className="text-[var(--text-muted)] ml-auto text-xs truncate max-w-[140px]">
                {r.nodeName}
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
