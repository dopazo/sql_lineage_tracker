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
      <div className="flex items-center gap-1">
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
          className="w-64 px-3 py-1.5 text-sm border border-slate-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-400"
        />
        {hasActiveTrace && (
          <button
            onClick={onClear}
            className="px-2 py-1.5 text-xs bg-slate-200 hover:bg-slate-300 rounded-md"
          >
            Clear
          </button>
        )}
      </div>

      {open && results.length > 0 && (
        <div className="absolute top-full left-0 mt-1 w-96 bg-white border border-slate-200 rounded-md shadow-lg z-50 max-h-64 overflow-y-auto">
          {results.map((r, i) => (
            <button
              key={`${r.nodeId}-${r.columnName}-${i}`}
              className="w-full text-left px-3 py-2 hover:bg-slate-50 flex items-center gap-2 text-sm"
              onMouseDown={() => {
                onSelect(r);
                setOpen(false);
              }}
            >
              <span className="font-medium text-slate-800">
                {r.columnName}
              </span>
              <span className="text-slate-400">{r.dataType}</span>
              <span className="text-slate-400 ml-auto text-xs truncate">
                {r.nodeName}
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
