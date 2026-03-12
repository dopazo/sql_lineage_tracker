import { useLayoutEffect, useEffect, useRef, useState, useMemo, useCallback } from "react";
import type { LineageNode } from "../types/graph";

interface MenuItem {
  label: string;
  icon: string;
  action: () => void;
  separator?: boolean;
  disabled?: boolean;
  accent?: string;
}

interface NodeContextMenuProps {
  node: LineageNode;
  position: { x: number; y: number };
  hasUpstreamEdges: boolean;
  hasDownstreamEdges: boolean;
  onClose: () => void;
  onViewDetails: () => void;
  onAddUpstream: () => void;
  onAddDownstream: () => void;
  onExpandNode?: () => void;
  onFocusConnections: () => void;
  onCopyName: () => void;
}

export function NodeContextMenu({
  node,
  position,
  hasUpstreamEdges,
  hasDownstreamEdges,
  onClose,
  onViewDetails,
  onAddUpstream,
  onAddDownstream,
  onExpandNode,
  onFocusConnections,
  onCopyName,
}: NodeContextMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);
  const [focusIndex, setFocusIndex] = useState(-1);
  const [adjustedPos, setAdjustedPos] = useState(position);

  const items = useMemo<MenuItem[]>(() => [
    {
      label: "View details",
      icon: "\u2139",
      action: onViewDetails,
    },
    {
      label: "Focus connections",
      icon: "\u29BF",
      action: onFocusConnections,
      disabled: !hasUpstreamEdges && !hasDownstreamEdges,
    },
    {
      label: "Add upstream source",
      icon: "\u2190",
      action: onAddUpstream,
      separator: true,
    },
    {
      label: "Add downstream target",
      icon: "\u2192",
      action: onAddDownstream,
    },
    ...(node.status === "truncated" && onExpandNode
      ? [{
          label: "Expand dependencies",
          icon: "\u25B6",
          action: onExpandNode,
          separator: true,
          accent: "text-blue-400",
        }]
      : []),
    {
      label: "Copy name",
      icon: "\u2398",
      action: onCopyName,
      separator: true,
    },
  ], [
    node.status, hasUpstreamEdges, hasDownstreamEdges,
    onViewDetails, onFocusConnections, onAddUpstream,
    onAddDownstream, onExpandNode, onCopyName,
  ]);

  const enabledIndices = useMemo(
    () => items.map((item, i) => (!item.disabled ? i : -1)).filter((i) => i >= 0),
    [items]
  );

  // Adjust position so menu stays within viewport (before paint)
  useLayoutEffect(() => {
    if (!menuRef.current) return;
    const rect = menuRef.current.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    setAdjustedPos({
      x: rect.right > vw ? position.x - rect.width : position.x,
      y: rect.bottom > vh ? position.y - rect.height : position.y,
    });
  }, [position]);

  // Close on outside click, scroll, escape
  useEffect(() => {
    const onMouseDown = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    const onScroll = () => onClose();

    document.addEventListener("mousedown", onMouseDown);
    document.addEventListener("scroll", onScroll, true);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
      document.removeEventListener("scroll", onScroll, true);
    };
  }, [onClose]);

  // Focus the menu container on mount for immediate keyboard access
  useEffect(() => {
    menuRef.current?.focus();
  }, []);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      switch (e.key) {
        case "Escape":
          e.preventDefault();
          onClose();
          break;
        case "ArrowDown": {
          e.preventDefault();
          const currentPos = enabledIndices.indexOf(focusIndex);
          const next = enabledIndices[(currentPos + 1) % enabledIndices.length];
          setFocusIndex(next);
          break;
        }
        case "ArrowUp": {
          e.preventDefault();
          const currentPos = enabledIndices.indexOf(focusIndex);
          const prev = enabledIndices[(currentPos - 1 + enabledIndices.length) % enabledIndices.length];
          setFocusIndex(prev);
          break;
        }
        case "Enter": {
          e.preventDefault();
          if (focusIndex >= 0 && !items[focusIndex].disabled) {
            items[focusIndex].action();
            onClose();
          }
          break;
        }
      }
    },
    [focusIndex, enabledIndices, items, onClose]
  );

  return (
    <div
      ref={menuRef}
      role="menu"
      aria-label={`Actions for ${node.dataset}.${node.name}`}
      tabIndex={-1}
      className="fixed z-50 min-w-[200px] py-1.5 rounded-lg border border-[var(--border-subtle)] bg-[var(--bg-surface)] shadow-xl shadow-black/40 backdrop-blur-sm outline-none"
      style={{ left: adjustedPos.x, top: adjustedPos.y }}
      onKeyDown={handleKeyDown}
    >
      {/* Header */}
      <div className="px-3 py-1.5 border-b border-[var(--border-subtle)] mb-1">
        <div className="text-[10px] text-[var(--text-muted)] font-[var(--font-mono)]">
          {node.dataset}
        </div>
        <div className="text-xs font-medium text-[var(--text-primary)] truncate max-w-[220px]">
          {node.name}
        </div>
      </div>

      {items.map((item, i) => (
        <div key={item.label}>
          {item.separator && (
            <div className="border-t border-[var(--border-subtle)] my-1" />
          )}
          <button
            role="menuitem"
            aria-disabled={item.disabled}
            className={`w-full flex items-center gap-2.5 px-3 py-1.5 text-xs transition-colors outline-none ${
              item.disabled
                ? "text-[var(--text-muted)] cursor-not-allowed opacity-50"
                : "text-[var(--text-secondary)] hover:bg-[var(--bg-hover)] hover:text-[var(--text-primary)] cursor-pointer"
            } ${i === focusIndex ? "bg-[var(--bg-hover)] text-[var(--text-primary)]" : ""} ${item.accent ?? ""}`}
            onClick={() => {
              if (!item.disabled) {
                item.action();
                onClose();
              }
            }}
            onMouseEnter={() => setFocusIndex(i)}
            disabled={item.disabled}
          >
            <span className="w-4 text-center text-[11px]">{item.icon}</span>
            {item.label}
          </button>
        </div>
      ))}
    </div>
  );
}
