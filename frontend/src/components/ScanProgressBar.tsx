import { useEffect, useRef, useState, useCallback } from "react";

interface ScanProgressBarProps {
  messages: string[];
  scanning: boolean;
  error: string | null;
  completed?: boolean;
  onDismiss?: () => void;
  /** Delay in ms before auto-dismiss starts after completion (default: 3000) */
  autoDismissDelay?: number;
}

export function ScanProgressBar({
  messages,
  scanning,
  error,
  completed,
  onDismiss,
  autoDismissDelay = 3000,
}: ScanProgressBarProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [fadingOut, setFadingOut] = useState(false);
  const [autoDismissCancelled, setAutoDismissCancelled] = useState(false);
  const fadeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dismissTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    const el = scrollRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
    }
  }, [messages]);

  // Reset state when a new scan starts
  useEffect(() => {
    if (scanning) {
      setFadingOut(false);
      setAutoDismissCancelled(false);
      if (fadeTimerRef.current) clearTimeout(fadeTimerRef.current);
      if (dismissTimerRef.current) clearTimeout(dismissTimerRef.current);
    }
  }, [scanning]);

  // Cleanup all timers on unmount
  useEffect(() => {
    return () => {
      if (fadeTimerRef.current) clearTimeout(fadeTimerRef.current);
      if (dismissTimerRef.current) clearTimeout(dismissTimerRef.current);
    };
  }, []);

  const startFadeOut = useCallback(() => {
    setFadingOut(true);
    // After fade animation completes (5s), actually dismiss
    dismissTimerRef.current = setTimeout(() => {
      onDismiss?.();
    }, 5000);
  }, [onDismiss]);

  // Auto-dismiss: start fade-out after delay when completed (not cancelled by hover)
  useEffect(() => {
    if (!completed || scanning || error || autoDismissCancelled) {
      if (fadeTimerRef.current) {
        clearTimeout(fadeTimerRef.current);
        fadeTimerRef.current = null;
      }
      return;
    }

    fadeTimerRef.current = setTimeout(() => {
      startFadeOut();
    }, autoDismissDelay);

    return () => {
      if (fadeTimerRef.current) {
        clearTimeout(fadeTimerRef.current);
        fadeTimerRef.current = null;
      }
    };
  }, [completed, scanning, error, autoDismissCancelled, autoDismissDelay, startFadeOut]);

  const handleMouseEnter = useCallback(() => {
    // If already fading out, cancel it
    if (fadingOut) {
      setFadingOut(false);
      if (dismissTimerRef.current) {
        clearTimeout(dismissTimerRef.current);
        dismissTimerRef.current = null;
      }
    }
    // Cancel auto-dismiss permanently for this scan
    setAutoDismissCancelled(true);
  }, [fadingOut]);

  if (!scanning && messages.length === 0 && !error) return null;

  return (
    <div
      className="rounded-lg bg-[var(--bg-deep)] border border-[var(--border-subtle)] overflow-hidden transition-opacity duration-[5000ms] ease-out"
      style={{ opacity: fadingOut ? 0 : 1 }}
      onMouseEnter={handleMouseEnter}
    >
      {/* Animated scan line */}
      {scanning && (
        <div className="h-0.5 bg-[var(--bg-elevated)] overflow-hidden">
          <div
            className="h-full w-1/3 bg-gradient-to-r from-transparent via-[var(--accent-cyan)] to-transparent"
            style={{ animation: "scan-line 1.5s ease-in-out infinite" }}
          />
        </div>
      )}

      {/* Completed accent line */}
      {completed && !scanning && (
        <div className="h-0.5 bg-[var(--accent-cyan)]/30" />
      )}

      <div className="px-4 py-3">
        <div className="flex items-center justify-between mb-2">
          {scanning && (
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-[var(--accent-cyan)] animate-pulse-glow" />
              <span className="text-xs font-medium text-[var(--accent-cyan)] font-[var(--font-mono)] uppercase tracking-wider">
                Scanning
              </span>
            </div>
          )}

          {completed && !scanning && (
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-emerald-400" />
              <span className="text-xs font-medium text-emerald-400 font-[var(--font-mono)] uppercase tracking-wider">
                Complete
              </span>
            </div>
          )}

          {error && !scanning && !completed && (
            <div className="text-sm text-red-400 flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-red-400" />
              {error}
            </div>
          )}

          {!scanning && (messages.length > 0 || error) && onDismiss && (
            <button
              onClick={onDismiss}
              className="text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors p-0.5 -mr-1"
              title="Close"
            >
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
                <path d="M3 3l8 8M11 3l-8 8" />
              </svg>
            </button>
          )}
        </div>

        {error && (completed || scanning) && (
          <div className="text-sm text-red-400 mb-2 flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-red-400" />
            {error}
          </div>
        )}

        <div ref={scrollRef} className="max-h-48 overflow-y-auto space-y-0.5">
          {messages.map((msg, i) => (
            <div
              key={i}
              className="text-xs text-[var(--text-muted)] font-[var(--font-mono)]"
              style={{
                opacity: i === messages.length - 1 ? 1 : 0.5,
              }}
            >
              <span className="text-[var(--text-muted)] mr-2 select-none">
                {">"}
              </span>
              {msg}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
