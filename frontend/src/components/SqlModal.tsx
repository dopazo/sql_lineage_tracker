import { useState, useCallback } from "react";
import { createPortal } from "react-dom";
import { Highlight, themes } from "prism-react-renderer";

interface SqlModalProps {
  code: string;
  title: string;
  onClose: () => void;
}

export function SqlModal({ code, title, onClose }: SqlModalProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [code]);

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
          className="glass-elevated rounded-xl w-full max-w-4xl max-h-[85vh] flex flex-col animate-fade-in pointer-events-auto"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between px-5 py-4 border-b border-[var(--border-subtle)]">
            <div className="min-w-0 flex-1">
              <h2 className="text-sm font-semibold text-[var(--text-primary)]">
                SQL
              </h2>
              <div className="text-xs font-[var(--font-mono)] text-[var(--text-muted)] truncate" title={title}>
                {title}
              </div>
            </div>
            <div className="flex items-center gap-2 ml-4 flex-shrink-0">
              <button
                onClick={handleCopy}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md bg-[var(--bg-hover)] text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--border-subtle)] transition-colors font-medium"
              >
                {copied ? (
                  <>
                    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <polyline points="4 8 7 11 12 5" />
                    </svg>
                    Copied
                  </>
                ) : (
                  <>
                    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                      <rect x="5" y="5" width="9" height="9" rx="1" />
                      <path d="M11 5V3a1 1 0 00-1-1H3a1 1 0 00-1 1v7a1 1 0 001 1h2" />
                    </svg>
                    Copy
                  </>
                )}
              </button>
              <button
                onClick={onClose}
                className="w-6 h-6 rounded-md flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-hover)] transition-colors"
              >
                &times;
              </button>
            </div>
          </div>

          {/* SQL Content */}
          <div className="flex-1 overflow-auto p-5">
            <Highlight theme={themes.nightOwl} code={code.trim()} language="sql">
              {({ style, tokens, getLineProps, getTokenProps }) => (
                <pre
                  className="text-sm p-4 rounded-lg border border-[var(--border-subtle)] overflow-x-auto whitespace-pre font-[var(--font-mono)] leading-relaxed"
                  style={{ ...style, background: "var(--bg-deep)" }}
                >
                  {tokens.map((line, i) => (
                    <div key={i} {...getLineProps({ line })}>
                      <span className="inline-block w-10 text-right mr-4 text-[var(--text-muted)] select-none opacity-50 text-xs">
                        {i + 1}
                      </span>
                      {line.map((token, key) => (
                        <span key={key} {...getTokenProps({ token })} />
                      ))}
                    </div>
                  ))}
                </pre>
              )}
            </Highlight>
          </div>
        </div>
      </div>
    </>,
    document.body
  );
}
