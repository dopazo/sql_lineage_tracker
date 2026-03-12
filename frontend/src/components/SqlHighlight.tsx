import { Highlight, themes } from "prism-react-renderer";

interface SqlHighlightProps {
  code: string;
}

export function SqlHighlight({ code }: SqlHighlightProps) {
  return (
    <Highlight theme={themes.nightOwl} code={code.trim()} language="sql">
      {({ style, tokens, getLineProps, getTokenProps }) => (
        <pre
          className="text-xs p-3 rounded-lg border border-[var(--border-subtle)] overflow-x-auto whitespace-pre-wrap max-h-48 font-[var(--font-mono)] leading-relaxed"
          style={{ ...style, background: "var(--bg-deep)" }}
        >
          {tokens.map((line, i) => (
            <div key={i} {...getLineProps({ line })}>
              {line.map((token, key) => (
                <span key={key} {...getTokenProps({ token })} />
              ))}
            </div>
          ))}
        </pre>
      )}
    </Highlight>
  );
}
