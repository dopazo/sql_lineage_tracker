export interface FuzzyMatch {
  score: number;
  indices: number[];
}

/**
 * Fuzzy match a pattern against text (fzf-style).
 * Returns null if no match, or { score, indices } with matched character positions.
 */
export function fuzzyMatch(pattern: string, text: string): FuzzyMatch | null {
  if (pattern.length === 0) return { score: 0, indices: [] };
  if (pattern.length > text.length) return null;

  const pLower = pattern.toLowerCase();
  const tLower = text.toLowerCase();

  // Quick check: all pattern chars exist in text (in order)
  let pi = 0;
  for (let ti = 0; ti < tLower.length && pi < pLower.length; ti++) {
    if (tLower[ti] === pLower[pi]) pi++;
  }
  if (pi < pLower.length) return null;

  // Exact substring match gets highest score
  const substringIdx = tLower.indexOf(pLower);
  if (substringIdx !== -1) {
    const indices = Array.from({ length: pattern.length }, (_, i) => substringIdx + i);
    let score = 100 + pattern.length * 10;
    if (substringIdx === 0) score += 50; // prefix bonus
    // Word boundary bonus
    if (substringIdx === 0 || isWordBoundary(text, substringIdx)) score += 30;
    // Shorter text = better match
    score -= (text.length - pattern.length) * 0.5;
    return { score, indices };
  }

  // Fuzzy matching with scoring
  const indices: number[] = [];
  let score = 0;
  let lastMatchIdx = -1;

  pi = 0;
  for (let ti = 0; ti < tLower.length && pi < pLower.length; ti++) {
    if (tLower[ti] !== pLower[pi]) continue;

    indices.push(ti);

    // Consecutive match bonus
    if (lastMatchIdx === ti - 1) {
      score += 15;
    } else {
      // Gap penalty (smaller gap = better)
      if (lastMatchIdx >= 0) {
        score -= (ti - lastMatchIdx - 1) * 0.5;
      }
    }

    // Word boundary bonus (after _, ., space, or camelCase transition)
    if (ti === 0 || isWordBoundary(text, ti)) {
      score += 20;
    }

    // First character bonus
    if (ti === 0 && pi === 0) score += 25;

    score += 5; // base match score

    lastMatchIdx = ti;
    pi++;
  }

  if (pi < pLower.length) return null;

  // Length penalty
  score -= (text.length - pattern.length) * 0.3;

  return { score, indices };
}

function isWordBoundary(text: string, idx: number): boolean {
  if (idx === 0) return true;
  const prev = text[idx - 1];
  const curr = text[idx];
  return (
    prev === "_" ||
    prev === "." ||
    prev === " " ||
    prev === "-" ||
    (prev >= "a" && prev <= "z" && curr >= "A" && curr <= "Z")
  );
}
