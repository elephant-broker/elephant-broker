import type { SearchResult } from "./types.js";

// Re-exported from the shared cross-plugin helper so the memory plugin and the
// context plugin cannot drift: both import from `openclaw-plugins/shared`.
// See that module for the extraction contract and the 5-102 RC-A hardening.
export { stripOpenClawEnvelope } from "../../shared/envelope.js";

/**
 * Format memory results for auto-recall context injection.
 * Returns XML-tagged string without fact IDs (just background knowledge).
 */
export function formatMemoryContext(results: SearchResult[]): string {
  if (results.length === 0) return "";

  const lines = results.map((r) => {
    const conf = r.confidence.toFixed(2);
    return `- [${r.category}] ${r.text} (confidence: ${conf})`;
  });

  return [
    '<relevant-memories source="elephantbroker">',
    ...lines,
    "</relevant-memories>",
  ].join("\n");
}
