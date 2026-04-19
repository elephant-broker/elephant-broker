import type { SearchResult } from "./types.js";

/**
 * OpenClaw wraps user prompts in a sender-metadata envelope before forwarding
 * to hooks. Retrieval needs the user's text alone — not the envelope — or
 * similarity search matches on the metadata preamble and returns 0 hits.
 *
 * Envelope shape:
 *   Sender (untrusted metadata):
 *   ```json
 *   {...}
 *   ```
 *
 *   [YYYY-MM-DD HH:MM UTC] <user's actual text>
 *
 * Extract the portion after the last `\n[<timestamp>] ` marker. For
 * non-enveloped prompts the regex misses and the raw trimmed input is returned.
 *
 * NOTE: duplicated from elephantbroker-context/src/engine.ts rather than
 * importing cross-package. Both plugins are small siblings and adding a
 * cross-package dep would require hoisting into a shared package.
 */
export function stripOpenClawEnvelope(prompt: string): string {
  if (!prompt) return "";
  const match = prompt.match(/\n\[[^\]]+\]\s+([\s\S]+)$/);
  return match ? match[1].trim() : prompt.trim();
}

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
