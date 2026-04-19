/**
 * Canonical OpenClaw envelope-stripping helper, shared by both ElephantBroker
 * plugins (context-engine and memory). Before this module existed, the two
 * plugins carried byte-identical duplicates of this function in
 * `elephantbroker-context/src/engine.ts` and `elephantbroker-memory/src/format.ts`
 * (PR #5 TODOs 5-004 / 5-104 / 5-206). The duplication was a live drift hazard:
 * a regex fix landed on one copy could silently skip the other.
 *
 * OpenClaw wraps user prompts in a sender-metadata envelope before forwarding
 * them to plugin hooks. Retrieval needs the user's plain text, not the
 * envelope — otherwise similarity search matches on the metadata preamble
 * and returns 0 hits. Envelope shape:
 *
 *   Sender (untrusted metadata):
 *   ```json
 *   {...}
 *   ```
 *
 *   [YYYY-MM-DD HH:MM UTC] <user's actual text>
 *
 * Extraction rules (PR #5, TODO 5-102 RC-A hardening):
 *
 * 1. Envelope detection is gated on the literal `Sender (untrusted metadata):`
 *    prefix. A non-enveloped prompt that happens to contain a bracketed token
 *    (e.g. `[ref]` inside the user's own sentence) is returned trimmed as-is.
 *    The previous regex-only match falsely identified such prompts as enveloped
 *    and truncated them.
 *
 * 2. The regex uses a greedy preamble (`^[\s\S]*\n`) so the capture anchors on
 *    the LAST `\n[<marker>] ` in the input — the envelope's timestamp marker
 *    always sits at the end. A non-greedy preamble would bind to the FIRST
 *    bracketed token, which in a malformed/nested envelope could be inside the
 *    metadata JSON.
 *
 * 3. If the gate passes but the regex still misses (malformed envelope), the
 *    input is returned trimmed rather than mangled — fail-safe, not fail-closed.
 */
export function stripOpenClawEnvelope(prompt: string): string {
  if (!prompt) return "";
  if (!prompt.startsWith("Sender (untrusted metadata):")) {
    return prompt.trim();
  }
  const match = prompt.match(/^[\s\S]*\n\[[^\]]+\]\s+([\s\S]+)$/);
  return match ? match[1].trim() : prompt.trim();
}
