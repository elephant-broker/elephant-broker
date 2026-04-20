import { describe, it, expect } from "vitest";
import { stripOpenClawEnvelope } from "./envelope.js";

// Shared coverage for the canonical envelope-stripping helper used by both
// plugins. Included by each plugin's vitest config via `../shared/**/*.test.ts`,
// so this file runs twice — once under `elephantbroker-context`, once under
// `elephantbroker-memory` — giving automatic parity (PR #5 TODOs 5-206, 5-406).

describe("stripOpenClawEnvelope (shared)", () => {
  const envelope = (userText: string, metadata = '{"label":"cli"}') =>
    "Sender (untrusted metadata):\n" +
    "```json\n" +
    `${metadata}\n` +
    "```\n" +
    "\n" +
    `[Sat 2026-04-18 15:30 UTC] ${userText}`;

  it("extracts user text from a well-formed OpenClaw envelope", () => {
    expect(stripOpenClawEnvelope(envelope("what is X?"))).toBe("what is X?");
  });

  it("preserves multi-line user text after the marker", () => {
    const input = envelope("line one\nline two\n  line three  ");
    expect(stripOpenClawEnvelope(input)).toBe("line one\nline two\n  line three");
  });

  // 5-102 RC-A regression: a plain-text prompt containing a bracketed token
  // must pass through unchanged. The earlier regex-only implementation matched
  // the bracket and truncated the prompt to everything after it, which broke
  // retrieval for any user asking about e.g. `[ref-42]` or `[2026-04-18]`.
  it("passes through plain text that contains a bracketed token (5-102 RC-A)", () => {
    const plain = "can you update the doc at [ref-42] to mention migrations?";
    expect(stripOpenClawEnvelope(plain)).toBe(plain);
  });

  it("passes through plain text that looks like a bracketed timestamp", () => {
    const plain = "I saw log line [2026-04-18 15:30 UTC] foo earlier, what caused it?";
    expect(stripOpenClawEnvelope(plain)).toBe(plain);
  });

  it("passes through ordinary plain text unchanged", () => {
    expect(stripOpenClawEnvelope("what is X?")).toBe("what is X?");
  });

  it("returns empty string for empty input", () => {
    expect(stripOpenClawEnvelope("")).toBe("");
  });

  it("trims leading/trailing whitespace on non-enveloped input", () => {
    expect(stripOpenClawEnvelope("   hello world   ")).toBe("hello world");
  });

  // Greedy-preamble guarantee: if an envelope happens to carry a bracketed
  // token inside the metadata JSON block, we must anchor on the LAST marker
  // (the real timestamp), not the first bracketed substring.
  it("anchors on the LAST bracketed marker when metadata contains brackets", () => {
    const input =
      "Sender (untrusted metadata):\n" +
      "```json\n" +
      '{"tags":["[inner-token]"]}\n' +
      "```\n" +
      "\n" +
      "[Sat 2026-04-18 15:30 UTC] real user question";
    expect(stripOpenClawEnvelope(input)).toBe("real user question");
  });

  // Fail-safe branch: if the Sender prefix is present but the timestamp
  // marker is missing (malformed envelope), return the trimmed input rather
  // than nothing. Better to let the caller see garbage than to silently
  // drop the prompt.
  it("returns trimmed input when envelope prefix is present but marker is missing", () => {
    const malformed =
      "Sender (untrusted metadata):\nno closing marker here";
    expect(stripOpenClawEnvelope(malformed)).toBe(malformed.trim());
  });
});
