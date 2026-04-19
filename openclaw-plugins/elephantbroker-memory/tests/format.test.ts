import { describe, it, expect } from "vitest";
import { formatMemoryContext, stripOpenClawEnvelope } from "../src/format.js";

describe("formatMemoryContext", () => {
  it("returns empty string for no results", () => {
    expect(formatMemoryContext([])).toBe("");
  });

  it("formats results with XML tags", () => {
    const results = [
      {
        id: "abc", text: "User prefers dark mode", category: "preference",
        scope: "global" as const, confidence: 0.95, memory_class: "semantic" as const,
        target_actor_ids: [], goal_ids: [], created_at: "", updated_at: "",
        use_count: 0, score: 0.9, source: "vector",
      },
    ];
    const output = formatMemoryContext(results);
    expect(output).toContain('<relevant-memories source="elephantbroker">');
    expect(output).toContain("[preference]");
    expect(output).toContain("dark mode");
    expect(output).toContain("</relevant-memories>");
  });

  it("includes confidence", () => {
    const results = [
      {
        id: "x", text: "fact", category: "general",
        scope: "session" as const, confidence: 0.88, memory_class: "episodic" as const,
        target_actor_ids: [], goal_ids: [], created_at: "", updated_at: "",
        use_count: 0, score: 0.5, source: "structural",
      },
    ];
    expect(formatMemoryContext(results)).toContain("0.88");
  });
});

// Wiring smoke test: confirms format.ts re-exports the shared helper under
// the same name. Exhaustive behavior coverage lives in
// openclaw-plugins/shared/envelope.test.ts (runs under this plugin via the
// include pattern in vitest.config.ts). This test only pins the re-export
// path so a refactor that breaks it fails immediately inside the memory
// plugin's own suite.
describe("stripOpenClawEnvelope re-export (memory plugin)", () => {
  it("is re-exported from ../src/format and strips enveloped prompts", () => {
    const envelope =
      "Sender (untrusted metadata):\n" +
      "```json\n" +
      "{\"label\":\"cli\"}\n" +
      "```\n" +
      "\n" +
      "[Sat 2026-04-18 15:30 UTC] hello from memory plugin";
    expect(stripOpenClawEnvelope(envelope)).toBe("hello from memory plugin");
  });
});
