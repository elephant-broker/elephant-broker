import { describe, it, expect } from "vitest";
import { formatMemoryContext } from "../src/format.js";

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
