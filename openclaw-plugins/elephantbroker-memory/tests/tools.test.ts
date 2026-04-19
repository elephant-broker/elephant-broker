// 5-603: memory_forget + memory_update must discriminate backend errors
// by HTTP status rather than swallowing everything as "not found".
//
// The tools now rely on HttpStatusError.status to route:
//   403 → forbidden (cross-tenant signal)
//   404 → not_found
//   422 → invalid_input (update only)
//   5xx → backend_error (do NOT mask as not_found)
//   other → error

import { describe, it, expect, vi } from "vitest";
import { HttpStatusError } from "../src/client.js";
import { createMemoryForgetTool } from "../src/tools/memory_forget.js";
import { createMemoryUpdateTool } from "../src/tools/memory_update.js";

type ForgetClient = { forget: (id: string) => Promise<void> };
type UpdateClient = {
  update: (id: string, body: Record<string, unknown>) => Promise<unknown>;
  search: (req: { query: string; max_results: number }) => Promise<Array<{ id: string; score: number; text: string }>>;
};

function parseResult(r: { content: Array<{ type: string; text: string }> }) {
  return JSON.parse(r.content[0].text);
}

describe("memory_forget error discrimination (TODO 5-603)", () => {
  it("404 → reason=not_found", async () => {
    const client = { forget: vi.fn().mockRejectedValue(new HttpStatusError(404, "Fact not found: abc")) };
    const tool = createMemoryForgetTool(client as unknown as Parameters<typeof createMemoryForgetTool>[0]);
    const res = await tool.execute("tc1", { fact_id: "abc" });
    const parsed = parseResult(res);
    expect(parsed.deleted).toBeNull();
    expect(parsed.reason).toBe("not_found");
  });

  it("403 → reason=forbidden (distinct from not_found)", async () => {
    const client = { forget: vi.fn().mockRejectedValue(new HttpStatusError(403, "Permission denied: fact abc belongs to another gateway")) };
    const tool = createMemoryForgetTool(client as unknown as Parameters<typeof createMemoryForgetTool>[0]);
    const res = await tool.execute("tc2", { fact_id: "abc" });
    const parsed = parseResult(res);
    expect(parsed.deleted).toBeNull();
    expect(parsed.reason).toBe("forbidden");
    expect(parsed.detail).toContain("Permission denied");
  });

  it("500 → reason=backend_error with status (does NOT mask as not_found)", async () => {
    const client = { forget: vi.fn().mockRejectedValue(new HttpStatusError(500, "Delete failed: 500")) };
    const tool = createMemoryForgetTool(client as unknown as Parameters<typeof createMemoryForgetTool>[0]);
    const res = await tool.execute("tc3", { fact_id: "abc" });
    const parsed = parseResult(res);
    expect(parsed.deleted).toBeNull();
    expect(parsed.reason).toBe("backend_error");
    expect(parsed.status).toBe(500);
    expect(parsed.reason).not.toBe("not_found");
  });

  it("503 → reason=backend_error with status (5xx umbrella)", async () => {
    const client = { forget: vi.fn().mockRejectedValue(new HttpStatusError(503, "Delete failed: 503")) };
    const tool = createMemoryForgetTool(client as unknown as Parameters<typeof createMemoryForgetTool>[0]);
    const res = await tool.execute("tc4", { fact_id: "abc" });
    const parsed = parseResult(res);
    expect(parsed.reason).toBe("backend_error");
    expect(parsed.status).toBe(503);
  });

  it("non-HttpStatusError → reason=error with detail", async () => {
    const client = { forget: vi.fn().mockRejectedValue(new Error("network down")) };
    const tool = createMemoryForgetTool(client as unknown as Parameters<typeof createMemoryForgetTool>[0]);
    const res = await tool.execute("tc5", { fact_id: "abc" });
    const parsed = parseResult(res);
    expect(parsed.deleted).toBeNull();
    expect(parsed.reason).toBe("error");
    expect(parsed.detail).toBe("network down");
  });

  it("success → deleted=fact_id, no error branches", async () => {
    const client = { forget: vi.fn().mockResolvedValue(undefined) };
    const tool = createMemoryForgetTool(client as unknown as Parameters<typeof createMemoryForgetTool>[0]);
    const res = await tool.execute("tc6", { fact_id: "abc" });
    const parsed = parseResult(res);
    expect(parsed.deleted).toBe("abc");
  });
});

describe("memory_update error discrimination (TODO 5-603)", () => {
  it("404 → reason=not_found", async () => {
    const client = {
      update: vi.fn().mockRejectedValue(new HttpStatusError(404, "Fact not found: abc")),
      search: vi.fn(),
    };
    const tool = createMemoryUpdateTool(client as unknown as Parameters<typeof createMemoryUpdateTool>[0]);
    const res = await tool.execute("tc1", { fact_id: "abc", new_text: "new" });
    const parsed = parseResult(res);
    expect(parsed.updated).toBeNull();
    expect(parsed.reason).toBe("not_found");
  });

  it("403 → reason=forbidden", async () => {
    const client = {
      update: vi.fn().mockRejectedValue(new HttpStatusError(403, "Permission denied: fact abc belongs to another gateway")),
      search: vi.fn(),
    };
    const tool = createMemoryUpdateTool(client as unknown as Parameters<typeof createMemoryUpdateTool>[0]);
    const res = await tool.execute("tc2", { fact_id: "abc", new_text: "new" });
    const parsed = parseResult(res);
    expect(parsed.updated).toBeNull();
    expect(parsed.reason).toBe("forbidden");
  });

  it("422 → reason=invalid_input", async () => {
    const client = {
      update: vi.fn().mockRejectedValue(new HttpStatusError(422, "Invalid update payload for abc")),
      search: vi.fn(),
    };
    const tool = createMemoryUpdateTool(client as unknown as Parameters<typeof createMemoryUpdateTool>[0]);
    const res = await tool.execute("tc3", { fact_id: "abc", updates: { category: 42 } });
    const parsed = parseResult(res);
    expect(parsed.updated).toBeNull();
    expect(parsed.reason).toBe("invalid_input");
  });

  it("500 → reason=backend_error (does NOT mask as not_found)", async () => {
    const client = {
      update: vi.fn().mockRejectedValue(new HttpStatusError(500, "Update failed: 500")),
      search: vi.fn(),
    };
    const tool = createMemoryUpdateTool(client as unknown as Parameters<typeof createMemoryUpdateTool>[0]);
    const res = await tool.execute("tc4", { fact_id: "abc", new_text: "new" });
    const parsed = parseResult(res);
    expect(parsed.reason).toBe("backend_error");
    expect(parsed.status).toBe(500);
    expect(parsed.reason).not.toBe("not_found");
  });

  it("non-HttpStatusError → reason=error with detail", async () => {
    const client = {
      update: vi.fn().mockRejectedValue(new Error("network down")),
      search: vi.fn(),
    };
    const tool = createMemoryUpdateTool(client as unknown as Parameters<typeof createMemoryUpdateTool>[0]);
    const res = await tool.execute("tc5", { fact_id: "abc", new_text: "new" });
    const parsed = parseResult(res);
    expect(parsed.reason).toBe("error");
    expect(parsed.detail).toBe("network down");
  });

  it("success → updated=fact_id, fact included", async () => {
    const client = {
      update: vi.fn().mockResolvedValue({ id: "abc", text: "new" }),
      search: vi.fn(),
    };
    const tool = createMemoryUpdateTool(client as unknown as Parameters<typeof createMemoryUpdateTool>[0]);
    const res = await tool.execute("tc6", { fact_id: "abc", new_text: "new" });
    const parsed = parseResult(res);
    expect(parsed.updated).toBe("abc");
    expect(parsed.fact.id).toBe("abc");
  });
});

describe("HttpStatusError shape", () => {
  it("is an Error with a numeric .status", () => {
    const err = new HttpStatusError(403, "forbidden");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("HttpStatusError");
    expect(err.status).toBe(403);
    expect(err.message).toBe("forbidden");
  });
});
