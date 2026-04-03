import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch and crypto
const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

describe("Plugin registration", () => {
  it("registers 24 tools", async () => {
    const { register } = await import("../src/index.js");
    const tools: unknown[] = [];
    const hooks: Record<string, unknown> = {};
    const api = {
      registerTool: (t: unknown) => tools.push(t),
      on: (event: string, handler: unknown) => { hooks[event] = handler; },
    };
    register(api);
    expect(tools.length).toBe(24);
  });

  it("registers 4 hooks via api.on()", async () => {
    const { register } = await import("../src/index.js");
    const hooks: string[] = [];
    const api = {
      registerTool: () => {},
      on: (event: string) => hooks.push(event),
    };
    register(api);
    expect(hooks).toContain("before_agent_start");
    expect(hooks).toContain("agent_end");
    expect(hooks).toContain("session_start");
    expect(hooks).toContain("session_end");
    expect(hooks.length).toBe(4);
  });

  it("tool names match spec", async () => {
    const { register } = await import("../src/index.js");
    const tools: Array<{ id: string }> = [];
    const api = {
      registerTool: (t: { id: string }) => tools.push(t),
      on: () => {},
    };
    register(api);
    const ids = tools.map(t => t.id).sort();
    expect(ids).toEqual([
      "admin_add_member",
      "admin_create_org",
      "admin_create_team",
      "admin_merge_actors",
      "admin_register_actor",
      "admin_remove_member",
      "artifact_search",
      "create_artifact",
      "goal_create",
      "guard_status",
      "guards_list",
      "memory_forget",
      "memory_get",
      "memory_search",
      "memory_store",
      "memory_update",
      "procedure_activate",
      "procedure_complete_step",
      "procedure_create",
      "procedure_session_status",
      "session_goals_add_blocker",
      "session_goals_list",
      "session_goals_progress",
      "session_goals_update_status",
    ]);
  });
});
