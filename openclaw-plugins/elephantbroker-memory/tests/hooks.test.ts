/**
 * Memory plugin hook behavioral tests — covers GF-02 (sessionKey from ctx),
 * GF-06 (actorId wiring), GF-08 (session_start split), and agent_end behavior.
 *
 * Addresses: H2 (memory plugin hook behavior untested).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch globally before importing plugin code
const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

function setupMockFetch() {
  mockFetch.mockImplementation(async (url: string, opts?: RequestInit) => {
    const urlStr = String(url);
    if (urlStr.includes("/memory/search")) {
      return { ok: true, json: async () => [] };
    }
    if (urlStr.includes("/memory/ingest-messages")) {
      // FULL mode gate returns 202 with status/message (not ingested_count)
      return { ok: true, status: 202, json: async () => ({ status: "buffered", message: "Full mode — extraction via context engine" }) };
    }
    if (urlStr.includes("/sessions/start")) {
      return { ok: true, json: async () => ({ ok: true }) };
    }
    if (urlStr.includes("/sessions/end")) {
      return { ok: true, json: async () => ({ ok: true }) };
    }
    return { ok: true, json: async () => ({}) };
  });
}

function createMockApi() {
  const hooks: Record<string, (...args: unknown[]) => unknown> = {};
  return {
    api: {
      registerTool: () => {},
      on: (event: string, handler: (...args: unknown[]) => unknown) => {
        hooks[event] = handler;
      },
      pluginConfig: {
        baseUrl: "http://localhost:8420",
        gatewayId: "gw-test",
        gatewayShortName: "test",
      },
    },
    hooks,
  };
}

describe("before_agent_start hook (GF-02 + GF-06)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMockFetch();
  });

  // GF-02: sessionKey and agentId come from ctx (2nd arg), not event (1st arg)
  it("reads sessionKey from ctx, not event (GF-02)", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    const handler = hooks["before_agent_start"];
    expect(handler).toBeDefined();

    // Pass sessionKey in ctx (correct) and different value in event (wrong)
    await handler(
      { prompt: "test query", sessionKey: "wrong:from:event" },
      { sessionKey: "correct:from:ctx", agentId: "main" },
    );

    // The search call should use the session key from ctx
    const searchCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/memory/search"),
    );
    expect(searchCall).toBeDefined();
    const body = JSON.parse(String(searchCall![1]?.body));
    expect(body.session_key).toBe("correct:from:ctx");
  });

  // GF-02: agentId comes from ctx, agent_key derived from gatewayId
  it("derives agentKey from ctx.agentId and gatewayId (GF-02)", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    await hooks["before_agent_start"](
      { prompt: "test" },
      { agentId: "worker-1" },
    );

    // Subsequent calls should include the derived agent_key in headers
    const searchCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/memory/search"),
    );
    expect(searchCall).toBeDefined();
    const headers = searchCall![1]?.headers as Record<string, string>;
    expect(headers["X-EB-Agent-Key"]).toBe("gw-test:worker-1");
  });

  // GF-02: prompt comes from event (1st arg), not ctx
  it("reads prompt from event, not ctx (GF-02)", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    await hooks["before_agent_start"](
      { prompt: "what is the capital of France?" },
      { sessionKey: "agent:test:main" },
    );

    const searchCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/memory/search"),
    );
    expect(searchCall).toBeDefined();
    const body = JSON.parse(String(searchCall![1]?.body));
    expect(body.query).toBe("what is the capital of France?");
  });

  it("skips search when prompt is empty", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    const result = await hooks["before_agent_start"]({}, {});
    expect(result).toEqual({});
    // No search call should have been made
    const searchCalls = mockFetch.mock.calls.filter(
      (c: unknown[]) => String(c[0]).includes("/memory/search"),
    );
    expect(searchCalls.length).toBe(0);
  });

  // GF-06: actorId populated from ctx.actorId or ctx.userId
  it("sets actorId from ctx.actorId (GF-06)", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    await hooks["before_agent_start"](
      { prompt: "test" },
      { actorId: "actor-123" },
    );

    const searchCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/memory/search"),
    );
    expect(searchCall).toBeDefined();
    const headers = searchCall![1]?.headers as Record<string, string>;
    expect(headers["X-EB-Actor-Id"]).toBe("actor-123");
  });

  it("falls back to ctx.userId for actorId (GF-06)", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    await hooks["before_agent_start"](
      { prompt: "test" },
      { userId: "user-456" },
    );

    const searchCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/memory/search"),
    );
    expect(searchCall).toBeDefined();
    const headers = searchCall![1]?.headers as Record<string, string>;
    expect(headers["X-EB-Actor-Id"]).toBe("user-456");
  });

  // TODO-5-212 / TF-ER-001 BUG-1: auto-recall XML block MUST land in
  // `prependSystemContext`, not `prependContext`. The context plugin's Surface B
  // owns `prependContext` (Phase 6 AD-4) for per-turn working-set items; the
  // memory plugin's cross-turn background belongs in the system-context slot.
  it("returns prependSystemContext (not prependContext) when memories exist (BUG-1)", async () => {
    mockFetch.mockImplementation(async (url: string) => {
      const urlStr = String(url);
      if (urlStr.includes("/memory/search")) {
        return {
          ok: true,
          json: async () => [
            {
              id: "00000000-0000-0000-0000-000000000001",
              text: "user prefers Postgres over MySQL",
              category: "preference",
              confidence: 0.95,
            },
          ],
        };
      }
      return { ok: true, json: async () => ({}) };
    });

    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    const result = await hooks["before_agent_start"](
      { prompt: "what database should I use?" },
      { sessionKey: "agent:test:main" },
    ) as Record<string, unknown>;

    expect(result).toHaveProperty("prependSystemContext");
    expect(result).not.toHaveProperty("prependContext");
    expect(String(result.prependSystemContext)).toContain('<relevant-memories source="elephantbroker">');
    expect(String(result.prependSystemContext)).toContain("Postgres");
  });
});

describe("session_start hook (GF-08)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMockFetch();
  });

  // GF-08: sessionId from event (1st arg), sessionKey from ctx (2nd arg)
  it("reads sessionId from event and sessionKey from ctx (GF-08)", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    await hooks["session_start"](
      { sessionId: "sid-from-event", resumedFrom: "old-session" },
      { sessionKey: "agent:test:main", parentSessionKey: "parent:sk" },
    );

    const startCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/sessions/start"),
    );
    expect(startCall).toBeDefined();
    const body = JSON.parse(String(startCall![1]?.body));
    expect(body.session_id).toBe("sid-from-event");
    expect(body.session_key).toBe("agent:test:main");
    expect(body.parent_session_key).toBe("parent:sk");
  });

  it("falls back to ctx.sessionId when event has none", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    await hooks["session_start"](
      {},
      { sessionId: "sid-from-ctx", sessionKey: "agent:test:main" },
    );

    const startCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/sessions/start"),
    );
    expect(startCall).toBeDefined();
    const body = JSON.parse(String(startCall![1]?.body));
    expect(body.session_id).toBe("sid-from-ctx");
  });

  it("generates UUID when no sessionId in event or ctx", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    await hooks["session_start"]({}, {});

    const startCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/sessions/start"),
    );
    expect(startCall).toBeDefined();
    const body = JSON.parse(String(startCall![1]?.body));
    // Should be a UUID (36 chars with dashes)
    expect(body.session_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });

  it("handles null event gracefully (defensive guard)", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    // Should not throw with null/undefined event
    await hooks["session_start"](null, { sessionKey: "agent:test:main" });

    const startCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/sessions/start"),
    );
    expect(startCall).toBeDefined();
  });

  it("logs resumedFrom when present", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => {});
    await hooks["session_start"](
      { sessionId: "new-sid", resumedFrom: "old-sid" },
      { sessionKey: "agent:test:main" },
    );

    const resumeLog = infoSpy.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("resumed from old-sid"),
    );
    expect(resumeLog).toBeDefined();
    infoSpy.mockRestore();
  });
});

describe("agent_end hook (ingest behavior)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMockFetch();
  });

  it("sends all messages for ingest (fire-and-forget)", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    const msgs = [
      { role: "user", content: "msg1" },
      { role: "assistant", content: "msg2" },
      { role: "user", content: "msg3" },
      { role: "assistant", content: "msg4" },
      { role: "user", content: "msg5" },
      { role: "assistant", content: "msg6" },
    ];

    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => {});
    hooks["agent_end"]({ messages: msgs }, {});
    infoSpy.mockRestore();

    // Wait for fire-and-forget to complete
    await new Promise((r) => setTimeout(r, 50));

    const ingestCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/memory/ingest-messages"),
    );
    expect(ingestCall).toBeDefined();
    const body = JSON.parse(String(ingestCall![1]?.body));
    // All messages sent (no slice)
    expect(body.messages.length).toBe(6);
    expect(body.messages[0].content).toBe("msg1");
  });

  it("skips ingest when messages empty", async () => {
    const { register } = await import("../src/index.js");
    const { api, hooks } = createMockApi();
    register(api);

    hooks["agent_end"]({ messages: [] }, {});

    await new Promise((r) => setTimeout(r, 50));

    const ingestCalls = mockFetch.mock.calls.filter(
      (c: unknown[]) => String(c[0]).includes("/memory/ingest-messages"),
    );
    expect(ingestCalls.length).toBe(0);
  });
});

describe("GF-06 config/env fallback", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMockFetch();
  });

  it("sets actorId from plugin config at init", async () => {
    const { register } = await import("../src/index.js");
    const hooks: Record<string, (...args: unknown[]) => unknown> = {};
    const api = {
      registerTool: () => {},
      on: (event: string, handler: (...args: unknown[]) => unknown) => {
        hooks[event] = handler;
      },
      pluginConfig: {
        baseUrl: "http://localhost:8420",
        gatewayId: "gw-test",
        actorId: "config-actor",
      },
    };
    register(api);

    // Trigger a search to see headers
    await hooks["before_agent_start"]({ prompt: "test" }, {});

    const searchCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/memory/search"),
    );
    expect(searchCall).toBeDefined();
    const headers = searchCall![1]?.headers as Record<string, string>;
    expect(headers["X-EB-Actor-Id"]).toBe("config-actor");
  });
});
