/**
 * Context plugin register() tests — covers hook wiring, overlay return keys,
 * agent_end buffer, and before_prompt_build session identity sourcing.
 *
 * Addresses: C1 (GF-01 camelCase keys), C2 (register test file),
 * H1 (before_prompt_build reads from ctx), H3 (hook names), L3 (agent_end wiring).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch globally before importing plugin code
const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

// Default mock responses for client HTTP calls
function setupMockFetch() {
  mockFetch.mockImplementation(async (url: string, opts?: RequestInit) => {
    const urlStr = String(url);
    if (urlStr.includes("/context/config")) {
      return { ok: true, json: async () => ({ ingest_batch_size: 6 }) };
    }
    if (urlStr.includes("/context/build-overlay")) {
      return {
        ok: true,
        json: async () => ({
          prepend_system_context: "system context here",
          append_system_context: "appended system",
          prepend_context: "prepended context",
        }),
      };
    }
    return { ok: true, json: async () => ({}) };
  });
}

describe("Context plugin registration", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMockFetch();
  });

  // H3: Verify hook names match OpenClaw's snake_case convention
  it("registers correct hook names (GF-03)", async () => {
    const { register } = await import("../src/index.js");
    const hooks: string[] = [];
    const api = {
      registerTool: () => {},
      on: (event: string) => hooks.push(event),
      registerContextEngine: () => {},
      pluginConfig: { gatewayId: "gw-test", gatewayShortName: "test" },
    };
    register(api);
    expect(hooks).toContain("before_prompt_build");
    expect(hooks).toContain("agent_end");
    expect(hooks).toContain("llm_input");
    expect(hooks).toContain("llm_output");
    expect(hooks).not.toContain("onLlmInput");
    expect(hooks).not.toContain("onLlmOutput");
    expect(hooks.length).toBe(4);
  });

  it("registers context engine via registerContextEngine", async () => {
    const { register } = await import("../src/index.js");
    let registeredId = "";
    const api = {
      registerTool: () => {},
      on: () => {},
      registerContextEngine: (id: string) => { registeredId = id; },
      pluginConfig: { gatewayId: "gw-test", gatewayShortName: "test" },
    };
    register(api);
    expect(registeredId).toBe("elephantbroker-context");
  });

  it("registers 0 tools", async () => {
    const { register } = await import("../src/index.js");
    const tools: unknown[] = [];
    const api = {
      registerTool: (t: unknown) => tools.push(t),
      on: () => {},
      registerContextEngine: () => {},
      pluginConfig: { gatewayId: "gw-test", gatewayShortName: "test" },
    };
    register(api);
    expect(tools.length).toBe(0);
  });
});

describe("before_prompt_build hook (GF-01 + H1)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMockFetch();
  });

  // C1: Verify overlay return uses camelCase keys for OpenClaw
  it("returns camelCase keys in overlay (GF-01)", async () => {
    const { register } = await import("../src/index.js");
    const hooks: Record<string, (...args: unknown[]) => unknown> = {};
    const api = {
      registerTool: () => {},
      on: (event: string, handler: (...args: unknown[]) => unknown) => {
        hooks[event] = handler;
      },
      registerContextEngine: () => {},
      pluginConfig: { gatewayId: "gw-test", gatewayShortName: "test" },
    };
    register(api);

    const handler = hooks["before_prompt_build"];
    expect(handler).toBeDefined();

    // Call with ctx containing session identity (H1 pattern)
    const result = await handler({}, { sessionKey: "agent:test:main", sessionId: "sid-1" }) as Record<string, unknown>;

    // Must use camelCase keys (GF-01)
    expect(result).toHaveProperty("prependSystemContext");
    expect(result).toHaveProperty("appendSystemContext");
    expect(result).toHaveProperty("prependContext");
    // Must NOT have snake_case keys
    expect(result).not.toHaveProperty("prepend_system_context");
    expect(result).not.toHaveProperty("append_system_context");
    expect(result).not.toHaveProperty("prepend_context");
  });

  // H1: Verify sessionKey is read from ctx (2nd arg), not event (1st arg)
  it("reads sessionKey from ctx, not event (H1)", async () => {
    const { register } = await import("../src/index.js");
    const hooks: Record<string, (...args: unknown[]) => unknown> = {};
    const api = {
      registerTool: () => {},
      on: (event: string, handler: (...args: unknown[]) => unknown) => {
        hooks[event] = handler;
      },
      registerContextEngine: () => {},
      pluginConfig: { gatewayId: "gw-test", gatewayShortName: "test" },
    };
    register(api);

    const handler = hooks["before_prompt_build"];

    // Pass sessionKey in ctx (2nd arg) only — event has wrong/no sessionKey
    await handler(
      { sessionKey: "wrong:from:event" },
      { sessionKey: "correct:from:ctx", sessionId: "sid-correct" },
    );

    // Verify the fetch call used the ctx values (POST body, not URL params)
    const buildOverlayCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/context/build-overlay"),
    );
    expect(buildOverlayCall).toBeDefined();
    const body = JSON.parse(String(buildOverlayCall![1]?.body));
    expect(body.session_key).toBe("correct:from:ctx");
  });

  it("returns empty object on error", async () => {
    mockFetch.mockImplementation(async () => {
      throw new Error("network error");
    });

    const { register } = await import("../src/index.js");
    const hooks: Record<string, (...args: unknown[]) => unknown> = {};
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const api = {
      registerTool: () => {},
      on: (event: string, handler: (...args: unknown[]) => unknown) => {
        hooks[event] = handler;
      },
      registerContextEngine: () => {},
      pluginConfig: { gatewayId: "gw-test", gatewayShortName: "test" },
    };
    register(api);

    const result = await hooks["before_prompt_build"]({}, {});
    expect(result).toEqual({});
    errSpy.mockRestore();
  });
});

describe("agent_end hook (GF-04 buffer wiring, L3)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMockFetch();
  });

  // L3: Verify agent_end hook calls setLastTurnMessages on engine
  it("buffers messages via setLastTurnMessages", async () => {
    const { register } = await import("../src/index.js");
    const hooks: Record<string, (...args: unknown[]) => unknown> = {};
    let engineRef: unknown = null;
    const api = {
      registerTool: () => {},
      on: (event: string, handler: (...args: unknown[]) => unknown) => {
        hooks[event] = handler;
      },
      registerContextEngine: (_id: string, factory: () => unknown) => {
        engineRef = factory();
      },
      pluginConfig: { gatewayId: "gw-test", gatewayShortName: "test" },
    };
    register(api);

    const handler = hooks["agent_end"];
    expect(handler).toBeDefined();

    const msgs = [
      { role: "user", content: "hello" },
      { role: "assistant", content: "hi" },
    ];

    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => {});
    handler({ messages: msgs }, {});
    infoSpy.mockRestore();

    // Engine should have buffered the messages — verify via afterTurn
    const engine = engineRef as { afterTurn: (p: { sessionId: string }) => Promise<void>; setSessionContext: (sk: string, sid: string) => void };
    engine.setSessionContext("sk", "sid");
    await engine.afterTurn({ sessionId: "sid" });

    // afterTurn should have sent the buffered messages
    const afterTurnCall = mockFetch.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes("/context/after-turn"),
    );
    expect(afterTurnCall).toBeDefined();
    const body = JSON.parse(String(afterTurnCall![1]?.body));
    expect(body.messages).toEqual(msgs);
  });

  it("handles empty messages array", async () => {
    const { register } = await import("../src/index.js");
    const hooks: Record<string, (...args: unknown[]) => unknown> = {};
    const api = {
      registerTool: () => {},
      on: (event: string, handler: (...args: unknown[]) => unknown) => {
        hooks[event] = handler;
      },
      registerContextEngine: () => {},
      pluginConfig: { gatewayId: "gw-test", gatewayShortName: "test" },
    };
    register(api);

    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => {});
    // Should not throw with missing messages
    hooks["agent_end"]({}, {});
    hooks["agent_end"]({ messages: [] }, {});
    infoSpy.mockRestore();
  });
});
