/**
 * ContextEnginePlugin vitest tests (Amendment 6.2.4 + Deployment Fixes).
 *
 * Tests plugin registration, degraded ingest buffering, lifecycle method calls
 * with OpenClaw's params-object interface, error degradation, and hook behavior.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { ContextEngineImpl } from "../src/engine.js";
import type { ContextEngineClient } from "../src/client.js";

// --- Mock Client Factory ---

function createMockClient(): ContextEngineClient {
  return {
    setAgentIdentity: vi.fn(),
    setSessionContext: vi.fn(),
    bootstrap: vi.fn().mockResolvedValue({ bootstrapped: true }),
    ingestBatch: vi.fn().mockResolvedValue({ ingested_count: 2 }),
    assemble: vi.fn().mockResolvedValue({ messages: [], estimated_tokens: 42, system_prompt_addition: "goals here" }),
    buildOverlay: vi.fn().mockResolvedValue({}),
    compact: vi.fn().mockResolvedValue({ ok: true, compacted: false }),
    afterTurn: vi.fn().mockResolvedValue(undefined),
    subagentSpawn: vi.fn().mockResolvedValue({
      parent_session_key: "parent", child_session_key: "child",
      rollback_key: "rk-1", parent_mapping_stored: true,
    }),
    subagentEnded: vi.fn().mockResolvedValue(undefined),
    dispose: vi.fn().mockResolvedValue(undefined),
    getConfig: vi.fn().mockResolvedValue({ ingest_batch_size: 6 }),
    reportContextWindow: vi.fn().mockResolvedValue(undefined),
    reportTokenUsage: vi.fn().mockResolvedValue(undefined),
  } as unknown as ContextEngineClient;
}

// --- TestPluginRegistration ---

describe("TestPluginRegistration", () => {
  it("owns compaction is true", () => {
    const client = createMockClient();
    const engine = new ContextEngineImpl(client);
    expect(engine.info.ownsCompaction).toBe(true);
  });

  it("info has correct id", () => {
    const client = createMockClient();
    const engine = new ContextEngineImpl(client);
    expect(engine.info.id).toBe("elephantbroker-context");
  });

  it("constructor accepts options object", () => {
    const client = createMockClient();
    const engine = new ContextEngineImpl(client, {
      batchSize: 10,
      profileName: "research",
      gatewayId: "gw-test",
    });
    expect(engine).toBeDefined();
    expect(engine.info.ownsCompaction).toBe(true);
  });
});

// --- TestDegradedIngestBuffering ---

describe("TestDegradedIngestBuffering", () => {
  let client: ContextEngineClient;
  let engine: ContextEngineImpl;

  beforeEach(() => {
    client = createMockClient();
    engine = new ContextEngineImpl(client, { batchSize: 3 });
    engine.setSessionContext("agent:main:main", "sid-1");
  });

  it("ingest buffers single message without HTTP call", async () => {
    await engine.ingest({ sessionId: "sid-1", message: { role: "user", content: "hello" } });
    expect(client.ingestBatch).not.toHaveBeenCalled();
  });

  it("ingest warns once on first degraded call", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    await engine.ingest({ sessionId: "sid-1", message: { role: "user", content: "msg1" } });
    await engine.ingest({ sessionId: "sid-1", message: { role: "user", content: "msg2" } });
    const ebWarnings = warnSpy.mock.calls.filter(c =>
      String(c[0]).includes("Degraded mode"),
    );
    expect(ebWarnings.length).toBe(1);
    warnSpy.mockRestore();
  });

  it("buffer flushes at batch size", async () => {
    await engine.ingest({ sessionId: "sid-1", message: { role: "user", content: "m1" } });
    await engine.ingest({ sessionId: "sid-1", message: { role: "user", content: "m2" } });
    expect(client.ingestBatch).not.toHaveBeenCalled();
    await engine.ingest({ sessionId: "sid-1", message: { role: "user", content: "m3" } }); // hits batchSize=3
    expect(client.ingestBatch).toHaveBeenCalledTimes(1);
    const call = (client.ingestBatch as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(call.messages.length).toBe(3);
  });

  it("empty buffer flush is noop", async () => {
    await engine.assemble({ sessionId: "sid-1", messages: [], tokenBudget: 8000 });
    expect(client.ingestBatch).not.toHaveBeenCalled();
  });

  it("ingestBatch bypasses buffer", async () => {
    await engine.ingestBatch({ sessionId: "sid-1", messages: [{ role: "user", content: "direct" }] });
    expect(client.ingestBatch).toHaveBeenCalledTimes(1);
  });

  it("buffer flush sends all messages", async () => {
    await engine.ingest({ sessionId: "sid-1", message: { role: "user", content: "a" } });
    await engine.ingest({ sessionId: "sid-1", message: { role: "assistant", content: "b" } });
    await engine.assemble({ sessionId: "sid-1", messages: [], tokenBudget: 8000 });
    expect(client.ingestBatch).toHaveBeenCalledTimes(1);
    const flushed = (client.ingestBatch as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(flushed.messages.length).toBe(2);
  });
});

// --- TestLifecycleMethods ---

describe("TestLifecycleMethods", () => {
  let client: ContextEngineClient;
  let engine: ContextEngineImpl;

  beforeEach(() => {
    client = createMockClient();
    engine = new ContextEngineImpl(client, { profileName: "research" });
  });

  it("bootstrap sets session context from params", async () => {
    const result = await engine.bootstrap({ sessionId: "sid-1", sessionKey: "agent:test:main" });
    expect(client.setSessionContext).toHaveBeenCalledWith("agent:test:main", "sid-1");
    expect(client.bootstrap).toHaveBeenCalledWith(
      expect.objectContaining({
        session_key: "agent:test:main",
        session_id: "sid-1",
        profile_name: "research",
      }),
    );
    expect(result.bootstrapped).toBe(true);
  });

  it("bootstrap uses default session key when not provided", async () => {
    await engine.bootstrap({ sessionId: "sid-2" });
    expect(client.setSessionContext).toHaveBeenCalledWith("agent:main:main", "sid-2");
  });

  it("assemble flushes buffer first", async () => {
    engine.setSessionContext("sk", "sid");
    await engine.ingest({ sessionId: "sid", message: { role: "user", content: "buffered" } });
    await engine.assemble({ sessionId: "sid", messages: [], tokenBudget: 8000 });
    expect(client.ingestBatch).toHaveBeenCalledTimes(1);
    expect(client.assemble).toHaveBeenCalledTimes(1);
  });

  it("assemble returns camelCase result", async () => {
    engine.setSessionContext("sk", "sid");
    const result = await engine.assemble({ sessionId: "sid", messages: [], tokenBudget: 8000 });
    expect(result.estimatedTokens).toBe(42);
    expect(result.systemPromptAddition).toBe("goals here");
    expect((result as Record<string, unknown>).estimated_tokens).toBeUndefined();
  });

  it("afterTurn flushes buffer first", async () => {
    engine.setSessionContext("sk", "sid");
    await engine.ingest({ sessionId: "sid", message: { role: "user", content: "buffered" } });
    await engine.afterTurn({ sessionId: "sid" });
    expect(client.ingestBatch).toHaveBeenCalledTimes(1);
    expect(client.afterTurn).toHaveBeenCalledTimes(1);
  });

  it("afterTurn sends empty messages when no lastTurnMessages set", async () => {
    engine.setSessionContext("sk", "sid");
    await engine.afterTurn({ sessionId: "sid" });
    expect(client.afterTurn).toHaveBeenCalledWith(
      expect.objectContaining({ messages: [], pre_prompt_message_count: 0 }),
    );
  });

  it("afterTurn forwards lastTurnMessages and clears buffer", async () => {
    engine.setSessionContext("sk", "sid");
    const msgs = [
      { role: "user", content: "hello" },
      { role: "assistant", content: "hi there" },
    ];
    engine.setLastTurnMessages(msgs);
    await engine.afterTurn({ sessionId: "sid" });
    expect(client.afterTurn).toHaveBeenCalledWith(
      expect.objectContaining({ messages: msgs }),
    );
    // Buffer should be cleared after use
    await engine.afterTurn({ sessionId: "sid" });
    expect(client.afterTurn).toHaveBeenLastCalledWith(
      expect.objectContaining({ messages: [] }),
    );
  });

  it("afterTurn prefers params.messages over lastTurnMessages buffer", async () => {
    engine.setSessionContext("sk", "sid");
    const bufferMsgs = [{ role: "user", content: "from buffer" }];
    const paramsMsgs = [
      { role: "user", content: "from params" },
      { role: "assistant", content: "reply" },
    ];
    engine.setLastTurnMessages(bufferMsgs);
    await engine.afterTurn({ sessionId: "sid", messages: paramsMsgs, prePromptMessageCount: 3 });
    expect(client.afterTurn).toHaveBeenCalledWith(
      expect.objectContaining({
        messages: paramsMsgs,
        pre_prompt_message_count: 3,
      }),
    );
  });

  it("compact calls endpoint with params", async () => {
    engine.setSessionContext("sk", "sid");
    const result = await engine.compact({ sessionId: "sid", force: true });
    expect(client.compact).toHaveBeenCalledWith(
      expect.objectContaining({ force: true }),
    );
    expect(result.ok).toBe(true);
  });

  // GF-15 (PR #11): OpenClaw calls dispose() after every run, not just at
  // session end. So dispose() must NOT call /context/dispose on the runtime
  // (that endpoint is for session teardown, not engine teardown). The fix
  // restricts dispose() to clearing per-turn transient state only.
  it("dispose does NOT call client.dispose endpoint (engine teardown only)", async () => {
    engine.setSessionContext("agent:main:main", "sid-1");
    await engine.dispose();
    expect(client.dispose).not.toHaveBeenCalled();
  });

  // PR #12: TS dispose race fix. dispose() previously wiped lastTurnMessages
  // before afterTurn could consume them, breaking successful-use tracking.
  // Fix: dispose() now PRESERVES lastTurnMessages so a subsequent afterTurn
  // can still send them to the runtime.
  it("dispose preserves lastTurnMessages buffer (PR #12 dispose-race fix)", async () => {
    engine.setSessionContext("sk", "sid");
    engine.setLastTurnMessages([{ role: "user", content: "lingering" }]);
    await engine.dispose();
    // After dispose, afterTurn should STILL receive the lingering message
    // because dispose preserves the buffer (the comment in src/engine.ts
    // line 223 explicitly lists lastTurnMessages as PRESERVED).
    await engine.afterTurn({ sessionId: "sid" });
    expect(client.afterTurn).toHaveBeenCalledWith(
      expect.objectContaining({
        messages: [{ role: "user", content: "lingering" }],
      }),
    );
  });

  it("prepareSubagentSpawn maps childSessionId", async () => {
    engine.setSessionContext("parent:sk", "sid");
    await engine.prepareSubagentSpawn({ sessionId: "sid", childSessionId: "child:sk" });
    expect(client.subagentSpawn).toHaveBeenCalledWith(
      expect.objectContaining({ parent_session_key: "parent:sk", child_session_key: "child:sk" }),
    );
  });

  it("prepareSubagentSpawn prefers childSessionKey over childSessionId", async () => {
    engine.setSessionContext("parent:sk", "sid");
    await engine.prepareSubagentSpawn({ sessionId: "sid", childSessionId: "old", childSessionKey: "child:key" });
    expect(client.subagentSpawn).toHaveBeenCalledWith(
      expect.objectContaining({ child_session_key: "child:key" }),
    );
  });

  it("onSubagentEnded maps childSessionId", async () => {
    engine.setSessionContext("sk", "sid");
    await engine.onSubagentEnded({ sessionId: "sid", childSessionId: "child:sk" });
    expect(client.subagentEnded).toHaveBeenCalledWith(
      expect.objectContaining({ child_session_key: "child:sk", reason: "completed" }),
    );
  });

  it("onSubagentEnded prefers childSessionKey and forwards reason", async () => {
    engine.setSessionContext("sk", "sid");
    await engine.onSubagentEnded({ sessionId: "sid", childSessionKey: "child:key", reason: "deleted" });
    expect(client.subagentEnded).toHaveBeenCalledWith(
      expect.objectContaining({ child_session_key: "child:key", reason: "deleted" }),
    );
  });

  // M2: subagent methods handle missing child keys gracefully
  it("prepareSubagentSpawn logs error and sends empty key when both child keys missing", async () => {
    engine.setSessionContext("parent:sk", "sid");
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    await engine.prepareSubagentSpawn({ sessionId: "sid" });
    expect(errSpy).toHaveBeenCalledWith(
      expect.stringContaining("neither childSessionKey nor childSessionId provided"),
    );
    expect(client.subagentSpawn).toHaveBeenCalledWith(
      expect.objectContaining({ child_session_key: "" }),
    );
    errSpy.mockRestore();
  });

  it("onSubagentEnded logs error and sends empty key when both child keys missing", async () => {
    engine.setSessionContext("sk", "sid");
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    await engine.onSubagentEnded({ sessionId: "sid" });
    expect(errSpy).toHaveBeenCalledWith(
      expect.stringContaining("neither childSessionKey nor childSessionId provided"),
    );
    expect(client.subagentEnded).toHaveBeenCalledWith(
      expect.objectContaining({ child_session_key: "" }),
    );
    errSpy.mockRestore();
  });

  // M4: afterTurn warns when messages are empty
  it("afterTurn warns when no messages available", async () => {
    engine.setSessionContext("sk", "sid");
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    await engine.afterTurn({ sessionId: "sid" });
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining("no messages available"),
    );
    warnSpy.mockRestore();
  });
});

// --- TestErrorDegradation ---

describe("TestErrorDegradation", () => {
  let client: ContextEngineClient;
  let engine: ContextEngineImpl;

  beforeEach(() => {
    client = createMockClient();
    engine = new ContextEngineImpl(client);
  });

  it("bootstrap failure returns fallback", async () => {
    (client.bootstrap as ReturnType<typeof vi.fn>).mockResolvedValue({ bootstrapped: false, reason: "Runtime error" });
    const result = await engine.bootstrap({ sessionId: "sid-1" });
    expect(result.bootstrapped).toBe(false);
  });

  it("assemble failure returns input messages", async () => {
    const msgs = [{ role: "user", content: "test" }];
    (client.assemble as ReturnType<typeof vi.fn>).mockResolvedValue({ messages: msgs, estimated_tokens: 0, system_prompt_addition: null });
    engine.setSessionContext("sk", "sid");
    const result = await engine.assemble({ sessionId: "sid", messages: msgs, tokenBudget: 8000 });
    expect(result.messages).toEqual(msgs);
    expect(result.estimatedTokens).toBe(0);
  });

  it("compact failure returns not compacted", async () => {
    (client.compact as ReturnType<typeof vi.fn>).mockResolvedValue({ ok: false, compacted: false, reason: "Runtime error" });
    engine.setSessionContext("sk", "sid");
    const result = await engine.compact({ sessionId: "sid" });
    expect(result.ok).toBe(false);
    expect(result.compacted).toBe(false);
  });

  it("afterTurn failure propagates error", async () => {
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    (client.afterTurn as ReturnType<typeof vi.fn>).mockRejectedValue(new Error("fail"));
    engine.setSessionContext("sk", "sid");
    await expect(engine.afterTurn({ sessionId: "sid" })).rejects.toThrow();
    errSpy.mockRestore();
  });

  it("llm hooks failure logs and continues", async () => {
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    (client.reportContextWindow as ReturnType<typeof vi.fn>).mockRejectedValue(new Error("fail"));
    (client.reportTokenUsage as ReturnType<typeof vi.fn>).mockRejectedValue(new Error("fail"));
    engine.setSessionContext("sk", "sid");
    engine.onLlmInput({ provider: "openai", model: "gpt-4" });
    engine.onLlmOutput({ input_tokens: 100, output_tokens: 50, total_tokens: 150 });
    await new Promise((r) => setTimeout(r, 50));
    errSpy.mockRestore();
  });
});

// --- TestSessionIdentity ---

describe("TestSessionIdentity", () => {
  let client: ContextEngineClient;
  let engine: ContextEngineImpl;

  beforeEach(() => {
    client = createMockClient();
    engine = new ContextEngineImpl(client, { gatewayId: "gw-test" });
  });

  it("bootstrap derives agent identity from gatewayId", async () => {
    await engine.bootstrap({ sessionId: "sid-1" });
    expect(client.setAgentIdentity).toHaveBeenCalledWith("main", "gw-test:main");
  });

  it("getSessionKey returns current session key", async () => {
    await engine.bootstrap({ sessionId: "sid-1", sessionKey: "agent:test:main" });
    expect(engine.getSessionKey()).toBe("agent:test:main");
    expect(engine.getSessionId()).toBe("sid-1");
  });

  it("identity propagated to client", () => {
    engine.setAgentIdentity("main", "gw:main");
    expect(client.setAgentIdentity).toHaveBeenCalledWith("main", "gw:main");
  });

  it("before_prompt_build can use engine getters", async () => {
    await engine.bootstrap({ sessionId: "sid-1", sessionKey: "agent:test:main" });
    const overlay = await client.buildOverlay(engine.getSessionKey(), engine.getSessionId());
    expect(client.buildOverlay).toHaveBeenCalledWith("agent:test:main", "sid-1");
  });

  it("onLlmInput fires once only", () => {
    engine.setSessionContext("sk", "sid");
    engine.onLlmInput({ provider: "openai", model: "gpt-4", context_window_tokens: 128000 });
    engine.onLlmInput({ provider: "openai", model: "gpt-4", context_window_tokens: 128000 });
    engine.onLlmInput({ provider: "openai", model: "gpt-4", context_window_tokens: 128000 });
    expect(client.reportContextWindow).toHaveBeenCalledTimes(1);
  });
});
