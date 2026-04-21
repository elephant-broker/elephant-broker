// src/client.ts
import { trace, context, propagation, SpanKind } from "@opentelemetry/api";
var tracer = trace.getTracer("elephantbroker.context-engine-plugin");
var ContextEngineClient = class {
  baseUrl;
  gatewayId;
  gatewayShortName;
  agentId = "";
  agentKey = "";
  currentSessionKey = "";
  currentSessionId = "";
  constructor(baseUrl = "http://localhost:8420", gatewayId, gatewayShortName) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.gatewayId = gatewayId || process.env.EB_GATEWAY_ID || "";
    if (!this.gatewayId) {
      throw new Error("EB_GATEWAY_ID is required for ContextEnginePlugin.");
    }
    this.gatewayShortName = gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME || this.gatewayId.substring(0, 8);
  }
  setAgentIdentity(agentId, agentKey) {
    this.agentId = agentId;
    this.agentKey = agentKey;
  }
  setSessionContext(sessionKey, sessionId) {
    this.currentSessionKey = sessionKey;
    this.currentSessionId = sessionId;
  }
  getHeaders() {
    const headers = {
      "Content-Type": "application/json",
      "X-EB-Gateway-ID": this.gatewayId
    };
    if (this.agentKey) headers["X-EB-Agent-Key"] = this.agentKey;
    if (this.agentId) headers["X-EB-Agent-ID"] = this.agentId;
    if (this.currentSessionKey) headers["X-EB-Session-Key"] = this.currentSessionKey;
    propagation.inject(context.active(), headers);
    return headers;
  }
  // --- Context Lifecycle ---
  async bootstrap(params) {
    return tracer.startActiveSpan("context.bootstrap", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/bootstrap`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(params)
        });
        if (!res.ok) throw new Error(`Bootstrap failed: ${res.status}`);
        return await res.json();
      } catch (err) {
        console.error(`[EB] Bootstrap failed: ${err}`);
        return { bootstrapped: false, reason: "Runtime error" };
      } finally {
        span.end();
      }
    });
  }
  async ingestBatch(params) {
    return tracer.startActiveSpan("context.ingestBatch", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/ingest-batch`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(params)
        });
        if (!res.ok) throw new Error(`IngestBatch failed: ${res.status}`);
        return await res.json();
      } catch (err) {
        console.error(`[EB] IngestBatch failed: ${err}`);
        return { ingested_count: 0, facts_stored: 0 };
      } finally {
        span.end();
      }
    });
  }
  async assemble(params) {
    return tracer.startActiveSpan("context.assemble", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/assemble`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(params)
        });
        if (!res.ok) throw new Error(`Assemble failed: ${res.status}`);
        return await res.json();
      } catch (err) {
        console.error(`[EB] Assemble failed: ${err}`);
        return { messages: params.messages || [], estimated_tokens: 0 };
      } finally {
        span.end();
      }
    });
  }
  async buildOverlay(sessionKey, sessionId) {
    return tracer.startActiveSpan("context.buildOverlay", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/build-overlay`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ session_key: sessionKey, session_id: sessionId })
        });
        if (!res.ok) throw new Error(`BuildOverlay failed: ${res.status}`);
        return await res.json();
      } catch (err) {
        console.error(`[EB] BuildOverlay failed: ${err}`);
        return {};
      } finally {
        span.end();
      }
    });
  }
  async compact(params) {
    return tracer.startActiveSpan("context.compact", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/compact`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(params)
        });
        if (!res.ok) throw new Error(`Compact failed: ${res.status}`);
        return await res.json();
      } catch (err) {
        console.error(`[EB] Compact failed: ${err}`);
        return { ok: false, compacted: false, reason: "Runtime error" };
      } finally {
        span.end();
      }
    });
  }
  async afterTurn(params) {
    return tracer.startActiveSpan("context.afterTurn", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/after-turn`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(params)
        });
        if (!res.ok) console.warn(`[EB] AfterTurn returned ${res.status}`);
      } catch (err) {
        console.error(`[EB] AfterTurn failed: ${err}`);
      } finally {
        span.end();
      }
    });
  }
  async subagentSpawn(params) {
    return tracer.startActiveSpan("context.subagentSpawn", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/subagent/spawn`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(params)
        });
        if (!res.ok) throw new Error(`SubagentSpawn failed: ${res.status}`);
        return await res.json();
      } catch (err) {
        console.error(`[EB] SubagentSpawn failed: ${err}`);
        return { parent_session_key: params.parent_session_key, child_session_key: params.child_session_key, rollback_key: "", parent_mapping_stored: false };
      } finally {
        span.end();
      }
    });
  }
  async subagentEnded(params) {
    return tracer.startActiveSpan("context.subagentEnded", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/subagent/ended`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(params)
        });
        if (!res.ok) console.warn(`[EB] SubagentEnded returned ${res.status}`);
      } catch (err) {
        console.error(`[EB] SubagentEnded failed: ${err}`);
      } finally {
        span.end();
      }
    });
  }
  async dispose(sessionKey, sessionId) {
    return tracer.startActiveSpan("context.dispose", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/dispose`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ session_key: sessionKey, session_id: sessionId })
        });
        if (!res.ok) console.warn(`[EB] Dispose returned ${res.status}`);
      } catch (err) {
        console.error(`[EB] Dispose failed: ${err}`);
      } finally {
        span.end();
      }
    });
  }
  async getConfig() {
    return tracer.startActiveSpan("context.getConfig", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/config`, { headers: this.getHeaders() });
        if (!res.ok) throw new Error(`GetConfig failed: ${res.status}`);
        return await res.json();
      } catch (err) {
        console.warn(`[EB] GetConfig failed, using defaults: ${err}`);
        return { ingest_batch_size: 6, ingest_batch_timeout_ms: 6e4 };
      } finally {
        span.end();
      }
    });
  }
  // --- Session Reporting ---
  async reportContextWindow(report) {
    try {
      await fetch(`${this.baseUrl}/sessions/context-window`, {
        method: "POST",
        headers: this.getHeaders(),
        body: JSON.stringify(report)
      });
    } catch (err) {
      console.error(`[EB] ReportContextWindow failed: ${err}`);
    }
  }
  async reportTokenUsage(report) {
    try {
      await fetch(`${this.baseUrl}/sessions/token-usage`, {
        method: "POST",
        headers: this.getHeaders(),
        body: JSON.stringify(report)
      });
    } catch (err) {
      console.error(`[EB] ReportTokenUsage failed: ${err}`);
    }
  }
};

// ../shared/envelope.ts
function stripOpenClawEnvelope(prompt) {
  if (!prompt) return "";
  if (!prompt.startsWith("Sender (untrusted metadata):")) {
    return prompt.trim();
  }
  const match = prompt.match(/^[\s\S]*\n\[[^\]]+\]\s+([\s\S]+)$/);
  return match ? match[1].trim() : prompt.trim();
}

// src/engine.ts
var ContextEngineImpl = class {
  info = {
    id: "elephantbroker-context",
    name: "ElephantBroker ContextEngine",
    ownsCompaction: true
  };
  client;
  currentSessionKey = "agent:main:main";
  currentSessionId = "";
  currentAgentId = "";
  currentAgentKey = "";
  profileName;
  gatewayId;
  batchSize;
  // Degraded ingest() buffering (AD-29)
  messageBuffer = [];
  degradedModeWarned = false;
  // Last turn messages buffer (GF-04: forwarded from agent_end hook)
  lastTurnMessages = [];
  // LLM event tracking (AD-11)
  modelReported = false;
  contextWindowTokens = null;
  constructor(client, options = {}) {
    this.client = client;
    this.batchSize = options.batchSize ?? 6;
    this.profileName = options.profileName ?? "coding";
    this.gatewayId = options.gatewayId ?? "";
  }
  // --- Public getters for hooks (before_prompt_build needs these) ---
  getSessionKey() {
    return this.currentSessionKey;
  }
  getSessionId() {
    return this.currentSessionId;
  }
  setSessionContext(sessionKey, sessionId) {
    this.currentSessionKey = sessionKey;
    this.currentSessionId = sessionId;
    this.client.setSessionContext(sessionKey, sessionId);
  }
  setLastTurnMessages(messages) {
    this.lastTurnMessages = messages;
  }
  setAgentIdentity(agentId, agentKey) {
    this.currentAgentId = agentId;
    this.currentAgentKey = agentKey;
    this.client.setAgentIdentity(agentId, agentKey);
  }
  // --- Lifecycle Methods (OpenClaw interface) ---
  async bootstrap(params) {
    const sessionId = params.sessionId || crypto.randomUUID();
    const sessionKey = params.sessionKey || this.currentSessionKey;
    this.setSessionContext(sessionKey, sessionId);
    if (this.gatewayId && !this.currentAgentKey) {
      this.setAgentIdentity("main", `${this.gatewayId}:main`);
    }
    return this.client.bootstrap({
      session_key: this.currentSessionKey,
      session_id: this.currentSessionId,
      profile_name: this.profileName,
      gateway_id: "",
      // Populated by middleware from X-EB-Gateway-ID header
      agent_key: this.currentAgentKey,
      is_subagent: false,
      parent_session_key: void 0
    });
  }
  async ingest(params) {
    if (!this.degradedModeWarned) {
      console.warn("[ElephantBroker] Degraded mode: receiving ingest() instead of ingestBatch(). Messages buffered in-memory.");
      this.degradedModeWarned = true;
    }
    this.messageBuffer.push(params.message);
    if (this.messageBuffer.length >= this.batchSize) {
      await this.flushBuffer();
    }
    return { ingested: true };
  }
  async ingestBatch(params) {
    return this.client.ingestBatch({
      session_id: this.currentSessionId,
      session_key: this.currentSessionKey,
      messages: params.messages,
      profile_name: this.profileName,
      is_heartbeat: false
    });
  }
  async assemble(params) {
    await this.flushBuffer();
    const result = await this.client.assemble({
      session_id: this.currentSessionId,
      session_key: this.currentSessionKey,
      messages: params.messages || [],
      profile_name: this.profileName,
      query: stripOpenClawEnvelope(params.prompt ?? ""),
      token_budget: params.tokenBudget,
      context_window_tokens: this.contextWindowTokens,
      goal_ids: void 0
    });
    return {
      messages: result.messages,
      estimatedTokens: result.estimated_tokens,
      systemPromptAddition: result.system_prompt_addition ?? void 0
    };
  }
  async compact(params) {
    return this.client.compact({
      session_id: this.currentSessionId,
      session_key: this.currentSessionKey,
      force: params.force || false,
      token_budget: void 0,
      current_token_count: void 0,
      compaction_target: void 0,
      custom_instructions: void 0,
      runtime_context: {}
    });
  }
  async afterTurn(params) {
    await this.flushBuffer();
    const messages = params.messages && params.messages.length > 0 ? params.messages : this.lastTurnMessages;
    this.lastTurnMessages = [];
    if (messages.length === 0) {
      console.warn("[EB] afterTurn: no messages available (params.messages and lastTurnMessages both empty)");
    }
    await this.client.afterTurn({
      session_id: this.currentSessionId,
      session_key: this.currentSessionKey,
      messages,
      // P4: use `in` (not `||`) so 0 is preserved and "absent" stays absent.
      // `|| 0` would collapse 0/undefined into the same wire value, hiding
      // whether OpenClaw sent the signal — the Python side uses has-key to
      // decide whether to trust the plugin or derive via tail-walker.
      ..."prePromptMessageCount" in params ? { pre_prompt_message_count: params.prePromptMessageCount } : {},
      is_heartbeat: false
    });
  }
  async prepareSubagentSpawn(params) {
    const childKey = params.childSessionKey || params.childSessionId;
    if (!childKey) {
      console.error("[EB] prepareSubagentSpawn: neither childSessionKey nor childSessionId provided");
    }
    return this.client.subagentSpawn({
      parent_session_key: this.currentSessionKey,
      child_session_key: childKey || "",
      ttl_ms: void 0
    });
  }
  async onSubagentEnded(params) {
    const childKey = params.childSessionKey || params.childSessionId;
    if (!childKey) {
      console.error("[EB] onSubagentEnded: neither childSessionKey nor childSessionId provided");
    }
    await this.client.subagentEnded({
      child_session_key: childKey || "",
      reason: params.reason || "completed"
    });
  }
  async dispose() {
    await this.flushBuffer();
    this.messageBuffer = [];
    this.degradedModeWarned = false;
    this.modelReported = false;
  }
  // --- LLM Event Handlers (called from hooks, not from OpenClaw engine interface) ---
  onLlmInput(event) {
    if (this.modelReported) return;
    this.modelReported = true;
    this.contextWindowTokens = event.context_window_tokens || null;
    this.client.reportContextWindow({
      session_key: this.currentSessionKey,
      session_id: this.currentSessionId,
      provider: event.provider || "unknown",
      model: event.model || "unknown",
      context_window_tokens: event.context_window_tokens || 128e3
    }).catch((err) => console.error(`[EB] onLlmInput report failed: ${err}`));
  }
  onLlmOutput(event) {
    this.client.reportTokenUsage({
      session_key: this.currentSessionKey,
      session_id: this.currentSessionId,
      input_tokens: event.input_tokens || 0,
      output_tokens: event.output_tokens || 0,
      total_tokens: event.total_tokens || 0
    }).catch((err) => console.error(`[EB] onLlmOutput report failed: ${err}`));
  }
  // --- Internal ---
  async flushBuffer() {
    if (this.messageBuffer.length === 0) return;
    const messages = [...this.messageBuffer];
    this.messageBuffer = [];
    try {
      await this.client.ingestBatch({
        session_id: this.currentSessionId,
        session_key: this.currentSessionKey,
        messages,
        profile_name: this.profileName
      });
    } catch (err) {
      console.error(`[EB] Buffer flush failed: ${err}`);
    }
  }
};

// src/index.ts
function register(api) {
  const cfg = api.pluginConfig || {};
  const baseUrl = cfg.baseUrl || process.env.EB_RUNTIME_URL || "http://localhost:8420";
  const profileName = cfg.profileName || process.env.EB_PROFILE || "coding";
  const gatewayId = cfg.gatewayId || process.env.EB_GATEWAY_ID;
  const gatewayShortName = cfg.gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME;
  const client = new ContextEngineClient(baseUrl, gatewayId, gatewayShortName);
  const engine = new ContextEngineImpl(client, { profileName, gatewayId });
  client.getConfig().then((config) => {
    if (config.ingest_batch_size) {
      Object.assign(engine, { batchSize: config.ingest_batch_size });
    }
  }).catch((err) => {
    console.warn(`[EB] Failed to fetch batch config, using defaults: ${err}`);
  });
  if (api.registerContextEngine) {
    api.registerContextEngine("elephantbroker-context", () => engine);
  }
  api.on("before_prompt_build", async (event, ctx) => {
    const hookCtx = ctx || {};
    try {
      const sk = hookCtx.sessionKey || engine.getSessionKey();
      const sid = hookCtx.sessionId || engine.getSessionId();
      const overlay = await client.buildOverlay(sk, sid);
      return {
        prependSystemContext: overlay.prepend_system_context || void 0,
        appendSystemContext: overlay.append_system_context || void 0,
        prependContext: overlay.prepend_context || void 0
      };
    } catch (err) {
      console.error(`[EB] before_prompt_build failed: ${err}`);
      return {};
    }
  });
  api.on("agent_end", (event, ctx) => {
    const hookEvent = event;
    const messages = hookEvent.messages || [];
    console.info(`[EB] Hook agent_end (context): buffering ${messages.length} messages for afterTurn`);
    engine.setLastTurnMessages(messages);
  });
  api.on("llm_input", (event, ctx) => {
    const llmEvent = event;
    engine.onLlmInput(llmEvent);
  });
  api.on("llm_output", (event, ctx) => {
    const llmEvent = event;
    engine.onLlmOutput(llmEvent);
  });
}
export {
  ContextEngineClient,
  ContextEngineImpl,
  register
};
