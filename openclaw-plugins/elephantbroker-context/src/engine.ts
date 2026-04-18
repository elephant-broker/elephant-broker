/**
 * ContextEngineImpl — implements OpenClaw's ContextEngine interface.
 *
 * Key behaviors:
 * - ownsCompaction: true — tells OpenClaw this plugin handles compaction
 * - 0 tools registered — all context decisions are internal
 * - Degraded ingest() mode: buffers single messages, flushes at batchSize (AD-29)
 * - Coexists with memory plugin (separate hooks, complementary)
 *
 * OpenClaw calls all lifecycle methods with single params objects (e.g.,
 * bootstrap({ sessionId })), NOT positional args. This engine maps OpenClaw's
 * camelCase params to the HTTP client's snake_case payloads.
 */
import type { ContextEngineClient } from "./client.js";
import type {
  AgentMessage,
  BootstrapResult,
  CompactResult,
  IngestBatchResult,
  OCAfterTurnParams,
  OCAssembleParams,
  OCAssembleResult,
  OCBootstrapParams,
  OCCompactParams,
  OCIngestBatchParams,
  OCIngestParams,
  OCSubagentEndedParams,
  OCSubagentSpawnParams,
  SubagentSpawnResult,
} from "./types.js";

export interface ContextEngineOptions {
  batchSize?: number;
  profileName?: string;
  gatewayId?: string;
}

/**
 * OpenClaw wraps user messages in a sender-metadata envelope before forwarding
 * to the context engine via `params.prompt`:
 *
 *   Sender (untrusted metadata):
 *   ```json
 *   {...}
 *   ```
 *
 *   [YYYY-MM-DD HH:MM UTC] <user's actual text>
 *
 * Retrieval needs just the user's text, not the envelope. Extract the portion
 * after the last `\n[<timestamp>] ` marker. If the pattern doesn't match
 * (non-enveloped prompt), return the input trimmed.
 */
export function stripOpenClawEnvelope(prompt: string): string {
  if (!prompt) return "";
  const match = prompt.match(/\n\[[^\]]+\]\s+([\s\S]+)$/);
  return match ? match[1].trim() : prompt.trim();
}

export class ContextEngineImpl {
  readonly info = {
    id: "elephantbroker-context",
    name: "ElephantBroker ContextEngine",
    ownsCompaction: true,
  };

  private client: ContextEngineClient;
  private currentSessionKey = "agent:main:main";
  private currentSessionId = "";
  private currentAgentId = "";
  private currentAgentKey = "";
  private profileName: string;
  private gatewayId: string;
  private batchSize: number;

  // Degraded ingest() buffering (AD-29)
  private messageBuffer: AgentMessage[] = [];
  private degradedModeWarned = false;

  // Last turn messages buffer (GF-04: forwarded from agent_end hook)
  private lastTurnMessages: AgentMessage[] = [];

  // LLM event tracking (AD-11)
  private modelReported = false;
  private contextWindowTokens: number | null = null;

  constructor(client: ContextEngineClient, options: ContextEngineOptions = {}) {
    this.client = client;
    this.batchSize = options.batchSize ?? 6;
    this.profileName = options.profileName ?? "coding";
    this.gatewayId = options.gatewayId ?? "";
  }

  // --- Public getters for hooks (before_prompt_build needs these) ---

  getSessionKey(): string { return this.currentSessionKey; }
  getSessionId(): string { return this.currentSessionId; }

  setSessionContext(sessionKey: string, sessionId: string): void {
    this.currentSessionKey = sessionKey;
    this.currentSessionId = sessionId;
    this.client.setSessionContext(sessionKey, sessionId);
  }

  setLastTurnMessages(messages: AgentMessage[]): void {
    this.lastTurnMessages = messages;
  }

  setAgentIdentity(agentId: string, agentKey: string): void {
    this.currentAgentId = agentId;
    this.currentAgentKey = agentKey;
    this.client.setAgentIdentity(agentId, agentKey);
  }

  // --- Lifecycle Methods (OpenClaw interface) ---

  async bootstrap(params: OCBootstrapParams): Promise<BootstrapResult> {
    const sessionId = params.sessionId || crypto.randomUUID();
    const sessionKey = params.sessionKey || this.currentSessionKey;
    this.setSessionContext(sessionKey, sessionId);

    // Derive agent identity if gatewayId is configured
    if (this.gatewayId && !this.currentAgentKey) {
      this.setAgentIdentity("main", `${this.gatewayId}:main`);
    }

    return this.client.bootstrap({
      session_key: this.currentSessionKey,
      session_id: this.currentSessionId,
      profile_name: this.profileName,
      gateway_id: "",  // Populated by middleware from X-EB-Gateway-ID header
      agent_key: this.currentAgentKey,
      is_subagent: false,
      parent_session_key: undefined,
    });
  }

  async ingest(params: OCIngestParams): Promise<{ ingested: boolean }> {
    // Degraded mode (AD-29): buffer single messages, flush at batchSize
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

  async ingestBatch(params: OCIngestBatchParams): Promise<IngestBatchResult> {
    // Primary path — no buffering needed
    return this.client.ingestBatch({
      session_id: this.currentSessionId,
      session_key: this.currentSessionKey,
      messages: params.messages,
      profile_name: this.profileName,
      is_heartbeat: false,
    });
  }

  async assemble(params: OCAssembleParams): Promise<OCAssembleResult> {
    // Flush degraded buffer before assembly (AD-29)
    await this.flushBuffer();
    const result = await this.client.assemble({
      session_id: this.currentSessionId,
      session_key: this.currentSessionKey,
      messages: params.messages || [],
      profile_name: this.profileName,
      query: stripOpenClawEnvelope(params.prompt ?? ""),
      token_budget: params.tokenBudget,
      context_window_tokens: this.contextWindowTokens,
      goal_ids: undefined,
    });
    // [DIAG-55-C] tool_use/tool_result pair audit — TD-55 hypothesis test
    try {
      const perMsg: Array<{ i: number; role: string; tool_use?: string[]; tool_result_for?: string[] }> = [];
      const allToolUseIds = new Set<string>();
      const allToolResultRefs = new Set<string>();
      (result.messages || []).forEach((m: any, i: number) => {
        const entry: { i: number; role: string; tool_use?: string[]; tool_result_for?: string[] } = { i, role: m.role };
        if (Array.isArray(m.content)) {
          const tu: string[] = m.content.filter((b: any) => b?.type === "tool_use").map((b: any) => b.id).filter(Boolean);
          const tr: string[] = m.content.filter((b: any) => b?.type === "tool_result").map((b: any) => b.tool_use_id).filter(Boolean);
          if (tu.length) { entry.tool_use = tu; if (m.role === "assistant") tu.forEach(id => allToolUseIds.add(id)); }
          if (tr.length) { entry.tool_result_for = tr; tr.forEach(id => allToolResultRefs.add(id)); }
        }
        perMsg.push(entry);
      });
      const orphanToolUses = [...allToolUseIds].filter(id => !allToolResultRefs.has(id));
      const orphanToolResults = [...allToolResultRefs].filter(id => !allToolUseIds.has(id));
      console.error(`[DIAG-55-C] session_id=${this.currentSessionId} session_key=${this.currentSessionKey} params_msgs=${(params.messages || []).length} result_msgs=${result.messages.length} orphan_tool_uses=${JSON.stringify(orphanToolUses)} orphan_tool_results=${JSON.stringify(orphanToolResults)} per_msg=${JSON.stringify(perMsg)}`);
    } catch (diagErr) {
      console.error(`[DIAG-55-C] audit_failed err=${diagErr}`);
    }
    // Map snake_case response to camelCase for OpenClaw
    return {
      messages: result.messages,
      estimatedTokens: result.estimated_tokens,
      systemPromptAddition: result.system_prompt_addition ?? undefined,
    };
  }

  async compact(params: OCCompactParams): Promise<CompactResult> {
    return this.client.compact({
      session_id: this.currentSessionId,
      session_key: this.currentSessionKey,
      force: params.force || false,
      token_budget: undefined,
      current_token_count: undefined,
      compaction_target: undefined,
      custom_instructions: undefined,
      runtime_context: {},
    });
  }

  async afterTurn(params: OCAfterTurnParams): Promise<void> {
    // Flush degraded buffer before after-turn (AD-29)
    await this.flushBuffer();
    // GF-04: read messages from OpenClaw params (primary) or fallback to agent_end buffer
    const messages = (params.messages && params.messages.length > 0) ? params.messages : this.lastTurnMessages;
    this.lastTurnMessages = [];
    if (messages.length === 0) {
      console.warn("[EB] afterTurn: no messages available (params.messages and lastTurnMessages both empty)");
    }
    await this.client.afterTurn({
      session_id: this.currentSessionId,
      session_key: this.currentSessionKey,
      messages,
      pre_prompt_message_count: params.prePromptMessageCount || 0,
      is_heartbeat: false,
    });
  }

  async prepareSubagentSpawn(params: OCSubagentSpawnParams): Promise<SubagentSpawnResult> {
    const childKey = params.childSessionKey || params.childSessionId;
    if (!childKey) {
      console.error("[EB] prepareSubagentSpawn: neither childSessionKey nor childSessionId provided");
    }
    return this.client.subagentSpawn({
      parent_session_key: this.currentSessionKey,
      child_session_key: childKey || "",
      ttl_ms: undefined,
    });
  }

  async onSubagentEnded(params: OCSubagentEndedParams): Promise<void> {
    const childKey = params.childSessionKey || params.childSessionId;
    if (!childKey) {
      console.error("[EB] onSubagentEnded: neither childSessionKey nor childSessionId provided");
    }
    await this.client.subagentEnded({
      child_session_key: childKey || "",
      reason: params.reason || "completed",
    });
  }

  async dispose(): Promise<void> {
    // Engine teardown: clear per-turn transient state only (GF-15)
    // DO NOT call /context/dispose — session state persists in Redis
    // DO NOT reset sessionKey/sessionId — stable across turns
    await this.flushBuffer();
    this.messageBuffer = [];
    this.degradedModeWarned = false;
    this.modelReported = false;
    // PRESERVED: currentSessionKey, currentSessionId, contextWindowTokens, lastTurnMessages
  }

  // --- LLM Event Handlers (called from hooks, not from OpenClaw engine interface) ---

  onLlmInput(event: { provider?: string; model?: string; context_window_tokens?: number }): void {
    if (this.modelReported) return;
    this.modelReported = true;
    this.contextWindowTokens = event.context_window_tokens || null;
    // Fire-and-forget
    this.client.reportContextWindow({
      session_key: this.currentSessionKey,
      session_id: this.currentSessionId,
      provider: event.provider || "unknown",
      model: event.model || "unknown",
      context_window_tokens: event.context_window_tokens || 128000,
    }).catch((err) => console.error(`[EB] onLlmInput report failed: ${err}`));
  }

  onLlmOutput(event: { input_tokens?: number; output_tokens?: number; total_tokens?: number }): void {
    // Fire-and-forget — every call
    this.client.reportTokenUsage({
      session_key: this.currentSessionKey,
      session_id: this.currentSessionId,
      input_tokens: event.input_tokens || 0,
      output_tokens: event.output_tokens || 0,
      total_tokens: event.total_tokens || 0,
    }).catch((err) => console.error(`[EB] onLlmOutput report failed: ${err}`));
  }

  // --- Internal ---

  private async flushBuffer(): Promise<void> {
    if (this.messageBuffer.length === 0) return;
    const messages = [...this.messageBuffer];
    this.messageBuffer = [];
    try {
      await this.client.ingestBatch({
        session_id: this.currentSessionId,
        session_key: this.currentSessionKey,
        messages,
        profile_name: this.profileName,
      });
    } catch (err) {
      console.error(`[EB] Buffer flush failed: ${err}`);
    }
  }
}
