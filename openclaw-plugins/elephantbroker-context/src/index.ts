/**
 * ElephantBroker ContextEngine Plugin — registers with OpenClaw's context-engine slot.
 *
 * Coexists with the memory-plugin (separate hooks, complementary):
 * - Memory plugin: tools (24) + hooks (before_agent_start, agent_end, session_start, session_end)
 * - Context plugin: 0 tools + registerContextEngine + hooks (before_prompt_build, agent_end, llm_input, llm_output)
 *
 * Key: ownsCompaction = true — tells OpenClaw this plugin handles compaction.
 *
 * OpenClaw calls engine lifecycle methods DIRECTLY (bootstrap, ingest, assemble, etc.)
 * when registered via registerContextEngine(). The session_start/session_end hooks are
 * NOT used — OpenClaw handles those through the engine's bootstrap() and dispose().
 */
import { ContextEngineClient } from "./client.js";
import { ContextEngineImpl } from "./engine.js";
import type { AgentMessage } from "./types.js";

export interface PluginAPI {
  registerTool(tool: unknown): void;
  on(event: string, handler: (...args: unknown[]) => unknown): void;
  registerContextEngine?(id: string, factory: () => unknown): void;
  pluginConfig?: Record<string, unknown>;
}

/**
 * Register the ElephantBroker ContextEngine Plugin with an OpenClaw-compatible host.
 *
 * Config resolution: api.pluginConfig (openclaw.json) > process.env > defaults
 */
export function register(api: PluginAPI) {
  const cfg = (api.pluginConfig || {}) as Record<string, string | undefined>;
  const baseUrl = cfg.baseUrl || process.env.EB_RUNTIME_URL || "http://localhost:8420";
  const profileName = cfg.profileName || process.env.EB_PROFILE || "coding";
  const gatewayId = cfg.gatewayId || process.env.EB_GATEWAY_ID;
  const gatewayShortName = cfg.gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME;
  const client = new ContextEngineClient(baseUrl, gatewayId, gatewayShortName);

  // Create engine with profile and gateway config
  const engine = new ContextEngineImpl(client, { profileName, gatewayId });

  // Fetch batch config from runtime (non-blocking, fire-and-forget).
  // Object.assign bypasses TypeScript private intentionally — OpenClaw's
  // ContextEngine interface exposes no setter for batchSize.
  client.getConfig().then((config) => {
    if (config.ingest_batch_size) {
      Object.assign(engine, { batchSize: config.ingest_batch_size });
    }
    // TODO(TD-29): ingest_batch_timeout_ms is fetched but not used —
    // timer-based flush in degraded mode is not yet implemented.
  }).catch((err) => {
    console.warn(`[EB] Failed to fetch batch config, using defaults: ${err}`);
  });

  // Context plugin registers 0 tools — all context decisions are internal
  // (Memory plugin registers the 24 agent-facing tools)

  // Register as the context engine with OpenClaw — OpenClaw calls
  // engine.bootstrap(), engine.assemble(), engine.dispose(), etc. directly
  if (api.registerContextEngine) {
    api.registerContextEngine("elephantbroker-context", () => engine);
  }

  // NOTE: session_start and session_end hooks are NOT registered here.
  // OpenClaw calls engine.bootstrap() and engine.dispose() directly when
  // a context engine is registered. Registering hooks too would cause
  // double bootstrap/dispose and race conditions.

  // Hook: before_prompt_build — Surface B (system prompt overlay)
  // This is NOT part of the ContextEngine interface — it's a separate hook.
  api.on("before_prompt_build", async (event: unknown, ctx: unknown) => {
    // H1: Read session identity from ctx (2nd arg), consistent with GF-02 pattern
    const hookCtx = (ctx || {}) as { sessionKey?: string; sessionId?: string };
    try {
      const sk = hookCtx.sessionKey || engine.getSessionKey();
      const sid = hookCtx.sessionId || engine.getSessionId();
      const overlay = await client.buildOverlay(sk, sid);
      return {
        prependSystemContext: overlay.prepend_system_context || undefined,
        appendSystemContext: overlay.append_system_context || undefined,
        prependContext: overlay.prepend_context || undefined,
      };
    } catch (err) {
      console.error(`[EB] before_prompt_build failed: ${err}`);
      return {};
    }
  });

  // Hook: agent_end — capture messages for afterTurn (GF-04)
  // Both plugins register agent_end: memory plugin uses it for ingest (fire-and-forget),
  // context plugin uses it to buffer messages for afterTurn(). This is intentional —
  // OpenClaw delivers hook events to all registered handlers.
  api.on("agent_end", (event: unknown, ctx: unknown) => {
    const hookEvent = event as { messages?: AgentMessage[] };
    const messages = hookEvent.messages || [];
    console.info(`[EB] Hook agent_end (context): buffering ${messages.length} messages for afterTurn`);
    engine.setLastTurnMessages(messages);
  });

  // Hook: llm_input — report context window (first call only)
  api.on("llm_input", (event: unknown, ctx: unknown) => {
    const llmEvent = event as { provider?: string; model?: string; context_window_tokens?: number };
    engine.onLlmInput(llmEvent);
  });

  // Hook: llm_output — report token usage (every call)
  api.on("llm_output", (event: unknown, ctx: unknown) => {
    const llmEvent = event as { input_tokens?: number; output_tokens?: number; total_tokens?: number };
    engine.onLlmOutput(llmEvent);
  });
}

export { ContextEngineClient } from "./client.js";
export { ContextEngineImpl } from "./engine.js";
export type * from "./types.js";
