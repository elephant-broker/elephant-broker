import { ElephantBrokerClient } from "./client.js";
import { formatMemoryContext, stripOpenClawEnvelope } from "./format.js";
import { createMemorySearchTool } from "./tools/memory_search.js";
import { createMemoryGetTool } from "./tools/memory_get.js";
import { createMemoryStoreTool } from "./tools/memory_store.js";
import { createMemoryForgetTool } from "./tools/memory_forget.js";
import { createMemoryUpdateTool } from "./tools/memory_update.js";
import { createSessionGoalsListTool } from "./tools/session_goals_list.js";
import { createGoalCreateTool } from "./tools/goal_create.js";
import { createAdminCreateOrgTool } from "./tools/admin_create_org.js";
import { createAdminCreateTeamTool } from "./tools/admin_create_team.js";
import { createAdminRegisterActorTool } from "./tools/admin_register_actor.js";
import { createAdminAddMemberTool } from "./tools/admin_add_member.js";
import { createAdminRemoveMemberTool } from "./tools/admin_remove_member.js";
import { createAdminMergeActorsTool } from "./tools/admin_merge_actors.js";
import { createSessionGoalsUpdateStatusTool } from "./tools/session_goals_update_status.js";
import { createSessionGoalsAddBlockerTool } from "./tools/session_goals_add_blocker.js";
import { createSessionGoalsProgressTool } from "./tools/session_goals_progress.js";
import { createProcedureCreateTool } from "./tools/procedure_create.js";
import { createProcedureActivateTool } from "./tools/procedure_activate.js";
import { createProcedureCompleteTool } from "./tools/procedure_complete.js";
import { createProcedureStatusTool } from "./tools/procedure_status.js";
import { createGuardsListTool } from "./tools/guards_list.js";
import { createGuardStatusTool } from "./tools/guard_status.js";
import { createArtifactSearchTool } from "./tools/artifact_search.js";
import { createArtifactCreateTool } from "./tools/create_artifact.js";

export interface PluginAPI {
  registerTool(tool: unknown): void;
  on(event: string, handler: (...args: unknown[]) => unknown): void;
  pluginConfig?: Record<string, unknown>;
}

/**
 * Register the ElephantBroker Memory Plugin with an OpenClaw-compatible host.
 *
 * Config resolution: api.pluginConfig (openclaw.json) > process.env > defaults
 */
export function register(api: PluginAPI) {
  const cfg = (api.pluginConfig || {}) as Record<string, string | undefined>;
  const baseUrl = cfg.baseUrl || process.env.EB_RUNTIME_URL || "http://localhost:8420";
  const profileName = cfg.profileName || process.env.EB_PROFILE || "coding";
  const gatewayId = cfg.gatewayId || process.env.EB_GATEWAY_ID;
  const gatewayShortName = cfg.gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME;
  const client = new ElephantBrokerClient(baseUrl, gatewayId, gatewayShortName);

  // GF-06: populate actor ID from config/env as fallback
  const configActorId = cfg.actorId || process.env.EB_ACTOR_ID;
  if (configActorId) {
    client.setActorId(configActorId);
  }

  let currentSessionKey = "agent:main:main";
  let currentSessionId: string = crypto.randomUUID();
  let currentAgentId = "";
  let currentAgentKey = "";

  // Sync client session context
  client.setSessionContext(currentSessionKey, currentSessionId);

  // Register 5 memory tools
  api.registerTool(createMemorySearchTool(client));
  api.registerTool(createMemoryGetTool(client));
  api.registerTool(createMemoryStoreTool(client));
  api.registerTool(createMemoryForgetTool(client));
  api.registerTool(createMemoryUpdateTool(client));

  // Register 4 session goal tools + 1 unified goal_create (replaces session_goals_create)
  api.registerTool(createSessionGoalsListTool(client));
  api.registerTool(createGoalCreateTool(client));
  api.registerTool(createSessionGoalsUpdateStatusTool(client));
  api.registerTool(createSessionGoalsAddBlockerTool(client));
  api.registerTool(createSessionGoalsProgressTool(client));

  // Register 4 procedure tools
  api.registerTool(createProcedureCreateTool(client));
  api.registerTool(createProcedureActivateTool(client));
  api.registerTool(createProcedureCompleteTool(client));
  api.registerTool(createProcedureStatusTool(client));

  // Register 2 guard tools
  api.registerTool(createGuardsListTool(client));
  api.registerTool(createGuardStatusTool(client));

  // Register 2 artifact tools (Amendment 6.2.3)
  api.registerTool(createArtifactSearchTool(client));
  api.registerTool(createArtifactCreateTool(client));

  // Register 6 admin tools (Phase 8 — authority-gated, server returns 403 if insufficient)
  api.registerTool(createAdminCreateOrgTool(client));
  api.registerTool(createAdminCreateTeamTool(client));
  api.registerTool(createAdminRegisterActorTool(client));
  api.registerTool(createAdminAddMemberTool(client));
  api.registerTool(createAdminRemoveMemberTool(client));
  api.registerTool(createAdminMergeActorsTool(client));

  // Hook: before_agent_start — learn agentId + auto-recall
  api.on("before_agent_start", async (event: unknown, ctx: unknown) => {
    const hookEvent = event as { prompt?: string };
    const hookCtx = (ctx || {}) as { sessionKey?: string; agentId?: string; actorId?: string; userId?: string };
    if (hookCtx.sessionKey) {
      currentSessionKey = hookCtx.sessionKey;
      client.setSessionContext(currentSessionKey, currentSessionId);
    }
    // Learn agentId from hook context
    if (hookCtx.agentId && gatewayId) {
      currentAgentId = hookCtx.agentId;
      currentAgentKey = `${gatewayId}:${hookCtx.agentId}`;
      client.setAgentIdentity(currentAgentId, currentAgentKey);
    }
    // GF-06: populate actor ID from session/actor context
    const actorId = hookCtx.actorId || hookCtx.userId;
    if (actorId) {
      client.setActorId(actorId);
    }

    const query = stripOpenClawEnvelope(hookEvent.prompt ?? "");
    if (!query) return {};

    console.info(`[EB] Hook before_agent_start: querying memories for session ${currentSessionKey}`);
    try {
      const results = await client.search({
        query,
        session_key: currentSessionKey,
        session_id: currentSessionId,
        profile_name: profileName,
        auto_recall: true,
        max_results: 10,
      });

      if (results.length > 0) {
        const contextStr = formatMemoryContext(results);
        console.info(`[EB] Auto-recall: injecting ${results.length} memories into context`);
        // Slot: `prependSystemContext` (NOT `prependContext`). TF-ER-001 BUG-1 / TODO-5-212:
        // Phase 6 AD-4 assigned `prependContext` to the context plugin's Surface B
        // working-set overlay (per-turn assembled items). The memory plugin's
        // auto-recall XML block is cross-turn background ("what we already know
        // about this agent/user") and belongs in the system-context slot.
        // Writing to `prependContext` here caused both plugins to land in the same
        // OpenClaw merge slot on every turn. See local/KNOWN-BUGS.md §BUG-1.
        return { prependSystemContext: contextStr };
      }
    } catch (err) {
      console.error(`[EB] Error: before_agent_start failed: ${err}`);
    }
    return {};
  });

  // Hook: agent_end — auto-capture (fire-and-forget, do NOT await)
  // Both plugins register agent_end: memory plugin uses it for ingest (fire-and-forget),
  // context plugin uses it to buffer messages for afterTurn(). This is intentional —
  // OpenClaw delivers hook events to all registered handlers.
  api.on("agent_end", (event: unknown, ctx: unknown) => {
    const hookContext = event as { messages?: Array<{ role: string; content: string | unknown[]; [key: string]: unknown }> };
    const allMessages = hookContext.messages || [];
    if (allMessages.length === 0) return;

    console.info(`[EB] Hook agent_end: sending ${allMessages.length} messages for ingest`);
    // Fire-and-forget — do not block the hook return
    client.ingestMessages({
      session_key: currentSessionKey,
      session_id: currentSessionId,
      messages: allMessages,
      profile_name: profileName,
    }).catch((err) => {
      console.error(`[EB] Error: agent_end ingest failed: ${err}`);
    });
  });

  // Hook: session_start
  // GF-07: This hook fires on FIRST TURN (inside attempt runner), not at sessions.create.
  // Our code handles this correctly — we update session state whenever the hook fires.
  api.on("session_start", async (event: unknown, ctx: unknown) => {
    // GF-08: OpenClaw passes { sessionId, resumedFrom } in event, sessionKey in ctx
    const hookEvent = (event || {}) as { sessionId?: string; resumedFrom?: string };
    const hookCtx = (ctx || {}) as { sessionKey?: string; sessionId?: string; parentSessionKey?: string };

    // Session ID from event (primary) or ctx (fallback)
    currentSessionId = hookEvent.sessionId || hookCtx.sessionId || crypto.randomUUID();

    // Session key from ctx (GF-02 pattern)
    if (hookCtx.sessionKey) {
      currentSessionKey = hookCtx.sessionKey;
    }

    client.cacheSessionKey(currentSessionKey, currentSessionId);
    console.info(`[EB] Hook session_start: session ${currentSessionId}${hookEvent.resumedFrom ? ` (resumed from ${hookEvent.resumedFrom})` : ""}`);

    try {
      await client.sessionStart({
        session_key: currentSessionKey,
        session_id: currentSessionId,
        parent_session_key: hookCtx.parentSessionKey,
        gateway_id: gatewayId,
        gateway_short_name: gatewayShortName,
        agent_id: currentAgentId,
        agent_key: currentAgentKey,
      });
    } catch (err) {
      console.error(`[EB] Error: session_start failed: ${err}`);
    }
  });

  // Hook: session_end
  api.on("session_end", async (event: unknown, ctx: unknown) => {
    console.info(`[EB] Hook session_end: flushing buffer and ending session ${currentSessionId}`);
    try {
      await client.sessionEnd({
        session_key: currentSessionKey,
        session_id: currentSessionId,
        gateway_id: gatewayId,
        agent_key: currentAgentKey,
      });
    } catch (err) {
      console.error(`[EB] Error: session_end failed: ${err}`);
    }
  });
}

export { ElephantBrokerClient, HttpStatusError } from "./client.js";
export { formatMemoryContext } from "./format.js";
export type * from "./types.js";
