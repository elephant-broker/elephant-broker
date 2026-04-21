// src/client.ts
import { trace, context, propagation, SpanKind } from "@opentelemetry/api";
var tracer = trace.getTracer("elephantbroker.memory-plugin");
var HttpStatusError = class extends Error {
  status;
  constructor(status, message) {
    super(message);
    this.name = "HttpStatusError";
    this.status = status;
  }
};
var ElephantBrokerClient = class {
  baseUrl;
  sessionKeyCache = /* @__PURE__ */ new Map();
  currentSessionKey = "agent:main:main";
  currentSessionId = "";
  // Gateway identity
  gatewayId;
  gatewayShortName;
  agentId = "";
  agentKey = "";
  actorId = "";
  // Phase 8: for admin API authorization
  constructor(baseUrl = "http://localhost:8420", gatewayId, gatewayShortName) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.gatewayId = gatewayId || process.env.EB_GATEWAY_ID || "";
    if (!this.gatewayId) {
      throw new Error(
        "EB_GATEWAY_ID is required. Set it via the gatewayId constructor option or EB_GATEWAY_ID env var."
      );
    }
    this.gatewayShortName = gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME || this.gatewayId.substring(0, 8);
  }
  setAgentIdentity(agentId, agentKey) {
    this.agentId = agentId;
    this.agentKey = agentKey;
  }
  getHeaders() {
    const headers = {
      "Content-Type": "application/json",
      "X-EB-Gateway-ID": this.gatewayId
    };
    if (this.agentKey) headers["X-EB-Agent-Key"] = this.agentKey;
    if (this.agentId) headers["X-EB-Agent-ID"] = this.agentId;
    if (this.currentSessionKey) headers["X-EB-Session-Key"] = this.currentSessionKey;
    if (this.actorId) headers["X-EB-Actor-Id"] = this.actorId;
    propagation.inject(context.active(), headers);
    return headers;
  }
  async search(request) {
    return tracer.startActiveSpan("memory.search", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/search`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request)
        });
        if (!res.ok) throw new Error(`Search failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async store(request) {
    return tracer.startActiveSpan("memory.store", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/store`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request)
        });
        if (res.status === 409) return null;
        if (!res.ok) throw new Error(`Store failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async getById(factId) {
    return tracer.startActiveSpan("memory.getById", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/${factId}`, {
          headers: this.getHeaders()
        });
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`Get failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async forget(factId) {
    return tracer.startActiveSpan("memory.forget", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/${factId}`, {
          method: "DELETE",
          headers: this.getHeaders()
        });
        if (res.status === 403) throw new HttpStatusError(403, `Permission denied: fact ${factId} belongs to another gateway`);
        if (res.status === 404) throw new HttpStatusError(404, `Fact not found: ${factId}`);
        if (!res.ok) throw new HttpStatusError(res.status, `Delete failed: ${res.status}`);
      } finally {
        span.end();
      }
    });
  }
  async update(factId, updates) {
    return tracer.startActiveSpan("memory.update", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/${factId}`, {
          method: "PATCH",
          headers: this.getHeaders(),
          body: JSON.stringify(updates)
        });
        if (res.status === 403) throw new HttpStatusError(403, `Permission denied: fact ${factId} belongs to another gateway`);
        if (res.status === 404) throw new HttpStatusError(404, `Fact not found: ${factId}`);
        if (res.status === 422) throw new HttpStatusError(422, `Invalid update payload for ${factId}`);
        if (!res.ok) throw new HttpStatusError(res.status, `Update failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async ingestMessages(request) {
    return tracer.startActiveSpan("memory.ingestMessages", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        await fetch(`${this.baseUrl}/memory/ingest-messages`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request)
        });
      } finally {
        span.end();
      }
    });
  }
  async sessionStart(request) {
    return tracer.startActiveSpan("memory.sessionStart", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        await fetch(`${this.baseUrl}/sessions/start`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request)
        });
      } finally {
        span.end();
      }
    });
  }
  async sessionEnd(request) {
    return tracer.startActiveSpan("memory.sessionEnd", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/sessions/end`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request)
        });
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  // --- Session Goal methods ---
  async listSessionGoals() {
    return tracer.startActiveSpan("goals.listSession", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId
        });
        const res = await fetch(`${this.baseUrl}/goals/session?${params}`, {
          headers: this.getHeaders()
        });
        if (!res.ok) throw new Error(`List session goals failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async createSessionGoal(request) {
    return tracer.startActiveSpan("goals.createSession", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId
        });
        const res = await fetch(`${this.baseUrl}/goals/session?${params}`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({
            title: request.title,
            description: request.description || "",
            parent_goal_id: request.parent_goal_id || null,
            success_criteria: request.success_criteria || []
          })
        });
        if (res.status === 409) return null;
        if (!res.ok) throw new Error(`Create session goal failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async updateSessionGoalStatus(goalId, request) {
    return tracer.startActiveSpan("goals.updateSessionStatus", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId
        });
        const res = await fetch(`${this.baseUrl}/goals/session/${goalId}?${params}`, {
          method: "PATCH",
          headers: this.getHeaders(),
          body: JSON.stringify({
            status: request.status,
            evidence: request.evidence
          })
        });
        if (res.status === 404) throw new Error(`Goal not found: ${goalId}`);
        if (!res.ok) throw new Error(`Update session goal status failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async addSessionGoalBlocker(goalId, request) {
    return tracer.startActiveSpan("goals.addSessionBlocker", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId
        });
        const res = await fetch(`${this.baseUrl}/goals/session/${goalId}/blocker?${params}`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ blocker: request.blocker })
        });
        if (res.status === 404) throw new Error(`Goal not found: ${goalId}`);
        if (!res.ok) throw new Error(`Add blocker failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async recordSessionGoalProgress(goalId, request) {
    return tracer.startActiveSpan("goals.recordSessionProgress", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId
        });
        const res = await fetch(`${this.baseUrl}/goals/session/${goalId}/progress?${params}`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ evidence: request.evidence })
        });
        if (res.status === 404) throw new Error(`Goal not found: ${goalId}`);
        if (!res.ok) throw new Error(`Record progress failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  // --- Procedure methods ---
  async createProcedure(request) {
    return tracer.startActiveSpan("procedures.create", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/procedures/`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({
            name: request.name,
            description: request.description || "",
            scope: request.scope || "session",
            steps: request.steps.map((s) => ({
              order: s.order,
              instruction: s.instruction,
              is_optional: s.is_optional || false
            })),
            enabled: request.enabled ?? true
          })
        });
        if (!res.ok) throw new Error(`Create procedure failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async activateProcedure(procedureId, request) {
    return tracer.startActiveSpan("procedures.activate", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/procedures/${procedureId}/activate`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ actor_id: request.actor_id })
        });
        if (res.status === 404) throw new Error(`Procedure not found: ${procedureId}`);
        if (!res.ok) throw new Error(`Activate procedure failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async completeProcedureStep(executionId, stepId, request) {
    return tracer.startActiveSpan("procedures.completeStep", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(
          `${this.baseUrl}/procedures/${executionId}/step/${stepId}/complete`,
          {
            method: "POST",
            headers: this.getHeaders(),
            body: JSON.stringify({
              evidence: request.evidence,
              proof_type: request.proof_type
            })
          }
        );
        if (res.status === 404) throw new Error(`Execution or step not found: ${executionId}/${stepId}`);
        if (!res.ok) throw new Error(`Complete step failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async getSessionProcedureStatus() {
    return tracer.startActiveSpan("procedures.sessionStatus", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId
        });
        const res = await fetch(`${this.baseUrl}/procedures/session/status?${params}`, {
          headers: this.getHeaders()
        });
        if (!res.ok) throw new Error(`Get session procedure status failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  // --- Guard tools ---
  async getActiveGuards() {
    return tracer.startActiveSpan("guards.getActive", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(
          `${this.baseUrl}/guards/active/${this.currentSessionId}`,
          { headers: this.getHeaders() }
        );
        if (!res.ok) throw new Error(`Get active guards failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async getGuardEventDetail(guardEventId) {
    return tracer.startActiveSpan("guards.getEventDetail", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_id: this.currentSessionId
        });
        const res = await fetch(
          `${this.baseUrl}/guards/events/detail/${guardEventId}?${params}`,
          { headers: this.getHeaders() }
        );
        if (res.status === 404) throw new Error(`Guard event not found: ${guardEventId}`);
        if (!res.ok) throw new Error(`Get guard event detail failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  // --- Artifact methods (Amendment 6.2.3) ---
  async searchArtifacts(query, maxResults = 5) {
    return tracer.startActiveSpan("artifacts.search", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/artifacts/search`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ query, max_results: maxResults })
        });
        if (!res.ok) throw new Error(`Search artifacts failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async searchSessionArtifacts(request) {
    return tracer.startActiveSpan("artifacts.searchSession", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/artifacts/session/search`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({
            session_key: request.session_key || this.currentSessionKey,
            session_id: request.session_id || this.currentSessionId,
            query: request.query,
            tool_name: request.tool_name,
            max_results: request.max_results || 5
          })
        });
        if (!res.ok) throw new Error(`Search session artifacts failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async getSessionArtifact(artifactId) {
    return tracer.startActiveSpan("artifacts.getSession", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId
        });
        const res = await fetch(
          `${this.baseUrl}/artifacts/session/${artifactId}?${params}`,
          { headers: this.getHeaders() }
        );
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`Get session artifact failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  async createArtifact(request) {
    return tracer.startActiveSpan("artifacts.create", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/artifacts/create`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({
            content: request.content,
            tool_name: request.tool_name || "manual",
            scope: request.scope || "session",
            session_key: request.session_key || this.currentSessionKey,
            session_id: request.session_id || this.currentSessionId,
            tags: request.tags || [],
            goal_id: request.goal_id,
            summary: request.summary
          })
        });
        if (!res.ok) throw new Error(`Create artifact failed: ${res.status}`);
        return await res.json();
      } finally {
        span.end();
      }
    });
  }
  // --- Session context management ---
  cacheSessionKey(sessionKey, sessionId) {
    this.sessionKeyCache.set(sessionKey, sessionId);
    this.currentSessionKey = sessionKey;
    this.currentSessionId = sessionId;
  }
  getCachedSessionId(sessionKey) {
    return this.sessionKeyCache.get(sessionKey);
  }
  setSessionContext(sessionKey, sessionId) {
    this.currentSessionKey = sessionKey;
    this.currentSessionId = sessionId;
  }
  getSessionKey() {
    return this.currentSessionKey;
  }
  getSessionId() {
    return this.currentSessionId;
  }
  setActorId(actorId) {
    this.actorId = actorId;
  }
  // --- Phase 8: Admin API methods ---
  async createPersistentGoal(request) {
    return tracer.startActiveSpan("admin.createPersistentGoal", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/goals`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request)
        });
        return await r.json();
      } finally {
        span.end();
      }
    });
  }
  async adminCreateOrg(request) {
    return tracer.startActiveSpan("admin.createOrg", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/organizations`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request)
        });
        return await r.json();
      } finally {
        span.end();
      }
    });
  }
  async adminCreateTeam(request) {
    return tracer.startActiveSpan("admin.createTeam", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/teams`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request)
        });
        return await r.json();
      } finally {
        span.end();
      }
    });
  }
  async adminRegisterActor(request) {
    return tracer.startActiveSpan("admin.registerActor", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/actors`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request)
        });
        return await r.json();
      } finally {
        span.end();
      }
    });
  }
  async adminAddMember(teamId, actorId) {
    return tracer.startActiveSpan("admin.addMember", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/teams/${teamId}/members`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ actor_id: actorId })
        });
        return await r.json();
      } finally {
        span.end();
      }
    });
  }
  async adminRemoveMember(teamId, actorId) {
    return tracer.startActiveSpan("admin.removeMember", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/teams/${teamId}/members/${actorId}`, {
          method: "DELETE",
          headers: this.getHeaders()
        });
        return await r.json();
      } finally {
        span.end();
      }
    });
  }
  async adminMergeActors(canonicalId, duplicateId) {
    return tracer.startActiveSpan("admin.mergeActors", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/actors/${canonicalId}/merge`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ duplicate_id: duplicateId })
        });
        return await r.json();
      } finally {
        span.end();
      }
    });
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

// src/format.ts
function formatMemoryContext(results) {
  if (results.length === 0) return "";
  const lines = results.map((r) => {
    const conf = r.confidence.toFixed(2);
    return `- [${r.category}] ${r.text} (confidence: ${conf})`;
  });
  return [
    '<relevant-memories source="elephantbroker">',
    ...lines,
    "</relevant-memories>"
  ].join("\n");
}

// src/tools/memory_search.ts
function createMemorySearchTool(client) {
  return {
    id: "memory_search",
    name: "memory_search",
    description: "Search long-term memory for relevant facts, preferences, and context.",
    parameters: {
      type: "object",
      properties: {
        query: { type: "string", description: "Search query" },
        max_results: { type: "number", description: "Maximum results to return" },
        scope: { type: "string", description: "Scope filter (global, session, actor)" },
        memory_class: { type: "string", description: "Memory class filter" }
      },
      required: ["query"]
    },
    async execute(toolCallId, params, signal) {
      const results = await client.search({
        query: params.query,
        max_results: params.max_results,
        scope: params.scope,
        memory_class: params.memory_class
      });
      return {
        content: [{ type: "text", text: JSON.stringify({
          results: results.map((r) => ({
            fact_id: r.id,
            text: r.text,
            category: r.category,
            memory_class: r.memory_class,
            confidence: r.confidence,
            score: r.score,
            created_at: r.created_at
          })),
          total: results.length
        }) }]
      };
    }
  };
}

// src/tools/memory_get.ts
function createMemoryGetTool(client) {
  return {
    id: "memory_get",
    name: "memory_get",
    description: "Get a specific memory fact by its ID.",
    parameters: {
      type: "object",
      properties: {
        fact_id: { type: "string", description: "The fact ID to retrieve" }
      },
      required: ["fact_id"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.getById(params.fact_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/memory_store.ts
function createMemoryStoreTool(client) {
  return {
    id: "memory_store",
    name: "memory_store",
    description: "Store a new fact in long-term memory.",
    parameters: {
      type: "object",
      properties: {
        text: { type: "string", description: "The fact text to store" },
        category: { type: "string", description: "Category (preference, decision, event, etc.)" },
        scope: { type: "string", description: "Visibility scope" },
        confidence: { type: "number", description: "Confidence 0.0-1.0" }
      },
      required: ["text"]
    },
    async execute(toolCallId, params, signal) {
      const sid = client.getSessionId();
      const result = await client.store({
        fact: {
          text: params.text,
          category: params.category || "general",
          scope: params.scope,
          confidence: params.confidence
        },
        session_key: client.getSessionKey(),
        ...sid ? { session_id: sid } : {}
      });
      if (result === null) {
        return {
          content: [{ type: "text", text: "Fact not stored: near-duplicate already exists in memory." }]
        };
      }
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/memory_forget.ts
function forgetErrorResult(err) {
  if (err instanceof HttpStatusError) {
    if (err.status === 404) {
      return { deleted: null, reason: "not_found" };
    }
    if (err.status === 403) {
      return { deleted: null, reason: "forbidden", detail: err.message };
    }
    if (err.status >= 500) {
      return { deleted: null, reason: "backend_error", status: err.status, detail: err.message };
    }
    return { deleted: null, reason: "error", status: err.status, detail: err.message };
  }
  return { deleted: null, reason: "error", detail: err instanceof Error ? err.message : String(err) };
}
function createMemoryForgetTool(client) {
  return {
    id: "memory_forget",
    name: "memory_forget",
    description: "Delete a memory. Provide fact_id for direct delete, or query to search-then-delete.",
    parameters: {
      type: "object",
      properties: {
        fact_id: { type: "string", description: "Direct fact ID to delete" },
        query: { type: "string", description: "Search query to find fact to delete" }
      }
    },
    async execute(toolCallId, params, signal) {
      if (params.fact_id) {
        try {
          await client.forget(params.fact_id);
          return {
            content: [{ type: "text", text: JSON.stringify({ deleted: params.fact_id }) }]
          };
        } catch (err) {
          return {
            content: [{ type: "text", text: JSON.stringify(forgetErrorResult(err)) }]
          };
        }
      }
      if (params.query) {
        const results = await client.search({ query: params.query, max_results: 1 });
        if (results.length > 0 && results[0].score > 0.7) {
          try {
            await client.forget(results[0].id);
            return {
              content: [{ type: "text", text: JSON.stringify({ deleted: results[0].id, text: results[0].text.slice(0, 80) }) }]
            };
          } catch (err) {
            return {
              content: [{ type: "text", text: JSON.stringify(forgetErrorResult(err)) }]
            };
          }
        }
        return {
          content: [{ type: "text", text: JSON.stringify({ deleted: null, reason: "no match above threshold" }) }]
        };
      }
      return {
        content: [{ type: "text", text: JSON.stringify({ deleted: null, reason: "provide fact_id or query" }) }]
      };
    }
  };
}

// src/tools/memory_update.ts
function updateErrorResult(err) {
  if (err instanceof HttpStatusError) {
    if (err.status === 404) {
      return { updated: null, reason: "not_found" };
    }
    if (err.status === 403) {
      return { updated: null, reason: "forbidden", detail: err.message };
    }
    if (err.status === 422) {
      return { updated: null, reason: "invalid_input", detail: err.message };
    }
    if (err.status >= 500) {
      return { updated: null, reason: "backend_error", status: err.status, detail: err.message };
    }
    return { updated: null, reason: "error", status: err.status, detail: err.message };
  }
  return { updated: null, reason: "error", detail: err instanceof Error ? err.message : String(err) };
}
function createMemoryUpdateTool(client) {
  return {
    id: "memory_update",
    name: "memory_update",
    description: "Update an existing memory. Provide fact_id for direct update, or query to search-then-update.",
    parameters: {
      type: "object",
      properties: {
        fact_id: { type: "string", description: "Direct fact ID to update" },
        query: { type: "string", description: "Search query to find fact to update" },
        new_text: { type: "string", description: "New text content" },
        updates: { type: "object", description: "Other field updates (confidence, category, etc.)" }
      }
    },
    async execute(toolCallId, params, signal) {
      const updateBody = { ...params.updates };
      if (params.new_text) updateBody.text = params.new_text;
      let targetId = params.fact_id;
      if (!targetId && params.query) {
        const results = await client.search({ query: params.query, max_results: 1 });
        if (results.length > 0 && results[0].score > 0.7) {
          targetId = results[0].id;
        } else {
          return {
            content: [{ type: "text", text: JSON.stringify({ updated: null, reason: "no match above threshold" }) }]
          };
        }
      }
      if (!targetId) return {
        content: [{ type: "text", text: JSON.stringify({ updated: null, reason: "provide fact_id or query" }) }]
      };
      try {
        const result = await client.update(targetId, updateBody);
        return {
          content: [{ type: "text", text: JSON.stringify({ updated: targetId, fact: result }) }]
        };
      } catch (err) {
        return {
          content: [{ type: "text", text: JSON.stringify(updateErrorResult(err)) }]
        };
      }
    }
  };
}

// src/tools/session_goals_list.ts
function createSessionGoalsListTool(client) {
  return {
    id: "session_goals_list",
    name: "session_goals_list",
    description: "Returns the full tree of session goals with their IDs, status, blockers, sub-goals, and confidence. Check this before creating goals to avoid duplicates.",
    parameters: {
      type: "object",
      properties: {}
    },
    async execute(toolCallId, params, signal) {
      const result = await client.listSessionGoals();
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/goal_create.ts
function createGoalCreateTool(client) {
  return {
    id: "goal_create",
    name: "goal_create",
    description: "Create a session goal (ephemeral, current session only). Goals are stored in Redis for fast access during the session and flushed to long-term memory on session end. Persistent goals are created via admin tools only. Always call session_goals_list first to avoid duplicates.",
    parameters: {
      type: "object",
      properties: {
        title: { type: "string", description: "Clear, specific goal title" },
        description: { type: "string", description: "What needs to be done and why" },
        parent_goal_id: {
          type: "string",
          description: "UUID of parent goal (for sub-tasks)"
        },
        success_criteria: {
          type: "array",
          items: { type: "string" },
          description: "How to know this goal is done"
        }
      },
      required: ["title"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.createSessionGoal({
        title: params.title,
        description: params.description,
        parent_goal_id: params.parent_goal_id,
        success_criteria: params.success_criteria || []
      });
      if (result === null) {
        return {
          content: [{ type: "text", text: JSON.stringify({ error: "duplicate", message: "A goal with this title already exists in the session" }) }]
        };
      }
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/admin_create_org.ts
function createAdminCreateOrgTool(client) {
  return {
    id: "admin_create_org",
    name: "admin_create_org",
    description: "Create a new organization. Requires authority_level >= 90 (system admin).",
    parameters: {
      type: "object",
      properties: {
        name: { type: "string", description: "Organization full name" },
        display_label: { type: "string", description: "Short label for logs/dashboard" }
      },
      required: ["name"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.adminCreateOrg(params);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/admin_create_team.ts
function createAdminCreateTeamTool(client) {
  return {
    id: "admin_create_team",
    name: "admin_create_team",
    description: "Create a new team within an organization. Requires authority_level >= 70 (org admin) and matching org.",
    parameters: {
      type: "object",
      properties: {
        name: { type: "string", description: "Team full name" },
        display_label: { type: "string", description: "Short label for logs/dashboard" },
        org_id: { type: "string", description: "Parent organization UUID" }
      },
      required: ["name", "org_id"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.adminCreateTeam(params);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/admin_register_actor.ts
function createAdminRegisterActorTool(client) {
  return {
    id: "admin_register_actor",
    name: "admin_register_actor",
    description: "Register a new actor (human or agent). Requires authority_level >= 70 (org admin).",
    parameters: {
      type: "object",
      properties: {
        display_name: { type: "string", description: "Actor display name" },
        type: {
          type: "string",
          enum: ["human_coordinator", "human_operator", "worker_agent", "manager_agent", "reviewer_agent", "supervisor_agent"],
          description: "Actor type"
        },
        authority_level: { type: "number", description: "Authority level (0-100)" },
        org_id: { type: "string", description: "Organization UUID" },
        team_ids: { type: "array", items: { type: "string" }, description: "Team UUIDs" },
        handles: { type: "array", items: { type: "string" }, description: "Platform-qualified handles (e.g. email:admin@acme.com)" }
      },
      required: ["display_name", "type"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.adminRegisterActor(params);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/admin_add_member.ts
function createAdminAddMemberTool(client) {
  return {
    id: "admin_add_member",
    name: "admin_add_member",
    description: "Add an actor to a team. Requires authority_level >= 50 (team lead) and matching team.",
    parameters: {
      type: "object",
      properties: {
        team_id: { type: "string", description: "Team UUID" },
        actor_id: { type: "string", description: "Actor UUID to add" }
      },
      required: ["team_id", "actor_id"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.adminAddMember(params.team_id, params.actor_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/admin_remove_member.ts
function createAdminRemoveMemberTool(client) {
  return {
    id: "admin_remove_member",
    name: "admin_remove_member",
    description: "Remove an actor from a team. Requires authority_level >= 50 (team lead) and matching team.",
    parameters: {
      type: "object",
      properties: {
        team_id: { type: "string", description: "Team UUID" },
        actor_id: { type: "string", description: "Actor UUID to remove" }
      },
      required: ["team_id", "actor_id"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.adminRemoveMember(params.team_id, params.actor_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/admin_merge_actors.ts
function createAdminMergeActorsTool(client) {
  return {
    id: "admin_merge_actors",
    name: "admin_merge_actors",
    description: "Merge a duplicate actor into a canonical one. Transfers all handles, team memberships, and graph edges (CREATED_BY, OWNS_GOAL, MEMBER_OF) from the duplicate to the canonical actor. Requires authority_level >= 70 (org admin).",
    parameters: {
      type: "object",
      properties: {
        canonical_id: { type: "string", description: "UUID of the canonical (surviving) actor" },
        duplicate_id: { type: "string", description: "UUID of the duplicate actor to merge and delete" }
      },
      required: ["canonical_id", "duplicate_id"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.adminMergeActors(params.canonical_id, params.duplicate_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/session_goals_update_status.ts
function createSessionGoalsUpdateStatusTool(client) {
  return {
    id: "session_goals_update_status",
    name: "session_goals_update_status",
    description: "Update a session goal's status. Get the goal_id from session_goals_list first. Use 'completed' with evidence when done, 'paused' to switch context, 'abandoned' when dropped.",
    parameters: {
      type: "object",
      properties: {
        goal_id: { type: "string", description: "UUID from session_goals_list" },
        status: {
          type: "string",
          enum: ["completed", "paused", "abandoned"],
          description: "New status for the goal"
        },
        evidence: { type: "string", description: "What was accomplished or why status changed" }
      },
      required: ["goal_id", "status"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.updateSessionGoalStatus(params.goal_id, {
        status: params.status,
        evidence: params.evidence
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/session_goals_add_blocker.ts
function createSessionGoalsAddBlockerTool(client) {
  return {
    id: "session_goals_add_blocker",
    name: "session_goals_add_blocker",
    description: "Report a blocker on a session goal. Blocked goals get elevated priority \u2014 they are always injected into your context. Get goal_id from session_goals_list.",
    parameters: {
      type: "object",
      properties: {
        goal_id: { type: "string", description: "UUID from session_goals_list" },
        blocker: { type: "string", description: "What is blocking progress" }
      },
      required: ["goal_id", "blocker"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.addSessionGoalBlocker(params.goal_id, {
        blocker: params.blocker
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/session_goals_progress.ts
function createSessionGoalsProgressTool(client) {
  return {
    id: "session_goals_progress",
    name: "session_goals_progress",
    description: "Record meaningful progress on a session goal. Increases the goal's confidence. Get goal_id from session_goals_list.",
    parameters: {
      type: "object",
      properties: {
        goal_id: { type: "string", description: "UUID from session_goals_list" },
        evidence: { type: "string", description: "What progress was made" }
      },
      required: ["goal_id", "evidence"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.recordSessionGoalProgress(params.goal_id, {
        evidence: params.evidence
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/procedure_create.ts
function createProcedureCreateTool(client) {
  return {
    id: "procedure_create",
    name: "procedure_create",
    description: "Create a new procedure \u2014 a known sequence of steps for accomplishing a task. Each step has an instruction and optional evidence requirements.",
    parameters: {
      type: "object",
      properties: {
        name: { type: "string", description: "Short name for the procedure" },
        description: { type: "string", description: "What this procedure accomplishes" },
        scope: { type: "string", description: "Visibility scope (session, actor, team, global)" },
        steps: {
          type: "array",
          items: {
            type: "object",
            properties: {
              order: { type: "number", description: "Step order (0-indexed)" },
              instruction: { type: "string", description: "What to do in this step" },
              is_optional: { type: "boolean", description: "Whether this step can be skipped" }
            },
            required: ["order", "instruction"]
          },
          description: "Ordered list of procedure steps"
        }
      },
      required: ["name", "steps"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.createProcedure({
        name: params.name,
        description: params.description,
        scope: params.scope,
        steps: params.steps
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/procedure_activate.ts
function createProcedureActivateTool(client) {
  return {
    id: "procedure_activate",
    name: "procedure_activate",
    description: "Activate a procedure for execution. Returns an execution object with an execution_id that tracks progress through the procedure steps.",
    parameters: {
      type: "object",
      properties: {
        procedure_id: { type: "string", description: "UUID of the procedure to activate" },
        actor_id: { type: "string", description: "UUID of the actor executing the procedure" }
      },
      required: ["procedure_id", "actor_id"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.activateProcedure(params.procedure_id, {
        actor_id: params.actor_id
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/procedure_complete.ts
function createProcedureCompleteTool(client) {
  return {
    id: "procedure_complete_step",
    name: "procedure_complete_step",
    description: "Mark a procedure step as complete. Provide the execution_id and step_id from the active procedure execution, plus optional evidence of completion.",
    parameters: {
      type: "object",
      properties: {
        execution_id: { type: "string", description: "UUID of the active procedure execution" },
        step_id: { type: "string", description: "UUID of the step to mark complete" },
        evidence: { type: "string", description: "Evidence that the step was completed" },
        proof_type: {
          type: "string",
          enum: ["diff_hash", "chunk_ref", "receipt", "version_record", "supervisor_sign_off"],
          description: "Type of proof provided"
        }
      },
      required: ["execution_id", "step_id"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.completeProcedureStep(params.execution_id, params.step_id, {
        evidence: params.evidence,
        proof_type: params.proof_type
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/procedure_status.ts
function createProcedureStatusTool(client) {
  return {
    id: "procedure_session_status",
    name: "procedure_session_status",
    description: "Get the status of all active procedure executions in the current session. Returns each execution with its procedure_id, current step, and completed steps.",
    parameters: {
      type: "object",
      properties: {}
    },
    async execute(toolCallId, params, signal) {
      const result = await client.getSessionProcedureStatus();
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/guards_list.ts
function createGuardsListTool(client) {
  return {
    id: "guards_list",
    name: "guards_list",
    description: "List active guard rules, pending approval requests, and recent guard events for the current session. Use this to understand what safety constraints are in effect and whether any actions are awaiting human approval.",
    parameters: {
      type: "object",
      properties: {}
    },
    async execute(toolCallId, params, signal) {
      const result = await client.getActiveGuards();
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/guard_status.ts
function createGuardStatusTool(client) {
  return {
    id: "guard_status",
    name: "guard_status",
    description: "Get detailed information about a specific guard event, including the matched rules, outcome, and approval status if applicable. Use the guard_event_id from guards_list results.",
    parameters: {
      type: "object",
      properties: {
        guard_event_id: {
          type: "string",
          description: "The guard event ID to look up (from guards_list results)"
        }
      },
      required: ["guard_event_id"]
    },
    async execute(toolCallId, params, signal) {
      const result = await client.getGuardEventDetail(params.guard_event_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }]
      };
    }
  };
}

// src/tools/artifact_search.ts
var UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function createArtifactSearchTool(client) {
  return {
    id: "artifact_search",
    name: "artifact_search",
    description: `Search or retrieve tool output artifacts by query or exact ID. When you see '[Tool output: X \u2014 summary \u2192 Call artifact_search("id") for full output]' in your context, use this tool with the provided ID to get the full content.`,
    parameters: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Search query text or exact artifact UUID for direct retrieval"
        },
        tool_name: {
          type: "string",
          description: "Optional filter by source tool name (e.g., 'bash', 'python')"
        },
        scope: {
          type: "string",
          enum: ["session", "persistent", "all"],
          description: "Search scope: 'session' (current session only), 'persistent' (knowledge graph), 'all' (both, default)"
        },
        max_results: {
          type: "number",
          description: "Maximum results to return (default 5, max 50)"
        }
      },
      required: ["query"]
    },
    async execute(toolCallId, params, signal) {
      const query = params.query?.trim() || "";
      const scope = params.scope || "all";
      const maxResults = Math.min(params.max_results || 5, 50);
      if (UUID_RE.test(query)) {
        if (scope === "session" || scope === "all") {
          try {
            const result = await client.getSessionArtifact(query);
            if (result) {
              return {
                content: [{ type: "text", text: JSON.stringify({
                  results: [{
                    artifact_id: result.artifact_id,
                    tool_name: result.tool_name,
                    summary: result.summary,
                    content: result.content,
                    created_at: result.created_at,
                    token_estimate: result.token_estimate,
                    scope: "session"
                  }],
                  total: 1,
                  lookup_type: "direct_session"
                }) }]
              };
            }
          } catch (err) {
            console.warn(`[EB] Session artifact lookup failed: ${err}`);
          }
        }
      }
      const allResults = [];
      if (scope === "session" || scope === "all") {
        try {
          const sessionResults = await client.searchSessionArtifacts({
            query,
            tool_name: params.tool_name,
            max_results: maxResults
          });
          allResults.push(
            ...sessionResults.map((r) => ({
              artifact_id: r.artifact_id,
              tool_name: r.tool_name,
              summary: r.summary,
              created_at: r.created_at,
              token_estimate: 0,
              scope: "session"
            }))
          );
        } catch (err) {
          console.warn(`[EB] Session artifact search failed: ${err}`);
        }
      }
      if (scope === "persistent" || scope === "all") {
        try {
          const persistentResults = await client.searchArtifacts(query, maxResults);
          allResults.push(
            ...persistentResults.map((r) => ({
              artifact_id: r.artifact_id,
              tool_name: r.tool_name,
              summary: r.summary,
              created_at: r.created_at,
              token_estimate: r.token_estimate || 0,
              scope: "persistent"
            }))
          );
        } catch (err) {
          console.warn(`[EB] Persistent artifact search failed: ${err}`);
        }
      }
      const seen = /* @__PURE__ */ new Set();
      const deduped = allResults.filter((r) => {
        if (seen.has(r.artifact_id)) return false;
        seen.add(r.artifact_id);
        return true;
      });
      return {
        content: [{ type: "text", text: JSON.stringify({
          results: deduped.slice(0, maxResults),
          total: deduped.length,
          lookup_type: "search"
        }) }]
      };
    }
  };
}

// src/tools/create_artifact.ts
function createArtifactCreateTool(client) {
  return {
    id: "create_artifact",
    name: "create_artifact",
    description: "Save content as a session-scoped (temporary, auto-expires) or persistent (permanent, stored in knowledge graph) artifact. Session artifacts are automatically cleaned up. Persistent artifacts survive across sessions.",
    parameters: {
      type: "object",
      properties: {
        content: {
          type: "string",
          description: "The artifact content to save"
        },
        tool_name: {
          type: "string",
          description: "Source tool or 'manual' (default: 'manual')"
        },
        scope: {
          type: "string",
          enum: ["session", "persistent"],
          description: "Scope: 'session' (temporary) or 'persistent' (permanent in knowledge graph)"
        },
        tags: {
          type: "array",
          items: { type: "string" },
          description: "Optional tags for categorization and search"
        },
        goal_id: {
          type: "string",
          description: "Optional link to a session goal (persistent scope only)"
        },
        summary: {
          type: "string",
          description: "Optional human summary (auto-generated from content if not provided)"
        }
      },
      required: ["content"]
    },
    async execute(toolCallId, params, signal) {
      const content = params.content?.trim() || "";
      if (!content) {
        return {
          content: [{ type: "text", text: JSON.stringify({ error: "content is required", artifact_id: null }) }]
        };
      }
      try {
        const result = await client.createArtifact({
          content,
          tool_name: params.tool_name || "manual",
          scope: params.scope || "session",
          tags: params.tags || [],
          goal_id: params.goal_id,
          summary: params.summary
        });
        const artifactId = "artifact_id" in result ? result.artifact_id : void 0;
        const scopeUsed = params.scope || "session";
        return {
          content: [{ type: "text", text: JSON.stringify({
            artifact_id: artifactId || "unknown",
            scope: scopeUsed,
            status: "created",
            message: scopeUsed === "persistent" ? "Artifact saved permanently to knowledge graph" : "Artifact saved for this session (auto-expires)"
          }) }]
        };
      } catch (err) {
        console.error(`[EB] Create artifact failed: ${err}`);
        return {
          content: [{ type: "text", text: JSON.stringify({
            error: `Failed to create artifact: ${err}`,
            artifact_id: null
          }) }]
        };
      }
    }
  };
}

// src/index.ts
function register(api) {
  const cfg = api.pluginConfig || {};
  const baseUrl = cfg.baseUrl || process.env.EB_RUNTIME_URL || "http://localhost:8420";
  const profileName = cfg.profileName || process.env.EB_PROFILE || "coding";
  const gatewayId = cfg.gatewayId || process.env.EB_GATEWAY_ID;
  const gatewayShortName = cfg.gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME;
  const client = new ElephantBrokerClient(baseUrl, gatewayId, gatewayShortName);
  const configActorId = cfg.actorId || process.env.EB_ACTOR_ID;
  if (configActorId) {
    client.setActorId(configActorId);
  }
  let currentSessionKey = "agent:main:main";
  let currentSessionId = crypto.randomUUID();
  let currentAgentId = "";
  let currentAgentKey = "";
  client.setSessionContext(currentSessionKey, currentSessionId);
  api.registerTool(createMemorySearchTool(client));
  api.registerTool(createMemoryGetTool(client));
  api.registerTool(createMemoryStoreTool(client));
  api.registerTool(createMemoryForgetTool(client));
  api.registerTool(createMemoryUpdateTool(client));
  api.registerTool(createSessionGoalsListTool(client));
  api.registerTool(createGoalCreateTool(client));
  api.registerTool(createSessionGoalsUpdateStatusTool(client));
  api.registerTool(createSessionGoalsAddBlockerTool(client));
  api.registerTool(createSessionGoalsProgressTool(client));
  api.registerTool(createProcedureCreateTool(client));
  api.registerTool(createProcedureActivateTool(client));
  api.registerTool(createProcedureCompleteTool(client));
  api.registerTool(createProcedureStatusTool(client));
  api.registerTool(createGuardsListTool(client));
  api.registerTool(createGuardStatusTool(client));
  api.registerTool(createArtifactSearchTool(client));
  api.registerTool(createArtifactCreateTool(client));
  api.registerTool(createAdminCreateOrgTool(client));
  api.registerTool(createAdminCreateTeamTool(client));
  api.registerTool(createAdminRegisterActorTool(client));
  api.registerTool(createAdminAddMemberTool(client));
  api.registerTool(createAdminRemoveMemberTool(client));
  api.registerTool(createAdminMergeActorsTool(client));
  api.on("before_agent_start", async (event, ctx) => {
    const hookEvent = event;
    const hookCtx = ctx || {};
    if (hookCtx.sessionKey) {
      currentSessionKey = hookCtx.sessionKey;
      client.setSessionContext(currentSessionKey, currentSessionId);
    }
    if (hookCtx.agentId && gatewayId) {
      currentAgentId = hookCtx.agentId;
      currentAgentKey = `${gatewayId}:${hookCtx.agentId}`;
      client.setAgentIdentity(currentAgentId, currentAgentKey);
    }
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
        max_results: 10
      });
      if (results.length > 0) {
        const contextStr = formatMemoryContext(results);
        console.info(`[EB] Auto-recall: injecting ${results.length} memories into context`);
        return { prependSystemContext: contextStr };
      }
    } catch (err) {
      console.error(`[EB] Error: before_agent_start failed: ${err}`);
    }
    return {};
  });
  api.on("agent_end", (event, ctx) => {
    const hookContext = event;
    const allMessages = hookContext.messages || [];
    if (allMessages.length === 0) return;
    console.info(`[EB] Hook agent_end: sending ${allMessages.length} messages for ingest`);
    client.ingestMessages({
      session_key: currentSessionKey,
      session_id: currentSessionId,
      messages: allMessages,
      profile_name: profileName
    }).catch((err) => {
      console.error(`[EB] Error: agent_end ingest failed: ${err}`);
    });
  });
  api.on("session_start", async (event, ctx) => {
    const hookEvent = event || {};
    const hookCtx = ctx || {};
    currentSessionId = hookEvent.sessionId || hookCtx.sessionId || crypto.randomUUID();
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
        agent_key: currentAgentKey
      });
    } catch (err) {
      console.error(`[EB] Error: session_start failed: ${err}`);
    }
  });
  api.on("session_end", async (event, ctx) => {
    console.info(`[EB] Hook session_end: flushing buffer and ending session ${currentSessionId}`);
    try {
      await client.sessionEnd({
        session_key: currentSessionKey,
        session_id: currentSessionId,
        gateway_id: gatewayId,
        agent_key: currentAgentKey
      });
    } catch (err) {
      console.error(`[EB] Error: session_end failed: ${err}`);
    }
  });
}
export {
  ElephantBrokerClient,
  HttpStatusError,
  formatMemoryContext,
  register
};
