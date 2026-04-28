import { trace, context, propagation, SpanKind } from "@opentelemetry/api";
import type {
  SearchRequest,
  SearchResult,
  StoreRequest,
  FactAssertion,
  IngestMessagesRequest,
  SessionStartRequest,
  SessionEndRequest,
  GoalState,
  GoalHierarchy,
  ProcedureDefinition,
  ProcedureExecution,
  SessionProcedureStatus,
  ToolArtifactSummary,
  SessionArtifactSummary,
  SessionArtifact,
  SessionArtifactSearchRequest,
  CreateArtifactRequest,
  CreateArtifactResponse,
} from "./types.js";

const tracer = trace.getTracer("elephantbroker.memory-plugin");

/**
 * Error thrown by client methods when the backend returns a non-2xx HTTP status.
 * Callers (tools) should discriminate on `.status` to map backend signals to
 * client-side result shapes rather than swallowing all failures uniformly.
 *
 * 403 — cross-tenant ownership rejection (post-C7 gateway-ownership check)
 * 404 — resource not found
 * 409 — conflict (dedup skip)
 * 422 — invalid input
 * 5xx — backend error — do NOT mask as not_found
 */
export class HttpStatusError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "HttpStatusError";
    this.status = status;
  }
}

export class ElephantBrokerClient {
  private baseUrl: string;
  private sessionKeyCache: Map<string, string> = new Map();
  private currentSessionKey = "agent:main:main";
  private currentSessionId = "";

  // Gateway identity
  private gatewayId: string;
  private gatewayShortName: string;
  private agentId = "";
  private agentKey = "";
  private actorId = "";  // Phase 8: for admin API authorization
  private profileName = "";  // C1.2b: enables eb_facts_stored_total{profile_name} attribution on /memory/store

  constructor(baseUrl: string = "http://localhost:8420", gatewayId?: string, gatewayShortName?: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.gatewayId = gatewayId || process.env.EB_GATEWAY_ID || "";
    if (!this.gatewayId) {
      throw new Error(
        "EB_GATEWAY_ID is required. Set it via the gatewayId constructor option or EB_GATEWAY_ID env var."
      );
    }
    this.gatewayShortName = gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME || this.gatewayId.substring(0, 8);
  }

  setAgentIdentity(agentId: string, agentKey: string): void {
    this.agentId = agentId;
    this.agentKey = agentKey;
  }

  private getHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-EB-Gateway-ID": this.gatewayId,
    };
    if (this.agentKey) headers["X-EB-Agent-Key"] = this.agentKey;
    if (this.agentId) headers["X-EB-Agent-ID"] = this.agentId;
    if (this.currentSessionKey) headers["X-EB-Session-Key"] = this.currentSessionKey;
    if (this.actorId) headers["X-EB-Actor-Id"] = this.actorId;
    // Inject W3C trace context
    propagation.inject(context.active(), headers);
    return headers;
  }

  async search(request: SearchRequest): Promise<SearchResult[]> {
    return tracer.startActiveSpan("memory.search", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/search`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request),
        });
        if (!res.ok) throw new Error(`Search failed: ${res.status}`);
        return (await res.json()) as SearchResult[];
      } finally {
        span.end();
      }
    });
  }

  async store(request: StoreRequest): Promise<FactAssertion | null> {
    return tracer.startActiveSpan("memory.store", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/store`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request),
        });
        if (res.status === 409) return null; // Near-duplicate detected (dedup skip)
        if (!res.ok) throw new Error(`Store failed: ${res.status}`);
        return (await res.json()) as FactAssertion;
      } finally {
        span.end();
      }
    });
  }

  async getById(factId: string): Promise<FactAssertion | null> {
    return tracer.startActiveSpan("memory.getById", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/${factId}`, {
          headers: this.getHeaders(),
        });
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`Get failed: ${res.status}`);
        return (await res.json()) as FactAssertion;
      } finally {
        span.end();
      }
    });
  }

  async forget(factId: string): Promise<void> {
    return tracer.startActiveSpan("memory.forget", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/${factId}`, {
          method: "DELETE",
          headers: this.getHeaders(),
        });
        if (res.status === 403) throw new HttpStatusError(403, `Permission denied: fact ${factId} belongs to another gateway`);
        if (res.status === 404) throw new HttpStatusError(404, `Fact not found: ${factId}`);
        if (!res.ok) throw new HttpStatusError(res.status, `Delete failed: ${res.status}`);
      } finally {
        span.end();
      }
    });
  }

  async update(factId: string, updates: Record<string, unknown>): Promise<FactAssertion> {
    return tracer.startActiveSpan("memory.update", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/memory/${factId}`, {
          method: "PATCH",
          headers: this.getHeaders(),
          body: JSON.stringify(updates),
        });
        if (res.status === 403) throw new HttpStatusError(403, `Permission denied: fact ${factId} belongs to another gateway`);
        if (res.status === 404) throw new HttpStatusError(404, `Fact not found: ${factId}`);
        if (res.status === 422) throw new HttpStatusError(422, `Invalid update payload for ${factId}`);
        if (!res.ok) throw new HttpStatusError(res.status, `Update failed: ${res.status}`);
        return (await res.json()) as FactAssertion;
      } finally {
        span.end();
      }
    });
  }

  async ingestMessages(request: IngestMessagesRequest): Promise<void> {
    return tracer.startActiveSpan("memory.ingestMessages", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        await fetch(`${this.baseUrl}/memory/ingest-messages`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request),
        });
      } finally {
        span.end();
      }
    });
  }

  async sessionStart(request: SessionStartRequest): Promise<void> {
    return tracer.startActiveSpan("memory.sessionStart", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        await fetch(`${this.baseUrl}/sessions/start`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request),
        });
      } finally {
        span.end();
      }
    });
  }

  async sessionEnd(request: SessionEndRequest): Promise<Record<string, unknown>> {
    return tracer.startActiveSpan("memory.sessionEnd", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/sessions/end`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(request),
        });
        return (await res.json()) as Record<string, unknown>;
      } finally {
        span.end();
      }
    });
  }

  // --- Session Goal methods ---

  async listSessionGoals(): Promise<GoalHierarchy> {
    return tracer.startActiveSpan("goals.listSession", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId,
        });
        const res = await fetch(`${this.baseUrl}/goals/session?${params}`, {
          headers: this.getHeaders(),
        });
        if (!res.ok) throw new Error(`List session goals failed: ${res.status}`);
        return (await res.json()) as GoalHierarchy;
      } finally {
        span.end();
      }
    });
  }

  async createSessionGoal(request: {
    title: string;
    description?: string;
    parent_goal_id?: string;
    success_criteria?: string[];
  }): Promise<GoalState | null> {
    return tracer.startActiveSpan("goals.createSession", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId,
        });
        const res = await fetch(`${this.baseUrl}/goals/session?${params}`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({
            title: request.title,
            description: request.description || "",
            parent_goal_id: request.parent_goal_id || null,
            success_criteria: request.success_criteria || [],
          }),
        });
        if (res.status === 409) return null; // Duplicate goal title in session
        if (!res.ok) throw new Error(`Create session goal failed: ${res.status}`);
        return (await res.json()) as GoalState;
      } finally {
        span.end();
      }
    });
  }

  async updateSessionGoalStatus(
    goalId: string,
    request: { status: string; evidence?: string },
  ): Promise<GoalState> {
    return tracer.startActiveSpan("goals.updateSessionStatus", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId,
        });
        const res = await fetch(`${this.baseUrl}/goals/session/${goalId}?${params}`, {
          method: "PATCH",
          headers: this.getHeaders(),
          body: JSON.stringify({
            status: request.status,
            evidence: request.evidence,
          }),
        });
        if (res.status === 404) throw new Error(`Goal not found: ${goalId}`);
        if (!res.ok) throw new Error(`Update session goal status failed: ${res.status}`);
        return (await res.json()) as GoalState;
      } finally {
        span.end();
      }
    });
  }

  async addSessionGoalBlocker(
    goalId: string,
    request: { blocker: string },
  ): Promise<GoalState> {
    return tracer.startActiveSpan("goals.addSessionBlocker", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId,
        });
        const res = await fetch(`${this.baseUrl}/goals/session/${goalId}/blocker?${params}`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ blocker: request.blocker }),
        });
        if (res.status === 404) throw new Error(`Goal not found: ${goalId}`);
        if (!res.ok) throw new Error(`Add blocker failed: ${res.status}`);
        return (await res.json()) as GoalState;
      } finally {
        span.end();
      }
    });
  }

  async recordSessionGoalProgress(
    goalId: string,
    request: { evidence: string },
  ): Promise<GoalState> {
    return tracer.startActiveSpan("goals.recordSessionProgress", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId,
        });
        const res = await fetch(`${this.baseUrl}/goals/session/${goalId}/progress?${params}`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ evidence: request.evidence }),
        });
        if (res.status === 404) throw new Error(`Goal not found: ${goalId}`);
        if (!res.ok) throw new Error(`Record progress failed: ${res.status}`);
        return (await res.json()) as GoalState;
      } finally {
        span.end();
      }
    });
  }

  // --- Procedure methods ---

  async createProcedure(request: {
    name: string;
    description?: string;
    scope?: string;
    steps: Array<{ order: number; instruction: string; is_optional?: boolean }>;
    enabled?: boolean;
    is_manual_only?: boolean;
    activation_modes?: string[];
  }): Promise<ProcedureDefinition> {
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
              is_optional: s.is_optional || false,
            })),
            enabled: request.enabled ?? true,
            is_manual_only: request.is_manual_only ?? true,
            ...(request.activation_modes ? { activation_modes: request.activation_modes } : {}),
          }),
        });
        if (!res.ok) throw new Error(`Create procedure failed: ${res.status}`);
        return (await res.json()) as ProcedureDefinition;
      } finally {
        span.end();
      }
    });
  }

  async activateProcedure(
    procedureId: string,
    request: { actor_id: string },
  ): Promise<ProcedureExecution> {
    return tracer.startActiveSpan("procedures.activate", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/procedures/${procedureId}/activate`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ actor_id: request.actor_id }),
        });
        if (res.status === 404) throw new Error(`Procedure not found: ${procedureId}`);
        if (!res.ok) throw new Error(`Activate procedure failed: ${res.status}`);
        return (await res.json()) as ProcedureExecution;
      } finally {
        span.end();
      }
    });
  }

  async completeProcedureStep(
    executionId: string,
    stepId: string,
    request: { evidence?: string; proof_type?: string },
  ): Promise<ProcedureExecution> {
    return tracer.startActiveSpan("procedures.completeStep", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(
          `${this.baseUrl}/procedures/${executionId}/step/${stepId}/complete`,
          {
            method: "POST",
            headers: this.getHeaders(),
            body: JSON.stringify({
              evidence: request.evidence,
              proof_type: request.proof_type,
            }),
          },
        );
        if (res.status === 404) throw new Error(`Execution or step not found: ${executionId}/${stepId}`);
        if (!res.ok) throw new Error(`Complete step failed: ${res.status}`);
        return (await res.json()) as ProcedureExecution;
      } finally {
        span.end();
      }
    });
  }

  async getSessionProcedureStatus(): Promise<SessionProcedureStatus> {
    return tracer.startActiveSpan("procedures.sessionStatus", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId,
        });
        const res = await fetch(`${this.baseUrl}/procedures/session/status?${params}`, {
          headers: this.getHeaders(),
        });
        if (!res.ok) throw new Error(`Get session procedure status failed: ${res.status}`);
        return (await res.json()) as SessionProcedureStatus;
      } finally {
        span.end();
      }
    });
  }

  // --- Guard tools ---

  async getActiveGuards(): Promise<Record<string, unknown>> {
    return tracer.startActiveSpan("guards.getActive", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(
          `${this.baseUrl}/guards/active/${this.currentSessionId}`,
          { headers: this.getHeaders() },
        );
        if (!res.ok) throw new Error(`Get active guards failed: ${res.status}`);
        return (await res.json()) as Record<string, unknown>;
      } finally {
        span.end();
      }
    });
  }

  async getGuardEventDetail(guardEventId: string): Promise<Record<string, unknown>> {
    return tracer.startActiveSpan("guards.getEventDetail", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_id: this.currentSessionId,
        });
        const res = await fetch(
          `${this.baseUrl}/guards/events/detail/${guardEventId}?${params}`,
          { headers: this.getHeaders() },
        );
        if (res.status === 404) throw new Error(`Guard event not found: ${guardEventId}`);
        if (!res.ok) throw new Error(`Get guard event detail failed: ${res.status}`);
        return (await res.json()) as Record<string, unknown>;
      } finally {
        span.end();
      }
    });
  }

  // --- Artifact methods (Amendment 6.2.3) ---

  async searchArtifacts(query: string, maxResults: number = 5): Promise<ToolArtifactSummary[]> {
    return tracer.startActiveSpan("artifacts.search", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/artifacts/search`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify({ query, max_results: maxResults }),
        });
        if (!res.ok) throw new Error(`Search artifacts failed: ${res.status}`);
        return (await res.json()) as ToolArtifactSummary[];
      } finally {
        span.end();
      }
    });
  }

  async searchSessionArtifacts(request: SessionArtifactSearchRequest): Promise<SessionArtifactSummary[]> {
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
            max_results: request.max_results || 5,
          }),
        });
        if (!res.ok) throw new Error(`Search session artifacts failed: ${res.status}`);
        return (await res.json()) as SessionArtifactSummary[];
      } finally {
        span.end();
      }
    });
  }

  async getSessionArtifact(artifactId: string): Promise<SessionArtifact | null> {
    return tracer.startActiveSpan("artifacts.getSession", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const params = new URLSearchParams({
          session_key: this.currentSessionKey,
          session_id: this.currentSessionId,
        });
        const res = await fetch(
          `${this.baseUrl}/artifacts/session/${artifactId}?${params}`,
          { headers: this.getHeaders() },
        );
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`Get session artifact failed: ${res.status}`);
        return (await res.json()) as SessionArtifact;
      } finally {
        span.end();
      }
    });
  }

  async createArtifact(request: CreateArtifactRequest): Promise<SessionArtifact | CreateArtifactResponse> {
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
            summary: request.summary,
          }),
        });
        if (!res.ok) throw new Error(`Create artifact failed: ${res.status}`);
        return (await res.json()) as SessionArtifact | CreateArtifactResponse;
      } finally {
        span.end();
      }
    });
  }

  // --- Session context management ---

  cacheSessionKey(sessionKey: string, sessionId: string): void {
    this.sessionKeyCache.set(sessionKey, sessionId);
    this.currentSessionKey = sessionKey;
    this.currentSessionId = sessionId;
  }

  getCachedSessionId(sessionKey: string): string | undefined {
    return this.sessionKeyCache.get(sessionKey);
  }

  setSessionContext(sessionKey: string, sessionId: string): void {
    this.currentSessionKey = sessionKey;
    this.currentSessionId = sessionId;
  }

  getSessionKey(): string {
    return this.currentSessionKey;
  }

  getSessionId(): string {
    return this.currentSessionId;
  }

  setActorId(actorId: string): void {
    this.actorId = actorId;
  }

  setProfileName(profileName: string): void {
    this.profileName = profileName;
  }

  getProfileName(): string {
    return this.profileName;
  }

  // --- Phase 8: Admin API methods ---

  async createPersistentGoal(request: {
    title: string; description?: string; scope: string;
    org_id?: string; team_id?: string; parent_goal_id?: string;
    success_criteria?: string[]; owner_actor_ids?: string[];
  }): Promise<unknown> {
    return tracer.startActiveSpan("admin.createPersistentGoal", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/goals`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(request),
        });
        return await r.json();
      } finally { span.end(); }
    });
  }

  async adminCreateOrg(request: { name: string; display_label?: string }): Promise<unknown> {
    return tracer.startActiveSpan("admin.createOrg", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/organizations`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(request),
        });
        return await r.json();
      } finally { span.end(); }
    });
  }

  async adminCreateTeam(request: { name: string; display_label?: string; org_id: string }): Promise<unknown> {
    return tracer.startActiveSpan("admin.createTeam", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/teams`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(request),
        });
        return await r.json();
      } finally { span.end(); }
    });
  }

  async adminRegisterActor(request: {
    display_name: string; type: string; authority_level?: number;
    org_id?: string; team_ids?: string[]; handles?: string[];
  }): Promise<unknown> {
    return tracer.startActiveSpan("admin.registerActor", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/actors`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(request),
        });
        return await r.json();
      } finally { span.end(); }
    });
  }

  async adminAddMember(teamId: string, actorId: string): Promise<unknown> {
    return tracer.startActiveSpan("admin.addMember", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/teams/${teamId}/members`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify({ actor_id: actorId }),
        });
        return await r.json();
      } finally { span.end(); }
    });
  }

  async adminRemoveMember(teamId: string, actorId: string): Promise<unknown> {
    return tracer.startActiveSpan("admin.removeMember", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/teams/${teamId}/members/${actorId}`, {
          method: "DELETE", headers: this.getHeaders(),
        });
        return await r.json();
      } finally { span.end(); }
    });
  }

  async adminMergeActors(canonicalId: string, duplicateId: string): Promise<unknown> {
    return tracer.startActiveSpan("admin.mergeActors", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const r = await fetch(`${this.baseUrl}/admin/actors/${canonicalId}/merge`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify({ duplicate_id: duplicateId }),
        });
        return await r.json();
      } finally { span.end(); }
    });
  }
}
