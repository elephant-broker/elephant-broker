/**
 * HTTP client for ElephantBroker ContextEngine Python runtime.
 * Mirrors the memory-plugin ElephantBrokerClient pattern with identity headers + W3C trace context.
 */
import { trace, context, propagation, SpanKind } from "@opentelemetry/api";
import type {
  BootstrapParams,
  BootstrapResult,
  IngestBatchParams,
  IngestBatchResult,
  AssembleParams,
  AssembleResult,
  CompactParams,
  CompactResult,
  AfterTurnParams,
  SubagentSpawnParams,
  SubagentSpawnResult,
  SubagentEndedParams,
  SystemPromptOverlay,
  ContextWindowReport,
  TokenUsageReport,
  BatchConfig,
} from "./types.js";

const tracer = trace.getTracer("elephantbroker.context-engine-plugin");

export class ContextEngineClient {
  private baseUrl: string;
  private gatewayId: string;
  private gatewayShortName: string;
  private agentId = "";
  private agentKey = "";
  private currentSessionKey = "";
  private currentSessionId = "";

  constructor(baseUrl: string = "http://localhost:8420", gatewayId?: string, gatewayShortName?: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.gatewayId = gatewayId || process.env.EB_GATEWAY_ID || "";
    if (!this.gatewayId) {
      throw new Error("EB_GATEWAY_ID is required for ContextEnginePlugin.");
    }
    this.gatewayShortName = gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME || this.gatewayId.substring(0, 8);
  }

  setAgentIdentity(agentId: string, agentKey: string): void {
    this.agentId = agentId;
    this.agentKey = agentKey;
  }

  setSessionContext(sessionKey: string, sessionId: string): void {
    this.currentSessionKey = sessionKey;
    this.currentSessionId = sessionId;
  }

  private getHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-EB-Gateway-ID": this.gatewayId,
    };
    if (this.agentKey) headers["X-EB-Agent-Key"] = this.agentKey;
    if (this.agentId) headers["X-EB-Agent-ID"] = this.agentId;
    if (this.currentSessionKey) headers["X-EB-Session-Key"] = this.currentSessionKey;
    propagation.inject(context.active(), headers);
    return headers;
  }

  // --- Context Lifecycle ---

  async bootstrap(params: BootstrapParams): Promise<BootstrapResult> {
    return tracer.startActiveSpan("context.bootstrap", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/bootstrap`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(params),
        });
        if (!res.ok) throw new Error(`Bootstrap failed: ${res.status}`);
        return (await res.json()) as BootstrapResult;
      } catch (err) {
        console.error(`[EB] Bootstrap failed: ${err}`);
        return { bootstrapped: false, reason: "Runtime error" };
      } finally { span.end(); }
    });
  }

  async ingestBatch(params: IngestBatchParams): Promise<IngestBatchResult> {
    return tracer.startActiveSpan("context.ingestBatch", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/ingest-batch`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(params),
        });
        if (!res.ok) throw new Error(`IngestBatch failed: ${res.status}`);
        return (await res.json()) as IngestBatchResult;
      } catch (err) {
        console.error(`[EB] IngestBatch failed: ${err}`);
        return { ingested_count: 0, facts_stored: 0 };
      } finally { span.end(); }
    });
  }

  async assemble(params: AssembleParams): Promise<AssembleResult> {
    return tracer.startActiveSpan("context.assemble", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/assemble`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(params),
        });
        if (!res.ok) throw new Error(`Assemble failed: ${res.status}`);
        return (await res.json()) as AssembleResult;
      } catch (err) {
        console.error(`[EB] Assemble failed: ${err}`);
        return { messages: params.messages || [], estimated_tokens: 0 };
      } finally { span.end(); }
    });
  }

  async buildOverlay(sessionKey: string, sessionId: string): Promise<SystemPromptOverlay> {
    return tracer.startActiveSpan("context.buildOverlay", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/build-overlay`, {
          method: "POST", headers: this.getHeaders(),
          body: JSON.stringify({ session_key: sessionKey, session_id: sessionId }),
        });
        if (!res.ok) throw new Error(`BuildOverlay failed: ${res.status}`);
        return (await res.json()) as SystemPromptOverlay;
      } catch (err) {
        console.error(`[EB] BuildOverlay failed: ${err}`);
        return {};
      } finally { span.end(); }
    });
  }

  async compact(params: CompactParams): Promise<CompactResult> {
    return tracer.startActiveSpan("context.compact", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/compact`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(params),
        });
        if (!res.ok) throw new Error(`Compact failed: ${res.status}`);
        return (await res.json()) as CompactResult;
      } catch (err) {
        console.error(`[EB] Compact failed: ${err}`);
        return { ok: false, compacted: false, reason: "Runtime error" };
      } finally { span.end(); }
    });
  }

  async afterTurn(params: AfterTurnParams): Promise<void> {
    return tracer.startActiveSpan("context.afterTurn", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/after-turn`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(params),
        });
        if (!res.ok) console.warn(`[EB] AfterTurn returned ${res.status}`);
      } catch (err) {
        console.error(`[EB] AfterTurn failed: ${err}`);
      } finally { span.end(); }
    });
  }

  async subagentSpawn(params: SubagentSpawnParams): Promise<SubagentSpawnResult> {
    return tracer.startActiveSpan("context.subagentSpawn", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/subagent/spawn`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(params),
        });
        if (!res.ok) throw new Error(`SubagentSpawn failed: ${res.status}`);
        return (await res.json()) as SubagentSpawnResult;
      } catch (err) {
        console.error(`[EB] SubagentSpawn failed: ${err}`);
        return { parent_session_key: params.parent_session_key, child_session_key: params.child_session_key, rollback_key: "", parent_mapping_stored: false };
      } finally { span.end(); }
    });
  }

  async subagentEnded(params: SubagentEndedParams): Promise<void> {
    return tracer.startActiveSpan("context.subagentEnded", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/subagent/ended`, {
          method: "POST", headers: this.getHeaders(), body: JSON.stringify(params),
        });
        if (!res.ok) console.warn(`[EB] SubagentEnded returned ${res.status}`);
      } catch (err) {
        console.error(`[EB] SubagentEnded failed: ${err}`);
      } finally { span.end(); }
    });
  }

  async dispose(_sessionKey: string, _sessionId: string): Promise<void> {
    // GF-15: dispose is engine teardown, not session end. The Python-side
    // /context/dispose endpoint is deprecated — no HTTP call needed.
    // Session cleanup happens via POST /sessions/end.
  }

  async getConfig(): Promise<BatchConfig> {
    return tracer.startActiveSpan("context.getConfig", { kind: SpanKind.CLIENT }, async (span) => {
      try {
        const res = await fetch(`${this.baseUrl}/context/config`, { headers: this.getHeaders() });
        if (!res.ok) throw new Error(`GetConfig failed: ${res.status}`);
        return (await res.json()) as BatchConfig;
      } catch (err) {
        console.warn(`[EB] GetConfig failed, using defaults: ${err}`);
        return { ingest_batch_size: 6, ingest_batch_timeout_ms: 60000 };
      } finally { span.end(); }
    });
  }

  // --- Session Reporting ---

  async reportContextWindow(report: ContextWindowReport): Promise<void> {
    try {
      await fetch(`${this.baseUrl}/sessions/context-window`, {
        method: "POST", headers: this.getHeaders(), body: JSON.stringify(report),
      });
    } catch (err) {
      console.error(`[EB] ReportContextWindow failed: ${err}`);
    }
  }

  async reportTokenUsage(report: TokenUsageReport): Promise<void> {
    try {
      await fetch(`${this.baseUrl}/sessions/token-usage`, {
        method: "POST", headers: this.getHeaders(), body: JSON.stringify(report),
      });
    } catch (err) {
      console.error(`[EB] ReportTokenUsage failed: ${err}`);
    }
  }
}
