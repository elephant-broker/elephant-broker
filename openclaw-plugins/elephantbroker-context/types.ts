/**
 * TypeScript types for ElephantBroker ContextEngine plugin.
 * Generated from Pydantic models in elephantbroker/schemas/context.py.
 */

export interface AgentMessage {
  role: string;
  content: string | unknown[];
  name?: string;
  metadata?: Record<string, string>;
  [key: string]: unknown;
}

// --- Bootstrap ---
export interface BootstrapParams {
  session_key: string;
  session_id: string;
  profile_name?: string;
  prior_session_id?: string;
  gateway_id?: string;
  agent_key?: string;
  is_subagent?: boolean;
  parent_session_key?: string;
  session_file?: string;
}

export interface BootstrapResult {
  bootstrapped: boolean;
  imported_messages?: AgentMessage[] | null;
  reason?: string | null;
}

// --- Ingest ---
export interface IngestParams {
  session_id: string;
  session_key: string;
  message: AgentMessage;
  is_heartbeat?: boolean;
}

export interface IngestResult {
  ingested: boolean;
}

export interface IngestBatchParams {
  session_id: string;
  session_key: string;
  messages: AgentMessage[];
  is_heartbeat?: boolean;
  profile_name?: string;
}

export interface IngestBatchResult {
  ingested_count: number;
  facts_stored: number;
}

// --- Assemble ---
export interface AssembleParams {
  session_id: string;
  session_key: string;
  messages?: AgentMessage[];
  profile_name?: string;
  query?: string;
  token_budget?: number | null;
  context_window_tokens?: number | null;
  goal_ids?: string[] | null;
}

export interface AssembleResult {
  messages: AgentMessage[];
  estimated_tokens: number;
  system_prompt_addition?: string | null;
}

// --- Compact ---
export interface CompactParams {
  session_id: string;
  session_key: string;
  force?: boolean;
  token_budget?: number | null;
  current_token_count?: number | null;
  compaction_target?: string | null;
  custom_instructions?: string | null;
  runtime_context?: Record<string, unknown>;
  session_file?: string | null;
}

export interface CompactResult {
  ok: boolean;
  compacted: boolean;
  reason?: string | null;
  result?: Record<string, unknown> | null;
}

// --- After Turn ---
export interface AfterTurnParams {
  session_id: string;
  session_key: string;
  messages?: AgentMessage[];
  pre_prompt_message_count?: number;
  auto_compaction_summary?: string | null;
  is_heartbeat?: boolean;
  token_budget?: number | null;
  runtime_context?: Record<string, unknown>;
  session_file?: string | null;
}

// --- Subagent ---
export interface SubagentSpawnParams {
  parent_session_key: string;
  child_session_key: string;
  ttl_ms?: number | null;
}

export interface SubagentSpawnResult {
  parent_session_key: string;
  child_session_key: string;
  rollback_key: string;
  parent_mapping_stored: boolean;
}

export interface SubagentEndedParams {
  child_session_key: string;
  reason?: "deleted" | "completed" | "swept" | "released";
}

// --- System Prompt Overlay (Surface B) ---
export interface SystemPromptOverlay {
  system_prompt?: string | null;
  prepend_context?: string | null;
  prepend_system_context?: string | null;
  append_system_context?: string | null;
}

// --- Session Reporting ---
export interface ContextWindowReport {
  session_key: string;
  session_id: string;
  gateway_id?: string;
  provider: string;
  model: string;
  context_window_tokens: number;
}

export interface TokenUsageReport {
  session_key: string;
  session_id: string;
  gateway_id?: string;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens?: number;
  total_tokens: number;
}

// --- Batch Config ---
export interface BatchConfig {
  ingest_batch_size?: number;
  ingest_batch_timeout_ms?: number;
  [key: string]: unknown;
}

// ---------------------------------------------------------------------------
// OpenClaw ContextEngine param types — what OpenClaw passes to engine methods
// directly when registered via api.registerContextEngine().
// These are distinct from the HTTP payload types above (BootstrapParams, etc.)
// which represent what client.ts sends to the Python runtime.
// ---------------------------------------------------------------------------

export interface OCBootstrapParams {
  sessionId: string;
  sessionFile?: string;
  sessionKey?: string;
}

export interface OCIngestParams {
  sessionId: string;
  message: AgentMessage;
  isHeartbeat?: boolean;
}

export interface OCIngestBatchParams {
  sessionId: string;
  messages: AgentMessage[];
}

export interface OCAssembleParams {
  sessionId: string;
  messages: AgentMessage[];
  tokenBudget: number;
}

export interface OCCompactParams {
  sessionId: string;
  force?: boolean;
  sessionFile?: string;
}

export interface OCAfterTurnParams {
  sessionId: string;
  messages?: AgentMessage[];
  prePromptMessageCount?: number;
}

export interface OCSubagentSpawnParams {
  sessionId: string;
  childSessionId?: string;
  childSessionKey?: string;
}

export interface OCSubagentEndedParams {
  sessionId: string;
  childSessionId?: string;
  childSessionKey?: string;
  reason?: "deleted" | "completed" | "swept" | "released";
}

/** OpenClaw's expected return from assemble() — camelCase fields. */
export interface OCAssembleResult {
  messages: AgentMessage[];
  estimatedTokens: number;
  systemPromptAddition?: string;
}
