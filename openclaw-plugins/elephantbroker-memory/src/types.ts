/** Memory classes for fact classification. */
export type MemoryClass = "episodic" | "semantic" | "procedural" | "policy" | "working_memory";

/** Scopes for memory visibility. */
export type Scope = "global" | "organization" | "team" | "actor" | "session" | "task" | "subagent" | "artifact";

/** A stored fact from ElephantBroker. */
export interface FactAssertion {
  id: string;
  text: string;
  category: string;
  scope: Scope;
  confidence: number;
  memory_class: MemoryClass;
  session_key?: string;
  session_id?: string;
  source_actor_id?: string;
  target_actor_ids: string[];
  goal_ids: string[];
  created_at: string;
  updated_at: string;
  last_used_at?: string;
  use_count: number;
  successful_use_count?: number;
  freshness_score?: number;
  provenance_refs?: string[];
  embedding_ref?: string;
  token_size?: number;
  gateway_id?: string;
}

/** Search result with scoring. */
export interface SearchResult extends FactAssertion {
  score: number;
  source: string;
}

/** Search request body. */
export interface SearchRequest {
  query: string;
  max_results?: number;
  min_score?: number;
  scope?: string;
  actor_id?: string;
  memory_class?: string;
  session_key?: string;
  profile_name?: string;
  auto_recall?: boolean;
}

/** Store request body. */
export interface StoreRequest {
  fact: {
    text: string;
    category?: string;
    scope?: string;
    confidence?: number;
  };
  session_key?: string;
  session_id?: string;
  dedup_threshold?: number;
}

/** Session lifecycle requests. */
export interface SessionStartRequest {
  session_key: string;
  session_id: string;
  parent_session_key?: string;
  gateway_id?: string;
  gateway_short_name?: string;
  agent_id?: string;
  agent_key?: string;
}

export interface SessionEndRequest {
  session_key: string;
  session_id: string;
  reason?: string;
  gateway_id?: string;
  agent_key?: string;
}

/** Ingest messages request. */
export interface IngestMessagesRequest {
  session_key: string;
  session_id: string;
  messages: Array<{ role: string; content: string | unknown[] }>;
  profile_name?: string;
}

// --- Session Goal types ---

/** Status of a goal. */
export type GoalStatus = "proposed" | "active" | "paused" | "completed" | "abandoned";

/** A single goal with its current state. */
export interface GoalState {
  id: string;
  title: string;
  description: string;
  status: GoalStatus;
  scope: Scope;
  parent_goal_id: string | null;
  created_at: string;
  updated_at: string;
  owner_actor_ids: string[];
  success_criteria: string[];
  blockers: string[];
  evidence: string[];
  confidence: number;
  gateway_id?: string;
}

/** Tree of goals with parent-child relationships. */
export interface GoalHierarchy {
  root_goals: GoalState[];
  children: Record<string, GoalState[]>;
}

/** Request to create a session goal. */
export interface CreateSessionGoalRequest {
  session_key: string;
  session_id: string;
  title: string;
  description?: string;
  parent_goal_id?: string;
  success_criteria?: string[];
}

/** Request to update a session goal's status. */
export interface UpdateSessionGoalStatusRequest {
  session_key: string;
  session_id: string;
  status: GoalStatus;
  evidence?: string;
}

/** Request to add a blocker to a session goal. */
export interface AddBlockerRequest {
  session_key: string;
  session_id: string;
  blocker: string;
}

/** Request to record progress on a session goal. */
export interface ProgressRequest {
  session_key: string;
  session_id: string;
  evidence: string;
}

// --- Procedure types ---

/** Types of proof that can satisfy an evidence requirement. */
export type ProofType = "diff_hash" | "chunk_ref" | "receipt" | "version_record" | "supervisor_sign_off";

/** What evidence is needed to prove a step was completed. */
export interface ProofRequirement {
  description: string;
  required: boolean;
  proof_type: ProofType;
}

/** A single step in a procedure. */
export interface ProcedureStep {
  step_id: string;
  order: number;
  instruction: string;
  required_evidence: ProofRequirement[];
  is_optional: boolean;
}

/** Activation mode flags for a procedure. */
export interface ProcedureActivation {
  manual: boolean;
  actor_default: boolean;
  trigger_word: string | null;
  task_classifier: string | null;
  goal_bound: boolean;
  supervisor_forced: boolean;
}

/** A stored procedure definition. */
export interface ProcedureDefinition {
  id: string;
  name: string;
  description: string;
  scope: Scope;
  steps: ProcedureStep[];
  activation_modes: ProcedureActivation[];
  required_evidence: ProofRequirement[];
  red_line_bindings: string[];
  enabled: boolean;
  source_actor_id: string | null;
  created_at: string;
  updated_at: string;
  version: number;
  gateway_id?: string;
}

/** An in-progress execution of a procedure. */
export interface ProcedureExecution {
  execution_id: string;
  procedure_id: string;
  current_step_index: number;
  started_at: string;
  completed_steps: string[];
  actor_id: string | null;
  goal_id: string | null;
}

/** Request to create a procedure. */
export interface CreateProcedureRequest {
  name: string;
  description?: string;
  scope?: string;
  steps: Array<{
    order: number;
    instruction: string;
    required_evidence?: Array<{ description: string; required?: boolean; proof_type?: ProofType }>;
    is_optional?: boolean;
  }>;
  enabled?: boolean;
}

/** Request to activate a procedure. */
export interface ActivateProcedureRequest {
  actor_id: string;
}

/** Request to complete a procedure step. */
export interface CompleteStepRequest {
  evidence?: string;
  proof_type?: ProofType;
}

/** Session procedure status response. */
export interface SessionProcedureStatus {
  executions: ProcedureExecution[];
}

// --- Artifact types (Amendment 6.2.3) ---

/** Persistent artifact summary (from Cognee graph). */
export interface ToolArtifactSummary {
  artifact_id: string;
  tool_name: string;
  summary: string;
  token_estimate: number;
  created_at: string;
  tags?: string[];
  score?: number;
}

/** Session-scoped artifact summary (from Redis HASH). */
export interface SessionArtifactSummary {
  artifact_id: string;
  tool_name: string;
  summary: string;
  created_at: string;
  content_hash?: string;
  injected_count?: number;
  searched_count?: number;
}

/** Full session artifact with content. */
export interface SessionArtifact extends SessionArtifactSummary {
  content: string;
  session_key: string;
  session_id: string;
  tags: string[];
  token_estimate: number;
}

/** Request for searching persistent artifacts. */
export interface ArtifactSearchRequest {
  query: string;
  max_results?: number;
}

/** Request for searching session-scoped artifacts. */
export interface SessionArtifactSearchRequest {
  session_key?: string;
  session_id?: string;
  query: string;
  tool_name?: string;
  max_results?: number;
}

/** Request for creating an artifact (session or persistent). */
export interface CreateArtifactRequest {
  content: string;
  tool_name?: string;
  scope?: "session" | "persistent";
  session_key?: string;
  session_id?: string;
  tags?: string[];
  goal_id?: string;
  summary?: string;
}

/** Response from artifact creation. */
export interface CreateArtifactResponse {
  artifact_id?: string;
  status?: string;
}
