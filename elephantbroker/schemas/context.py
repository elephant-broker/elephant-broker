"""Context engine I/O schemas — maps 1:1 to OpenClaw's ContextEngine TypeScript types."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class AgentMessage(BaseModel):
    """A message in the agent conversation — maps to OpenClaw's AgentMessage.

    Content is stored as-is (string, multipart array, tool-use blocks, etc.)
    to preserve OpenClaw's format through the pipeline. Internal code that needs
    plain text for scoring/embedding/hashing uses content_as_text(msg).

    Extra fields from OpenClaw (timestamp, usage, api, provider, etc.) are
    preserved through the pipeline via extra="allow" so that provider-specific
    message properties (e.g. tool_use_id) survive the round-trip.
    """
    model_config = {"extra": "allow"}

    role: str
    content: Any  # str | list[dict] — preserved as-is from OpenClaw
    name: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def content_as_text(msg: AgentMessage) -> str:
    """Extract plain text from message content for scoring/embedding/hashing.

    Handles all OpenClaw content formats:
      - String: "hello" → "hello"
      - Multipart: [{"type": "text", "text": "hello"}] → "hello"
      - Tool-use: [{"type": "tool_use", ...}] → "" (no text)
      - Mixed: [{"type": "text", "text": "hi"}, {"type": "tool_use", ...}] → "hi"
    """
    v = msg.content
    if isinstance(v, str):
        return v
    if isinstance(v, list):
        return "\n".join(
            part["text"] for part in v
            if isinstance(part, dict) and part.get("type") == "text" and part.get("text")
        )
    return str(v) if v is not None else ""


class BootstrapResult(BaseModel):
    """Result of ContextEngine.bootstrap — maps to OpenClaw's BootstrapResult."""
    bootstrapped: bool
    imported_messages: list[AgentMessage] | None = None
    reason: str | None = None


class IngestResult(BaseModel):
    """Result of ContextEngine.ingest — maps to OpenClaw's IngestResult."""
    ingested: bool


class IngestBatchResult(BaseModel):
    """Result of ContextEngine.ingestBatch — maps to OpenClaw's IngestBatchResult."""
    ingested_count: int = Field(ge=0)
    facts_stored: int = Field(default=0, ge=0)


class AssembleResult(BaseModel):
    """Result of ContextEngine.assemble — maps to OpenClaw's AssembleResult."""
    messages: list[AgentMessage] = Field(default_factory=list)
    estimated_tokens: int = Field(default=0, ge=0)
    system_prompt_addition: str | None = None


class CompactResultDetail(BaseModel):
    """Detailed compaction result information."""
    summary: str | None = None
    first_kept_entry_id: str | None = None
    tokens_before: int = Field(ge=0)
    tokens_after: int | None = None
    details: str | None = None


class CompactResult(BaseModel):
    """Result of ContextEngine.compact — maps to OpenClaw's CompactResult."""
    ok: bool
    compacted: bool
    reason: str | None = None
    result: CompactResultDetail | None = None


SubagentEndReason = Literal["deleted", "completed", "swept", "released"]


class SystemPromptOverlay(BaseModel):
    """Data injected via before_prompt_build hook — maps to OpenClaw hook return."""
    system_prompt: str | None = None
    prepend_context: str | None = None
    prepend_system_context: str | None = None
    append_system_context: str | None = None


class SubagentPacket(BaseModel):
    """Context packet prepared for a subagent spawn."""
    parent_session_key: str
    child_session_key: str
    context_summary: str = ""
    inherited_goals: list[uuid.UUID] = Field(default_factory=list)
    inherited_facts_count: int = Field(default=0, ge=0)
    ttl_ms: int | None = None


class ContextEngineRuntimeContext(BaseModel):
    """Runtime context passed to compact and afterTurn — maps to OpenClaw's runtimeContext."""
    active_tools: list[str] = Field(default_factory=list)
    active_goals: list[str] = Field(default_factory=list)
    custom_data: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Phase 6: Lifecycle parameter types
# ---------------------------------------------------------------------------


class BootstrapParams(BaseModel):
    """Parameters for ContextEngine.bootstrap — maps to OpenClaw ContextEngine interface."""
    session_key: str
    session_id: str
    profile_name: str = "coding"
    prior_session_id: str | None = None
    gateway_id: str = ""
    agent_key: str = ""
    is_subagent: bool = False
    parent_session_key: str | None = None
    session_file: str | None = None


class IngestParams(BaseModel):
    """Parameters for ContextEngine.ingest (single message)."""
    session_id: str
    session_key: str
    message: AgentMessage
    is_heartbeat: bool = False


class IngestBatchParams(BaseModel):
    """Parameters for ContextEngine.ingestBatch."""
    session_id: str
    session_key: str
    messages: list[AgentMessage]
    is_heartbeat: bool = False
    profile_name: str = "coding"


class AssembleParams(BaseModel):
    """Parameters for ContextEngine.assemble."""
    session_id: str
    session_key: str
    messages: list[AgentMessage] = Field(default_factory=list)
    profile_name: str = "coding"
    query: str = ""
    token_budget: int | None = None
    context_window_tokens: int | None = None
    goal_ids: list[str] | None = None


class CompactParams(BaseModel):
    """Parameters for ContextEngine.compact."""
    session_id: str
    session_key: str
    force: bool = False
    token_budget: int | None = None
    current_token_count: int | None = None
    compaction_target: str | None = None
    custom_instructions: str | None = None
    runtime_context: dict = Field(default_factory=dict)
    session_file: str | None = None
    trigger_reason: str = "explicit"


class AfterTurnParams(BaseModel):
    """Parameters for ContextEngine.afterTurn."""
    session_id: str
    session_key: str
    messages: list[AgentMessage] = Field(default_factory=list)
    pre_prompt_message_count: int = 0
    auto_compaction_summary: str | None = None
    is_heartbeat: bool = False
    token_budget: int | None = None
    runtime_context: dict = Field(default_factory=dict)
    session_file: str | None = None


class SubagentSpawnParams(BaseModel):
    """Parameters for ContextEngine.prepareSubagentSpawn."""
    parent_session_key: str
    child_session_key: str
    ttl_ms: int | None = None


class SubagentEndedParams(BaseModel):
    """Parameters for ContextEngine.onSubagentEnded."""
    child_session_key: str
    reason: SubagentEndReason = "completed"


class SubagentSpawnResult(BaseModel):
    """Result of prepareSubagentSpawn."""
    parent_session_key: str
    child_session_key: str
    rollback_key: str = ""
    parent_mapping_stored: bool = False


# ---------------------------------------------------------------------------
# Phase 6: Session state models
# ---------------------------------------------------------------------------


class SessionContext(BaseModel):
    """Session-scoped context state persisted in Redis."""
    session_key: str
    session_id: str
    profile_name: str
    profile: ProfilePolicy  # type: ignore[name-defined]  # forward ref resolved below
    actor_id: str | None = None
    gateway_id: str = ""
    agent_key: str = ""
    org_id: str = ""
    team_ids: list[str] = Field(default_factory=list)
    org_label: str = ""
    team_label: str = ""
    bootstrapped_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_turn_at: datetime | None = None
    turn_count: int = 0
    last_snapshot_id: str | None = None
    compact_count: int = 0
    context_window_tokens: int | None = None
    provider: str | None = None
    model: str | None = None
    fact_last_injection_turn: dict[str, int] = Field(default_factory=dict)
    goal_inject_history: dict[str, dict] = Field(default_factory=dict)
    parent_session_key: str | None = None
    # Phase 9 RT-1/RT-2 turn counters
    rt1_turn_counter: int = 0
    rt1_last_batch_at: datetime | None = None
    rt2_turn_counter: int = 0


class SessionCompactState(BaseModel):
    """Compaction state snapshot persisted in Redis."""
    session_key: str
    session_id: str
    goal_summary: str = ""
    decisions_made: list[str] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)
    active_blockers: list[str] = Field(default_factory=list)
    constraint_digest: list[str] = Field(default_factory=list)
    evidence_bundle_refs: list[str] = Field(default_factory=list)
    actor_context_summary: str = ""
    compressed_digest: str = ""
    preserved_item_ids: list[str] = Field(default_factory=list)
    token_count: int = 0
    turn_count_at_compaction: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CompactionContext(BaseModel):
    """Input context for CompactionEngine.compact_with_context."""
    session_key: str
    session_id: str
    messages: list[AgentMessage]
    current_goals: list = Field(default_factory=list)  # list[GoalState]
    token_budget: int = 4000
    force: bool = False
    current_token_count: int | None = None
    profile: ProfilePolicy | None = None  # type: ignore[name-defined]
    trigger_reason: str = "explicit"


# ---------------------------------------------------------------------------
# Phase 6: API types
# ---------------------------------------------------------------------------


class ContextWindowReport(BaseModel):
    """Report context window size from the TS plugin."""
    session_key: str
    session_id: str
    gateway_id: str = ""
    provider: str
    model: str
    context_window_tokens: int = Field(ge=1000)


class TokenUsageReport(BaseModel):
    """Report token usage from the TS plugin."""
    session_key: str
    session_id: str
    gateway_id: str = ""
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    cache_read_tokens: int = Field(default=0, ge=0)
    total_tokens: int = Field(ge=0)


class BuildOverlayRequest(BaseModel):
    """Request to build system prompt overlay."""
    session_key: str
    session_id: str


class SubagentRollbackRequest(BaseModel):
    """Request to rollback a subagent spawn."""
    parent_session_key: str
    child_session_key: str
    rollback_key: str


# Resolve forward reference for SessionContext.profile
from elephantbroker.schemas.profile import ProfilePolicy  # noqa: E402

SessionContext.model_rebuild()
CompactionContext.model_rebuild()
