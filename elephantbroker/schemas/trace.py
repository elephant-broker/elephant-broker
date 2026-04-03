"""Audit trail schemas."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class TraceEventType(StrEnum):
    """Types of traceable events."""
    INPUT_RECEIVED = "input_received"
    RETRIEVAL_PERFORMED = "retrieval_performed"
    RETRIEVAL_SOURCE_RESULT = "retrieval_source_result"
    TOOL_INVOKED = "tool_invoked"
    ARTIFACT_CREATED = "artifact_created"
    CLAIM_MADE = "claim_made"
    CLAIM_VERIFIED = "claim_verified"
    PROCEDURE_STEP_PASSED = "procedure_step_passed"
    PROCEDURE_STEP_FAILED = "procedure_step_failed"
    GUARD_TRIGGERED = "guard_triggered"
    COMPACTION_ACTION = "compaction_action"
    SUBAGENT_SPAWNED = "subagent_spawned"
    SUBAGENT_ENDED = "subagent_ended"
    CONTEXT_ASSEMBLED = "context_assembled"
    SCORING_COMPLETED = "scoring_completed"
    FACT_EXTRACTED = "fact_extracted"
    FACT_SUPERSEDED = "fact_superseded"
    MEMORY_CLASS_ASSIGNED = "memory_class_assigned"
    DEDUP_TRIGGERED = "dedup_triggered"
    SESSION_BOUNDARY = "session_boundary"
    INGEST_BUFFER_FLUSH = "ingest_buffer_flush"
    GDPR_DELETE = "gdpr_delete"
    COGNEE_COGNIFY_COMPLETED = "cognee_cognify_completed"
    DEGRADED_OPERATION = "degraded_operation"
    # Phase 7 additions
    GUARD_PASSED = "guard_passed"
    GUARD_NEAR_MISS = "guard_near_miss"
    CONSTRAINT_REINJECTED = "constraint_reinjected"
    PROCEDURE_COMPLETION_CHECKED = "procedure_completion_checked"
    # Phase 6 additions
    BOOTSTRAP_COMPLETED = "bootstrap_completed"
    AFTER_TURN_COMPLETED = "after_turn_completed"
    TOKEN_USAGE_REPORTED = "token_usage_reported"
    CONTEXT_WINDOW_REPORTED = "context_window_reported"
    SUCCESSFUL_USE_TRACKED = "successful_use_tracked"
    SUBAGENT_PARENT_MAPPED = "subagent_parent_mapped"
    # Phase 8 additions
    PROFILE_RESOLVED = "profile_resolved"
    ORG_CREATED = "org_created"
    TEAM_CREATED = "team_created"
    MEMBER_ADDED = "member_added"
    MEMBER_REMOVED = "member_removed"
    ACTOR_MERGED = "actor_merged"
    AUTHORITY_CHECK_FAILED = "authority_check_failed"
    HANDLE_RESOLVED = "handle_resolved"
    PERSISTENT_GOAL_CREATED = "persistent_goal_created"
    BOOTSTRAP_ORG_CREATED = "bootstrap_org_created"
    # Phase 5 additions (session goal lifecycle)
    SESSION_GOAL_CREATED = "session_goal_created"
    SESSION_GOAL_UPDATED = "session_goal_updated"
    SESSION_GOAL_BLOCKER_ADDED = "session_goal_blocker_added"
    SESSION_GOAL_PROGRESS = "session_goal_progress"
    # Phase 9 additions
    CONSOLIDATION_STARTED = "consolidation_started"
    CONSOLIDATION_STAGE_COMPLETED = "consolidation_stage_completed"
    CONSOLIDATION_COMPLETED = "consolidation_completed"


class TraceEvent(BaseModel):
    """A single auditable event."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_type: TraceEventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    session_id: uuid.UUID | None = None
    actor_ids: list[uuid.UUID] = Field(default_factory=list)
    artifact_ids: list[uuid.UUID] = Field(default_factory=list)
    claim_ids: list[uuid.UUID] = Field(default_factory=list)
    procedure_ids: list[uuid.UUID] = Field(default_factory=list)
    goal_ids: list[uuid.UUID] = Field(default_factory=list)
    payload: dict[str, object] = Field(default_factory=dict)
    parent_event_id: uuid.UUID | None = None
    gateway_id: str | None = None
    agent_id: str | None = None
    agent_key: str | None = None
    session_key: str | None = None


class TraceQuery(BaseModel):
    """Query parameters for searching the trace ledger."""
    event_types: list[TraceEventType] | None = None
    session_id: uuid.UUID | None = None
    actor_ids: list[uuid.UUID] | None = None
    from_timestamp: datetime | None = None
    to_timestamp: datetime | None = None
    limit: int = Field(default=100, ge=1, le=10000)
    offset: int = Field(default=0, ge=0)
    session_key: str | None = None          # Filter by stable OpenClaw session key
    gateway_id: str | None = None           # Filter by gateway (multi-gateway isolation)


class SessionListItem(BaseModel):
    """A single session entry for the session listing endpoint."""
    session_id: uuid.UUID
    session_key: str
    first_event_at: datetime
    last_event_at: datetime
    event_count: int


class SessionListResponse(BaseModel):
    """Paginated response for the session listing endpoint."""
    sessions: list[SessionListItem]
    total_count: int


class SessionSummary(BaseModel):
    """Aggregated summary statistics for a single session's trace events."""
    session_id: uuid.UUID
    total_events: int
    event_counts: dict[str, int]                  # count per TraceEventType value
    error_events: list[dict]                       # DEGRADED_OPERATION events (full payload)
    first_event_at: datetime | None
    last_event_at: datetime | None
    duration_seconds: float | None                 # last - first
    turn_count: int                                # count of AFTER_TURN_COMPLETED
    facts_extracted: int                           # count of FACT_EXTRACTED
    facts_superseded: int                          # count of FACT_SUPERSEDED
    dedup_triggered: int                           # count of DEDUP_TRIGGERED
    retrieval_count: int                           # count of RETRIEVAL_PERFORMED
    compaction_count: int                          # count of COMPACTION_ACTION
    guard_triggers: int                            # count of GUARD_TRIGGERED
    guard_near_misses: int                         # count of GUARD_NEAR_MISS
    context_assembled: int                         # count of CONTEXT_ASSEMBLED
    scoring_completed: int                         # count of SCORING_COMPLETED
    successful_use_tracked: int                    # count of SUCCESSFUL_USE_TRACKED
    bootstrap_completed: bool                      # any BOOTSTRAP_COMPLETED event exists
