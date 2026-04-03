"""Pipeline input/output schemas for Cognee-powered ingest pipelines."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field

from elephantbroker.schemas.actor import ActorRef
from elephantbroker.schemas.artifact import ArtifactSummary, ToolArtifact
from elephantbroker.schemas.fact import FactAssertion
from elephantbroker.schemas.procedure import ProcedureDefinition


class MessageRole(StrEnum):
    """Roles for conversation messages."""
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"


class TurnInput(BaseModel):
    """Input to the Turn Ingest Pipeline."""
    session_key: str
    session_id: uuid.UUID | None = None
    actor_context: dict[str, str] = Field(default_factory=dict)
    messages: list[dict] = Field(default_factory=list)
    goal_ids: list[uuid.UUID] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    profile_name: str = "coding"


class TurnIngestResult(BaseModel):
    """Output of the Turn Ingest Pipeline."""
    facts_extracted: list[FactAssertion] = Field(default_factory=list)
    facts_stored: int = 0
    facts_superseded: int = 0
    actors_resolved: list[ActorRef] = Field(default_factory=list)
    memory_classes_assigned: dict[str, int] = Field(default_factory=dict)
    trace_event_id: uuid.UUID | None = None


class ArtifactInput(BaseModel):
    """Input to the Artifact Ingest Pipeline."""
    tool_name: str
    tool_args: dict = Field(default_factory=dict)
    tool_output: str = ""
    session_id: uuid.UUID | None = None
    actor_id: uuid.UUID | None = None
    goal_id: uuid.UUID | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    gateway_id: str = ""


class ArtifactIngestResult(BaseModel):
    """Output of the Artifact Ingest Pipeline."""
    artifact: ToolArtifact | None = None
    summary: ArtifactSummary | None = None
    facts_extracted: list[FactAssertion] = Field(default_factory=list)
    is_duplicate: bool = False
    trace_event_id: uuid.UUID | None = None


class ProcedureIngestResult(BaseModel):
    """Output of the Procedure Ingest Pipeline."""
    procedure: ProcedureDefinition | None = None
    is_new: bool = True
    previous_version: int | None = None
    edges_created: int = 0
    trace_event_id: uuid.UUID | None = None


class SessionEndRequest(BaseModel):
    """Request to end a session."""
    session_key: str
    session_id: str
    reason: str = "reset"
    gateway_id: str = ""
    agent_key: str = ""


class SessionStartRequest(BaseModel):
    """Request to start a session."""
    session_key: str
    session_id: str
    parent_session_key: str | None = None
    gateway_id: str = ""
    gateway_short_name: str = ""
    agent_id: str = ""
    agent_key: str = ""
