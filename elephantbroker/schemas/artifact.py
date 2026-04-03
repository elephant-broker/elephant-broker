"""Tool artifact storage schemas."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field


class ArtifactHash(BaseModel):
    """Content-addressable hash for an artifact."""
    algorithm: str = "sha256"
    value: str = Field(min_length=1)


class ArtifactSummary(BaseModel):
    """Compact summary of a tool artifact for context injection."""
    artifact_id: uuid.UUID
    tool_name: str
    summary: str
    token_estimate: int = Field(default=0, ge=0)
    created_at: datetime


class ToolArtifact(BaseModel):
    """A stored output from a tool invocation."""
    artifact_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    tool_name: str = Field(min_length=1)
    content: str
    content_hash: ArtifactHash | None = None
    summary: str = ""
    session_id: uuid.UUID | None = None
    actor_id: uuid.UUID | None = None
    goal_id: uuid.UUID | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    token_estimate: int = Field(default=0, ge=0)
    tags: list[str] = Field(default_factory=list)
    gateway_id: str = ""


# ---------------------------------------------------------------------------
# Phase 6: Session-scoped artifacts
# ---------------------------------------------------------------------------


class SessionArtifact(BaseModel):
    """A session-scoped tool artifact stored in Redis."""
    artifact_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    tool_name: str
    content: str
    summary: str = ""
    content_hash: str = ""
    session_key: str = ""
    session_id: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    token_estimate: int = 0
    tags: list[str] = Field(default_factory=list)
    injected_count: int = 0
    searched_count: int = 0


class SessionArtifactSearchRequest(BaseModel):
    """Request to search session artifacts."""
    session_key: str
    session_id: str
    query: str
    tool_name: str | None = None
    max_results: int = Field(default=5, ge=1, le=50)


class CreateArtifactRequest(BaseModel):
    """Request to create an artifact (session or persistent)."""
    content: str = Field(min_length=1)
    tool_name: str = "manual"
    scope: Literal["session", "persistent"] = "session"
    session_key: str
    session_id: str
    tags: list[str] = Field(default_factory=list)
    goal_id: str | None = None
    summary: str | None = None
