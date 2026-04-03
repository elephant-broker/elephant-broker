"""HITL intent and callback models.

Duplicated from elephantbroker.runtime.guards.hitl_client for deployment independence.
The HITL middleware is a standalone service that does NOT import from elephantbroker.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, Field


class NotificationIntent(BaseModel):
    """Payload for INFORM/WARN outcomes — fire-and-forget notification."""

    guard_event_id: uuid.UUID
    session_id: uuid.UUID
    session_key: str = ""
    gateway_id: str = ""
    agent_key: str = ""
    action_summary: str = ""
    decision_domain: str = ""
    outcome: str = "inform"
    matched_rules: list[str] = Field(default_factory=list)
    explanation: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ApprovalIntent(BaseModel):
    """Payload for REQUIRE_APPROVAL outcomes — approval request with callbacks."""

    request_id: uuid.UUID
    guard_event_id: uuid.UUID
    session_id: uuid.UUID
    session_key: str = ""
    gateway_id: str = ""
    agent_key: str = ""
    action_summary: str = ""
    decision_domain: str = ""
    matched_rules: list[str] = Field(default_factory=list)
    explanation: str = ""
    approve_callback_url: str = ""
    reject_callback_url: str = ""
    timeout_seconds: int = 300
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ApproveCallback(BaseModel):
    """Callback payload for approving an action."""

    request_id: uuid.UUID
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    message: str = ""
    approved_by: str = ""


class RejectCallback(BaseModel):
    """Callback payload for rejecting an action."""

    request_id: uuid.UUID
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    reason: str
    rejected_by: str = ""
