"""Unit tests for hitl_middleware.models — 12 tests."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from hitl_middleware.models import (
    ApprovalIntent,
    ApproveCallback,
    NotificationIntent,
    RejectCallback,
)


# --- NotificationIntent ---


class TestNotificationIntent:
    def test_required_fields_only(self):
        """NotificationIntent requires guard_event_id and session_id."""
        gid = uuid.uuid4()
        sid = uuid.uuid4()
        n = NotificationIntent(guard_event_id=gid, session_id=sid)
        assert n.guard_event_id == gid
        assert n.session_id == sid

    def test_defaults(self):
        """Default values are set correctly."""
        n = NotificationIntent(guard_event_id=uuid.uuid4(), session_id=uuid.uuid4())
        assert n.session_key == ""
        assert n.gateway_id == ""
        assert n.agent_key == ""
        assert n.action_summary == ""
        assert n.outcome == "inform"
        assert n.matched_rules == []
        assert n.explanation == ""

    def test_serialization_round_trip(self):
        """Model serializes to JSON and deserializes back."""
        gid = uuid.uuid4()
        sid = uuid.uuid4()
        n = NotificationIntent(
            guard_event_id=gid, session_id=sid, action_summary="test action"
        )
        data = n.model_dump(mode="json")
        restored = NotificationIntent.model_validate(data)
        assert restored.guard_event_id == gid
        assert restored.action_summary == "test action"

    def test_timestamp_default_set(self):
        """Timestamp defaults to approximately now."""
        before = datetime.now(UTC)
        n = NotificationIntent(guard_event_id=uuid.uuid4(), session_id=uuid.uuid4())
        after = datetime.now(UTC)
        assert before <= n.timestamp <= after

    def test_matched_rules_list(self):
        """matched_rules accepts a list of strings."""
        n = NotificationIntent(
            guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(),
            matched_rules=["rule-1", "rule-2"],
        )
        assert n.matched_rules == ["rule-1", "rule-2"]

    def test_missing_required_guard_event_id(self):
        """Missing guard_event_id raises ValidationError."""
        with pytest.raises(ValidationError):
            NotificationIntent(session_id=uuid.uuid4())


# --- ApprovalIntent ---


class TestApprovalIntent:
    def test_all_fields(self):
        """ApprovalIntent accepts all fields."""
        rid = uuid.uuid4()
        gid = uuid.uuid4()
        sid = uuid.uuid4()
        a = ApprovalIntent(
            request_id=rid,
            guard_event_id=gid,
            session_id=sid,
            approve_callback_url="http://example.com/approve",
            reject_callback_url="http://example.com/reject",
            timeout_seconds=600,
        )
        assert a.request_id == rid
        assert a.approve_callback_url == "http://example.com/approve"
        assert a.timeout_seconds == 600

    def test_timeout_default(self):
        """timeout_seconds defaults to 300."""
        a = ApprovalIntent(
            request_id=uuid.uuid4(),
            guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(),
        )
        assert a.timeout_seconds == 300


# --- ApproveCallback ---


class TestApproveCallback:
    def test_optional_message(self):
        """ApproveCallback message is optional, defaults to empty string."""
        cb = ApproveCallback(request_id=uuid.uuid4())
        assert cb.message == ""

    def test_with_message(self):
        """ApproveCallback accepts optional message."""
        rid = uuid.uuid4()
        cb = ApproveCallback(request_id=rid, message="Looks good", approved_by="admin")
        assert cb.message == "Looks good"
        assert cb.approved_by == "admin"


# --- RejectCallback ---


class TestRejectCallback:
    def test_reason_required(self):
        """RejectCallback requires reason field."""
        with pytest.raises(ValidationError):
            RejectCallback(request_id=uuid.uuid4())

    def test_reject_with_reason(self):
        """RejectCallback accepts reason and optional rejected_by."""
        rid = uuid.uuid4()
        cb = RejectCallback(request_id=rid, reason="Policy violation", rejected_by="admin")
        assert cb.reason == "Policy violation"
        assert cb.rejected_by == "admin"
