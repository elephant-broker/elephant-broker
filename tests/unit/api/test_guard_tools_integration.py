"""Integration tests for guard API endpoints called by TS tools.

Tests the guard API routes with a mocked container that includes
real guard engine sub-components (schemas, session state, approval
request objects) while keeping infrastructure (Redis, Neo4j) mocked.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from elephantbroker.api.app import create_app
from elephantbroker.runtime.guards.rules import StaticRuleRegistry
from elephantbroker.schemas.guards import (
    ApprovalRequest,
    ApprovalStatus,
    AutonomyLevel,
    GuardEvent,
    GuardOutcome,
    StaticRule,
    StaticRulePatternType,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_container():
    """Minimal container mock wired for guard API routes."""
    c = MagicMock()

    # Guard engine mock with real session dict
    c.guard_engine = MagicMock()
    c.guard_engine.preflight_check = AsyncMock()
    c.guard_engine.get_guard_history = AsyncMock(return_value=[])
    c.guard_engine.load_session_rules = AsyncMock()
    c.guard_engine.reinject_constraints = AsyncMock(return_value=[])
    c.guard_engine._sessions = {}
    c.guard_engine._approvals = MagicMock()
    c.guard_engine._approvals.get_for_session = AsyncMock(return_value=[])
    c.guard_engine._approvals.get = AsyncMock(return_value=None)
    c.guard_engine._approvals.approve = AsyncMock()
    c.guard_engine._approvals.reject = AsyncMock()

    # Other container attributes the middleware/app may access
    c.approval_queue = c.guard_engine._approvals
    c.redis = AsyncMock()
    c.trace_ledger = MagicMock()
    c.trace_ledger.append_event = AsyncMock()

    # Gateway identity defaults
    c.config = MagicMock()
    c.config.gateway = MagicMock()
    c.config.gateway.gateway_id = "test"

    return c


def _make_session_state(session_id, *, agent_id="agent-1", constraints=None, rules=None, procedure_bindings=None):
    """Build a _SessionGuardState-compatible MagicMock."""
    state = MagicMock()
    state.session_id = session_id
    state.session_key = f"test:{session_id}"
    state.agent_id = agent_id
    state.session_constraints = constraints or []
    state.active_procedure_bindings = procedure_bindings or []

    if rules is not None:
        registry = StaticRuleRegistry()
        registry.load_rules(policy_rules=rules, builtin_rules=[])
        state.rule_registry = registry
    else:
        state.rule_registry = None

    return state


def _make_event(*, outcome=GuardOutcome.PASS, summary="test action", event_id=None, session_id=None, matched_rules=None):
    """Build a real GuardEvent."""
    return GuardEvent(
        id=event_id or uuid.uuid4(),
        session_id=session_id or uuid.uuid4(),
        input_summary=summary,
        outcome=outcome,
        matched_rules=matched_rules or [],
        timestamp=datetime.now(UTC),
    )


def _make_approval(*, request_id=None, guard_event_id=None, session_id=None, status=ApprovalStatus.PENDING, summary="deploy action"):
    """Build a real ApprovalRequest."""
    return ApprovalRequest(
        id=request_id or uuid.uuid4(),
        guard_event_id=guard_event_id or uuid.uuid4(),
        session_id=session_id or uuid.uuid4(),
        action_summary=summary,
        status=status,
    )


@pytest.fixture
async def client(mock_container):
    app = create_app(mock_container)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# Tests (10)
# ---------------------------------------------------------------------------


class TestGuardToolsIntegration:
    """Integration tests for guard API routes."""

    async def test_active_guards_empty_session(self, client, mock_container):
        """GET /guards/active/{sid} for an unknown session returns 404."""
        sid = uuid.uuid4()
        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 404
        data = r.json()
        assert data["detail"]["code"] == "SESSION_NOT_FOUND"

    async def test_active_guards_with_loaded_rules(self, client, mock_container):
        """GET /guards/active/{sid} returns loaded static rules."""
        sid = uuid.uuid4()
        rules = [
            StaticRule(id="r1", pattern="rm -rf", outcome=GuardOutcome.BLOCK, source="policy"),
            StaticRule(id="r2", pattern="sudo", outcome=GuardOutcome.WARN, source="builtin"),
        ]
        mock_container.guard_engine._sessions[sid] = _make_session_state(sid, rules=rules)

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert len(data["active_rules"]) == 2
        ids = {rule["id"] for rule in data["active_rules"]}
        assert ids == {"r1", "r2"}
        assert data["active_rules"][0]["outcome"] in ("block", "warn")

    async def test_active_guards_with_pending_approval(self, client, mock_container):
        """GET /guards/active/{sid} populates pending_approvals from queue."""
        sid = uuid.uuid4()
        mock_container.guard_engine._sessions[sid] = _make_session_state(sid)

        approval = _make_approval(session_id=sid, summary="deploy to prod")
        mock_container.guard_engine._approvals.get_for_session = AsyncMock(return_value=[approval])

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert len(data["pending_approvals"]) == 1
        assert data["pending_approvals"][0]["action_summary"] == "deploy to prod"
        assert data["pending_approvals"][0]["status"] == "pending"

    async def test_active_guards_with_recent_events(self, client, mock_container):
        """GET /guards/active/{sid} returns recent guard events."""
        sid = uuid.uuid4()
        mock_container.guard_engine._sessions[sid] = _make_session_state(sid)

        events = [
            _make_event(outcome=GuardOutcome.PASS, summary="safe action", session_id=sid),
            _make_event(outcome=GuardOutcome.WARN, summary="risky call", session_id=sid),
        ]
        mock_container.guard_engine.get_guard_history = AsyncMock(return_value=events)

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert len(data["recent_events"]) == 2
        outcomes = [e["outcome"] for e in data["recent_events"]]
        assert "pass" in outcomes
        assert "warn" in outcomes

    async def test_active_guards_with_constraints(self, client, mock_container):
        """GET /guards/active/{sid} returns session constraints."""
        sid = uuid.uuid4()
        constraints = ["Do not modify production database", "Require code review"]
        mock_container.guard_engine._sessions[sid] = _make_session_state(sid, constraints=constraints)

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert data["constraints"] == constraints

    async def test_event_detail_found(self, client, mock_container):
        """GET /guards/events/detail/{id} returns the matching event."""
        sid = uuid.uuid4()
        event_id = uuid.uuid4()
        event = _make_event(
            event_id=event_id,
            session_id=sid,
            outcome=GuardOutcome.BLOCK,
            summary="dangerous tool call",
            matched_rules=["no-delete"],
        )
        mock_container.guard_engine.get_guard_history = AsyncMock(return_value=[event])

        r = await client.get(f"/guards/events/detail/{event_id}", params={"session_id": str(sid)})
        assert r.status_code == 200
        data = r.json()
        assert data["event"]["id"] == str(event_id)
        assert data["event"]["outcome"] == "block"
        assert data["event"]["input_summary"] == "dangerous tool call"

    async def test_event_detail_with_approval(self, client, mock_container):
        """GET /guards/events/detail/{id} returns approval when one matches the event."""
        sid = uuid.uuid4()
        event_id = uuid.uuid4()
        approval_id = uuid.uuid4()

        event = _make_event(
            event_id=event_id,
            session_id=sid,
            outcome=GuardOutcome.REQUIRE_APPROVAL,
            summary="needs approval",
        )
        approval = _make_approval(
            request_id=approval_id,
            guard_event_id=event_id,
            session_id=sid,
            status=ApprovalStatus.PENDING,
            summary="needs approval",
        )

        mock_container.guard_engine.get_guard_history = AsyncMock(return_value=[event])
        mock_container.guard_engine._sessions[sid] = _make_session_state(sid)
        mock_container.guard_engine._approvals.get_for_session = AsyncMock(return_value=[approval])

        r = await client.get(f"/guards/events/detail/{event_id}", params={"session_id": str(sid)})
        assert r.status_code == 200
        data = r.json()
        assert "event" in data
        assert "approval" in data
        assert data["approval"]["request_id"] == str(approval_id)
        assert data["approval"]["status"] == "pending"

    async def test_event_detail_not_found(self, client, mock_container):
        """GET /guards/events/detail/{id} for unknown event returns 404."""
        sid = uuid.uuid4()
        unknown_id = uuid.uuid4()
        mock_container.guard_engine.get_guard_history = AsyncMock(return_value=[])

        r = await client.get(f"/guards/events/detail/{unknown_id}", params={"session_id": str(sid)})
        assert r.status_code == 404
        data = r.json()
        assert data["detail"]["code"] == "EVENT_NOT_FOUND"

    async def test_active_guards_session_isolation(self, client, mock_container):
        """Two sessions with different rules return session-specific data."""
        sid_a = uuid.uuid4()
        sid_b = uuid.uuid4()

        rules_a = [StaticRule(id="rule-a", pattern="alpha", outcome=GuardOutcome.BLOCK, source="policy")]
        rules_b = [StaticRule(id="rule-b", pattern="beta", outcome=GuardOutcome.WARN, source="policy")]

        mock_container.guard_engine._sessions[sid_a] = _make_session_state(
            sid_a, agent_id="agent-a", rules=rules_a, constraints=["constraint-a"],
        )
        mock_container.guard_engine._sessions[sid_b] = _make_session_state(
            sid_b, agent_id="agent-b", rules=rules_b, constraints=["constraint-b"],
        )

        r_a = await client.get(f"/guards/active/{sid_a}")
        r_b = await client.get(f"/guards/active/{sid_b}")

        assert r_a.status_code == 200
        assert r_b.status_code == 200

        data_a = r_a.json()
        data_b = r_b.json()

        assert len(data_a["active_rules"]) == 1
        assert data_a["active_rules"][0]["id"] == "rule-a"
        assert data_a["constraints"] == ["constraint-a"]

        assert len(data_b["active_rules"]) == 1
        assert data_b["active_rules"][0]["id"] == "rule-b"
        assert data_b["constraints"] == ["constraint-b"]

    async def test_approval_patch_updates_status(self, client, mock_container):
        """PATCH /guards/approvals/{request_id} with status=approved calls approve()."""
        request_id = uuid.uuid4()
        approved_approval = _make_approval(
            request_id=request_id,
            status=ApprovalStatus.APPROVED,
            summary="deploy v2",
        )
        mock_container.guard_engine._approvals.approve = AsyncMock(return_value=approved_approval)

        r = await client.patch(
            f"/guards/approvals/{request_id}",
            json={"status": "approved", "agent_id": "agent-1", "message": "Looks good"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["request"]["status"] == "approved"
        assert data["request"]["action_summary"] == "deploy v2"

        # Verify approve() was called with correct core args
        call_args = mock_container.guard_engine._approvals.approve.call_args
        assert call_args[0] == (request_id, "agent-1")
        assert call_args[1]["message"] == "Looks good"
