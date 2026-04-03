"""E2E tests for guard TS tools — full API flow with realistic scenarios.

Tests exercise the complete guard API surface (active rules, event detail,
approval lifecycle) through the same mocked-container pattern used in
integration tests but with richer, more realistic multi-step scenarios.
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
    """Container mock for full-flow guard API tests."""
    c = MagicMock()

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

    c.approval_queue = c.guard_engine._approvals
    c.redis = AsyncMock()
    c.trace_ledger = MagicMock()
    c.trace_ledger.append_event = AsyncMock()

    c.config = MagicMock()
    c.config.gateway = MagicMock()
    c.config.gateway.gateway_id = "test"

    return c


def _session_state(session_id, *, agent_id="agent-e2e", constraints=None, rules=None, procedure_bindings=None):
    """Build session guard state for e2e scenarios."""
    state = MagicMock()
    state.session_id = session_id
    state.session_key = f"e2e:{session_id}"
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


def _event(*, outcome=GuardOutcome.PASS, summary="action", event_id=None, session_id=None, matched_rules=None):
    return GuardEvent(
        id=event_id or uuid.uuid4(),
        session_id=session_id or uuid.uuid4(),
        input_summary=summary,
        outcome=outcome,
        matched_rules=matched_rules or [],
        timestamp=datetime.now(UTC),
    )


def _approval(*, request_id=None, guard_event_id=None, session_id=None, status=ApprovalStatus.PENDING, summary="action"):
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


class TestGuardToolsE2E:
    """End-to-end tests for guard TS tool API flows."""

    async def test_guards_list_empty_session(self, client, mock_container):
        """New session with no rules loaded returns 404 (session not found)."""
        sid = uuid.uuid4()
        # Session not registered — should 404
        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 404

    async def test_guards_list_shows_rules(self, client, mock_container):
        """Session with loaded rules returns them via GET /guards/active."""
        sid = uuid.uuid4()
        rules = [
            StaticRule(id="no-drop", pattern="DROP TABLE", pattern_type=StaticRulePatternType.PHRASE, outcome=GuardOutcome.BLOCK, source="policy"),
            StaticRule(id="warn-delete", pattern="DELETE FROM", pattern_type=StaticRulePatternType.PHRASE, outcome=GuardOutcome.WARN, source="builtin"),
            StaticRule(id="no-truncate", pattern="TRUNCATE", pattern_type=StaticRulePatternType.KEYWORD, outcome=GuardOutcome.BLOCK, source="policy"),
        ]
        mock_container.guard_engine._sessions[sid] = _session_state(sid, rules=rules)

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert len(data["active_rules"]) == 3
        rule_ids = {rule["id"] for rule in data["active_rules"]}
        assert "no-drop" in rule_ids
        assert "warn-delete" in rule_ids
        assert "no-truncate" in rule_ids

    async def test_guards_list_shows_pending(self, client, mock_container):
        """Pending approval request visible in guards list."""
        sid = uuid.uuid4()
        mock_container.guard_engine._sessions[sid] = _session_state(sid)

        pending = _approval(
            session_id=sid,
            summary="deploy infrastructure changes",
            status=ApprovalStatus.PENDING,
        )
        mock_container.guard_engine._approvals.get_for_session = AsyncMock(return_value=[pending])

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert len(data["pending_approvals"]) == 1
        pa = data["pending_approvals"][0]
        assert pa["status"] == "pending"
        assert "infrastructure" in pa["action_summary"]

    async def test_guard_status_pass_event(self, client, mock_container):
        """Event detail for a PASS event returns the event data."""
        sid = uuid.uuid4()
        eid = uuid.uuid4()
        pass_event = _event(
            event_id=eid,
            session_id=sid,
            outcome=GuardOutcome.PASS,
            summary="read-only query executed",
        )
        mock_container.guard_engine.get_guard_history = AsyncMock(return_value=[pass_event])

        r = await client.get(f"/guards/events/detail/{eid}", params={"session_id": str(sid)})
        assert r.status_code == 200
        data = r.json()
        assert data["event"]["outcome"] == "pass"
        assert data["event"]["input_summary"] == "read-only query executed"
        assert "approval" not in data

    async def test_guard_status_block_event(self, client, mock_container):
        """BLOCK event detail shows matched_rules in the response."""
        sid = uuid.uuid4()
        eid = uuid.uuid4()
        block_event = _event(
            event_id=eid,
            session_id=sid,
            outcome=GuardOutcome.BLOCK,
            summary="attempted production delete",
            matched_rules=["no-prod-delete", "require-review"],
        )
        mock_container.guard_engine.get_guard_history = AsyncMock(return_value=[block_event])

        r = await client.get(f"/guards/events/detail/{eid}", params={"session_id": str(sid)})
        assert r.status_code == 200
        data = r.json()
        assert data["event"]["outcome"] == "block"
        assert "no-prod-delete" in data["event"]["matched_rules"]
        assert "require-review" in data["event"]["matched_rules"]

    async def test_guard_status_approval(self, client, mock_container):
        """REQUIRE_APPROVAL event has an approval object attached in detail."""
        sid = uuid.uuid4()
        eid = uuid.uuid4()
        aid = uuid.uuid4()

        approval_event = _event(
            event_id=eid,
            session_id=sid,
            outcome=GuardOutcome.REQUIRE_APPROVAL,
            summary="deploy to staging",
        )
        approval_req = _approval(
            request_id=aid,
            guard_event_id=eid,
            session_id=sid,
            status=ApprovalStatus.PENDING,
            summary="deploy to staging",
        )

        mock_container.guard_engine.get_guard_history = AsyncMock(return_value=[approval_event])
        mock_container.guard_engine._sessions[sid] = _session_state(sid)
        mock_container.guard_engine._approvals.get_for_session = AsyncMock(return_value=[approval_req])

        r = await client.get(f"/guards/events/detail/{eid}", params={"session_id": str(sid)})
        assert r.status_code == 200
        data = r.json()
        assert data["event"]["outcome"] == "require_approval"
        assert "approval" in data
        assert data["approval"]["request_id"] == str(aid)
        assert data["approval"]["status"] == "pending"
        assert data["approval"]["timeout_at"] is not None

    async def test_guard_status_invalid_id(self, client, mock_container):
        """Nonexistent event ID returns 404."""
        sid = uuid.uuid4()
        fake_eid = uuid.uuid4()

        # Return events that do not match the requested ID
        other_event = _event(session_id=sid, summary="unrelated")
        mock_container.guard_engine.get_guard_history = AsyncMock(return_value=[other_event])

        r = await client.get(f"/guards/events/detail/{fake_eid}", params={"session_id": str(sid)})
        assert r.status_code == 404
        data = r.json()
        assert data["detail"]["code"] == "EVENT_NOT_FOUND"

    async def test_guards_list_after_procedure(self, client, mock_container):
        """Session with active procedure bindings shows them via rules loaded from bindings."""
        sid = uuid.uuid4()

        # Procedure bindings typically add extra rules via load_rules(procedure_bindings=...)
        # Simulate a session where procedure-sourced rules are present
        proc_rules = [
            StaticRule(id="proc-audit-log", pattern="audit_log", pattern_type=StaticRulePatternType.KEYWORD, outcome=GuardOutcome.INFORM, source="procedure:deploy-v2"),
            StaticRule(id="proc-no-rollback", pattern="rollback", pattern_type=StaticRulePatternType.KEYWORD, outcome=GuardOutcome.REQUIRE_APPROVAL, source="procedure:deploy-v2"),
        ]
        state = _session_state(
            sid,
            rules=proc_rules,
            procedure_bindings=["deploy-v2"],
        )
        mock_container.guard_engine._sessions[sid] = state

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert len(data["active_rules"]) == 2
        sources = {rule["source"] for rule in data["active_rules"]}
        assert "procedure:deploy-v2" in sources

    async def test_guards_list_constraints(self, client, mock_container):
        """Session constraints from WARN events are returned in the response."""
        sid = uuid.uuid4()
        constraints = [
            "WARN: avoid modifying shared config files without review",
            "WARN: rate limit exceeded on external API calls",
        ]
        mock_container.guard_engine._sessions[sid] = _session_state(sid, constraints=constraints)

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert len(data["constraints"]) == 2
        assert "shared config" in data["constraints"][0]
        assert "rate limit" in data["constraints"][1]

    async def test_session_isolation(self, client, mock_container):
        """Two sessions return session-specific data from all guard tool queries."""
        sid_1 = uuid.uuid4()
        sid_2 = uuid.uuid4()
        eid_1 = uuid.uuid4()
        eid_2 = uuid.uuid4()

        # Session 1: one rule, one event, one constraint
        rules_1 = [StaticRule(id="s1-rule", pattern="alpha", outcome=GuardOutcome.BLOCK, source="s1")]
        mock_container.guard_engine._sessions[sid_1] = _session_state(
            sid_1, agent_id="agent-1", rules=rules_1, constraints=["s1-constraint"],
        )
        event_1 = _event(event_id=eid_1, session_id=sid_1, outcome=GuardOutcome.PASS, summary="s1 action")

        # Session 2: different rule, different event, different constraint
        rules_2 = [StaticRule(id="s2-rule", pattern="beta", outcome=GuardOutcome.WARN, source="s2")]
        mock_container.guard_engine._sessions[sid_2] = _session_state(
            sid_2, agent_id="agent-2", rules=rules_2, constraints=["s2-constraint"],
        )
        event_2 = _event(event_id=eid_2, session_id=sid_2, outcome=GuardOutcome.BLOCK, summary="s2 action")

        # Mock get_guard_history to return session-specific events
        async def session_history(session_id):
            if session_id == sid_1:
                return [event_1]
            elif session_id == sid_2:
                return [event_2]
            return []

        mock_container.guard_engine.get_guard_history = AsyncMock(side_effect=session_history)

        # Verify active guards isolation
        r1 = await client.get(f"/guards/active/{sid_1}")
        r2 = await client.get(f"/guards/active/{sid_2}")
        assert r1.status_code == 200
        assert r2.status_code == 200

        d1 = r1.json()
        d2 = r2.json()

        assert d1["active_rules"][0]["id"] == "s1-rule"
        assert d1["constraints"] == ["s1-constraint"]
        assert d1["recent_events"][0]["outcome"] == "pass"

        assert d2["active_rules"][0]["id"] == "s2-rule"
        assert d2["constraints"] == ["s2-constraint"]
        assert d2["recent_events"][0]["outcome"] == "block"

        # Verify event detail isolation
        r1_detail = await client.get(f"/guards/events/detail/{eid_1}", params={"session_id": str(sid_1)})
        r2_detail = await client.get(f"/guards/events/detail/{eid_2}", params={"session_id": str(sid_2)})

        assert r1_detail.status_code == 200
        assert r2_detail.status_code == 200
        assert r1_detail.json()["event"]["input_summary"] == "s1 action"
        assert r2_detail.json()["event"]["input_summary"] == "s2 action"

        # Cross-session access should fail (event not in other session's history)
        r_cross = await client.get(f"/guards/events/detail/{eid_1}", params={"session_id": str(sid_2)})
        assert r_cross.status_code == 404
