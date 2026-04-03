"""Tests for guard API routes.

Covers: GET /guards/active/{session_id}, GET /guards/events/{session_id},
GET /guards/rules/{session_id}, POST /guards/check/{session_id},
and error cases (missing session, no engine).
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from elephantbroker.schemas.guards import GuardEvent, GuardOutcome, GuardResult
from elephantbroker.schemas.profile import GuardPolicy


# ---------------------------------------------------------------------------
# Lightweight stub for _SessionGuardState
# ---------------------------------------------------------------------------

@dataclass
class _FakeRuleRegistry:
    _rules: list = field(default_factory=list)


@dataclass
class _FakeRule:
    id: str = "r1"
    pattern: str = "exec"
    pattern_type: type = field(default=None)
    outcome: GuardOutcome = GuardOutcome.BLOCK
    enabled: bool = True
    source: str = "test"

    def __post_init__(self):
        if self.pattern_type is None:
            from elephantbroker.schemas.guards import StaticRulePatternType
            self.pattern_type = StaticRulePatternType.KEYWORD


@dataclass
class _FakeSessionState:
    session_id: uuid.UUID = field(default_factory=uuid.uuid4)
    session_key: str = "agent:main:main"
    agent_id: str = "agent-1"
    rule_registry: _FakeRuleRegistry | None = None
    guard_policy: GuardPolicy = field(default_factory=GuardPolicy)
    session_constraints: list[str] = field(default_factory=list)
    structural_validators: list = field(default_factory=list)
    active_procedure_bindings: list = field(default_factory=list)


def _setup_guard_engine(container, session_id: uuid.UUID, state: _FakeSessionState | None = None):
    """Wire a guard engine mock with a session state into the container."""
    engine = container.guard_engine
    engine._sessions = {}
    engine._approvals = None
    engine._redis = None
    engine._keys = None
    if state is not None:
        engine._sessions[session_id] = state
    engine.get_guard_history = AsyncMock(return_value=[])
    engine.preflight_check = AsyncMock(return_value=GuardResult(outcome=GuardOutcome.PASS))
    engine.load_session_rules = AsyncMock()
    return engine


# ---------------------------------------------------------------------------
# GET /guards/active/{session_id}
# ---------------------------------------------------------------------------

class TestGetActiveRules:
    async def test_returns_active_rules_and_constraints(self, client, container):
        sid = uuid.uuid4()
        rule = _FakeRule()
        state = _FakeSessionState(
            session_id=sid,
            rule_registry=_FakeRuleRegistry(_rules=[rule]),
            session_constraints=["no-rm-rf"],
        )
        _setup_guard_engine(container, sid, state)

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert len(data["active_rules"]) == 1
        assert data["active_rules"][0]["pattern"] == "exec"
        assert data["constraints"] == ["no-rm-rf"]

    async def test_session_not_found_404(self, client, container):
        sid = uuid.uuid4()
        _setup_guard_engine(container, sid, state=None)

        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 404

    async def test_guard_engine_unavailable_503(self, client, container):
        container.guard_engine = None
        sid = uuid.uuid4()
        r = await client.get(f"/guards/active/{sid}")
        assert r.status_code == 503


# ---------------------------------------------------------------------------
# GET /guards/rules/{session_id}
# ---------------------------------------------------------------------------

class TestGetLoadedRules:
    async def test_returns_rule_details(self, client, container):
        sid = uuid.uuid4()
        rule = _FakeRule(id="r2", pattern="secret/*", enabled=True)
        state = _FakeSessionState(
            session_id=sid,
            rule_registry=_FakeRuleRegistry(_rules=[rule]),
        )
        _setup_guard_engine(container, sid, state)

        r = await client.get(f"/guards/rules/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert data["rules_count"] == 1
        assert data["rules"][0]["id"] == "r2"
        assert data["rules"][0]["pattern"] == "secret/*"

    async def test_session_not_found_404(self, client, container):
        sid = uuid.uuid4()
        _setup_guard_engine(container, sid, state=None)

        r = await client.get(f"/guards/rules/{sid}")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# GET /guards/events/{session_id}
# ---------------------------------------------------------------------------

class TestGetGuardEvents:
    async def test_returns_events(self, client, container):
        sid = uuid.uuid4()
        ev = GuardEvent(
            session_id=sid,
            outcome=GuardOutcome.WARN,
            input_summary="tried to delete /",
        )
        state = _FakeSessionState(session_id=sid)
        engine = _setup_guard_engine(container, sid, state)
        engine.get_guard_history = AsyncMock(return_value=[ev])

        r = await client.get(f"/guards/events/{sid}")
        assert r.status_code == 200
        data = r.json()
        assert len(data["events"]) == 1
        assert data["events"][0]["outcome"] == "warn"

    async def test_empty_history(self, client, container):
        sid = uuid.uuid4()
        state = _FakeSessionState(session_id=sid)
        _setup_guard_engine(container, sid, state)

        r = await client.get(f"/guards/events/{sid}")
        assert r.status_code == 200
        assert r.json()["events"] == []


# ---------------------------------------------------------------------------
# POST /guards/check/{session_id}
# ---------------------------------------------------------------------------

class TestPreflightCheck:
    async def test_check_returns_result(self, client, container):
        sid = uuid.uuid4()
        state = _FakeSessionState(session_id=sid)
        engine = _setup_guard_engine(container, sid, state)
        engine.preflight_check = AsyncMock(
            return_value=GuardResult(outcome=GuardOutcome.BLOCK, explanation="dangerous"),
        )

        r = await client.post(
            f"/guards/check/{sid}",
            json={"messages": [{"role": "user", "content": "rm -rf /"}]},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["outcome"] == "block"
        assert data["explanation"] == "dangerous"

    async def test_check_missing_messages_400(self, client, container):
        sid = uuid.uuid4()
        state = _FakeSessionState(session_id=sid)
        _setup_guard_engine(container, sid, state)

        r = await client.post(f"/guards/check/{sid}", json={"messages": []})
        assert r.status_code == 400

    async def test_check_engine_unavailable_503(self, client, container):
        container.guard_engine = None
        sid = uuid.uuid4()
        r = await client.post(
            f"/guards/check/{sid}",
            json={"messages": [{"role": "user", "content": "hello"}]},
        )
        assert r.status_code == 503
