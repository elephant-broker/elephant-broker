"""Tests for RedLineGuardEngine — basic interface tests."""
import uuid

from elephantbroker.runtime.guards.engine import RedLineGuardEngine
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.context import AgentMessage
from elephantbroker.schemas.guards import GuardOutcome


class TestGuardEngine:
    def _make(self):
        return RedLineGuardEngine(TraceLedger())

    async def test_preflight_unknown_session_raises(self):
        from elephantbroker.runtime.guards.engine import GuardRulesNotLoadedError
        import pytest
        engine = self._make()
        msgs = [AgentMessage(role="user", content="test")]
        with pytest.raises(GuardRulesNotLoadedError):
            await engine.preflight_check(uuid.uuid4(), msgs)

    async def test_reinject_empty(self):
        engine = self._make()
        result = await engine.reinject_constraints(uuid.uuid4())
        assert result == []

    async def test_history_empty(self):
        engine = self._make()
        result = await engine.get_guard_history(uuid.uuid4())
        assert result == []
