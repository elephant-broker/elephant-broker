"""Detailed tests for ConsolidationEngine — individual methods and error paths."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.consolidation.engine import (
    ConsolidationAlreadyRunningError,
    ConsolidationEngine,
)
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.consolidation import ConsolidationContext, StageResult
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.trace import TraceEventType


def _make_engine(**overrides):
    defaults = {
        "trace_ledger": TraceLedger(gateway_id="gw"),
        "graph": AsyncMock(),
        "vector": AsyncMock(),
        "redis": None,
        "redis_keys": None,
        "metrics": None,
        "config": ElephantBrokerConfig(),
        "gateway_id": "gw",
    }
    defaults.update(overrides)
    return ConsolidationEngine(**defaults)


class TestEngineRunConsolidation:
    async def test_returns_report_with_org_and_gateway(self):
        engine = _make_engine()
        report = await engine.run_consolidation("org-1", "gw-1")
        assert report.org_id == "org-1"
        assert report.gateway_id == "gw-1"

    async def test_report_has_completed_at(self):
        engine = _make_engine()
        report = await engine.run_consolidation("org", "gw")
        assert report.completed_at is not None

    async def test_emits_started_event(self):
        ledger = TraceLedger(gateway_id="gw")
        engine = _make_engine(trace_ledger=ledger)
        await engine.run_consolidation("org", "gw")
        events = await ledger.query_trace(MagicMock(
            event_types=[TraceEventType.CONSOLIDATION_STARTED],
            session_id=None, actor_ids=None,
            from_timestamp=None, to_timestamp=None, offset=0, limit=10,
            session_key=None, gateway_id=None,
        ))
        assert len(events) >= 1

    async def test_emits_completed_event_with_gateway(self):
        ledger = TraceLedger(gateway_id="gw")
        engine = _make_engine(trace_ledger=ledger)
        await engine.run_consolidation("org", "gw-test")
        events = await ledger.query_trace(MagicMock(
            event_types=[TraceEventType.CONSOLIDATION_COMPLETED],
            session_id=None, actor_ids=None,
            from_timestamp=None, to_timestamp=None, offset=0, limit=10,
            session_key=None, gateway_id=None,
        ))
        assert len(events) >= 1
        assert events[0].payload.get("gateway_id") == "gw-test"

    async def test_stores_report_in_report_store(self):
        store = AsyncMock()
        engine = _make_engine(report_store=store)
        await engine.run_consolidation("org", "gw")
        store.save_report.assert_called_once()


class TestEngineRedisLock:
    async def test_acquires_redis_lock(self, monkeypatch):
        monkeypatch.setattr("cognee.tasks.storage.add_data_points", AsyncMock())
        redis = MagicMock()
        redis.set = AsyncMock()  # async set for status writes
        keys = MagicMock()
        keys.consolidation_lock.return_value = "eb:gw:consolidation_lock"
        keys.consolidation_status.return_value = "eb:gw:consolidation_status"
        lock = MagicMock()
        lock.acquire = AsyncMock(return_value=True)
        lock.release = AsyncMock()
        redis.lock.return_value = lock
        engine = _make_engine(redis=redis, redis_keys=keys)
        await engine.run_consolidation("org", "gw")
        lock.acquire.assert_called_once()
        lock.release.assert_called_once()

    async def test_raises_when_locked(self):
        redis = MagicMock()  # redis.lock() is sync
        keys = MagicMock()
        keys.consolidation_lock.return_value = "lock"
        keys.consolidation_status.return_value = "status"
        lock = MagicMock()
        lock.acquire = AsyncMock(return_value=False)  # acquire is async
        redis.lock.return_value = lock
        engine = _make_engine(redis=redis, redis_keys=keys)
        with pytest.raises(ConsolidationAlreadyRunningError):
            await engine.run_consolidation("org", "gw")


class TestEngineCleanup:
    async def test_cleanup_calls_all_stores(self):
        scoring_ledger = AsyncMock()
        report_store = AsyncMock()
        proc_audit = AsyncMock()
        goal_audit = AsyncMock()
        engine = _make_engine(
            scoring_ledger_store=scoring_ledger,
            report_store=report_store,
            procedure_audit_store=proc_audit,
            session_goal_audit_store=goal_audit,
        )
        await engine._run_cleanup()
        scoring_ledger.cleanup_old.assert_called_once()
        report_store.cleanup_old.assert_called_once()
        proc_audit.cleanup_old.assert_called_once()
        goal_audit.cleanup_old.assert_called_once()


class TestEngineRunStage:
    async def test_run_stage_returns_result(self):
        engine = _make_engine()
        ctx = ConsolidationContext(org_id="org", gateway_id="gw")
        result = await engine.run_stage(3, "org", "gw", ctx)
        assert result.stage == 3

    async def test_run_stage_unavailable_returns_skipped(self):
        engine = _make_engine()
        engine._stages = {}
        ctx = ConsolidationContext(org_id="org", gateway_id="gw")
        result = await engine.run_stage(99, "org", "gw", ctx)
        assert result.details.get("skipped") is True


class TestEngineGetReport:
    async def test_returns_none_without_store(self):
        engine = _make_engine()
        assert await engine.get_consolidation_report("x") is None

    async def test_delegates_to_store(self):
        store = AsyncMock()
        store.get_report = AsyncMock(return_value=None)
        engine = _make_engine(report_store=store)
        await engine.get_consolidation_report("report-1")
        store.get_report.assert_called_once_with("report-1")
