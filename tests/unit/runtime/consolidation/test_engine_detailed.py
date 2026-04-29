"""Detailed tests for ConsolidationEngine — individual methods and error paths."""
from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.consolidation.engine import (
    ConsolidationAlreadyRunningError,
    ConsolidationEngine,
)
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.consolidation import ConsolidationContext, StageResult
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.fact import FactAssertion, FactCategory
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


class TestEngineDefaultProfileResolution:
    """BUG-B9-4 regression — ensure profile_id=None resolves to the configured
    default rather than skipping resolution entirely. Pre-fix, the engine ran
    with ``profile=None`` and Stage 9 fell back to a hardcoded "coding" key,
    diverging from any operator override.
    """

    async def test_profile_id_none_resolves_default(self):
        registry = AsyncMock()
        registry.resolve_profile = AsyncMock(return_value=None)
        engine = _make_engine()
        engine._profiles = registry
        report = await engine.run_consolidation("org", "gw", profile_id=None)
        registry.resolve_profile.assert_called_once()
        call_args = registry.resolve_profile.call_args
        # First positional arg is the resolved profile name
        assert call_args.args[0] == "coding"
        # Resolved id is reflected on the report and downstream context
        assert report.profile_id == "coding"
        # Report completed without crashing
        assert report.completed_at is not None

    async def test_profile_id_explicit_overrides_default(self):
        registry = AsyncMock()
        registry.resolve_profile = AsyncMock(return_value=None)
        engine = _make_engine()
        engine._profiles = registry
        report = await engine.run_consolidation("org", "gw", profile_id="research")
        registry.resolve_profile.assert_called_once()
        assert registry.resolve_profile.call_args.args[0] == "research"
        assert report.profile_id == "research"


class TestApplyFactUpsertsPreservesCascadePointer:
    """TODO-5-008 — Site 6 of cascade-pointer-wipe cluster.

    _apply_fact_upserts takes FactAssertion objects (no storage-backend
    id in scope) and MERGEs them through add_data_points. Without a
    graph round-trip to recover cognee_data_id, the MERGE wipes the
    graph property and re-orphans TD-50 cascades for any
    consolidation-touched fact on a later delete.
    """

    async def test_preserves_cognee_data_id_via_graph_roundtrip(self, monkeypatch):
        """Consolidation batch upsert must fetch existing cognee_data_ids
        and forward them into from_schema() — otherwise MERGE overwrites
        the node property with None."""
        graph = AsyncMock()
        fact = FactAssertion(text="consolidation touch", category=FactCategory.GENERAL)
        expected_data_id = str(uuid.uuid4())
        graph.query_cypher = AsyncMock(return_value=[
            {"eb_id": str(fact.id), "cognee_data_id": expected_data_id},
        ])

        recorded = []

        async def fake_add(data_points, context=None, custom_edges=None, embed_triplets=False):
            recorded.extend(list(data_points))
            return list(data_points)

        monkeypatch.setattr("cognee.tasks.storage.add_data_points", fake_add)

        engine = _make_engine(graph=graph, gateway_id="gw-cons")
        await engine._apply_fact_upserts([fact], gateway_id="gw-cons")

        assert len(recorded) == 1
        assert recorded[0].cognee_data_id == expected_data_id, (
            "Consolidation upsert dropped the cascade pointer — would "
            "re-orphan TD-50 on later delete of this fact."
        )
        # And the Cypher was gateway-scoped (TODO-5-008 + FIX-GATEWAY-IDENTITY).
        call_kwargs = graph.query_cypher.call_args
        assert call_kwargs is not None
        params = call_kwargs.args[1] if len(call_kwargs.args) > 1 else call_kwargs.kwargs.get("params", {})
        assert params["gw"] == "gw-cons"
