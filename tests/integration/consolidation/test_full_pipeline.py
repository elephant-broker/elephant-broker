"""Integration tests for the full 9-stage consolidation pipeline.

These tests use mocked adapters (no Docker required) but exercise the full
engine orchestration path with real stage instances.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.consolidation.engine import ConsolidationEngine
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.consolidation import ConsolidationConfig
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.trace import TraceEventType
from tests.fixtures.factories import make_fact_assertion


def _make_engine(facts=None, profile_name="coding"):
    """Create a ConsolidationEngine with mocked adapters but real stage instances."""
    from elephantbroker.runtime.profiles.registry import ProfileRegistry

    ledger = TraceLedger(gateway_id="test-gw")
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_batch = AsyncMock(return_value=[])
    profile_reg = ProfileRegistry(ledger)
    scoring_tuner = AsyncMock()
    scoring_tuner.apply_feedback = AsyncMock()

    # Graph returns facts when queried
    fact_list = facts or []
    fact_props = []
    for f in fact_list:
        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        dp = FactDataPoint.from_schema(f)
        props = {k: v for k, v in dp.__dict__.items() if not k.startswith("_")}
        fact_props.append({"props": props})

    call_count = [0]
    async def mock_cypher(cypher, params):
        call_count[0] += 1
        if "FactDataPoint" in cypher and "RETURN" in cypher:
            if call_count[0] <= 2:  # First load + potential reload
                return fact_props
            return []
        return []
    graph.query_cypher = mock_cypher

    config = ElephantBrokerConfig()

    engine = ConsolidationEngine(
        trace_ledger=ledger,
        graph=graph,
        vector=vector,
        memory_store=AsyncMock(),
        embedding_service=embeddings,
        profile_registry=profile_reg,
        scoring_tuner=scoring_tuner,
        evidence_engine=AsyncMock(),
        procedure_engine=AsyncMock(),
        llm_client=AsyncMock(),
        redis=None,
        redis_keys=None,
        metrics=None,
        config=config,
        report_store=None,
        trace_query_client=None,
        scoring_ledger_store=None,
        gateway_id="test-gw",
    )
    return engine, ledger


class TestConsolidationEndToEnd:
    async def test_full_pipeline_runs_all_stages(self):
        engine, ledger = _make_engine()
        report = await engine.run_consolidation("org", "test-gw", "coding")
        assert report.status in ("completed", "partial", "failed")
        assert report.org_id == "org"
        assert report.gateway_id == "test-gw"

    async def test_pipeline_produces_report(self):
        engine, _ = _make_engine()
        report = await engine.run_consolidation("org", "gw")
        assert report.summary is not None
        assert report.completed_at is not None

    async def test_pipeline_emits_trace_events(self):
        engine, ledger = _make_engine()
        await engine.run_consolidation("org", "gw")
        events = await ledger.query_trace(MagicMock(
            event_types=None, session_id=None, actor_ids=None,
            from_timestamp=None, to_timestamp=None, offset=0, limit=100,
            session_key=None, gateway_id=None,
        ))
        event_types = {e.event_type for e in events}
        assert TraceEventType.CONSOLIDATION_STARTED in event_types
        assert TraceEventType.CONSOLIDATION_COMPLETED in event_types

    async def test_pipeline_with_facts(self, monkeypatch):
        monkeypatch.setattr("cognee.tasks.storage.add_data_points", AsyncMock())
        try:
            monkeypatch.setattr("cognee.add", AsyncMock())
        except Exception:
            pass
        facts = [
            make_fact_assertion(text=f"fact {i}", confidence=0.8, use_count=3,
                                successful_use_count=2, session_key=f"s{i % 3}")
            for i in range(10)
        ]
        engine, _ = _make_engine(facts=facts)
        report = await engine.run_consolidation("org", "gw", "coding")
        assert report.status in ("completed", "partial")
        assert len(report.stage_results) > 0

    async def test_stage_results_have_timing(self):
        engine, _ = _make_engine()
        report = await engine.run_consolidation("org", "gw")
        for sr in report.stage_results:
            assert sr.duration_ms >= 0
            assert sr.name != ""

    async def test_completed_report_has_summary(self):
        engine, _ = _make_engine()
        report = await engine.run_consolidation("org", "gw")
        s = report.summary
        assert hasattr(s, "duplicates_merged")
        assert hasattr(s, "facts_strengthened")
        assert hasattr(s, "facts_decayed")
        assert hasattr(s, "facts_archived")

    async def test_gateway_isolation(self, monkeypatch):
        """Two runs with different gateway IDs produce independent reports."""
        monkeypatch.setattr("cognee.tasks.storage.add_data_points", AsyncMock())
        try:
            monkeypatch.setattr("cognee.add", AsyncMock())
        except Exception:
            pass
        engine_a, _ = _make_engine()
        engine_b, _ = _make_engine()
        report_a = await engine_a.run_consolidation("org", "gw-a")
        report_b = await engine_b.run_consolidation("org", "gw-b")
        assert report_a.gateway_id == "gw-a"
        assert report_b.gateway_id == "gw-b"
        assert report_a.id != report_b.id

    async def test_consolidation_started_event_has_gateway(self):
        engine, ledger = _make_engine()
        await engine.run_consolidation("org", "gw-test")
        events = await ledger.query_trace(MagicMock(
            event_types=[TraceEventType.CONSOLIDATION_STARTED],
            session_id=None, actor_ids=None,
            from_timestamp=None, to_timestamp=None, offset=0, limit=10,
            session_key=None, gateway_id=None,
        ))
        assert len(events) >= 1
        assert events[0].payload.get("gateway_id") == "gw-test"

    async def test_consolidation_completed_event_has_summary(self):
        engine, ledger = _make_engine()
        await engine.run_consolidation("org", "gw")
        events = await ledger.query_trace(MagicMock(
            event_types=[TraceEventType.CONSOLIDATION_COMPLETED],
            session_id=None, actor_ids=None,
            from_timestamp=None, to_timestamp=None, offset=0, limit=10,
            session_key=None, gateway_id=None,
        ))
        assert len(events) >= 1
        assert "summary" in events[0].payload

    async def test_run_stage_individually(self):
        from elephantbroker.schemas.consolidation import ConsolidationContext
        engine, _ = _make_engine()
        ctx = ConsolidationContext(org_id="org", gateway_id="gw")
        result = await engine.run_stage(3, "org", "gw", ctx)
        assert result.stage == 3
        assert result.name == "strengthen"
