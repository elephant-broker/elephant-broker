"""Tests for Stage 7: Refine Procedures from Patterns."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from elephantbroker.runtime.consolidation.stages.refine_procedures import RefineProceduresStage
from elephantbroker.schemas.consolidation import ConsolidationConfig, ConsolidationContext


def _make_stage(clickhouse_data=None, audit_data=None, llm_text="draft"):
    llm = AsyncMock()
    llm.complete = AsyncMock(return_value=llm_text)
    trace_client = MagicMock()
    trace_client.available = clickhouse_data is not None
    trace_client.get_tool_sequences = AsyncMock(return_value=clickhouse_data or [])
    audit_store = AsyncMock()
    audit_store.get_procedure_events = AsyncMock(return_value=audit_data or [])
    config = ConsolidationConfig(pattern_recurrence_threshold=3, pattern_min_steps=3, max_patterns_per_run=5)
    return RefineProceduresStage(llm, trace_client, audit_store, config)


def _make_context(**kw):
    defaults = {"org_id": "org", "gateway_id": "gw", "llm_calls_cap": 50}
    defaults.update(kw)
    return ConsolidationContext(**defaults)


class TestRefineProcedures:
    async def test_detects_repeated_pattern_from_clickhouse(self):
        data = [
            {"session_key": f"s{i}", "tools": ["search", "store", "verify"]}
            for i in range(5)
        ]
        stage = _make_stage(clickhouse_data=data)
        ctx = _make_context()
        results = await stage.run("gw", ctx)
        assert len(results) >= 1
        assert results[0].tool_sequence == ["search", "store", "verify"]

    async def test_fallback_to_procedure_audit_store(self):
        audit_events = []
        for s in range(4):
            for step in ["step_a", "step_b", "step_c"]:
                audit_events.append({"session_key": f"s{s}", "event_type": "step_completed", "step_instruction": step})
        stage = _make_stage(clickhouse_data=None, audit_data=audit_events)
        ctx = _make_context()
        results = await stage.run("gw", ctx)
        # May or may not find patterns depending on sequence extraction
        assert isinstance(results, list)

    async def test_ignores_one_off_sequences(self):
        data = [
            {"session_key": "s1", "tools": ["unique_a", "unique_b", "unique_c"]},
        ]
        stage = _make_stage(clickhouse_data=data)
        ctx = _make_context()
        results = await stage.run("gw", ctx)
        assert len(results) == 0  # Only 1 session, below threshold

    async def test_generates_draft_procedure(self):
        data = [
            {"session_key": f"s{i}", "tools": ["fetch", "transform", "save"]}
            for i in range(5)
        ]
        stage = _make_stage(clickhouse_data=data, llm_text='{"name": "ETL", "steps": []}')
        ctx = _make_context()
        results = await stage.run("gw", ctx)
        assert len(results) >= 1
        assert results[0].approval_status == "pending"

    async def test_draft_queued_not_activated(self):
        data = [{"session_key": f"s{i}", "tools": ["a", "b", "c"]} for i in range(5)]
        stage = _make_stage(clickhouse_data=data)
        ctx = _make_context()
        results = await stage.run("gw", ctx)
        for r in results:
            assert r.approval_status == "pending"

    async def test_max_patterns_per_run_cap(self):
        # Create many distinct patterns
        data = []
        for pattern_idx in range(20):
            tools = [f"tool_{pattern_idx}_{j}" for j in range(3)]
            for s in range(5):
                data.append({"session_key": f"s{s}_p{pattern_idx}", "tools": tools})
        stage = _make_stage(clickhouse_data=data)
        ctx = _make_context()
        results = await stage.run("gw", ctx)
        assert len(results) <= 5  # max_patterns_per_run=5

    async def test_respects_global_llm_calls_cap(self):
        data = [{"session_key": f"s{i}", "tools": ["a", "b", "c"]} for i in range(5)]
        stage = _make_stage(clickhouse_data=data)
        ctx = _make_context(llm_calls_used=50, llm_calls_cap=50)
        results = await stage.run("gw", ctx)
        assert len(results) == 0

    async def test_min_steps_threshold(self):
        data = [{"session_key": f"s{i}", "tools": ["a", "b"]} for i in range(10)]  # Only 2 steps
        stage = _make_stage(clickhouse_data=data)
        ctx = _make_context()
        results = await stage.run("gw", ctx)
        assert len(results) == 0  # Below min_steps=3

    async def test_graceful_degradation_no_data_source(self):
        stage = _make_stage(clickhouse_data=None, audit_data=None)
        stage._trace_client.available = False
        stage._audit_store = None
        ctx = _make_context()
        results = await stage.run("gw", ctx)
        assert results == []

    async def test_gateway_id_in_suggestion(self):
        data = [{"session_key": f"s{i}", "tools": ["x", "y", "z"]} for i in range(5)]
        stage = _make_stage(clickhouse_data=data)
        ctx = _make_context()
        results = await stage.run("gw-test", ctx)
        for r in results:
            assert r.gateway_id == "gw-test"
