"""Tests for RedlineIndexRefreshPipeline (Phase 7 — §7.7)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.pipelines.redline_index_refresh.pipeline import RedlineIndexRefreshPipeline
from elephantbroker.runtime.trace.ledger import TraceLedger


class TestRedlineRefreshPipeline:
    async def test_delegates_to_load_session_rules(self):
        guard = AsyncMock()
        guard.load_session_rules = AsyncMock()
        pipeline = RedlineIndexRefreshPipeline(guard_engine=guard, trace_ledger=TraceLedger())
        sid = uuid.uuid4()
        await pipeline.run(sid, "coding", session_key="sk", agent_id="main")
        guard.load_session_rules.assert_called_once_with(
            session_id=sid, profile_name="coding",
            active_procedure_ids=None, session_key="sk", agent_id="main",
        )

    async def test_emits_trace_event(self):
        guard = AsyncMock()
        ledger = TraceLedger()
        pipeline = RedlineIndexRefreshPipeline(guard_engine=guard, trace_ledger=ledger)
        await pipeline.run(uuid.uuid4(), "coding")
        from elephantbroker.schemas.trace import TraceQuery
        events = await ledger.query_trace(TraceQuery())
        assert len(events) >= 1
        assert events[0].payload["action"] == "redline_refresh"

    async def test_handles_empty_procedures(self):
        guard = AsyncMock()
        pipeline = RedlineIndexRefreshPipeline(guard_engine=guard, trace_ledger=TraceLedger())
        await pipeline.run(uuid.uuid4(), "coding", active_procedure_ids=[])
        guard.load_session_rules.assert_called_once()

    async def test_passes_procedure_ids(self):
        guard = AsyncMock()
        pipeline = RedlineIndexRefreshPipeline(guard_engine=guard, trace_ledger=TraceLedger())
        pids = [uuid.uuid4(), uuid.uuid4()]
        await pipeline.run(uuid.uuid4(), "coding", active_procedure_ids=pids)
        call_kwargs = guard.load_session_rules.call_args.kwargs
        assert call_kwargs["active_procedure_ids"] == pids
