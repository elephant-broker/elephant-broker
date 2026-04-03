"""Unit tests for ProcedureIngestPipeline."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.pipelines.procedure_ingest.pipeline import ProcedureIngestPipeline
from elephantbroker.schemas.pipeline import ProcedureIngestResult
from elephantbroker.schemas.procedure import ProcedureDefinition


def _make_trace():
    trace = MagicMock()
    trace.append_event = AsyncMock(side_effect=lambda e: e)
    return trace


def _make_graph(existing_records=None):
    graph = MagicMock()
    if existing_records:
        graph.query_cypher = AsyncMock(return_value=existing_records)
    else:
        graph.query_cypher = AsyncMock(return_value=[])
    graph.add_relation = AsyncMock()
    return graph


class TestProcedureIngestPipeline:
    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_stores_new_procedure(self, mock_add_dp):
        graph = _make_graph()
        trace = _make_trace()
        pipe = ProcedureIngestPipeline(graph, trace)
        proc = ProcedureDefinition(name="deploy", description="Deploy to prod")
        result = await pipe.run(proc)
        assert isinstance(result, ProcedureIngestResult)
        assert result.is_new is True
        assert result.previous_version is None
        assert result.procedure.name == "deploy"
        mock_add_dp.assert_called_once()

    async def test_validates_required_fields(self):
        """Procedure without a name should raise ValueError."""
        graph = _make_graph()
        trace = _make_trace()
        pipe = ProcedureIngestPipeline(graph, trace)
        # ProcedureDefinition has min_length=1 on name, so we need to bypass
        # validation by creating one with a name then blanking it
        proc = ProcedureDefinition(name="temp")
        proc.name = ""
        with pytest.raises(ValueError, match="Procedure name is required"):
            await pipe.run(proc)

    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_emits_trace_event(self, mock_add_dp):
        graph = _make_graph()
        trace = _make_trace()
        pipe = ProcedureIngestPipeline(graph, trace)
        proc = ProcedureDefinition(name="test-proc", description="Test")
        result = await pipe.run(proc)
        trace.append_event.assert_called_once()
        event = trace.append_event.call_args[0][0]
        assert event.event_type.value == "input_received"
        assert result.trace_event_id is not None

    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_versions_existing_procedure(self, mock_add_dp):
        """Existing procedure should bump version and create SUPERSEDES edge."""
        existing = [{"props": {"name": "deploy", "dp_version": 2, "eb_id": "old-id",
                                "description": "", "scope": "session",
                                "eb_created_at": 0, "eb_updated_at": 0,
                                "source_actor_id": None, "id": "00000000-0000-0000-0000-000000000001"}}]
        graph = _make_graph(existing_records=existing)
        trace = _make_trace()
        pipe = ProcedureIngestPipeline(graph, trace)
        proc = ProcedureDefinition(name="deploy", description="Deploy v2")
        result = await pipe.run(proc)
        assert result.is_new is False
        assert result.previous_version == 2
        assert result.procedure.version == 3
        # SUPERSEDES edge should be created
        graph.add_relation.assert_called()

    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_no_trigger_words_graceful(self, mock_add_dp):
        """ProcedureDefinition has no trigger_words attr; should handle gracefully."""
        graph = _make_graph()
        trace = _make_trace()
        pipe = ProcedureIngestPipeline(graph, trace)
        proc = ProcedureDefinition(name="no-triggers", description="No triggers")
        result = await pipe.run(proc)
        # Should not crash and edges_created should be 0 for new proc
        assert result.edges_created == 0
