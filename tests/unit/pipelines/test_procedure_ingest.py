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
        proc = ProcedureDefinition(name="deploy", description="Deploy to prod", is_manual_only=True)
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
        proc = ProcedureDefinition(name="temp", is_manual_only=True)
        proc.name = ""
        with pytest.raises(ValueError, match="Procedure name is required"):
            await pipe.run(proc)

    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_emits_trace_event(self, mock_add_dp):
        graph = _make_graph()
        trace = _make_trace()
        pipe = ProcedureIngestPipeline(graph, trace)
        proc = ProcedureDefinition(name="test-proc", description="Test", is_manual_only=True)
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
        proc = ProcedureDefinition(name="deploy", description="Deploy v2", is_manual_only=True)
        result = await pipe.run(proc)
        assert result.is_new is False
        assert result.previous_version == 2
        assert result.procedure.version == 3
        # SUPERSEDES edge should be created
        graph.add_relation.assert_called()

    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_versions_existing_procedure_with_cognee_injected_props(self, mock_add_dp):
        """C3.1 regression: version detection must survive Cognee-injected
        Neo4j props (`_metadata`, `_labels`, `id`) that the previous broken
        `ProcedureDataPoint(**existing_props)` reconstruction would have
        rejected as Pydantic extras, silently falling back to is_new=True
        and overwriting the existing version.

        The earlier `test_versions_existing_procedure` uses a clean
        hand-written fixture that doesn't exercise the bug path. This
        fixture mirrors real `query_cypher` output: the underscore-prefixed
        keys + the bare `id` (Cognee's base DataPoint UUID, distinct from
        `eb_id`) are exactly what `clean_graph_props()` strips before
        DataPoint reconstruction in other call sites.
        """
        existing = [{"props": {
            # Real ProcedureDataPoint fields stored in Neo4j
            "name": "deploy",
            "dp_version": 4,
            "eb_id": "real-eb-uuid",
            "description": "Production deploy",
            "scope": "session",
            "eb_created_at": 1700000000,
            "eb_updated_at": 1700001000,
            "source_actor_id": None,
            "gateway_id": "gw-test",
            "is_manual_only": True,
            "steps_json": "[]",
            "activation_modes_json": "[]",
            # Cognee-injected fields that the OLD reconstruction path would
            # have rejected — these are the load-bearing regression bits.
            "_metadata": '{"index_fields": ["name", "description"]}',
            "_labels": ["ProcedureDataPoint"],
            "id": "cognee-base-uuid-different-from-eb-id",
        }}]
        graph = _make_graph(existing_records=existing)
        trace = _make_trace()
        pipe = ProcedureIngestPipeline(graph, trace, gateway_id="gw-test")
        proc = ProcedureDefinition(name="deploy", description="Deploy v5", is_manual_only=True)
        result = await pipe.run(proc)
        assert result.is_new is False
        assert result.previous_version == 4
        assert result.procedure.version == 5
        # SUPERSEDES edge must reference the eb_id from existing props,
        # NOT the Cognee-injected `id` — pins the eb_id-vs-id distinction.
        graph.add_relation.assert_called()
        call_args = graph.add_relation.call_args[0]
        assert call_args[1] == "real-eb-uuid"
        assert call_args[2] == "SUPERSEDES"

    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_no_trigger_words_graceful(self, mock_add_dp):
        """ProcedureDefinition has no trigger_words attr; should handle gracefully."""
        graph = _make_graph()
        trace = _make_trace()
        pipe = ProcedureIngestPipeline(graph, trace)
        proc = ProcedureDefinition(name="no-triggers", description="No triggers", is_manual_only=True)
        result = await pipe.run(proc)
        # Should not crash and edges_created should be 0 for new proc
        assert result.edges_created == 0


class TestProcedureIngestPipelineMetrics:
    """Gap #3: inc_pipeline('procedure_ingest', 'success') must be emitted."""

    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_inc_pipeline_success_on_happy_path(self, mock_add_dp):
        """inc_pipeline('procedure_ingest', 'success') called on successful ingest."""
        graph = _make_graph()
        trace = _make_trace()
        metrics = MagicMock()
        pipe = ProcedureIngestPipeline(graph, trace, metrics=metrics)
        proc = ProcedureDefinition(name="deploy", description="Deploy", is_manual_only=True)
        await pipe.run(proc)
        metrics.inc_pipeline.assert_called_once_with("procedure_ingest", "success")

    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_inc_pipeline_error_on_validation_error(self, mock_add_dp):
        """ValueError on empty name triggers error metric (B2.3b outer try/except)."""
        graph = _make_graph()
        trace = _make_trace()
        metrics = MagicMock()
        pipe = ProcedureIngestPipeline(graph, trace, metrics=metrics)
        proc = ProcedureDefinition(name="temp", is_manual_only=True)
        proc.name = ""
        with pytest.raises(ValueError):
            await pipe.run(proc)
        metrics.inc_pipeline.assert_called_once_with("procedure_ingest", "error")

    @patch("elephantbroker.pipelines.procedure_ingest.pipeline.add_data_points", new_callable=AsyncMock)
    async def test_inc_pipeline_error_on_run_exception(self, mock_add_dp):
        """Gap #13: inc_pipeline('procedure_ingest', 'error') fires when run() raises."""
        graph = _make_graph()
        trace = _make_trace()
        trace.append_event = AsyncMock(side_effect=RuntimeError("trace exploded"))
        metrics = MagicMock()
        pipe = ProcedureIngestPipeline(graph, trace, metrics=metrics)
        proc = ProcedureDefinition(name="deploy", description="Deploy", is_manual_only=True)
        with pytest.raises(RuntimeError, match="trace exploded"):
            await pipe.run(proc)
        metrics.inc_pipeline.assert_called_once_with("procedure_ingest", "error")
