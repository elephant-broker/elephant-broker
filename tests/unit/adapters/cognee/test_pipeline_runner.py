"""Unit tests for PipelineRunner with mocked run_tasks."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

from elephantbroker.runtime.adapters.cognee.pipeline_runner import PipelineRunner


async def _fake_run_tasks_success(**kwargs):
    """Mock generator that yields two results."""
    yield "result_1"
    yield "result_2"


async def _fake_run_tasks_error(**kwargs):
    """Mock generator that raises."""
    raise RuntimeError("pipeline failed")
    yield  # noqa: F841 - unreachable yield makes this an async generator


class TestPipelineRunner:
    async def test_run_collects_outputs(self):
        runner = PipelineRunner()
        with patch(
            "elephantbroker.runtime.adapters.cognee.pipeline_runner.run_tasks",
            return_value=_fake_run_tasks_success(),
        ):
            result = await runner.run("test_pipe", tasks=[], input_data=["hello"])
            assert result.success is True
            assert result.outputs == ["result_1", "result_2"]
            assert result.pipeline_name == "test_pipe"

    async def test_run_captures_errors(self):
        runner = PipelineRunner()
        with patch(
            "elephantbroker.runtime.adapters.cognee.pipeline_runner.run_tasks",
            return_value=_fake_run_tasks_error(),
        ):
            result = await runner.run("fail_pipe", tasks=[])
            assert result.success is False
            assert len(result.errors) == 1
            assert "pipeline failed" in result.errors[0]

    async def test_run_records_timing(self):
        runner = PipelineRunner()
        with patch(
            "elephantbroker.runtime.adapters.cognee.pipeline_runner.run_tasks",
            return_value=_fake_run_tasks_success(),
        ):
            result = await runner.run("timed_pipe", tasks=[])
            assert "timed_pipe" in result.task_durations_ms
            assert result.task_durations_ms["timed_pipe"] >= 0

    async def test_run_with_trace_emits_event(self):
        runner = PipelineRunner()
        mock_ledger = AsyncMock()

        with patch(
            "elephantbroker.runtime.adapters.cognee.pipeline_runner.run_tasks",
            return_value=_fake_run_tasks_success(),
        ):
            result = await runner.run_with_trace("traced_pipe", tasks=[], trace_ledger=mock_ledger)
            assert result.success is True
            mock_ledger.append_event.assert_awaited_once()
            event = mock_ledger.append_event.call_args[0][0]
            assert event.payload["pipeline_name"] == "traced_pipe"

    async def test_run_uses_provided_dataset_id(self):
        runner = PipelineRunner()
        ds_id = uuid.uuid4()
        with patch(
            "elephantbroker.runtime.adapters.cognee.pipeline_runner.run_tasks",
            return_value=_fake_run_tasks_success(),
        ) as mock_rt:
            await runner.run("ds_pipe", tasks=[], dataset_id=ds_id)
            call_kwargs = mock_rt.call_args[1]
            assert call_kwargs["dataset_id"] == ds_id

    async def test_run_with_trace_handles_none_ledger(self):
        """G1 (TF-FN-010): run_with_trace(trace_ledger=None) gracefully degrades to a
        non-traced run without raising.

        Pins the #189 contract -- callers that hold an optional TraceLedger (e.g., modules
        constructed without a ledger in some tier configurations) can still invoke
        run_with_trace() safely; the if-guard skips event emission when the ledger is None.
        """
        runner = PipelineRunner()
        with patch(
            "elephantbroker.runtime.adapters.cognee.pipeline_runner.run_tasks",
            return_value=_fake_run_tasks_success(),
        ):
            result = await runner.run_with_trace(
                "notrace_pipe", tasks=[], trace_ledger=None,
            )
            assert result.success is True
            assert result.outputs == ["result_1", "result_2"]
            assert result.pipeline_name == "notrace_pipe"
