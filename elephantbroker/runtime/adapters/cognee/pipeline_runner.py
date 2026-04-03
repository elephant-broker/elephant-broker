"""Pipeline runner — wraps Cognee's Task/run_tasks with trace support."""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from cognee.modules.pipelines import run_tasks
from cognee.modules.pipelines.tasks.task import Task

from elephantbroker.schemas.trace import TraceEvent, TraceEventType


@dataclass
class PipelineResult:
    """Outcome of a pipeline execution."""
    pipeline_name: str
    success: bool
    outputs: list[Any] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    task_durations_ms: dict[str, float] = field(default_factory=dict)


class PipelineRunner:
    """Composes Cognee Task objects and executes them via run_tasks.

    Wraps individual async task functions in Cognee's ``Task`` wrapper,
    runs the pipeline, and collects outputs/errors/timing.
    """

    async def run(
        self,
        pipeline_name: str,
        tasks: list[Task],
        input_data: list[Any] | None = None,
        dataset_id: uuid.UUID | None = None,
    ) -> PipelineResult:
        """Execute a pipeline and return the result."""
        ds_id = dataset_id or uuid.uuid4()
        outputs: list[Any] = []
        errors: list[str] = []
        task_durations: dict[str, float] = {}

        t0 = time.monotonic()
        try:
            pipeline = run_tasks(
                tasks=tasks,
                dataset_id=ds_id,
                data=input_data,
                pipeline_name=pipeline_name,
            )
            async for result in pipeline:
                outputs.append(result)
            success = True
        except Exception as exc:
            errors.append(str(exc))
            success = False

        elapsed_ms = (time.monotonic() - t0) * 1000
        task_durations[pipeline_name] = elapsed_ms

        return PipelineResult(
            pipeline_name=pipeline_name,
            success=success,
            outputs=outputs,
            errors=errors,
            task_durations_ms=task_durations,
        )

    async def run_with_trace(
        self,
        pipeline_name: str,
        tasks: list[Task],
        input_data: list[Any] | None = None,
        trace_ledger: Any | None = None,
        dataset_id: uuid.UUID | None = None,
    ) -> PipelineResult:
        """Execute a pipeline and emit trace events to the ledger."""
        result = await self.run(pipeline_name, tasks, input_data, dataset_id)

        if trace_ledger is not None:
            event = TraceEvent(
                event_type=TraceEventType.TOOL_INVOKED,
                payload={
                    "pipeline_name": result.pipeline_name,
                    "success": result.success,
                    "output_count": len(result.outputs),
                    "errors": result.errors,
                    "task_durations_ms": result.task_durations_ms,
                },
            )
            await trace_ledger.append_event(event)

        return result
