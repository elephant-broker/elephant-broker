"""Procedure ingest pipeline -- stores and versions procedures."""
from __future__ import annotations

import logging

from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import ProcedureDataPoint
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.metrics import inc_pipeline
from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.pipeline import ProcedureIngestResult
from elephantbroker.schemas.procedure import ProcedureDefinition
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.pipelines.procedure_ingest")


class ProcedureIngestPipeline:
    """Stores and versions procedure definitions with graph edges."""

    def __init__(
        self, graph, trace_ledger: ITraceLedger, dataset_name: str = "elephantbroker",
        gateway_id: str = "", metrics=None,
    ):
        self._graph = graph
        self._trace = trace_ledger
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id
        self._metrics = metrics

    @traced
    async def run(self, procedure: ProcedureDefinition) -> ProcedureIngestResult:
        """Run the procedure ingest pipeline."""
        if not procedure.name:
            raise ValueError("Procedure name is required")

        # Check existing
        previous_version = None
        is_new = True
        existing_props = None
        try:
            scope_val = procedure.scope.value if hasattr(procedure.scope, "value") else str(procedure.scope)
            cypher = (
                "MATCH (p:ProcedureDataPoint) WHERE p.name = $name AND p.scope = $scope "
                "AND p.gateway_id = $gateway_id "
                "RETURN properties(p) AS props LIMIT 1"
            )
            records = await self._graph.query_cypher(
                cypher, {"name": procedure.name, "scope": scope_val, "gateway_id": self._gateway_id},
            )
            if records:
                existing_props = records[0]["props"]
                existing = ProcedureDataPoint(**existing_props)
                previous_version = existing.dp_version
                procedure.version = previous_version + 1
                is_new = False
        except Exception:
            pass

        # Stamp gateway and store via Cognee add_data_points
        procedure.gateway_id = self._gateway_id
        dp = ProcedureDataPoint.from_schema(procedure)
        await add_data_points([dp])

        # Edges
        edges_created = 0
        if not is_new and existing_props:
            try:
                await self._graph.add_relation(
                    str(procedure.id), str(existing_props["eb_id"]), "SUPERSEDES",
                )
                edges_created += 1
            except Exception:
                pass

        # Trigger words (ProcedureDefinition may not have trigger_words)
        trigger_words = getattr(procedure, "trigger_words", [])
        for trigger in trigger_words:
            try:
                await self._graph.add_relation(
                    str(procedure.id), trigger, "HAS_TRIGGER",
                )
                edges_created += 1
            except Exception:
                pass

        trace_event = TraceEvent(
            event_type=TraceEventType.INPUT_RECEIVED,
            payload={
                "action": "procedure_ingest",
                "name": procedure.name,
                "version": procedure.version,
            },
        )
        await self._trace.append_event(trace_event)

        if self._metrics:
            self._metrics.inc_pipeline("procedure_ingest", "success")
        else:
            inc_pipeline("procedure_ingest", "success")

        return ProcedureIngestResult(
            procedure=procedure,
            is_new=is_new,
            previous_version=previous_version,
            edges_created=edges_created,
            trace_event_id=trace_event.id,
        )
