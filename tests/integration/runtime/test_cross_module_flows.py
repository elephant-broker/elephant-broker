"""Integration tests for cross-module workflows."""
from __future__ import annotations

import uuid

import pytest

from cognee.tasks.storage import add_data_points

from elephantbroker.schemas.trace import TraceQuery
from tests.fixtures.factories import make_actor_ref, make_fact_assertion, make_goal_state


@pytest.mark.integration
class TestCrossModuleFlows:
    async def test_actor_to_fact_to_trace(self, actor_registry, memory_facade, trace_ledger):
        """Register actor -> store fact with actor as source -> trace has events."""
        actor = make_actor_ref()
        await actor_registry.register_actor(actor)

        fact = make_fact_assertion(source_actor_id=actor.id, text="Actor created this fact")
        await memory_facade.store(fact)

        events = await trace_ledger.query_trace(TraceQuery())
        assert len(events) >= 2  # register + store

    async def test_goal_to_procedure_to_evidence(
        self, goal_manager, procedure_engine, evidence_engine, graph_adapter, trace_ledger,
    ):
        """Create goal -> store procedure bound to goal -> activate -> record claim."""
        from elephantbroker.runtime.adapters.cognee.datapoints import ProcedureDataPoint
        from tests.fixtures.factories import make_claim_record, make_procedure_definition

        goal = make_goal_state()
        await goal_manager.set_goal(goal)

        proc = make_procedure_definition()
        proc.gateway_id = procedure_engine._gateway_id  # Match engine's gateway_id for filtered lookup
        dp = ProcedureDataPoint.from_schema(proc)
        await add_data_points([dp])

        execution = await procedure_engine.activate(proc.id, uuid.uuid4())
        assert execution.procedure_id == proc.id

        claim = make_claim_record(goal_id=goal.id)
        await evidence_engine.record_claim(claim)

        events = await trace_ledger.query_trace(TraceQuery())
        assert len(events) >= 3

    async def test_profile_drives_scoring_weights(self, profile_registry):
        """Resolve coding profile -> verify weights match spec values."""
        policy = await profile_registry.resolve_profile("coding")
        assert policy.scoring_weights.turn_relevance == 1.5
        assert policy.scoring_weights.recency == 1.2
        assert policy.scoring_weights.contradiction_penalty == -1.0
