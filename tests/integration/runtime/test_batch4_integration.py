"""Integration tests protecting Batch 4 cross-phase flows.

Requires live Neo4j + Qdrant + Redis (docker-compose.test.yml).
Covers:
1. Gateway isolation: store + search + delete
2. Cross-session retrieval
3. Procedure create → activate → complete → verify completion
4. GDPR delete chain (store → delete → verify removal)
5. Session boundary fact persistence
6. GLOBAL goal visibility
7. FactDataPoint session_key persisted in Neo4j
"""
from __future__ import annotations

import json
import uuid

import pytest

from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus, EvidenceRef
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.procedure import ProofRequirement, ProofType, ProcedureDefinition, ProcedureStep
from tests.fixtures.factories import make_fact_assertion


# ---------------------------------------------------------------------------
# 1. Gateway isolation: store + search + delete
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestGatewayIsolationIntegration:
    """Store facts with one gateway, verify invisible to another."""

    async def test_search_isolation_by_gateway(self, memory_facade, graph_adapter, vector_adapter, embedding_service, trace_ledger):
        """Facts stored by gw-A should not appear in gw-B search."""
        from elephantbroker.runtime.memory.facade import MemoryStoreFacade

        facade_a = MemoryStoreFacade(
            graph_adapter, vector_adapter, embedding_service, trace_ledger,
            dataset_name="test_integration", gateway_id="gw-alpha",
        )
        facade_b = MemoryStoreFacade(
            graph_adapter, vector_adapter, embedding_service, trace_ledger,
            dataset_name="test_integration", gateway_id="gw-beta",
        )

        fact = make_fact_assertion(text="Secret alpha data", gateway_id="gw-alpha")
        await facade_a.store(fact)

        # Search as gw-beta — should find 0
        results_b = await facade_b.search("Secret alpha data", caller_gateway_id="gw-beta")
        alpha_found = [r for r in results_b if "alpha" in r.text.lower()]
        assert len(alpha_found) == 0

        # Search as gw-alpha — should find 1
        results_a = await facade_a.search("Secret alpha data", caller_gateway_id="gw-alpha")
        assert len(results_a) >= 1

    async def test_delete_blocks_wrong_gateway(self, memory_facade, graph_adapter, vector_adapter, embedding_service, trace_ledger):
        """Delete with wrong gateway_id should raise PermissionError."""
        from elephantbroker.runtime.memory.facade import MemoryStoreFacade

        facade = MemoryStoreFacade(
            graph_adapter, vector_adapter, embedding_service, trace_ledger,
            dataset_name="test_integration", gateway_id="gw-owner",
        )
        fact = make_fact_assertion(text="Owned fact", gateway_id="gw-owner")
        await facade.store(fact)

        with pytest.raises(PermissionError):
            await facade.delete(fact.id, caller_gateway_id="gw-attacker")

    async def test_delete_succeeds_correct_gateway(self, memory_facade, graph_adapter, vector_adapter, embedding_service, trace_ledger):
        """Delete with correct gateway_id should succeed."""
        from elephantbroker.runtime.memory.facade import MemoryStoreFacade

        facade = MemoryStoreFacade(
            graph_adapter, vector_adapter, embedding_service, trace_ledger,
            dataset_name="test_integration", gateway_id="gw-owner",
        )
        fact = make_fact_assertion(text="To be deleted", gateway_id="gw-owner")
        await facade.store(fact)

        await facade.delete(fact.id, caller_gateway_id="gw-owner")

        # Verify removal
        with pytest.raises(KeyError):
            await facade.delete(fact.id, caller_gateway_id="gw-owner")


# ---------------------------------------------------------------------------
# 2. Cross-session retrieval
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCrossSessionRetrieval:
    """Verify cross-session fact visibility via the EXPLICIT memory_search path.

    NOTE: This class tests the explicit-search path. The auto-recall companion
    (cross-session via before_agent_start → orchestrator → prependContext) lives
    at tests/integration/runtime/test_memory_facade.py::test_store_fact_then_auto_recall_returns_it
    (added in TD-50 Phase 4 follow-up commit ab5bec1). The two together cover
    both retrieval surfaces.
    """

    async def test_fact_visible_across_sessions(self, memory_facade):
        sid_a = str(uuid.uuid4())
        sid_b = str(uuid.uuid4())

        fact = make_fact_assertion(
            text="Infrastructure uses Terraform",
            session_key="agent:main:main",
            session_id=uuid.UUID(sid_a),
            scope=Scope.SESSION,
        )
        await memory_facade.store(fact)

        # Search without session_key filter (cross-session)
        results = await memory_facade.search("Terraform infrastructure", max_results=5)
        assert len(results) >= 1


# ---------------------------------------------------------------------------
# 3. Procedure create → activate → complete → verify
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestProcedureLifecycleIntegration:
    """Full procedure lifecycle with evidence gate."""

    async def test_procedure_completion_gate(self, procedure_engine, evidence_engine):
        step1_id = uuid.uuid4()
        step2_id = uuid.uuid4()
        proc = ProcedureDefinition(
            name="Deploy checklist",
            steps=[
                ProcedureStep(step_id=step1_id, order=0, instruction="Run tests", required_evidence=[ProofRequirement(proof_type=ProofType.CHUNK_REF, required=True, description="test results")]),
                ProcedureStep(step_id=step2_id, order=1, instruction="Deploy", required_evidence=[ProofRequirement(proof_type=ProofType.CHUNK_REF, required=True, description="deploy output")]),
            ],
            is_manual_only=True,
        )
        await procedure_engine.store_procedure(proc)
        execution = await procedure_engine.activate(proc.id, uuid.uuid4())

        # Before any evidence — incomplete
        result = await evidence_engine.check_completion_requirements(proc.id)
        assert result.complete is False

        # Complete step 1 only
        claim1 = ClaimRecord(claim_text="Tests passed", procedure_id=proc.id, step_id=step1_id)
        claim1 = await evidence_engine.record_claim(claim1)
        ev1 = EvidenceRef(type="chunk_ref", ref_value="All 100 tests pass")
        await evidence_engine.attach_evidence(claim1.id, ev1)
        await evidence_engine.verify(claim1.id)

        result = await evidence_engine.check_completion_requirements(proc.id)
        assert result.complete is False

        # Complete step 2
        claim2 = ClaimRecord(claim_text="Deployed", procedure_id=proc.id, step_id=step2_id)
        claim2 = await evidence_engine.record_claim(claim2)
        ev2 = EvidenceRef(type="chunk_ref", ref_value="Deploy successful")
        await evidence_engine.attach_evidence(claim2.id, ev2)
        await evidence_engine.verify(claim2.id)

        result = await evidence_engine.check_completion_requirements(proc.id)
        assert result.complete is True


# ---------------------------------------------------------------------------
# 4. GDPR delete chain
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestGDPRDeleteChain:
    """Store → delete → verify fact removed from Neo4j."""

    async def test_full_delete_chain(self, memory_facade, graph_adapter):
        fact = make_fact_assertion(text="PII data to delete")
        await memory_facade.store(fact)

        # Verify stored
        entity = await graph_adapter.get_entity(str(fact.id))
        assert entity is not None

        # Delete (pass caller_gateway_id matching the stored fact's gateway)
        await memory_facade.delete(fact.id, caller_gateway_id="local")

        # Verify removed from graph
        entity = await graph_adapter.get_entity(str(fact.id))
        assert entity is None


# ---------------------------------------------------------------------------
# 5. Session boundary fact persistence
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSessionBoundaryPersistence:
    """Facts survive session boundary (different session_ids, same session_key)."""

    async def test_facts_persist_across_session_ids(self, memory_facade):
        sk = "agent:main:main"
        sid1 = uuid.uuid4()
        sid2 = uuid.uuid4()

        fact = make_fact_assertion(
            text="Grafana Loki for log aggregation",
            session_key=sk,
            session_id=sid1,
            scope=Scope.SESSION,
        )
        await memory_facade.store(fact)

        # Search with same session_key, different session_id
        results = await memory_facade.search(
            "Grafana Loki", max_results=5,
            session_key=sk,
        )
        assert len(results) >= 1


# ---------------------------------------------------------------------------
# 6. GLOBAL goal visibility
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestGlobalGoalVisibility:
    """GLOBAL-scoped goals should be visible across sessions."""

    async def test_global_goal_persists(self, goal_manager):
        goal = GoalState(
            title="Reduce API latency to <100ms",
            scope=Scope.GLOBAL,
        )
        await goal_manager.set_goal(goal)

        # Retrieve
        entity = await goal_manager._graph.get_entity(str(goal.id))
        assert entity is not None
        assert entity.get("scope") == "global"


# ---------------------------------------------------------------------------
# 7. FactDataPoint session_key persisted in Neo4j
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestFactDataPointSessionKeyPersisted:
    """Verify session_key is stored as Neo4j property (not skipped)."""

    async def test_session_key_in_neo4j(self, memory_facade, graph_adapter):
        fact = make_fact_assertion(
            text="Session key test",
            session_key="agent:main:test",
        )
        await memory_facade.store(fact)

        entity = await graph_adapter.get_entity(str(fact.id))
        assert entity is not None
        assert entity.get("session_key") == "agent:main:test"

    async def test_empty_session_key_in_neo4j(self, memory_facade, graph_adapter):
        """Even empty session_key should be persisted as '' not absent."""
        fact = make_fact_assertion(text="No session key test")
        fact.session_key = None  # Will be coerced to "" by FactDataPoint.from_schema
        await memory_facade.store(fact)

        entity = await graph_adapter.get_entity(str(fact.id))
        assert entity is not None
        # session_key should be present (empty string), not absent
        assert "session_key" in entity
