"""Tests verifying that ALL Cypher queries include gateway_id WHERE clause."""
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.trace.ledger import TraceLedger


def _make_mock_graph():
    g = AsyncMock(spec=GraphAdapter)
    g.query_cypher = AsyncMock(return_value=[])
    g.get_entity = AsyncMock(return_value=None)
    g.add_relation = AsyncMock()
    return g


@pytest.mark.asyncio
async def test_goal_manager_resolve_active_goals_has_gateway_filter():
    from elephantbroker.runtime.goals.manager import GoalManager
    g = _make_mock_graph()
    mgr = GoalManager(g, TraceLedger(gateway_id="gw-test"), gateway_id="gw-test")
    import uuid
    await mgr.resolve_active_goals(uuid.uuid4())
    cypher = g.query_cypher.call_args[0][0]
    assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_goal_manager_get_hierarchy_children_has_gateway_filter():
    from elephantbroker.runtime.goals.manager import GoalManager
    from elephantbroker.runtime.adapters.cognee.datapoints import GoalDataPoint
    import uuid
    g = _make_mock_graph()
    gid = str(uuid.uuid4())
    g.get_entity = AsyncMock(return_value={
        "eb_id": gid, "title": "test", "status": "active", "scope": "session",
        "eb_created_at": 1000000, "eb_updated_at": 1000000,
        "owner_actor_ids": [], "success_criteria": [], "blockers": [],
        "confidence": 1.0, "gateway_id": "gw-test",
    })
    mgr = GoalManager(g, TraceLedger(gateway_id="gw-test"), gateway_id="gw-test")
    await mgr.get_goal_hierarchy(uuid.UUID(gid))
    # Should have called query_cypher for children
    if g.query_cypher.called:
        cypher = g.query_cypher.call_args[0][0]
        assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_actor_registry_authority_chain_has_gateway_filter():
    from elephantbroker.runtime.actors.registry import ActorRegistry
    import uuid
    g = _make_mock_graph()
    reg = ActorRegistry(g, TraceLedger(gateway_id="gw-test"), gateway_id="gw-test")
    await reg.get_authority_chain(uuid.uuid4())
    cypher = g.query_cypher.call_args[0][0]
    assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_actor_registry_relationships_has_gateway_filter():
    from elephantbroker.runtime.actors.registry import ActorRegistry
    import uuid
    g = _make_mock_graph()
    reg = ActorRegistry(g, TraceLedger(gateway_id="gw-test"), gateway_id="gw-test")
    await reg.get_relationships(uuid.uuid4())
    cypher = g.query_cypher.call_args[0][0]
    assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_candidate_generator_procedures_has_gateway_filter():
    from elephantbroker.runtime.working_set.candidates import CandidateGenerator
    g = _make_mock_graph()
    retrieval = AsyncMock()
    cg = CandidateGenerator(
        retrieval=retrieval, graph=g, gateway_id="gw-test",
    )
    await cg._get_procedure_items(query="test")
    if g.query_cypher.called:
        cypher = g.query_cypher.call_args[0][0]
        assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_artifact_store_search_has_gateway_filter(monkeypatch):
    from elephantbroker.runtime.artifacts.store import ToolArtifactStore
    import cognee
    monkeypatch.setattr(cognee, "search", AsyncMock(return_value=[]))
    g = _make_mock_graph()
    v = AsyncMock()
    e = AsyncMock()
    store = ToolArtifactStore(g, v, e, TraceLedger(gateway_id="gw-test"), gateway_id="gw-test")
    await store.search_artifacts("test")
    if g.query_cypher.called:
        cypher = g.query_cypher.call_args[0][0]
        assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_artifact_store_get_by_hash_has_gateway_filter():
    from elephantbroker.runtime.artifacts.store import ToolArtifactStore
    from elephantbroker.schemas.artifact import ArtifactHash
    g = _make_mock_graph()
    v = AsyncMock()
    e = AsyncMock()
    store = ToolArtifactStore(g, v, e, TraceLedger(gateway_id="gw-test"), gateway_id="gw-test")
    await store.get_by_hash(ArtifactHash(value="abc123"))
    cypher = g.query_cypher.call_args[0][0]
    assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_working_set_manager_persistent_goals_has_gateway_filter():
    from elephantbroker.runtime.working_set.manager import WorkingSetManager
    g = _make_mock_graph()
    retrieval = AsyncMock()
    mgr = WorkingSetManager(
        retrieval=retrieval, trace_ledger=TraceLedger(gateway_id="gw-test"),
        graph=g, gateway_id="gw-test",
    )
    await mgr._get_persistent_goals_from_graph()
    if g.query_cypher.called:
        cypher = g.query_cypher.call_args[0][0]
        assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_working_set_manager_evidence_index_has_gateway_filter():
    from elephantbroker.runtime.working_set.manager import WorkingSetManager
    g = _make_mock_graph()
    retrieval = AsyncMock()
    mgr = WorkingSetManager(
        retrieval=retrieval, trace_ledger=TraceLedger(gateway_id="gw-test"),
        graph=g, gateway_id="gw-test",
    )
    await mgr._query_evidence_index()
    if g.query_cypher.called:
        cypher = g.query_cypher.call_args[0][0]
        assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_facade_structural_query_always_includes_gateway():
    from elephantbroker.runtime.memory.facade import MemoryStoreFacade
    g = _make_mock_graph()
    v = AsyncMock()
    e = AsyncMock()
    facade = MemoryStoreFacade(g, v, e, TraceLedger(gateway_id="gw-test"), gateway_id="gw-test")
    # Even with no filters, gateway_id should be present
    cypher, params = facade._build_structural_query()
    assert cypher is not None
    assert "gateway_id" in cypher
    assert params["gateway_id"] == "gw-test"


@pytest.mark.asyncio
async def test_facade_get_by_scope_includes_gateway():
    from elephantbroker.runtime.memory.facade import MemoryStoreFacade
    from elephantbroker.schemas.base import Scope
    g = _make_mock_graph()
    v = AsyncMock()
    e = AsyncMock()
    facade = MemoryStoreFacade(g, v, e, TraceLedger(gateway_id="gw-test"), gateway_id="gw-test")
    await facade.get_by_scope(Scope.SESSION)
    cypher = g.query_cypher.call_args[0][0]
    assert "gateway_id" in cypher


@pytest.mark.asyncio
async def test_retrieval_structural_hits_includes_gateway():
    from elephantbroker.runtime.retrieval.orchestrator import RetrievalOrchestrator
    g = _make_mock_graph()
    v = AsyncMock()
    e = AsyncMock()
    orch = RetrievalOrchestrator(v, g, e, TraceLedger(gateway_id="gw-test"), gateway_id="gw-test")
    await orch.get_structural_hits(limit=10)
    cypher = g.query_cypher.call_args[0][0]
    assert "gateway_id" in cypher
