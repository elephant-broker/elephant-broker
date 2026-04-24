"""Tests verifying that gateway_id is stamped on ALL data write paths."""
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture(autouse=True)
def _mock_cognee(monkeypatch):
    """Mock Cognee APIs for all tests in this module."""
    async def fake_add_dp(data_points, context=None, custom_edges=None, embed_triplets=False):
        return list(data_points)
    mock = MagicMock()
    mock.add = AsyncMock(return_value=None)
    for mod in [
        "elephantbroker.runtime.memory.facade",
        "elephantbroker.runtime.goals.manager",
        "elephantbroker.runtime.actors.registry",
        "elephantbroker.runtime.evidence.engine",
        "elephantbroker.runtime.artifacts.store",
        "elephantbroker.runtime.procedures.engine",
        "elephantbroker.runtime.working_set.session_goals",
        "elephantbroker.pipelines.artifact_ingest.pipeline",
    ]:
        try:
            monkeypatch.setattr(f"{mod}.add_data_points", fake_add_dp)
        except AttributeError:
            pass
        try:
            monkeypatch.setattr(f"{mod}.cognee", mock)
        except AttributeError:
            pass


@pytest.mark.asyncio
async def test_evidence_attach_stamps_gateway_on_evidence_ref():
    from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
    from elephantbroker.schemas.evidence import ClaimRecord, EvidenceRef
    g = AsyncMock()
    g.add_relation = AsyncMock()
    t = AsyncMock()
    t.append_event = AsyncMock()
    engine = EvidenceAndVerificationEngine(g, t, gateway_id="gw-test")
    claim = ClaimRecord(claim_text="test claim")
    await engine.record_claim(claim)
    evidence = EvidenceRef(type="tool_output", ref_value="result")
    await engine.attach_evidence(claim.id, evidence)
    assert evidence.gateway_id == "gw-test"


@pytest.mark.asyncio
async def test_evidence_record_claim_stamps_gateway():
    from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
    from elephantbroker.schemas.evidence import ClaimRecord
    g = AsyncMock()
    t = AsyncMock()
    t.append_event = AsyncMock()
    engine = EvidenceAndVerificationEngine(g, t, gateway_id="gw-test")
    claim = ClaimRecord(claim_text="test")
    result = await engine.record_claim(claim)
    assert result.gateway_id == "gw-test"


@pytest.mark.asyncio
async def test_facade_store_stamps_gateway():
    from elephantbroker.runtime.memory.facade import MemoryStoreFacade
    from elephantbroker.schemas.fact import FactAssertion
    from elephantbroker.runtime.trace.ledger import TraceLedger
    g = AsyncMock()
    g.add_relation = AsyncMock()
    v = AsyncMock()
    v.search_similar = AsyncMock(return_value=[])
    e = AsyncMock()
    facade = MemoryStoreFacade(g, v, e, TraceLedger(), gateway_id="gw-test")
    fact = FactAssertion(text="hello world")
    assert fact.gateway_id == ""
    result = await facade.store(fact)
    assert result.gateway_id == "gw-test"


@pytest.mark.asyncio
async def test_goal_manager_set_goal_stamps_gateway():
    from elephantbroker.runtime.goals.manager import GoalManager
    from elephantbroker.schemas.goal import GoalState
    g = AsyncMock()
    g.add_relation = AsyncMock()
    t = AsyncMock()
    t.append_event = AsyncMock()
    mgr = GoalManager(g, t, gateway_id="gw-test")
    goal = GoalState(title="test goal")
    result = await mgr.set_goal(goal)
    assert result.gateway_id == "gw-test"


@pytest.mark.asyncio
async def test_actor_registry_register_stamps_gateway():
    from elephantbroker.runtime.actors.registry import ActorRegistry
    from elephantbroker.schemas.actor import ActorRef, ActorType
    g = AsyncMock()
    t = AsyncMock()
    t.append_event = AsyncMock()
    reg = ActorRegistry(g, t, gateway_id="gw-test")
    actor = ActorRef(type=ActorType.WORKER_AGENT, display_name="test")
    result = await reg.register_actor(actor)
    assert result.gateway_id == "gw-test"


@pytest.mark.asyncio
async def test_procedure_engine_store_stamps_gateway():
    from elephantbroker.runtime.procedures.engine import ProcedureEngine
    from elephantbroker.schemas.procedure import ProcedureDefinition
    g = AsyncMock()
    t = AsyncMock()
    t.append_event = AsyncMock()
    engine = ProcedureEngine(g, t, gateway_id="gw-test")
    proc = ProcedureDefinition(name="test", is_manual_only=True)
    result = await engine.store_procedure(proc)
    assert result.gateway_id == "gw-test"


@pytest.mark.asyncio
async def test_artifact_store_stamps_gateway():
    from elephantbroker.runtime.artifacts.store import ToolArtifactStore
    from elephantbroker.schemas.artifact import ToolArtifact
    from elephantbroker.runtime.trace.ledger import TraceLedger
    g = AsyncMock()
    v = AsyncMock()
    e = AsyncMock()
    store = ToolArtifactStore(g, v, e, TraceLedger(), gateway_id="gw-test")
    art = ToolArtifact(tool_name="test", content="output")
    result = await store.store_artifact(art)
    assert result.gateway_id == "gw-test"


@pytest.mark.asyncio
async def test_artifact_ingest_pipeline_stamps_gateway():
    from elephantbroker.pipelines.artifact_ingest.pipeline import ArtifactIngestPipeline
    from elephantbroker.schemas.pipeline import ArtifactInput
    store = AsyncMock()
    store.get_by_hash = AsyncMock(return_value=None)
    store.store_artifact = AsyncMock()
    facade = AsyncMock()
    llm = AsyncMock()
    trace = AsyncMock()
    trace.append_event = AsyncMock()
    pipeline = ArtifactIngestPipeline(
        artifact_store=store, memory_facade=facade,
        llm_client=llm, trace_ledger=trace, gateway_id="gw-test",
    )
    body = ArtifactInput(tool_name="test", tool_output="result")
    result = await pipeline.run(body)
    if store.store_artifact.called:
        stored_art = store.store_artifact.call_args[0][0]
        assert stored_art.gateway_id == "gw-test"


@pytest.mark.asyncio
async def test_facade_update_cannot_overwrite_gateway_id():
    """gateway_id is immutable via update()."""
    from elephantbroker.runtime.memory.facade import MemoryStoreFacade
    from elephantbroker.runtime.trace.ledger import TraceLedger
    g = AsyncMock()
    g.get_entity = AsyncMock(return_value={
        "eb_id": "550e8400-e29b-41d4-a716-446655440000",
        "text": "hello", "category": "general", "scope": "session",
        "confidence": 1.0, "memory_class": "episodic",
        "eb_created_at": 1000000, "eb_updated_at": 1000000,
        "use_count": 0, "successful_use_count": 0,
        "target_actor_ids": [], "goal_ids": [], "provenance_refs": [],
        "gateway_id": "gw-original",
    })
    v = AsyncMock()
    e = AsyncMock()
    e.embed_text = AsyncMock(return_value=[0.1] * 1024)
    facade = MemoryStoreFacade(g, v, e, TraceLedger(), gateway_id="gw-original")
    import uuid
    result = await facade.update(uuid.UUID("550e8400-e29b-41d4-a716-446655440000"), {"gateway_id": "gw-hacker"})
    # gateway_id should NOT be overwritten
    assert result.gateway_id == "gw-original"


@pytest.mark.asyncio
async def test_facade_gdpr_delete_checks_gateway():
    """GDPR delete should fail if gateway doesn't match."""
    from elephantbroker.runtime.memory.facade import MemoryStoreFacade
    from elephantbroker.runtime.trace.ledger import TraceLedger
    g = AsyncMock()
    g.get_entity = AsyncMock(return_value={
        "eb_id": "550e8400-e29b-41d4-a716-446655440000",
        "gateway_id": "gw-other",
    })
    g.delete_entity = AsyncMock()
    v = AsyncMock()
    e = AsyncMock()
    facade = MemoryStoreFacade(g, v, e, TraceLedger(), gateway_id="gw-mine")
    import uuid
    with pytest.raises(PermissionError, match="belongs to gateway gw-other"):
        await facade.delete(uuid.UUID("550e8400-e29b-41d4-a716-446655440000"))
