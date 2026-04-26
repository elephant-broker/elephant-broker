"""Shared fixtures for API tests."""
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from elephantbroker.api.app import create_app
from elephantbroker.runtime.actors.registry import ActorRegistry
from elephantbroker.runtime.artifacts.store import ToolArtifactStore
from elephantbroker.runtime.compaction.engine import CompactionEngine
from elephantbroker.runtime.consolidation.engine import ConsolidationEngine
from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.runtime.context.assembler import ContextAssembler
from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
from elephantbroker.runtime.goals.manager import GoalManager
from elephantbroker.runtime.guards.engine import RedLineGuardEngine
from elephantbroker.runtime.memory.facade import MemoryStoreFacade
from elephantbroker.runtime.procedures.engine import ProcedureEngine
from elephantbroker.runtime.profiles.registry import ProfileRegistry
from elephantbroker.runtime.rerank.orchestrator import RerankOrchestrator
from elephantbroker.runtime.retrieval.orchestrator import RetrievalOrchestrator
from elephantbroker.runtime.stats.engine import StatsAndTelemetryEngine
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.runtime.working_set.manager import WorkingSetManager
from elephantbroker.runtime.working_set.scoring_tuner import ScoringTuner
from elephantbroker.schemas.tiers import BusinessTier


@pytest.fixture
def mock_graph():
    g = AsyncMock()
    g.get_entity = AsyncMock(return_value=None)
    g.add_relation = AsyncMock()
    g.query_cypher = AsyncMock(return_value=[])
    g.close = AsyncMock()
    return g


@pytest.fixture
def mock_vector():
    v = AsyncMock()
    v.search_similar = AsyncMock(return_value=[])
    v.close = AsyncMock()
    return v


@pytest.fixture
def mock_embeddings():
    e = AsyncMock()
    e.embed_text = AsyncMock(return_value=[0.1] * 1024)
    e.embed_batch = AsyncMock(return_value=[[0.1] * 1024])
    e.close = AsyncMock()
    return e


@pytest.fixture
def container(mock_graph, mock_vector, mock_embeddings):
    c = RuntimeContainer()
    c.tier = BusinessTier.FULL
    c.graph = mock_graph
    c.vector = mock_vector
    c.embeddings = mock_embeddings

    c.trace_ledger = TraceLedger()
    c.profile_registry = ProfileRegistry(c.trace_ledger)
    c.stats = StatsAndTelemetryEngine(c.trace_ledger)
    c.scoring_tuner = ScoringTuner(c.trace_ledger, c.profile_registry)
    c.actor_registry = ActorRegistry(mock_graph, c.trace_ledger, dataset_name="test")
    c.goal_manager = GoalManager(mock_graph, c.trace_ledger, dataset_name="test")
    c.memory_store = MemoryStoreFacade(mock_graph, mock_vector, mock_embeddings, c.trace_ledger, dataset_name="test")
    c.procedure_engine = ProcedureEngine(mock_graph, c.trace_ledger, dataset_name="test")
    c.evidence_engine = EvidenceAndVerificationEngine(mock_graph, c.trace_ledger, dataset_name="test")
    c.artifact_store = ToolArtifactStore(mock_graph, mock_vector, mock_embeddings, c.trace_ledger, dataset_name="test")
    c.retrieval = RetrievalOrchestrator(mock_vector, mock_graph, mock_embeddings, c.trace_ledger, dataset_name="test")
    c.rerank = RerankOrchestrator(c.trace_ledger)
    c.working_set_manager = WorkingSetManager(c.retrieval, c.trace_ledger)
    c.context_assembler = ContextAssembler(c.working_set_manager, c.trace_ledger)
    c.compaction_engine = CompactionEngine(c.trace_ledger)
    c.guard_engine = RedLineGuardEngine(c.trace_ledger)
    c.consolidation = ConsolidationEngine(c.trace_ledger)

    # Gateway identity
    c.redis_keys = RedisKeyBuilder("local")
    c.metrics_ctx = MetricsContext("local")
    # R2-P4 / #1505: public gateway_id attribute used by health endpoints.
    c.gateway_id = "local"

    # Phase 4: mock LLM client
    c.llm_client = AsyncMock()
    c.llm_client.close = AsyncMock()
    c.llm_client.complete = AsyncMock(return_value="")
    c.llm_client.complete_json = AsyncMock(return_value={})

    # Phase 4: pipelines and buffer (None by default in API tests)
    c.turn_ingest = None
    c.artifact_ingest = None
    c.procedure_ingest = None
    c.ingest_buffer = None
    c.session_goal_store = None
    c.redis = None

    # Phase 6: Context lifecycle mocks
    from elephantbroker.schemas.context import (
        AssembleResult, BootstrapResult, CompactResult, IngestBatchResult,
        IngestResult, SubagentSpawnResult, SystemPromptOverlay,
    )
    lifecycle = AsyncMock()
    lifecycle.bootstrap = AsyncMock(return_value=BootstrapResult(bootstrapped=True))
    lifecycle.ingest = AsyncMock(return_value=IngestResult(ingested=True))
    lifecycle.ingest_batch = AsyncMock(return_value=IngestBatchResult(ingested_count=1))
    lifecycle.assemble = AsyncMock(return_value=AssembleResult())
    lifecycle.compact = AsyncMock(return_value=CompactResult(ok=True, compacted=False))
    lifecycle.after_turn = AsyncMock(return_value=None)
    lifecycle.build_overlay = AsyncMock(return_value=SystemPromptOverlay())
    lifecycle.prepare_subagent_spawn = AsyncMock(return_value=SubagentSpawnResult(
        parent_session_key="p", child_session_key="c", parent_mapping_stored=True,
    ))
    lifecycle.on_subagent_ended = AsyncMock(return_value=None)
    lifecycle.dispose = AsyncMock(return_value=None)
    lifecycle.session_end = AsyncMock(return_value={"goals_flushed": 0})
    c.context_lifecycle = lifecycle
    c.session_context_store = AsyncMock()
    c.session_artifact_store = AsyncMock()
    c.session_artifact_store.search = AsyncMock(return_value=[])
    c.session_artifact_store.get = AsyncMock(return_value=None)
    c.compaction_llm_client = AsyncMock()

    # Phase 10: admin routes — authority store + bootstrap mode
    authority = AsyncMock()
    authority.get_rule = AsyncMock(return_value={"min_authority_level": 0})
    authority.get_rules = AsyncMock(return_value={})
    authority.set_rule = AsyncMock()
    c.authority_store = authority
    c._bootstrap_mode = False
    c._bootstrap_checked = False

    return c


@pytest.fixture(autouse=True)
def _clear_health_probe_caches():
    """Clear module-level health probe caches between tests.

    Both LLM and embedding probes cache per-gateway for 60s. Without
    explicit clearing, results leak between tests.
    """
    from elephantbroker.api.routes import health as _health_module
    _health_module._llm_probe_cache.clear()
    _health_module._embedding_probe_cache.clear()
    yield
    _health_module._llm_probe_cache.clear()
    _health_module._embedding_probe_cache.clear()


@pytest.fixture(autouse=True)
def _mock_cognee_apis(monkeypatch):
    """Mock add_data_points and cognee.add across all runtime modules for API tests."""
    async def fake_add_data_points(data_points, context=None, custom_edges=None, embed_triplets=False):
        return list(data_points)

    mock_cognee = MagicMock()
    mock_cognee.add = AsyncMock(return_value=None)
    mock_cognee.search = AsyncMock(return_value=[])

    modules = [
        "elephantbroker.runtime.actors.registry",
        "elephantbroker.runtime.goals.manager",
        "elephantbroker.runtime.memory.facade",
        "elephantbroker.runtime.evidence.engine",
        "elephantbroker.runtime.artifacts.store",
        "elephantbroker.runtime.procedures.engine",
        "elephantbroker.pipelines.procedure_ingest.pipeline",
        "elephantbroker.pipelines.turn_ingest.pipeline",
        "elephantbroker.runtime.working_set.session_goals",
        "elephantbroker.api.routes.sessions",
        "elephantbroker.api.routes.admin",
    ]
    for mod in modules:
        try:
            monkeypatch.setattr(f"{mod}.add_data_points", fake_add_data_points)
        except AttributeError:
            pass
        try:
            monkeypatch.setattr(f"{mod}.cognee", mock_cognee)
        except AttributeError:
            pass


@pytest.fixture
async def client(container):
    app = create_app(container)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
