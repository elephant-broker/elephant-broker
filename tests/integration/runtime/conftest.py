"""Phase 3 integration test fixtures — runtime modules with real adapters."""
from __future__ import annotations

import pytest_asyncio

from elephantbroker.runtime.actors.registry import ActorRegistry
from elephantbroker.runtime.adapters.cognee.embeddings import EmbeddingService
from elephantbroker.runtime.artifacts.store import ToolArtifactStore
from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
from elephantbroker.runtime.goals.manager import GoalManager
from elephantbroker.runtime.memory.facade import MemoryStoreFacade
from elephantbroker.runtime.procedures.engine import ProcedureEngine
from elephantbroker.runtime.profiles.registry import ProfileRegistry
from elephantbroker.runtime.retrieval.orchestrator import RetrievalOrchestrator
from elephantbroker.runtime.trace.ledger import TraceLedger


@pytest_asyncio.fixture
async def trace_ledger():
    return TraceLedger()


@pytest_asyncio.fixture
async def profile_registry(trace_ledger):
    return ProfileRegistry(trace_ledger)


@pytest_asyncio.fixture
async def embedding_service(cognee_config):
    svc = EmbeddingService(cognee_config)
    yield svc
    try:
        await svc.close()
    except Exception:
        pass


@pytest_asyncio.fixture
async def actor_registry(graph_adapter, trace_ledger):
    return ActorRegistry(graph_adapter, trace_ledger, dataset_name="test_integration")


@pytest_asyncio.fixture
async def goal_manager(graph_adapter, trace_ledger):
    return GoalManager(graph_adapter, trace_ledger, dataset_name="test_integration")


@pytest_asyncio.fixture
async def memory_facade(graph_adapter, vector_adapter, embedding_service, trace_ledger):
    return MemoryStoreFacade(graph_adapter, vector_adapter, embedding_service, trace_ledger, dataset_name="test_integration")


@pytest_asyncio.fixture
async def procedure_engine(graph_adapter, trace_ledger):
    return ProcedureEngine(graph_adapter, trace_ledger, dataset_name="test_integration")


@pytest_asyncio.fixture
async def evidence_engine(graph_adapter, trace_ledger):
    return EvidenceAndVerificationEngine(graph_adapter, trace_ledger, dataset_name="test_integration")


@pytest_asyncio.fixture
async def artifact_store(graph_adapter, vector_adapter, embedding_service, trace_ledger):
    return ToolArtifactStore(graph_adapter, vector_adapter, embedding_service, trace_ledger, dataset_name="test_integration")


@pytest_asyncio.fixture
async def retrieval_orchestrator(vector_adapter, graph_adapter, embedding_service, trace_ledger):
    return RetrievalOrchestrator(vector_adapter, graph_adapter, embedding_service, trace_ledger)
