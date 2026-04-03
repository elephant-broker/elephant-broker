"""Shared fixtures for guard pipeline integration tests.

All external infrastructure (Redis, Neo4j graph, embedding service) is mocked.
The 'integration' aspect is testing the guard engine with its real sub-components
(StaticRuleRegistry, SemanticGuardIndex, AutonomyClassifier) wired together.
"""
from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock

from elephantbroker.runtime.guards.engine import RedLineGuardEngine
from elephantbroker.runtime.guards.autonomy import AutonomyClassifier, ToolDomainRegistry
from elephantbroker.runtime.guards.approval_queue import ApprovalQueue
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.runtime.profiles.registry import ProfileRegistry
from elephantbroker.schemas.config import GuardConfig, HitlConfig, ElephantBrokerConfig

GATEWAY_ID = "test-gw"

# ---------------------------------------------------------------------------
# Override parent autouse fixtures that require live infrastructure.
# These guard tests use fully mocked infra — no Neo4j, Qdrant, or Redis needed.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def cognee_config():
    """Stub cognee_config so the session-scoped parent fixtures don't fail."""
    os.environ.setdefault("COGNEE_SKIP_CONNECTION_TEST", "true")
    os.environ.setdefault("LLM_API_KEY", "test-unused")
    os.environ.setdefault("ENABLE_BACKEND_ACCESS_CONTROL", "false")
    return ElephantBrokerConfig().cognee


@pytest_asyncio.fixture(autouse=True, scope="session")
async def configure_cognee_once(cognee_config):
    """No-op override: guard tests don't need Cognee configured."""
    yield


@pytest_asyncio.fixture(autouse=True)
async def reset_cognee_graph_engine():
    """No-op override: guard tests don't use Cognee's graph engine."""
    yield


@pytest_asyncio.fixture(autouse=True)
async def cleanup_neo4j(request, cognee_config):
    """No-op override: guard tests don't touch Neo4j."""
    yield


@pytest_asyncio.fixture(autouse=True)
async def cleanup_qdrant(request, cognee_config):
    """No-op override: guard tests don't touch Qdrant."""
    yield


@pytest.fixture
def trace_ledger():
    return TraceLedger(gateway_id=GATEWAY_ID)


@pytest.fixture
def redis_keys():
    return RedisKeyBuilder(GATEWAY_ID)


@pytest.fixture
def metrics():
    return MetricsContext(GATEWAY_ID)


@pytest.fixture
def mock_redis():
    r = AsyncMock()
    r.lpush = AsyncMock()
    r.ltrim = AsyncMock()
    r.expire = AsyncMock()
    r.lrange = AsyncMock(return_value=[])
    r.get = AsyncMock(return_value=None)
    r.set = AsyncMock()
    r.setex = AsyncMock()
    r.delete = AsyncMock()
    r.smembers = AsyncMock(return_value=set())
    r.sadd = AsyncMock()
    r.ttl = AsyncMock(return_value=300)
    r.pipeline = MagicMock()
    return r


@pytest.fixture
def mock_graph():
    g = AsyncMock()
    g.query_cypher = AsyncMock(return_value=[])
    g.get_entity = AsyncMock(return_value=None)
    return g


@pytest.fixture
def mock_embedding():
    e = AsyncMock()
    e.embed_text = AsyncMock(return_value=[0.1] * 1536)
    e.embed_batch = AsyncMock(return_value=[[0.1] * 1536])
    return e


@pytest.fixture
def profile_registry(trace_ledger):
    return ProfileRegistry(trace_ledger)


@pytest.fixture
def approval_queue(mock_redis, redis_keys):
    return ApprovalQueue(redis=mock_redis, redis_keys=redis_keys, config=HitlConfig())


@pytest.fixture
def guard_engine(
    trace_ledger, mock_embedding, mock_graph, mock_redis,
    profile_registry, redis_keys, metrics, approval_queue,
):
    classifier = AutonomyClassifier(
        tool_registry=ToolDomainRegistry(),
        redis=mock_redis,
        redis_keys=redis_keys,
    )
    engine = RedLineGuardEngine(
        trace_ledger=trace_ledger,
        embedding_service=mock_embedding,
        graph=mock_graph,
        llm_client=None,
        profile_registry=profile_registry,
        redis=mock_redis,
        config=GuardConfig(),
        gateway_id=GATEWAY_ID,
        redis_keys=redis_keys,
        metrics=metrics,
        approval_queue=approval_queue,
        autonomy_classifier=classifier,
    )
    return engine
