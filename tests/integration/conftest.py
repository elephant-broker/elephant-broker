"""Shared fixtures for integration tests requiring live infrastructure."""
from __future__ import annotations

import os

import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase

from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
from elephantbroker.schemas.config import ElephantBrokerConfig, LLMConfig

# ---------------------------------------------------------------------------
# Default test infrastructure credentials (match docker-compose.test.yml).
# These allow running `pytest tests/integration/` directly without the shell
# script.  The shell script exports the same values, so setdefault is safe.
# ---------------------------------------------------------------------------
os.environ.setdefault("EB_NEO4J_URI", "bolt://localhost:17687")
os.environ.setdefault("EB_NEO4J_USER", "neo4j")
os.environ.setdefault("EB_NEO4J_PASSWORD", "testpassword")
os.environ.setdefault("EB_QDRANT_URL", "http://localhost:16333")
os.environ.setdefault("EB_REDIS_URL", "redis://localhost:16379")
# Pin embedding model + dims to known-working OpenAI values regardless of
# what the schema default happens to be at any given time. Cognee uses tiktoken
# for tokenization and tiktoken only knows OpenAI model names — passing it a
# Gemini model name (e.g. text-embedding-004) raises KeyError at engine init.
# Tests must be deterministic, so we pin to a tiktoken-mappable name.
os.environ.setdefault("EB_EMBEDDING_MODEL", "openai/text-embedding-3-large")
os.environ.setdefault("EB_EMBEDDING_DIMENSIONS", "1024")


@pytest.fixture(scope="session")
def cognee_config():
    """CogneeConfig wired to the Docker Compose test services."""
    # Ensure Cognee doesn't try LLM connection tests
    os.environ.setdefault("COGNEE_SKIP_CONNECTION_TEST", "true")
    os.environ.setdefault("LLM_API_KEY", "test-unused")
    os.environ.setdefault("ENABLE_BACKEND_ACCESS_CONTROL", "false")
    # Ensure embedding API key is available for Qdrant collection setup
    os.environ.setdefault("EB_EMBEDDING_API_KEY", os.environ.get("EB_LLM_API_KEY", ""))
    return ElephantBrokerConfig.load().cognee


@pytest.fixture(scope="session")
def llm_config():
    """LLMConfig from EB_LLM_* env vars, or None if not set."""
    model = os.environ.get("EB_LLM_MODEL")
    endpoint = os.environ.get("EB_LLM_ENDPOINT")
    api_key = os.environ.get("EB_LLM_API_KEY")
    if not any([model, endpoint, api_key]):
        return None
    return LLMConfig(
        model=model or "openai/gemini/gemini-2.5-pro",
        endpoint=endpoint or "http://localhost:8811/v1",
        api_key=api_key or "",
    )


# ---------- Cognee singleton management ----------

@pytest_asyncio.fixture(autouse=True)
async def reset_cognee_graph_engine():
    """Clear Cognee's cached graph engine singleton before each test.

    Cognee's ``_create_graph_engine()`` uses ``@lru_cache``, which binds the
    Neo4j driver to the event loop of the first caller.  With per-function
    test loops the cached driver goes stale on every second test, producing
    ``RuntimeError: Future attached to a different loop``.

    Clearing the cache forces Cognee to create a fresh driver on the current
    loop.  This is safe for pipeline tests too — it only resets the connection,
    not the stored data in Neo4j.
    """
    try:
        from cognee.infrastructure.databases.graph.get_graph_engine import (
            _create_graph_engine,
        )
        _create_graph_engine.cache_clear()
    except Exception:
        pass
    yield
    try:
        from cognee.infrastructure.databases.graph.get_graph_engine import (
            _create_graph_engine,
        )
        _create_graph_engine.cache_clear()
    except Exception:
        pass


@pytest_asyncio.fixture(autouse=True, scope="session")
async def configure_cognee_once(cognee_config, llm_config):
    """Run configure_cognee() once per session so add_data_points() finds Neo4j/Qdrant."""
    from elephantbroker.runtime.adapters.cognee.config import configure_cognee
    await configure_cognee(cognee_config, llm_config)


# ---------- Neo4j ----------

@pytest_asyncio.fixture(scope="session")
async def _neo4j_cleanup_driver(cognee_config):
    """Session-scoped Neo4j driver shared by cleanup and test fixtures.

    Reusing one driver avoids opening a new TCP+auth handshake per test,
    which was triggering Neo4j's AuthenticationRateLimit when tests ran fast.
    """
    driver = AsyncGraphDatabase.driver(
        cognee_config.neo4j_uri,
        auth=(cognee_config.neo4j_user, cognee_config.neo4j_password),
    )
    yield driver
    try:
        await driver.close()
    except Exception:
        pass


@pytest_asyncio.fixture
async def neo4j_driver(_neo4j_cleanup_driver):
    """Per-test alias — yields the shared session-scoped driver."""
    yield _neo4j_cleanup_driver


@pytest_asyncio.fixture(autouse=True)
async def cleanup_neo4j(request, _neo4j_cleanup_driver):
    """Delete all nodes and relationships after each test.

    Uses the session-scoped driver to avoid connection churn.

    Skipped for pipeline tests — cognee.cognify() depends on data persisted
    by cognee.add() across multiple test functions.
    """
    yield
    if request.node.get_closest_marker("pipeline"):
        return
    try:
        async with _neo4j_cleanup_driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")
    except Exception:
        pass  # Best-effort cleanup; container may be unavailable


# ---------- Qdrant ----------

@pytest_asyncio.fixture
async def qdrant_client(cognee_config):
    from qdrant_client import AsyncQdrantClient
    client = AsyncQdrantClient(url=cognee_config.qdrant_url)
    yield client
    try:
        await client.close()
    except Exception:
        pass


@pytest_asyncio.fixture(autouse=True)
async def cleanup_qdrant(request, cognee_config):
    """Clear test data from Qdrant collections after each test.

    Deletes points (not collections) to avoid 404 errors when the next test's
    add_data_points() tries to upsert before create_vector_index() runs.

    Skipped for pipeline tests — cognee.cognify() stores vectors that
    subsequent search tests depend on.
    """
    yield
    if request.node.get_closest_marker("pipeline"):
        return
    try:
        from qdrant_client import AsyncQdrantClient, models
        client = AsyncQdrantClient(url=cognee_config.qdrant_url)
        try:
            eb_prefixes = ("FactDataPoint_", "ActorDataPoint_", "GoalDataPoint_",
                           "ProcedureDataPoint_", "ClaimDataPoint_", "EvidenceDataPoint_",
                           "ArtifactDataPoint_")
            collections = await client.get_collections()
            for col in collections.collections:
                if col.name.startswith("test_"):
                    # Test-only collections can be deleted entirely
                    await client.delete_collection(col.name)
                elif col.name.startswith(eb_prefixes):
                    # EB collections: clear points but keep collection structure
                    # so add_data_points() upserts don't hit 404
                    count = await client.count(collection_name=col.name)
                    if count.count > 0:
                        # Scroll all point IDs and delete them
                        records, _offset = await client.scroll(
                            collection_name=col.name, limit=10000,
                        )
                        if records:
                            point_ids = [r.id for r in records]
                            await client.delete(
                                collection_name=col.name,
                                points_selector=models.PointIdsList(points=point_ids),
                            )
        finally:
            await client.close()
    except Exception:
        pass


# ---------- Redis ----------

@pytest_asyncio.fixture
async def redis_client(cognee_config):
    import redis.asyncio as aioredis
    infra = ElephantBrokerConfig.load().infra
    client = await aioredis.from_url(infra.redis_url, decode_responses=True)
    yield client
    try:
        await client.aclose()
    except Exception:
        pass


# ---------- Adapters ----------

@pytest_asyncio.fixture
async def graph_adapter(cognee_config):
    adapter = GraphAdapter(cognee_config)
    yield adapter
    try:
        await adapter.close()
    except Exception:
        pass


@pytest_asyncio.fixture
async def vector_adapter(cognee_config):
    adapter = VectorAdapter(cognee_config)
    yield adapter
    try:
        await adapter.close()
    except Exception:
        pass
