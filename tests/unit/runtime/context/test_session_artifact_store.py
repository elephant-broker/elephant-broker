"""Tests for SessionArtifactStore."""
from __future__ import annotations

from unittest.mock import AsyncMock

from elephantbroker.runtime.context.session_artifact_store import SessionArtifactStore
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.artifact import SessionArtifact
from elephantbroker.schemas.config import ElephantBrokerConfig
from tests.fixtures.factories import make_profile_policy, make_session_artifact


def _make_store(redis=None, artifact_store=None):
    redis = redis or AsyncMock()
    config = ElephantBrokerConfig()
    keys = RedisKeyBuilder("test")
    return SessionArtifactStore(
        redis=redis, config=config, redis_keys=keys,
        artifact_store=artifact_store,
    ), redis


class TestSessionArtifactStore:
    async def test_store_and_get(self):
        store, redis = _make_store()
        artifact = make_session_artifact()
        aid = str(artifact.artifact_id)
        redis.hget = AsyncMock(return_value=artifact.model_dump_json())

        result = await store.store("sk", "sid", artifact)
        assert result.artifact_id == artifact.artifact_id
        redis.hset.assert_called_once()

        loaded = await store.get("sk", "sid", aid)
        assert loaded is not None
        assert loaded.tool_name == "test-tool"

    async def test_get_missing_returns_none(self):
        store, redis = _make_store()
        redis.hget = AsyncMock(return_value=None)
        result = await store.get("sk", "sid", "missing-id")
        assert result is None

    async def test_get_by_hash(self):
        store, redis = _make_store()
        artifact = make_session_artifact(content_hash="abc123")
        redis.hgetall = AsyncMock(return_value={"x": artifact.model_dump_json()})

        result = await store.get_by_hash("sk", "sid", "abc123")
        assert result is not None
        assert result.content_hash == "abc123"

    async def test_get_by_hash_not_found(self):
        store, redis = _make_store()
        artifact = make_session_artifact(content_hash="other")
        redis.hgetall = AsyncMock(return_value={"x": artifact.model_dump_json()})

        result = await store.get_by_hash("sk", "sid", "abc123")
        assert result is None

    async def test_search_jaccard(self):
        store, redis = _make_store()
        a1 = make_session_artifact(summary="postgresql query results", tool_name="psql")
        a2 = make_session_artifact(summary="file listing output", tool_name="ls")
        redis.hgetall = AsyncMock(return_value={
            "1": a1.model_dump_json(),
            "2": a2.model_dump_json(),
        })

        results = await store.search("sk", "sid", "postgresql query")
        assert len(results) >= 1
        assert results[0].tool_name == "psql"

    async def test_search_with_tool_filter(self):
        store, redis = _make_store()
        a1 = make_session_artifact(summary="data output", tool_name="psql")
        a2 = make_session_artifact(summary="data output", tool_name="ls")
        redis.hgetall = AsyncMock(return_value={
            "1": a1.model_dump_json(),
            "2": a2.model_dump_json(),
        })

        results = await store.search("sk", "sid", "data output", tool_name="psql")
        assert len(results) == 1
        assert results[0].tool_name == "psql"

    async def test_search_orders_results_by_descending_jaccard_score(self):
        """TF-06-006 V4: search() returns artifacts ordered by Jaccard score
        descending. Crafted so all three artifacts match the query, but each
        with a different overlap → strict ordering must be preserved."""
        store, redis = _make_store()
        # Query: "postgres timescale compression"
        # high overlap (3/3 query tokens present, low extra tokens)
        a_high = make_session_artifact(
            summary="postgres timescale compression", tool_name="psql",
        )
        # medium overlap (2/3 query tokens, more extra)
        a_mid = make_session_artifact(
            summary="postgres timescale tuning configuration values", tool_name="psql",
        )
        # low overlap (1/3 query tokens, lots of extra)
        a_low = make_session_artifact(
            summary="postgres notes for the team about indexing strategies",
            tool_name="psql",
        )
        # Insert intentionally out-of-order in the HASH
        redis.hgetall = AsyncMock(return_value={
            "low": a_low.model_dump_json(),
            "high": a_high.model_dump_json(),
            "mid": a_mid.model_dump_json(),
        })

        results = await store.search("sk", "sid", "postgres timescale compression")
        assert len(results) == 3
        assert results[0].summary == a_high.summary
        assert results[1].summary == a_mid.summary
        assert results[2].summary == a_low.summary

    async def test_list_all(self):
        store, redis = _make_store()
        a1 = make_session_artifact()
        a2 = make_session_artifact()
        redis.hgetall = AsyncMock(return_value={
            "1": a1.model_dump_json(),
            "2": a2.model_dump_json(),
        })

        results = await store.list_all("sk", "sid")
        assert len(results) == 2

    async def test_increment_injected(self):
        store, redis = _make_store()
        artifact = make_session_artifact(injected_count=0)
        redis.hget = AsyncMock(return_value=artifact.model_dump_json())

        await store.increment_injected("sk", "sid", str(artifact.artifact_id))
        redis.hset.assert_called()

    async def test_increment_searched(self):
        store, redis = _make_store()
        artifact = make_session_artifact(searched_count=0)
        redis.hget = AsyncMock(return_value=artifact.model_dump_json())

        await store.increment_searched("sk", "sid", str(artifact.artifact_id))
        redis.hset.assert_called()

    async def test_promote_to_persistent(self):
        mock_artifact_store = AsyncMock()
        from elephantbroker.schemas.artifact import ToolArtifact
        mock_artifact_store.store_artifact = AsyncMock(return_value=ToolArtifact(
            tool_name="test", content="data"
        ))
        store, redis = _make_store(artifact_store=mock_artifact_store)
        artifact = make_session_artifact(session_id="")
        redis.hget = AsyncMock(return_value=artifact.model_dump_json())

        result = await store.promote_to_persistent("sk", "sid", str(artifact.artifact_id))
        assert result is not None
        mock_artifact_store.store_artifact.assert_called_once()

    async def test_promote_missing_returns_none(self):
        store, redis = _make_store()
        redis.hget = AsyncMock(return_value=None)
        result = await store.promote_to_persistent("sk", "sid", "missing")
        assert result is None

    async def test_ttl_applied_on_store(self):
        store, redis = _make_store()
        artifact = make_session_artifact()
        await store.store("sk", "sid", artifact)
        redis.expire.assert_called_once()

    # --- Amendment 6.1: profile TTL passthrough ---

    async def test_store_with_profile_uses_profile_ttl(self):
        """BUG-4: profile TTL should be used when passed."""
        redis = AsyncMock()
        config = ElephantBrokerConfig(consolidation_min_retention_seconds=172800)
        keys = RedisKeyBuilder("test")
        store = SessionArtifactStore(redis=redis, config=config, redis_keys=keys)
        artifact = SessionArtifact(tool_name="test", content="x")
        profile = make_profile_policy(session_data_ttl_seconds=604800)
        await store.store("sk", "sid", artifact, profile=profile)
        redis.expire.assert_called_once()
        assert redis.expire.call_args[0][1] == 604800  # max(604800, 172800)

    async def test_store_without_profile_uses_fallback(self):
        """Without profile, falls back to max(86400, consolidation_min_retention)."""
        redis = AsyncMock()
        config = ElephantBrokerConfig(consolidation_min_retention_seconds=172800)
        keys = RedisKeyBuilder("test")
        store = SessionArtifactStore(redis=redis, config=config, redis_keys=keys)
        artifact = SessionArtifact(tool_name="test", content="x")
        await store.store("sk", "sid", artifact)  # no profile
        redis.expire.assert_called_once()
        assert redis.expire.call_args[0][1] == 172800  # max(86400, 172800)
