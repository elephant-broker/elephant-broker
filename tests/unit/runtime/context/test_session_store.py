"""Tests for SessionContextStore."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.context.session_store import SessionContextStore
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.config import ElephantBrokerConfig
from tests.fixtures.factories import make_profile_policy, make_session_context


def _make_store(redis=None):
    redis = redis or AsyncMock()
    config = ElephantBrokerConfig()
    keys = RedisKeyBuilder("test")
    return SessionContextStore(redis=redis, config=config, redis_keys=keys), redis


class TestSessionContextStore:
    async def test_save_and_get(self):
        store, redis = _make_store()
        ctx = make_session_context()
        redis.get = AsyncMock(return_value=ctx.model_dump_json())

        await store.save(ctx)
        redis.setex.assert_called_once()

        loaded = await store.get(ctx.session_key, ctx.session_id)
        assert loaded is not None
        assert loaded.session_key == ctx.session_key

    async def test_get_missing_returns_none(self):
        store, redis = _make_store()
        redis.get = AsyncMock(return_value=None)
        result = await store.get("sk", "sid")
        assert result is None

    async def test_delete(self):
        store, redis = _make_store()
        await store.delete("sk", "sid")
        redis.delete.assert_called_once()

    async def test_ttl_computation_uses_max(self):
        config = ElephantBrokerConfig(consolidation_min_retention_seconds=86400)
        keys = RedisKeyBuilder("test")
        redis = AsyncMock()
        store = SessionContextStore(redis=redis, config=config, redis_keys=keys)

        profile = make_profile_policy(session_data_ttl_seconds=3600)
        assert store._effective_ttl(profile) == 86400  # max(3600, 86400)

        profile2 = make_profile_policy(session_data_ttl_seconds=200000)
        assert store._effective_ttl(profile2) == 200000  # max(200000, 86400)

    async def test_compact_state_roundtrip(self):
        from elephantbroker.schemas.context import SessionCompactState
        store, redis = _make_store()
        state = SessionCompactState(session_key="sk", session_id="sid", goal_summary="test")
        redis.get = AsyncMock(return_value=state.model_dump_json())

        await store.save_compact_state(state)
        redis.setex.assert_called()

        loaded = await store.get_compact_state("sk", "sid")
        assert loaded is not None
        assert loaded.goal_summary == "test"

    async def test_compact_ids_accumulation(self):
        store, redis = _make_store()
        redis.sadd = AsyncMock()
        redis.expire = AsyncMock()
        redis.smembers = AsyncMock(return_value={"id1", "id2"})

        await store.add_compact_ids("sk", "sid", ["id1", "id2"])
        redis.sadd.assert_called_once()

        ids = await store.get_compact_ids("sk", "sid")
        assert ids == {"id1", "id2"}

    async def test_compact_ids_empty_noop(self):
        store, redis = _make_store()
        redis.sadd = AsyncMock()
        await store.add_compact_ids("sk", "sid", [])
        redis.sadd.assert_not_called()

    async def test_save_context_window(self):
        store, redis = _make_store()
        ctx = make_session_context()
        redis.get = AsyncMock(return_value=ctx.model_dump_json())

        await store.save_context_window("sk", ctx.session_id, {
            "context_window_tokens": 200000, "provider": "anthropic", "model": "claude-4"
        })
        # save() is called inside save_context_window
        assert redis.setex.called

    async def test_get_context_window_missing(self):
        store, redis = _make_store()
        redis.get = AsyncMock(return_value=None)
        result = await store.get_context_window("sk", "sid")
        assert result is None
