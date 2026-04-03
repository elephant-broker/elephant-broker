"""Tests for SUBAGENT_INHERIT isolation."""
from __future__ import annotations

from unittest.mock import AsyncMock

from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.runtime.retrieval.isolation import resolve_effective_session_keys
from elephantbroker.schemas.profile import IsolationScope


class TestResolveEffectiveSessionKeys:
    async def test_non_subagent_inherit_returns_single(self):
        result = await resolve_effective_session_keys(
            "sk", IsolationScope.SESSION_KEY, None, None
        )
        assert result == ["sk"]

    async def test_global_scope_returns_single(self):
        result = await resolve_effective_session_keys(
            "sk", IsolationScope.GLOBAL, None, None
        )
        assert result == ["sk"]

    async def test_single_parent_walkup(self):
        redis = AsyncMock()
        keys = RedisKeyBuilder("test")
        # C -> B (B has no parent)
        redis.get = AsyncMock(side_effect=["B", None])
        result = await resolve_effective_session_keys(
            "C", IsolationScope.SUBAGENT_INHERIT, redis, keys
        )
        assert result == ["C", "B"]

    async def test_transitive_chain(self):
        redis = AsyncMock()
        keys = RedisKeyBuilder("test")
        # C -> B -> A (A has no parent)
        redis.get = AsyncMock(side_effect=["B", "A", None])
        result = await resolve_effective_session_keys(
            "C", IsolationScope.SUBAGENT_INHERIT, redis, keys
        )
        assert result == ["C", "B", "A"]

    async def test_cycle_detection(self):
        redis = AsyncMock()
        keys = RedisKeyBuilder("test")
        # A -> B -> A (cycle)
        redis.get = AsyncMock(side_effect=["B", "A"])
        result = await resolve_effective_session_keys(
            "A", IsolationScope.SUBAGENT_INHERIT, redis, keys
        )
        assert result == ["A", "B"]  # Stops at cycle

    async def test_max_depth_cutoff(self):
        redis = AsyncMock()
        keys = RedisKeyBuilder("test")
        # Chain longer than max_depth=2
        redis.get = AsyncMock(side_effect=["p1", "p2", "p3", "p4"])
        result = await resolve_effective_session_keys(
            "child", IsolationScope.SUBAGENT_INHERIT, redis, keys, max_depth=2
        )
        assert result == ["child", "p1", "p2"]  # Stopped at depth 2

    async def test_expired_parent_graceful_stop(self):
        redis = AsyncMock()
        keys = RedisKeyBuilder("test")
        redis.get = AsyncMock(return_value=None)
        result = await resolve_effective_session_keys(
            "sk", IsolationScope.SUBAGENT_INHERIT, redis, keys
        )
        assert result == ["sk"]

    async def test_redis_error_graceful(self):
        redis = AsyncMock()
        keys = RedisKeyBuilder("test")
        redis.get = AsyncMock(side_effect=Exception("Redis down"))
        result = await resolve_effective_session_keys(
            "sk", IsolationScope.SUBAGENT_INHERIT, redis, keys
        )
        assert result == ["sk"]

    async def test_empty_session_key(self):
        result = await resolve_effective_session_keys(
            "", IsolationScope.SUBAGENT_INHERIT, None, None
        )
        assert result == [""]

    async def test_no_redis_returns_single(self):
        keys = RedisKeyBuilder("test")
        result = await resolve_effective_session_keys(
            "sk", IsolationScope.SUBAGENT_INHERIT, None, keys
        )
        assert result == ["sk"]
