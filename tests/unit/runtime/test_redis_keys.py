"""Tests for runtime/redis_keys.py — gateway-scoped Redis key builder."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.redis_keys import RedisKeyBuilder, touch_session_keys


def test_ingest_buffer_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    assert keys.ingest_buffer("agent:main:main") == "eb:gw-prod:ingest_buffer:agent:main:main"


def test_recent_facts_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    assert keys.recent_facts("agent:main:main") == "eb:gw-prod:recent_facts:agent:main:main"


def test_session_goals_key_includes_gateway_and_session():
    keys = RedisKeyBuilder("gw-prod")
    result = keys.session_goals("agent:main:main")
    assert result == "eb:gw-prod:session_goals:agent:main:main"


def test_ws_snapshot_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    result = keys.ws_snapshot("agent:main:main", "sid1")
    assert result == "eb:gw-prod:ws_snapshot:agent:main:main:sid1"


def test_compact_state_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    result = keys.compact_state("sk", "sid")
    assert result == "eb:gw-prod:compact_state:sk:sid"


def test_session_parent_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    assert keys.session_parent("sk") == "eb:gw-prod:session_parent:sk"


def test_embedding_cache_not_gateway_scoped():
    result = RedisKeyBuilder.embedding_cache("abc123")
    assert result == "eb:emb_cache:abc123"
    # Same regardless of gateway
    keys_a = RedisKeyBuilder("gw-a")
    keys_b = RedisKeyBuilder("gw-b")
    assert keys_a.embedding_cache("abc") == keys_b.embedding_cache("abc")


def test_different_gateways_different_keys():
    keys_a = RedisKeyBuilder("gw-a")
    keys_b = RedisKeyBuilder("gw-b")
    assert keys_a.session_goals("sk") != keys_b.session_goals("sk")


def test_same_gateway_same_keys():
    keys1 = RedisKeyBuilder("gw-prod")
    keys2 = RedisKeyBuilder("gw-prod")
    assert keys1.ingest_buffer("sk") == keys2.ingest_buffer("sk")


def test_prefix_property():
    keys = RedisKeyBuilder("gw-test")
    assert keys.prefix == "eb:gw-test"


# ---------------------------------------------------------------------------
# touch_session_keys tests (Amendment 6.1)
# ---------------------------------------------------------------------------


def _make_pipeline_mock(results=None):
    pipe = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock(return_value=results or [1] * 10)
    return pipe


@pytest.mark.asyncio
async def test_touch_session_keys_expires_all_base_keys():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock()
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)

    await touch_session_keys(keys, redis, "sk", "sid", 172800)

    assert pipe.expire.call_count == 10
    expected = [
        "eb:gw-test:session_context:sk:sid",
        "eb:gw-test:session_messages:sk:sid",
        "eb:gw-test:session_goals:sk",
        "eb:gw-test:session_artifacts:sk:sid",
        "eb:gw-test:ws_snapshot:sk:sid",
        "eb:gw-test:compact_state:sk:sid",
        "eb:gw-test:compact_state_obj:sk:sid",
        "eb:gw-test:procedure_exec:sk:sid",
        "eb:gw-test:guard_history:sk:sid",
        "eb:gw-test:fact_domains:sk:sid",
    ]
    actual = [call.args[0] for call in pipe.expire.call_args_list]
    assert actual == expected
    for call in pipe.expire.call_args_list:
        assert call.args[1] == 172800
    pipe.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_touch_returns_count_of_existing_keys():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock(results=[1, 1, 0, 0, 1, 0, 0, 0, 0, 0])
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)
    count = await touch_session_keys(keys, redis, "sk", "sid", 172800)
    assert count == 3


@pytest.mark.asyncio
async def test_touch_include_parent_touches_parent_and_children():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock(results=[1] * 11)
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)
    redis.get = AsyncMock(return_value="parent-sk")
    redis.expire = AsyncMock()

    await touch_session_keys(keys, redis, "sk", "sid", 172800, include_parent=True)

    assert pipe.expire.call_count == 11  # 10 base (8+2 Phase 7) + session_parent
    redis.get.assert_awaited_once()  # looked up parent
    redis.expire.assert_awaited_once_with("eb:gw-test:session_children:parent-sk", 172800)


@pytest.mark.asyncio
async def test_touch_include_parent_no_parent_found():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock(results=[1] * 11)
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)
    redis.get = AsyncMock(return_value=None)
    redis.expire = AsyncMock()

    await touch_session_keys(keys, redis, "sk", "sid", 172800, include_parent=True)
    redis.expire.assert_not_awaited()  # no children to touch


@pytest.mark.asyncio
async def test_touch_no_keys_exist():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock(results=[0] * 10)
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)
    count = await touch_session_keys(keys, redis, "sk", "sid", 172800)
    assert count == 0
