"""Unit tests for AsyncInjectionAnalyzer (AD-24, Amendment 6.2.6)."""
from __future__ import annotations

import math
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.context.async_analyzer import AsyncInjectionAnalyzer
from elephantbroker.runtime.working_set.candidates import _cosine_sim


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(enabled=True, threshold=0.6, batch_size=20):
    cfg = MagicMock()
    cfg.enabled = enabled
    cfg.topic_continuation_threshold = threshold
    cfg.batch_size = batch_size
    return cfg


def _make_analyzer(enabled=True, threshold=0.6, batch_size=20):
    embeddings = AsyncMock()
    redis = AsyncMock()
    keys = MagicMock()
    keys.fact_async_use = MagicMock(side_effect=lambda sid: f"eb:test:fact_async_use:{sid}")
    metrics = MagicMock()
    config = _make_config(enabled=enabled, threshold=threshold, batch_size=batch_size)
    analyzer = AsyncInjectionAnalyzer(
        embeddings=embeddings, redis=redis, redis_keys=keys, config=config,
        gateway_id="test-gw", metrics=metrics,
    )
    return analyzer, embeddings, redis, keys


def _make_snapshot(items):
    snapshot = MagicMock()
    snapshot.items = items
    return snapshot


def _make_item(item_id="item-1", source_id="src-1", text="test fact"):
    item = MagicMock()
    item.id = item_id
    item.source_id = source_id
    item.text = text
    return item


def _make_msg(role="assistant", content="test response"):
    msg = MagicMock()
    msg.role = role
    msg.content = content
    return msg


# ---------------------------------------------------------------------------
# TestAnalyzerGating
# ---------------------------------------------------------------------------

class TestAnalyzerGating:
    """Tests for early-return conditions."""

    @pytest.mark.asyncio
    async def test_disabled_returns_immediately(self):
        analyzer, embeddings, _, _ = _make_analyzer(enabled=False)
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        embeddings.embed_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_assistant_messages_returns(self):
        analyzer, embeddings, _, _ = _make_analyzer()
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg(role="user"), _make_msg(role="tool")]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        embeddings.embed_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_snapshot_returns(self):
        analyzer, embeddings, _, _ = _make_analyzer()
        snapshot = _make_snapshot([])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        embeddings.embed_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_enabled_with_messages_calls_embed(self):
        analyzer, embeddings, _, _ = _make_analyzer()
        embeddings.embed_batch.return_value = [[1.0, 0.0], [0.0, 1.0]]
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        embeddings.embed_batch.assert_called_once()


# ---------------------------------------------------------------------------
# TestSimilarityDetection
# ---------------------------------------------------------------------------

class TestSimilarityDetection:
    """Tests for cosine similarity-based detection."""

    @pytest.mark.asyncio
    async def test_high_similarity_increments_redis(self):
        analyzer, embeddings, redis, keys = _make_analyzer()
        # Identical vectors → cosine = 1.0, well above 0.6
        embeddings.embed_batch.return_value = [[1.0, 0.0], [1.0, 0.0]]
        item = _make_item()
        snapshot = _make_snapshot([item])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        redis.incrbyfloat.assert_called_once()
        redis.expire.assert_called_once()

    @pytest.mark.asyncio
    async def test_low_similarity_no_increment(self):
        analyzer, embeddings, redis, _ = _make_analyzer()
        # Orthogonal vectors → cosine = 0.0
        embeddings.embed_batch.return_value = [[1.0, 0.0], [0.0, 1.0]]
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        redis.incrbyfloat.assert_not_called()

    @pytest.mark.asyncio
    async def test_exactly_at_threshold_no_increment(self):
        """Threshold is strict > (not >=), so exactly 0.6 should NOT increment."""
        analyzer, embeddings, redis, _ = _make_analyzer(threshold=0.6)
        # Craft vectors with cosine = 0.6: cos(θ)=0.6 → θ=53.13°
        # a=[1,0], b=[0.6, 0.8] → dot=0.6, |a|=1, |b|=1 → cos=0.6
        embeddings.embed_batch.return_value = [[1.0, 0.0], [0.6, 0.8]]
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        redis.incrbyfloat.assert_not_called()

    @pytest.mark.asyncio
    async def test_just_above_threshold_increments(self):
        analyzer, embeddings, redis, _ = _make_analyzer(threshold=0.6)
        # a=[1,0], b=[0.61, 0.7922...] → cos≈0.6100 > 0.6
        b1 = 0.61
        b2 = math.sqrt(1 - b1**2)
        embeddings.embed_batch.return_value = [[1.0, 0.0], [b1, b2]]
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        redis.incrbyfloat.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_items_each_scored_independently(self):
        analyzer, embeddings, redis, _ = _make_analyzer()
        # Item 0: identical to response (cos=1.0) → matches
        # Item 1: orthogonal to response (cos=0.0) → no match
        # Item 2: identical to response (cos=1.0) → matches
        embeddings.embed_batch.return_value = [
            [1.0, 0.0],  # item 0
            [0.0, 1.0],  # item 1
            [1.0, 0.0],  # item 2
            [1.0, 0.0],  # response
        ]
        items = [_make_item(f"i{i}", f"s{i}") for i in range(3)]
        snapshot = _make_snapshot(items)
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        assert redis.incrbyfloat.call_count == 2

    @pytest.mark.asyncio
    async def test_max_sim_across_responses(self):
        analyzer, embeddings, redis, _ = _make_analyzer()
        # Item: [1,0], response1: [0,1] (cos=0), response2: [1,0] (cos=1.0) → max=1.0 → match
        embeddings.embed_batch.return_value = [
            [1.0, 0.0],  # item
            [0.0, 1.0],  # response 1
            [1.0, 0.0],  # response 2
        ]
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg(content="r1"), _make_msg(content="r2")]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        redis.incrbyfloat.assert_called_once()

    @pytest.mark.asyncio
    async def test_mixed_roles_filters_assistant_only(self):
        analyzer, embeddings, redis, _ = _make_analyzer()
        # Only assistant messages used; user/tool ignored
        embeddings.embed_batch.return_value = [[1.0, 0.0], [1.0, 0.0]]
        snapshot = _make_snapshot([_make_item()])
        msgs = [
            _make_msg(role="user", content="ignored"),
            _make_msg(role="tool", content="ignored"),
            _make_msg(role="assistant", content="used"),
        ]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        # embed_batch should receive 1 item text + 1 assistant text (not 3)
        call_args = embeddings.embed_batch.call_args[0][0]
        assert len(call_args) == 2  # 1 item + 1 assistant


# ---------------------------------------------------------------------------
# TestBatchingAndLimits
# ---------------------------------------------------------------------------

class TestBatchingAndLimits:
    """Tests for batch_size cap and error handling."""

    @pytest.mark.asyncio
    async def test_batch_size_cap(self):
        analyzer, embeddings, _, _ = _make_analyzer(batch_size=3)
        embeddings.embed_batch.return_value = [[0.0, 1.0]] * 4  # 3 items + 1 response
        items = [_make_item(f"i{i}", f"s{i}") for i in range(10)]
        snapshot = _make_snapshot(items)
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        # Should only embed first 3 items + 1 response = 4 texts
        call_args = embeddings.embed_batch.call_args[0][0]
        assert len(call_args) == 4

    @pytest.mark.asyncio
    async def test_single_item_single_response(self):
        analyzer, embeddings, redis, _ = _make_analyzer()
        embeddings.embed_batch.return_value = [[1.0, 0.0], [1.0, 0.0]]
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        redis.incrbyfloat.assert_called_once()

    @pytest.mark.asyncio
    async def test_large_batch_all_below_threshold(self):
        analyzer, embeddings, redis, _ = _make_analyzer(batch_size=5)
        # All items orthogonal to response
        embeddings.embed_batch.return_value = [[0.0, 1.0]] * 5 + [[1.0, 0.0]]
        items = [_make_item(f"i{i}", f"s{i}") for i in range(5)]
        snapshot = _make_snapshot(items)
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        redis.incrbyfloat.assert_not_called()

    @pytest.mark.asyncio
    async def test_embed_batch_failure_does_not_crash(self):
        analyzer, embeddings, redis, _ = _make_analyzer()
        embeddings.embed_batch.side_effect = RuntimeError("API down")
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg()]
        # Should not raise
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        redis.incrbyfloat.assert_not_called()

    @pytest.mark.asyncio
    async def test_redis_failure_does_not_crash(self):
        analyzer, embeddings, redis, _ = _make_analyzer()
        embeddings.embed_batch.return_value = [[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]]
        redis.incrbyfloat.side_effect = [RuntimeError("Redis down"), None]
        items = [_make_item("i0", "s0"), _make_item("i1", "s1")]
        snapshot = _make_snapshot(items)
        msgs = [_make_msg()]
        # Should not raise — first item fails, second still processes
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        assert redis.incrbyfloat.call_count == 2  # Both attempted


# ---------------------------------------------------------------------------
# TestRedisIntegration
# ---------------------------------------------------------------------------

class TestRedisIntegration:
    """Tests for Redis key format, TTL, and value correctness."""

    @pytest.mark.asyncio
    async def test_redis_expire_set_for_each_match(self):
        analyzer, embeddings, redis, _ = _make_analyzer()
        # 2 items, both match
        embeddings.embed_batch.return_value = [[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]]
        items = [_make_item("i0", "s0"), _make_item("i1", "s1")]
        snapshot = _make_snapshot(items)
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        assert redis.expire.call_count == 2
        for call in redis.expire.call_args_list:
            assert call[0][1] == 86400  # TTL

    @pytest.mark.asyncio
    async def test_fact_async_use_key_format(self):
        analyzer, embeddings, redis, keys = _make_analyzer()
        embeddings.embed_batch.return_value = [[1.0, 0.0], [1.0, 0.0]]
        item = _make_item(source_id="my-source-id")
        snapshot = _make_snapshot([item])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        keys.fact_async_use.assert_called_with("my-source-id")

    @pytest.mark.asyncio
    async def test_incrbyfloat_passes_similarity_value(self):
        analyzer, embeddings, redis, _ = _make_analyzer()
        # a=[1,0], b=[0.9, 0.4359...] → cos≈0.9 (not 1.0)
        b1 = 0.9
        b2 = math.sqrt(1 - b1**2)
        embeddings.embed_batch.return_value = [[1.0, 0.0], [b1, b2]]
        snapshot = _make_snapshot([_make_item()])
        msgs = [_make_msg()]
        await analyzer.analyze(snapshot, msgs, "sk", "sid")
        # The value passed to incrbyfloat should be close to 0.9, not 1.0
        incr_value = redis.incrbyfloat.call_args[0][1]
        assert 0.89 < incr_value < 0.91

    @pytest.mark.asyncio
    async def test_cosine_sim_import_correctness(self):
        """Verify the imported _cosine_sim works correctly."""
        assert _cosine_sim([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
        assert _cosine_sim([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)
        assert _cosine_sim([], []) == 0.0
        assert _cosine_sim([1.0], []) == 0.0
