"""Tests for CachedEmbeddingService."""
import hashlib
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.adapters.cognee.cached_embeddings import CachedEmbeddingService
from elephantbroker.schemas.config import EmbeddingCacheConfig


@pytest.fixture
def mock_inner():
    inner = AsyncMock()
    inner.embed_text = AsyncMock(return_value=[0.1, 0.2, 0.3])
    inner.embed_batch = AsyncMock(return_value=[[0.1, 0.2], [0.3, 0.4]])
    inner.get_dimension = MagicMock(return_value=1024)
    inner.close = AsyncMock()
    return inner


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.setex = AsyncMock()
    redis.mget = AsyncMock(return_value=[None, None])
    pipe = AsyncMock()
    pipe.setex = MagicMock()
    pipe.execute = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)
    return redis


class TestCachedEmbeddingService:
    @pytest.mark.asyncio
    async def test_embed_text_cache_miss(self, mock_inner, mock_redis):
        svc = CachedEmbeddingService(mock_inner, mock_redis)
        result = await svc.embed_text("hello")
        assert result == [0.1, 0.2, 0.3]
        mock_inner.embed_text.assert_called_once_with("hello")
        mock_redis.setex.assert_called_once()

    @pytest.mark.asyncio
    async def test_embed_text_cache_hit(self, mock_inner, mock_redis):
        mock_redis.get = AsyncMock(return_value=json.dumps([0.5, 0.6]))
        svc = CachedEmbeddingService(mock_inner, mock_redis)
        result = await svc.embed_text("hello")
        assert result == [0.5, 0.6]
        mock_inner.embed_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_embed_batch_all_cached(self, mock_inner, mock_redis):
        mock_redis.mget = AsyncMock(return_value=[
            json.dumps([0.1, 0.2]),
            json.dumps([0.3, 0.4]),
        ])
        svc = CachedEmbeddingService(mock_inner, mock_redis)
        result = await svc.embed_batch(["a", "b"])
        assert result == [[0.1, 0.2], [0.3, 0.4]]
        mock_inner.embed_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_embed_batch_all_misses(self, mock_inner, mock_redis):
        mock_redis.mget = AsyncMock(return_value=[None, None])
        svc = CachedEmbeddingService(mock_inner, mock_redis)
        result = await svc.embed_batch(["a", "b"])
        assert result == [[0.1, 0.2], [0.3, 0.4]]
        mock_inner.embed_batch.assert_called_once_with(["a", "b"])

    @pytest.mark.asyncio
    async def test_embed_batch_partial_hits(self, mock_inner, mock_redis):
        mock_redis.mget = AsyncMock(return_value=[
            json.dumps([0.9, 0.9]),
            None,
        ])
        mock_inner.embed_batch = AsyncMock(return_value=[[0.3, 0.4]])
        svc = CachedEmbeddingService(mock_inner, mock_redis)
        result = await svc.embed_batch(["a", "b"])
        assert result[0] == [0.9, 0.9]
        assert result[1] == [0.3, 0.4]
        mock_inner.embed_batch.assert_called_once_with(["b"])

    @pytest.mark.asyncio
    async def test_embed_batch_preserves_order(self, mock_inner, mock_redis):
        mock_redis.mget = AsyncMock(return_value=[None, json.dumps([0.5]), None])
        mock_inner.embed_batch = AsyncMock(return_value=[[0.1], [0.3]])
        svc = CachedEmbeddingService(mock_inner, mock_redis)
        result = await svc.embed_batch(["a", "b", "c"])
        assert result == [[0.1], [0.5], [0.3]]

    @pytest.mark.asyncio
    async def test_disabled_cache_passes_through(self, mock_inner, mock_redis):
        config = EmbeddingCacheConfig(enabled=False)
        svc = CachedEmbeddingService(mock_inner, mock_redis, config)
        result = await svc.embed_text("hello")
        assert result == [0.1, 0.2, 0.3]
        mock_redis.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_redis_passes_through(self, mock_inner):
        svc = CachedEmbeddingService(mock_inner, redis=None)
        result = await svc.embed_text("hello")
        assert result == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_cache_ttl_applied(self, mock_inner, mock_redis):
        config = EmbeddingCacheConfig(ttl_seconds=7200)
        svc = CachedEmbeddingService(mock_inner, mock_redis, config)
        await svc.embed_text("hello")
        args = mock_redis.setex.call_args
        assert args[0][1] == 7200

    @pytest.mark.asyncio
    async def test_empty_batch_returns_empty(self, mock_inner, mock_redis):
        svc = CachedEmbeddingService(mock_inner, mock_redis)
        result = await svc.embed_batch([])
        assert result == []

    def test_get_dimension_delegates(self, mock_inner, mock_redis):
        svc = CachedEmbeddingService(mock_inner, mock_redis)
        assert svc.get_dimension() == 1024

    # ------------------------------------------------------------------
    # TF-FN-009 additions
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_redis_failure_silently_swallows(self, mock_inner):
        """G6 (#194): Redis errors (mget, pipeline.execute) are caught and the service
        degrades to pure pass-through. Inner embedding still runs; caller sees no error.

        Pins the resilience contract: Redis is cache-only, its unavailability must NOT
        propagate failures to callers that only need embeddings.
        """
        mock_redis = MagicMock()
        mock_redis.mget = AsyncMock(side_effect=Exception("redis down"))
        mock_redis.setex = AsyncMock(side_effect=Exception("redis down"))
        pipe = AsyncMock()
        pipe.setex = MagicMock()
        pipe.execute = AsyncMock(side_effect=Exception("redis down"))
        mock_redis.pipeline = MagicMock(return_value=pipe)

        svc = CachedEmbeddingService(mock_inner, redis=mock_redis)
        result = await svc.embed_batch(["x"])

        # Inner still called despite redis failure; result returned cleanly.
        mock_inner.embed_batch.assert_awaited_once_with(["x"])
        assert result == [[0.1, 0.2]]

    def test_cache_key_format(self, mock_inner, mock_redis):
        """G7 (#191): _cache_key returns `{prefix}:{sha256(text)[:32]}`.

        Pins the cache key contract. A change in hash (algorithm or truncation length)
        invalidates every existing Redis entry on the next deploy -- operators MUST
        be notified via a migration note.
        """
        svc = CachedEmbeddingService(mock_inner, mock_redis)
        key = svc._cache_key("hello")
        expected_h = hashlib.sha256("hello".encode()).hexdigest()[:32]
        assert key == f"{svc._config.key_prefix}:{expected_h}"

    @pytest.mark.asyncio
    async def test_close_does_not_close_redis(self):
        """G8 (#198): close() only closes the inner EmbeddingService; Redis lifecycle
        is owned by RuntimeContainer, not by this service.

        Pins the ownership contract. If a future change makes CachedEmbeddingService
        close Redis too, container.close() would hit a double-close error and all
        other consumers of the same Redis connection would fail.
        """
        mock_redis = MagicMock()
        mock_redis.close = AsyncMock()
        mock_inner = AsyncMock()
        svc = CachedEmbeddingService(mock_inner, redis=mock_redis)
        await svc.close()
        assert mock_inner.close.await_count == 1
        assert mock_redis.close.await_count == 0

    @pytest.mark.asyncio
    async def test_embed_batch_truncated_inner_response_substitutes_empty_with_warning_log(self, caplog):
        """G9 (#1163): inner.embed_batch truncated-response defensive guard.

        When inner returns fewer embeddings than miss_texts requested, missing positions
        end up as None in `results`, which the final comprehension substitutes with [].

        NEW in this commit: a WARNING log fires before the substitution (CLAUDE.md
        labeled-diagnostic safety net pattern). If observed in production, file a TD
        with the call context and consider raising instead of substituting.
        """
        mock_inner = MagicMock()
        mock_inner.embed_batch = AsyncMock(return_value=[[1.0, 2.0]])  # 1 embedding for 2 requested
        mock_redis = MagicMock()
        mock_redis.mget = AsyncMock(return_value=[None, None])  # 2 misses
        pipe = AsyncMock()
        pipe.setex = MagicMock()
        pipe.execute = AsyncMock()
        mock_redis.pipeline = MagicMock(return_value=pipe)

        svc = CachedEmbeddingService(mock_inner, redis=mock_redis)

        with caplog.at_level(logging.WARNING, logger="elephantbroker.adapters.cached_embeddings"):
            result = await svc.embed_batch(["a", "b"])

        assert result == [[1.0, 2.0], []]
        assert "embed_batch returned 1/2 None entries" in caplog.text

    @pytest.mark.asyncio
    async def test_corrupted_json_treated_as_miss(self):
        """TF-05-010: a corrupted Redis cache entry is treated as a miss,
        not a fatal error.

        Pins ``cached_embeddings.py:85-98``: the per-entry
        ``try: json.loads(val) ... except Exception: pass`` falls through
        to the miss path (appends ``None`` to results, records the index
        in ``miss_indices``, increments ``inc_embedding_cache("miss")``).
        Without this guard, a single corrupted entry from a Redis key
        collision or a partially-written value would propagate
        ``json.JSONDecodeError`` out of the service and break every
        caller of ``embed_batch`` until the bad key expired.

        Mocks ``mget`` to return ``[b"not valid json"]`` for a single-text
        batch; asserts inner ``embed_batch`` was called with the original
        text (i.e. it was treated as a miss), the result equals the
        embedding the inner service returned, and no exception
        propagates.
        """
        mock_inner = AsyncMock()
        mock_inner.embed_batch = AsyncMock(return_value=[[7.7, 8.8]])
        mock_redis = AsyncMock()
        mock_redis.mget = AsyncMock(return_value=[b"not valid json"])
        pipe = AsyncMock()
        pipe.setex = MagicMock()
        pipe.execute = AsyncMock()
        mock_redis.pipeline = MagicMock(return_value=pipe)

        svc = CachedEmbeddingService(mock_inner, redis=mock_redis)
        # No raise: corrupted bytes are silently treated as a miss.
        result = await svc.embed_batch(["hello"])
        assert result == [[7.7, 8.8]]
        # Inner was called with the original text — proving the corrupted
        # entry was funneled into the miss path.
        mock_inner.embed_batch.assert_called_once_with(["hello"])


class TestEmbeddingCacheObservability:
    """Step 0 audit gaps #9 + #10: observe_embedding_cache_{batch_size, latency_seconds}
    were defined in MetricsContext but never called in production code."""

    @pytest.fixture
    def mock_metrics(self):
        m = MagicMock()
        m.observe_embedding_cache_batch = MagicMock()
        m.observe_embedding_cache_latency = MagicMock()
        m.inc_embedding_cache = MagicMock()
        return m

    @pytest.fixture
    def _svc(self, mock_inner, mock_redis, mock_metrics):
        """CachedEmbeddingService with all mocks including MetricsContext."""
        return CachedEmbeddingService(mock_inner, mock_redis, metrics=mock_metrics)

    @pytest.mark.asyncio
    async def test_observe_batch_size_called_with_len_texts(self, _svc, mock_metrics, mock_redis):
        """Gap #9: observe_embedding_cache_batch records len(texts) on entry."""
        mock_redis.mget = AsyncMock(return_value=[None, None, None])
        await _svc.embed_batch(["a", "b", "c"])
        mock_metrics.observe_embedding_cache_batch.assert_called_once_with(3)

    @pytest.mark.asyncio
    async def test_observe_latency_mget(self, _svc, mock_metrics, mock_redis):
        """Gap #10: mget latency observed with operation='mget'."""
        mock_redis.mget = AsyncMock(return_value=[None, None])
        await _svc.embed_batch(["a", "b"])
        latency_calls = mock_metrics.observe_embedding_cache_latency.call_args_list
        ops = [c.args[0] for c in latency_calls]
        assert "mget" in ops

    @pytest.mark.asyncio
    async def test_observe_latency_embed(self, _svc, mock_metrics, mock_redis):
        """Gap #10: embed latency observed with operation='embed' when cache misses exist."""
        mock_redis.mget = AsyncMock(return_value=[None])
        await _svc.embed_batch(["a"])
        latency_calls = mock_metrics.observe_embedding_cache_latency.call_args_list
        ops = [c.args[0] for c in latency_calls]
        assert "embed" in ops

    @pytest.mark.asyncio
    async def test_observe_latency_pipeline(self, _svc, mock_metrics, mock_redis):
        """Gap #10: pipeline write latency observed with operation='pipeline'."""
        mock_redis.mget = AsyncMock(return_value=[None])
        await _svc.embed_batch(["a"])
        latency_calls = mock_metrics.observe_embedding_cache_latency.call_args_list
        ops = [c.args[0] for c in latency_calls]
        assert "pipeline" in ops

    @pytest.mark.asyncio
    async def test_all_three_latency_ops_on_miss(self, _svc, mock_metrics, mock_redis):
        """Gap #10: all 3 operation labels emitted on a cache-miss embed_batch call."""
        mock_redis.mget = AsyncMock(return_value=[None])
        await _svc.embed_batch(["a"])
        latency_calls = mock_metrics.observe_embedding_cache_latency.call_args_list
        ops = {c.args[0] for c in latency_calls}
        assert ops == {"mget", "embed", "pipeline"}

    @pytest.mark.asyncio
    async def test_latency_values_are_positive_floats(self, _svc, mock_metrics, mock_redis):
        """Gap #10: duration arguments are positive floats (not None, not negative)."""
        mock_redis.mget = AsyncMock(return_value=[None])
        await _svc.embed_batch(["a"])
        for call in mock_metrics.observe_embedding_cache_latency.call_args_list:
            duration = call.args[1]
            assert isinstance(duration, float)
            assert duration >= 0.0

    @pytest.mark.asyncio
    async def test_no_embed_latency_on_full_cache_hit(self, mock_inner, mock_redis, mock_metrics):
        """Gap #10: embed + pipeline latency NOT emitted when all texts are cached."""
        import json as _json
        mock_redis.mget = AsyncMock(return_value=[_json.dumps([0.1])])
        svc = CachedEmbeddingService(mock_inner, mock_redis, metrics=mock_metrics)
        await svc.embed_batch(["a"])
        latency_calls = mock_metrics.observe_embedding_cache_latency.call_args_list
        ops = {c.args[0] for c in latency_calls}
        # mget always runs, but embed and pipeline should NOT
        assert "mget" in ops
        assert "embed" not in ops
        assert "pipeline" not in ops

    @pytest.mark.asyncio
    async def test_no_metrics_when_metrics_is_none(self, mock_inner, mock_redis):
        """Guard: when metrics=None, no observe calls are made (no AttributeError)."""
        mock_redis.mget = AsyncMock(return_value=[None])
        svc = CachedEmbeddingService(mock_inner, mock_redis, metrics=None)
        result = await svc.embed_batch(["a"])
        # Should complete successfully without any metrics calls
        assert len(result) == 1
