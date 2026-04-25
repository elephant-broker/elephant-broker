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
