"""Tests for CachedEmbeddingService."""
import json
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
