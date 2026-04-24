"""Redis-backed embedding cache wrapping EmbeddingService."""
from __future__ import annotations

import hashlib
import json
import logging

from elephantbroker.runtime.adapters.cognee.embeddings import EmbeddingService
from elephantbroker.schemas.config import EmbeddingCacheConfig

logger = logging.getLogger("elephantbroker.adapters.cached_embeddings")


class CachedEmbeddingService:
    """Wraps EmbeddingService with Redis-backed caching.

    If disabled or no Redis: passes through to inner service.
    Cache key: {prefix}:{sha256(text)[:32]}
    Embeddings stored as JSON-encoded float lists with TTL.
    """

    def __init__(
        self, inner: EmbeddingService, redis=None,
        config: EmbeddingCacheConfig | None = None,
        metrics=None,
    ) -> None:
        self._inner = inner
        self._redis = redis
        self._config = config or EmbeddingCacheConfig()
        self._metrics = metrics

    @property
    def _enabled(self) -> bool:
        return self._config.enabled and self._redis is not None

    def _cache_key(self, text: str) -> str:
        h = hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]
        return f"{self._config.key_prefix}:{h}"

    async def embed_text(self, text: str) -> list[float]:
        if not self._enabled:
            return await self._inner.embed_text(text)
        key = self._cache_key(text)
        try:
            cached = await self._redis.get(key)
            if cached is not None:
                if self._metrics:
                    self._metrics.inc_embedding_cache("hit")
                return json.loads(cached)
        except Exception:
            pass
        if self._metrics:
            self._metrics.inc_embedding_cache("miss")
        embedding = await self._inner.embed_text(text)
        try:
            await self._redis.setex(key, self._config.ttl_seconds, json.dumps(embedding))
        except Exception:
            pass
        return embedding

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if not self._enabled:
            return await self._inner.embed_batch(texts)

        keys = [self._cache_key(t) for t in texts]
        # Batch read from cache
        cached_values: list = []
        try:
            cached_values = await self._redis.mget(*keys)
        except Exception:
            cached_values = [None] * len(texts)

        results: list[list[float] | None] = []
        miss_indices: list[int] = []
        miss_texts: list[str] = []
        for i, val in enumerate(cached_values):
            if val is not None:
                try:
                    results.append(json.loads(val))
                    if self._metrics:
                        self._metrics.inc_embedding_cache("hit")
                    continue
                except Exception:
                    pass
            results.append(None)
            miss_indices.append(i)
            miss_texts.append(texts[i])
            if self._metrics:
                self._metrics.inc_embedding_cache("miss")

        # Embed misses
        if miss_texts:
            new_embeddings = await self._inner.embed_batch(miss_texts)
            # Write misses to cache
            try:
                pipe = self._redis.pipeline()
                for j, idx in enumerate(miss_indices):
                    if j < len(new_embeddings):
                        results[idx] = new_embeddings[j]
                        pipe.setex(keys[idx], self._config.ttl_seconds, json.dumps(new_embeddings[j]))
                await pipe.execute()
            except Exception:
                # Still populate results even if cache write fails
                for j, idx in enumerate(miss_indices):
                    if j < len(new_embeddings) and results[idx] is None:
                        results[idx] = new_embeddings[j]

        # ── Diagnostic safety net ── shouldn't happen if inner.embed_batch honors its
        # contract (one embedding per input). If this fires, file a TD with the call
        # context and consider raising instead of substituting. See GAP #1163.
        none_count = sum(1 for r in results if r is None)
        if none_count > 0:
            logger.warning(
                "embed_batch returned %d/%d None entries — substituting with empty vectors. "
                "This indicates inner.embed_batch returned a truncated response (#1163).",
                none_count, len(results),
            )
        return [r if r is not None else [] for r in results]

    def get_dimension(self) -> int:
        return self._inner.get_dimension()

    async def close(self) -> None:
        await self._inner.close()
