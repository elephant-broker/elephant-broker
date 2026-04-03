"""AsyncInjectionAnalyzer — optional background embedding-based topic continuation analysis (AD-24).

Off by default. Fires after assemble() via asyncio.create_task(). Detects whether injected
facts were used by the agent via cosine similarity between fact embeddings and assistant
response embeddings. Results stored as atomic Redis INCRBYFLOAT counters for Phase 9
ScoringTuner weight adjustment.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import GatewayLoggerAdapter, traced
from elephantbroker.runtime.working_set.candidates import _cosine_sim

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.cognee.cached_embeddings import CachedEmbeddingService
    from elephantbroker.runtime.metrics import MetricsContext
    from elephantbroker.runtime.redis_keys import RedisKeyBuilder
    from elephantbroker.schemas.config import AsyncAnalysisConfig
    from elephantbroker.schemas.context import AgentMessage
    from elephantbroker.schemas.working_set import WorkingSetSnapshot


class AsyncInjectionAnalyzer:
    """Background embedding-based topic continuation analysis.

    Off by default (``AsyncAnalysisConfig.enabled = False``).
    Fires after ``assemble()`` via ``asyncio.create_task()`` — non-blocking.

    How it works:
    1. Batch-embeds injected fact texts + assistant response texts in one API call.
    2. Computes cosine similarity between each fact and each response.
    3. If max similarity > threshold (default 0.6), atomically increments a Redis counter
       (``fact_async_use:{source_id}``) that Phase 9's ScoringTuner reads.
    """

    def __init__(
        self,
        embeddings: CachedEmbeddingService,
        redis,
        redis_keys: RedisKeyBuilder,
        config: AsyncAnalysisConfig,
        gateway_id: str = "local",
        metrics: MetricsContext | None = None,
    ) -> None:
        self._embeddings = embeddings
        self._redis = redis
        self._keys = redis_keys
        self._enabled = config.enabled
        self._threshold = config.topic_continuation_threshold
        self._batch_size = config.batch_size
        self._metrics = metrics
        self._log = GatewayLoggerAdapter(
            logging.getLogger("elephantbroker.runtime.context.async_analyzer"),
            {"gateway_id": gateway_id},
        )

    @traced
    async def analyze(
        self,
        snapshot: WorkingSetSnapshot,
        messages: list[AgentMessage],
        session_key: str,
        session_id: str,
    ) -> None:
        """Non-blocking analysis. Call via ``asyncio.create_task()``."""
        if not self._enabled:
            return

        if not snapshot or not snapshot.items:
            return

        post_injection = [m for m in messages if m.role == "assistant"]
        if not post_injection:
            return

        items = snapshot.items[: self._batch_size]
        item_texts = [item.text for item in items]
        from elephantbroker.schemas.context import content_as_text
        response_texts = [content_as_text(m) for m in post_injection]

        if self._metrics:
            self._metrics.inc_async_analysis_call()
            self._metrics.observe_async_analysis_items(len(items))

        try:
            all_embs = await self._embeddings.embed_batch(item_texts + response_texts)
        except Exception:
            self._log.debug("embed_batch failed", exc_info=True)
            return

        item_embs = all_embs[: len(item_texts)]
        response_embs = all_embs[len(item_texts) :]

        matches = 0
        for i, item in enumerate(items):
            try:
                max_sim = max(
                    (_cosine_sim(item_embs[i], resp_emb) for resp_emb in response_embs),
                    default=0.0,
                )
                if self._metrics:
                    self._metrics.observe_async_analysis_similarity(max_sim)

                if max_sim > self._threshold:
                    key = self._keys.fact_async_use(str(item.source_id))
                    await self._redis.incrbyfloat(key, max_sim)
                    await self._redis.expire(key, 86400)
                    matches += 1
                    if self._metrics:
                        self._metrics.inc_async_analysis_match()
            except Exception:
                self._log.debug("failed processing item %s", item.id, exc_info=True)
                continue

        self._log.debug(
            "Async analysis complete: %d items, %d matches (threshold=%.2f)",
            len(items), matches, self._threshold,
        )
