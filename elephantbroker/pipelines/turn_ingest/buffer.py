"""Ingest buffer -- batches messages in Redis before LLM extraction."""
from __future__ import annotations

import json
import logging
import time

from elephantbroker.runtime.interfaces.ingest_buffer import IIngestBuffer
from elephantbroker.runtime.observability import traced
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.config import LLMConfig

logger = logging.getLogger("elephantbroker.pipelines.turn_ingest.buffer")


class IngestBuffer(IIngestBuffer):
    """Redis-backed message buffer for batching before fact extraction."""

    def __init__(self, redis, config: LLMConfig, redis_keys=None) -> None:
        self._redis = redis
        self._config = config
        self._last_flush: dict[str, float] = {}
        self._keys = redis_keys if redis_keys is not None else RedisKeyBuilder(gateway_id="")

    @traced
    async def add_messages(self, session_key: str, messages: list[dict]) -> bool:
        """Add messages to the buffer. Returns True if batch size reached.

        Note: In FULL mode, the P1 gate on /memory/ingest-messages prevents this
        method from being called — extraction is handled by ContextLifecycle.ingest_batch().
        The overflow guard below is only reachable in MEMORY_ONLY mode (TODO(TD-15)).
        """
        key = self._keys.ingest_buffer(session_key)
        max_size = self._config.ingest_batch_size * 3  # 3x = ~3 flushes of headroom
        pipe = self._redis.pipeline()
        for msg in messages:
            pipe.rpush(key, json.dumps(msg))
        pipe.ltrim(key, -max_size, -1)
        pipe.expire(key, self._config.ingest_buffer_ttl_seconds)
        await pipe.execute()
        size = await self._redis.llen(key)
        return size >= self._config.ingest_batch_size

    @traced
    async def flush(self, session_key: str) -> list[dict]:
        """Atomically drain all buffered messages for a session."""
        key = self._keys.ingest_buffer(session_key)
        pipe = self._redis.pipeline()
        pipe.lrange(key, 0, -1)
        pipe.delete(key)
        results = await pipe.execute()
        self._last_flush[session_key] = time.time()
        raw = results[0] if results else []
        return [json.loads(item) for item in raw]

    @traced
    async def force_flush(self, session_key: str) -> list[dict]:
        """Force-flush regardless of batch size or timeout."""
        return await self.flush(session_key)

    async def check_timeout_flush(self, session_key: str) -> bool:
        """Check whether enough time has elapsed since last flush."""
        last = self._last_flush.get(session_key, 0)
        return (time.time() - last) >= self._config.ingest_batch_timeout_seconds

    @traced
    async def load_recent_facts(self, session_key: str) -> list[dict]:
        """Load recently extracted facts for extraction context."""
        key = self._keys.recent_facts(session_key)
        data = await self._redis.get(key)
        if data:
            return json.loads(data)
        return []

    @traced
    async def update_recent_facts(
        self, session_key: str, new_facts: list[dict], max_count: int = 20,
    ) -> None:
        """Update the recent facts window, keeping only the last max_count."""
        key = self._keys.recent_facts(session_key)
        trimmed = new_facts[-max_count:]
        await self._redis.set(
            key, json.dumps(trimmed), ex=self._config.extraction_context_ttl_seconds,
        )

    @traced
    async def scrub_fact_from_recent(self, session_key: str, fact_id: str) -> int:
        """Remove a fact entry from the recent_facts extraction-context window.

        Called on GDPR delete. Without this, the deleted fact's text stays in
        the extraction prompt's "PREVIOUSLY EXTRACTED FACTS" block and the LLM
        may re-extract it as a new FactDataPoint within the TTL window.

        Returns count of entries removed (0 if key missing or id not present).
        """
        key = self._keys.recent_facts(session_key)
        data = await self._redis.get(key)
        if not data:
            return 0
        try:
            entries = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return 0
        if not isinstance(entries, list):
            return 0
        target = str(fact_id)
        filtered = [e for e in entries if not (isinstance(e, dict) and e.get("id") == target)]
        removed = len(entries) - len(filtered)
        if removed == 0:
            return 0
        if filtered:
            await self._redis.set(
                key, json.dumps(filtered), ex=self._config.extraction_context_ttl_seconds,
            )
        else:
            await self._redis.delete(key)
        return removed
