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


# 5-101: Atomic scrub of a fact entry from the JSON-serialized recent_facts
# list. The previous read-modify-write pattern (GET → filter in Python → SET)
# lost concurrent writes: two scrubs racing against the same key could each
# SET back a filtered list that had dropped the other's removal. Redis Lua is
# single-threaded — the whole script runs atomically against the server —
# which eliminates the lost-update window without needing WATCH/MULTI retry
# loops. Empty result path DELs the key rather than rewriting `[]`, since
# cjson would re-encode an empty Lua table as `{}` (object, not array).
#
# 5-317: Non-table fall-through (decode failure or non-array JSON) also DELs
# the key. Defense-in-depth: the previous `return 0` silently left corrupt
# state in place, so every subsequent call would re-hit the same non-table
# value and leak extraction-context noise until the TTL elapsed. DEL on the
# bad-shape path forces a clean re-seed on the next update_recent_facts().
_SCRUB_LUA = """
local data = redis.call('GET', KEYS[1])
if not data then return 0 end
local ok, entries = pcall(cjson.decode, data)
if not ok or type(entries) ~= 'table' then
  redis.call('DEL', KEYS[1])
  return 0
end
local filtered = {}
local removed = 0
for _, e in ipairs(entries) do
  if type(e) == 'table' and tostring(e.id) == ARGV[1] then
    removed = removed + 1
  else
    table.insert(filtered, e)
  end
end
if removed == 0 then return 0 end
if #filtered == 0 then
  redis.call('DEL', KEYS[1])
else
  redis.call('SET', KEYS[1], cjson.encode(filtered), 'EX', tonumber(ARGV[2]))
end
return removed
"""


class IngestBuffer(IIngestBuffer):
    """Redis-backed message buffer for batching before fact extraction."""

    def __init__(self, redis, config: LLMConfig, redis_keys=None) -> None:
        self._redis = redis
        self._config = config
        self._last_flush: dict[str, float] = {}
        self._keys = redis_keys if redis_keys is not None else RedisKeyBuilder(gateway_id="")

    @traced
    async def add_messages(
        self,
        session_key: str,
        messages: list[dict],
        *,
        effective_batch_size: int | None = None,
    ) -> bool:
        """Add messages to the buffer. Returns True if batch size reached.

        Note: In FULL mode, the P1 gate on /memory/ingest-messages prevents this
        method from being called — extraction is handled by ContextLifecycle.ingest_batch().
        The overflow guard below is only reachable in MEMORY_ONLY mode (TODO(TD-15)).

        ``effective_batch_size`` (P6): when provided, overrides both the flush
        threshold and the 3x overflow guard for the duration of this call.
        Lets callers that have a resolved ``ProfilePolicy`` apply a per-profile
        override without mutating the gateway-wide singleton's config. When
        ``None``, behavior is byte-identical to the pre-P6 path.
        """
        batch_size = (
            effective_batch_size
            if effective_batch_size is not None
            else self._config.ingest_batch_size
        )
        key = self._keys.ingest_buffer(session_key)
        max_size = batch_size * 3  # 3x = ~3 flushes of headroom
        pipe = self._redis.pipeline()
        for msg in messages:
            pipe.rpush(key, json.dumps(msg))
        pipe.ltrim(key, -max_size, -1)
        pipe.expire(key, self._config.ingest_buffer_ttl_seconds)
        await pipe.execute()
        size = await self._redis.llen(key)
        return size >= batch_size

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

        5-101: Runs as a single atomic Lua eval to eliminate the lost-update
        race present in the prior GET → filter → SET pattern.

        Returns count of entries removed (0 if key missing or id not present).
        """
        key = self._keys.recent_facts(session_key)
        removed = await self._redis.eval(
            _SCRUB_LUA, 1, key,
            str(fact_id), str(self._config.extraction_context_ttl_seconds),
        )
        return int(removed) if removed is not None else 0
