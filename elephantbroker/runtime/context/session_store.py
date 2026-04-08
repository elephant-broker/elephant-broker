"""SessionContextStore — Redis-backed session context persistence."""
from __future__ import annotations

import logging

from elephantbroker.runtime.observability import GatewayLoggerAdapter
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.context import SessionCompactState, SessionContext


class SessionContextStore:
    """CRUD for SessionContext and SessionCompactState in Redis."""

    def __init__(self, redis, config: ElephantBrokerConfig,
                 redis_keys: RedisKeyBuilder, gateway_id: str = "") -> None:
        self._redis = redis
        self._config = config
        self._keys = redis_keys
        self._log = GatewayLoggerAdapter(
            logging.getLogger("elephantbroker.runtime.context.session_store"),
            {"gateway_id": gateway_id},
        )

    def _effective_ttl(self, profile) -> int:
        """Ensure TTL is at least consolidation_min_retention_seconds."""
        profile_ttl = getattr(profile, "session_data_ttl_seconds", 86400)
        return max(profile_ttl, self._config.consolidation_min_retention_seconds)

    async def get(self, sk: str, sid: str) -> SessionContext | None:
        """Load SessionContext from Redis. Returns None if missing."""
        try:
            raw = await self._redis.get(self._keys.session_context(sk, sid))
            if raw is None:
                return None
            self._log.debug("Loaded SessionContext for %s/%s", sk, sid)
            return SessionContext.model_validate_json(raw)
        except Exception as exc:
            self._log.warning("Failed to load SessionContext: %s", exc)
            return None

    async def save(self, ctx: SessionContext) -> None:
        """Persist SessionContext to Redis with effective TTL."""
        ttl = self._effective_ttl(ctx.profile)
        await self._redis.setex(
            self._keys.session_context(ctx.session_key, ctx.session_id),
            ttl,
            ctx.model_dump_json(),
        )
        self._log.debug("Saved SessionContext for %s/%s (ttl=%d)", ctx.session_key, ctx.session_id, ttl)

    async def delete(self, sk: str, sid: str) -> None:
        """Delete SessionContext from Redis."""
        await self._redis.delete(self._keys.session_context(sk, sid))
        self._log.debug("Deleted SessionContext for %s/%s", sk, sid)

    async def get_compact_state(self, sk: str, sid: str) -> SessionCompactState | None:
        """Load SessionCompactState from Redis."""
        try:
            raw = await self._redis.get(self._keys.compact_state_obj(sk, sid))
            if raw is None:
                return None
            return SessionCompactState.model_validate_json(raw)
        except Exception:
            return None

    async def save_compact_state(self, state: SessionCompactState) -> None:
        """Persist SessionCompactState to Redis."""
        # Use a default TTL since we don't have the profile here
        ttl = self._config.consolidation_min_retention_seconds
        await self._redis.setex(
            self._keys.compact_state_obj(state.session_key, state.session_id),
            ttl,
            state.model_dump_json(),
        )

    async def add_compact_ids(self, sk: str, sid: str, ids: list[str]) -> None:
        """Add compacted item IDs to the compact_state SET (Phase 5 contract)."""
        if not ids:
            return
        key = self._keys.compact_state(sk, sid)
        await self._redis.sadd(key, *ids)
        await self._redis.expire(key, self._config.consolidation_min_retention_seconds)

    async def get_compact_ids(self, sk: str, sid: str) -> set[str]:
        """Get compacted item IDs from the compact_state SET."""
        try:
            return await self._redis.smembers(self._keys.compact_state(sk, sid))
        except Exception:
            return set()

    async def get_context_window(self, sk: str, sid: str) -> dict | None:
        """Get stored context window report data."""
        try:
            import json
            raw = await self._redis.get(self._keys.session_context(sk, sid))
            if raw is None:
                return None
            data = json.loads(raw)
            return {"context_window_tokens": data.get("context_window_tokens"),
                    "provider": data.get("provider"), "model": data.get("model")}
        except Exception:
            return None

    async def save_context_window(self, sk: str, sid: str, data: dict) -> None:
        """Save context window report — updates SessionContext fields if loaded."""
        ctx = await self.get(sk, sid)
        if ctx is not None:
            ctx.context_window_tokens = data.get("context_window_tokens")
            ctx.provider = data.get("provider")
            ctx.model = data.get("model")
            await self.save(ctx)
