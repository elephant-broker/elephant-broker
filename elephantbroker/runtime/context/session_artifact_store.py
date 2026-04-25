"""SessionArtifactStore — Redis HASH-backed session artifact storage."""
from __future__ import annotations

import json
import logging

from elephantbroker.runtime.identity_utils import assert_same_gateway
from elephantbroker.runtime.observability import GatewayLoggerAdapter
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.artifact import SessionArtifact, ToolArtifact
from elephantbroker.schemas.config import ElephantBrokerConfig


class SessionArtifactStore:
    """Session-scoped tool artifact storage backed by Redis HASH."""

    def __init__(self, redis, config: ElephantBrokerConfig,
                 redis_keys: RedisKeyBuilder, artifact_store=None,
                 trace_ledger=None, gateway_id: str = "") -> None:
        self._redis = redis
        self._config = config
        self._keys = redis_keys
        self._artifact_store = artifact_store
        self._trace = trace_ledger
        self._gateway_id = gateway_id
        self._log = GatewayLoggerAdapter(
            logging.getLogger("elephantbroker.runtime.context.session_artifact_store"),
            {"gateway_id": gateway_id},
        )

    def _effective_ttl(self, profile=None) -> int:
        profile_ttl = getattr(profile, "session_data_ttl_seconds", 86400) if profile else 86400
        return max(profile_ttl, self._config.consolidation_min_retention_seconds)

    async def store(self, sk: str, sid: str, artifact: SessionArtifact,
                    profile=None) -> SessionArtifact:
        """Store a session artifact in Redis HASH."""
        key = self._keys.session_artifacts(sk, sid)
        field = str(artifact.artifact_id)
        await self._redis.hset(key, field, artifact.model_dump_json())
        # Set TTL on first store (idempotent — EXPIRE resets)
        await self._redis.expire(key, self._effective_ttl(profile))
        self._log.debug("Stored artifact %s for %s/%s", field, sk, sid)
        return artifact

    async def get(self, sk: str, sid: str, artifact_id: str) -> SessionArtifact | None:
        """Get a single artifact by ID — O(1)."""
        try:
            raw = await self._redis.hget(self._keys.session_artifacts(sk, sid), artifact_id)
            if raw is None:
                return None
            return SessionArtifact.model_validate_json(raw)
        except Exception:
            return None

    async def get_by_hash(self, sk: str, sid: str, content_hash: str) -> SessionArtifact | None:
        """Find artifact by content hash — scans HASH (small set, typically <100)."""
        try:
            all_raw = await self._redis.hgetall(self._keys.session_artifacts(sk, sid))
            for raw in all_raw.values():
                artifact = SessionArtifact.model_validate_json(raw)
                if artifact.content_hash == content_hash:
                    return artifact
        except Exception:
            pass
        return None

    async def search(self, sk: str, sid: str, query: str,
                     tool_name: str | None = None, max_results: int = 5) -> list[SessionArtifact]:
        """Search artifacts by Jaccard keyword overlap on summary+tool_name."""
        try:
            all_raw = await self._redis.hgetall(self._keys.session_artifacts(sk, sid))
        except Exception:
            return []

        query_tokens = set(query.lower().split())
        scored: list[tuple[float, SessionArtifact]] = []

        for raw in all_raw.values():
            artifact = SessionArtifact.model_validate_json(raw)
            if tool_name and artifact.tool_name != tool_name:
                continue
            artifact_tokens = set(f"{artifact.summary} {artifact.tool_name}".lower().split())
            intersection = query_tokens & artifact_tokens
            union = query_tokens | artifact_tokens
            score = len(intersection) / len(union) if union else 0.0
            if score > 0:
                scored.append((score, artifact))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [a for _, a in scored[:max_results]]

    async def list_all(self, sk: str, sid: str) -> list[SessionArtifact]:
        """Return all artifacts for a session."""
        try:
            all_raw = await self._redis.hgetall(self._keys.session_artifacts(sk, sid))
            return [SessionArtifact.model_validate_json(raw) for raw in all_raw.values()]
        except Exception:
            return []

    async def increment_injected(self, sk: str, sid: str, artifact_id: str) -> None:
        """Increment injected_count for an artifact."""
        artifact = await self.get(sk, sid, artifact_id)
        if artifact is not None:
            artifact.injected_count += 1
            await self._redis.hset(
                self._keys.session_artifacts(sk, sid),
                artifact_id,
                artifact.model_dump_json(),
            )

    async def increment_searched(self, sk: str, sid: str, artifact_id: str) -> None:
        """Increment searched_count for an artifact."""
        artifact = await self.get(sk, sid, artifact_id)
        if artifact is not None:
            artifact.searched_count += 1
            await self._redis.hset(
                self._keys.session_artifacts(sk, sid),
                artifact_id,
                artifact.model_dump_json(),
            )

    async def promote_to_persistent(self, sk: str, sid: str,
                                     artifact_id: str) -> ToolArtifact | None:
        """Promote a session artifact to persistent storage via Cognee."""
        artifact = await self.get(sk, sid, artifact_id)
        if artifact is None or self._artifact_store is None:
            return None

        import uuid as _uuid
        tool_artifact = ToolArtifact(
            artifact_id=artifact.artifact_id,
            tool_name=artifact.tool_name,
            content=artifact.content,
            summary=artifact.summary,
            session_id=_uuid.UUID(artifact.session_id) if artifact.session_id else None,
            created_at=artifact.created_at,
            token_estimate=artifact.token_estimate,
            tags=artifact.tags,
            gateway_id=self._gateway_id,
        )
        result = await self._artifact_store.store_artifact(tool_artifact)

        # Create graph edges (AD-21) via artifact_store's graph adapter
        graph = getattr(self._artifact_store, "_graph", None)
        if graph:
            artifact_node_id = str(result.artifact_id)
            # CREATED_BY: artifact → agent actor (derive from gateway_id)
            from elephantbroker.runtime.identity import deterministic_uuid_from
            if self._gateway_id:
                agent_actor_id = str(deterministic_uuid_from(self._gateway_id))
                try:
                    await graph.add_relation(artifact_node_id, agent_actor_id, "CREATED_BY")
                except Exception:
                    pass
            # SERVES_GOAL: artifact → goal (if goal_id set)
            if result.goal_id:
                try:
                    # R2-P7 / link-spam guard: validate goal belongs to
                    # the caller's gateway. PermissionError surfaces as
                    # 403 via R2-P5 middleware.
                    await assert_same_gateway(graph, str(result.goal_id), self._gateway_id)
                    await graph.add_relation(artifact_node_id, str(result.goal_id), "SERVES_GOAL")
                except PermissionError:
                    # Re-raise so cross-gateway link attempt becomes 403.
                    raise
                except Exception:
                    pass
            # OWNED_BY: artifact → agent actor (for visibility filtering)
            if self._gateway_id:
                try:
                    await graph.add_relation(artifact_node_id, agent_actor_id, "OWNED_BY")
                except Exception:
                    pass

        self._log.info("Promoted artifact %s to persistent storage", artifact_id)
        return result
