"""Gateway-scoped Redis key builder — replaces all hardcoded ``f"eb:..."`` patterns."""
from __future__ import annotations

import logging

logger = logging.getLogger("elephantbroker.runtime.redis_keys")


class RedisKeyBuilder:
    """Builds Redis keys namespaced by gateway_id.

    Every key (except embedding cache) is prefixed with ``eb:{gateway_id}:``.
    This guarantees two gateways sharing the same Redis instance never collide.
    """

    def __init__(self, gateway_id: str) -> None:
        # 5-214: Empty gateway_id is a valid constructor argument (the turn-ingest
        # buffer's default branch explicitly passes "" when the container hasn't
        # injected a builder yet, and test conftests pass "" as a sentinel). But
        # in a real runtime this produces `eb::...` double-colon keys that look
        # nearly identical to real ones and silently bypass multi-gateway
        # isolation — a bootstrap bug in which the container failed to wire
        # gateway_id would go undetected. Emit a WARNING so the gap surfaces in
        # logs without breaking legitimate empty-id paths (tests, default-branch
        # fallback). The warning is intentionally non-fatal per team-lead
        # direction ("least intrusive").
        if gateway_id == "":
            logger.warning(
                "RedisKeyBuilder constructed with empty gateway_id — keys will "
                "be prefixed 'eb::' which bypasses multi-gateway isolation. "
                "This is expected in tests and the buffer's default branch, but "
                "indicates a bootstrap gap if seen in production.",
            )
        self._prefix = f"eb:{gateway_id}"

    @property
    def prefix(self) -> str:
        return self._prefix

    # --- Ingest buffer ---

    def ingest_buffer(self, session_key: str) -> str:
        return f"{self._prefix}:ingest_buffer:{session_key}"

    def recent_facts(self, session_key: str) -> str:
        return f"{self._prefix}:recent_facts:{session_key}"

    # --- Session goals ---

    def session_goals(self, session_key: str) -> str:
        """Session goals keyed by session_key only.

        Key format changed from ``{sk}:{sid}`` to ``{sk}`` in PR #11 (ISSUE-15/18).
        Old ``{sk}:{sid}`` keys will be orphaned on deploy and expire via TTL.
        """
        return f"{self._prefix}:session_goals:{session_key}"

    # --- Working set ---

    def ws_snapshot(self, session_key: str, session_id: str) -> str:
        return f"{self._prefix}:ws_snapshot:{session_key}:{session_id}"

    def ws_snapshot_scan_pattern(self, session_id: str) -> str:
        """Glob pattern for scanning ws_snapshot keys across all session_keys.

        Used by WorkingSetManager.get_working_set() when resolving a snapshot
        from a session_id without knowing the routing session_key.
        """
        return f"{self._prefix}:ws_snapshot:*:{session_id}"

    def compact_state(self, session_key: str, session_id: str) -> str:
        return f"{self._prefix}:compact_state:{session_key}:{session_id}"

    # --- Subagent tracking ---

    def session_parent(self, session_key: str) -> str:
        return f"{self._prefix}:session_parent:{session_key}"

    def session_context(self, session_key: str, session_id: str) -> str:
        return f"{self._prefix}:session_context:{session_key}:{session_id}"

    # --- Phase 6+ keys (defined now so future phases use the builder) ---

    def compact_state_obj(self, session_key: str, session_id: str) -> str:
        return f"{self._prefix}:compact_state_obj:{session_key}:{session_id}"

    def session_artifacts(self, session_key: str, session_id: str) -> str:
        return f"{self._prefix}:session_artifacts:{session_key}:{session_id}"

    def procedure_exec(self, session_key: str, session_id: str) -> str:
        return f"{self._prefix}:procedure_exec:{session_key}:{session_id}"

    def session_messages(self, session_key: str, session_id: str) -> str:
        return f"{self._prefix}:session_messages:{session_key}:{session_id}"

    def fact_async_use(self, source_id: str) -> str:
        return f"{self._prefix}:fact_async_use:{source_id}"

    def guard_history(self, session_key: str, session_id: str) -> str:
        return f"{self._prefix}:guard_history:{session_key}:{session_id}"

    def guard_history_scan_pattern(self) -> str:
        """Glob pattern for scanning all guard_history keys in this gateway.

        Used by DomainDiscoveryTask to aggregate uncategorized action patterns
        across every (session_key, session_id) pair.
        """
        return f"{self._prefix}:guard_history:*"

    def approval(self, agent_id: str, request_id: str) -> str:
        return f"{self._prefix}:{agent_id}:approval:{request_id}"

    def approvals_by_session(self, agent_id: str, session_id: str) -> str:
        return f"{self._prefix}:{agent_id}:approvals_by_session:{session_id}"

    def fact_domains(self, session_key: str, session_id: str) -> str:
        return f"{self._prefix}:fact_domains:{session_key}:{session_id}"

    def session_children(self, parent_session_key: str) -> str:
        return f"{self._prefix}:session_children:{parent_session_key}"

    # --- Consolidation ---

    def consolidation_lock(self) -> str:
        """Distributed lock for consolidation run (AD-10). One per gateway."""
        return f"{self._prefix}:consolidation_lock"

    def consolidation_status(self) -> str:
        """Current consolidation status (running/idle/last_run_at)."""
        return f"{self._prefix}:consolidation_status"

    # --- Global (NOT gateway-scoped) ---

    @staticmethod
    def embedding_cache(text_hash: str) -> str:
        """Embedding cache is intentionally global — same text = same embedding."""
        return f"eb:emb_cache:{text_hash}"


async def touch_session_keys(
    keys: RedisKeyBuilder,
    redis,
    sk: str,
    sid: str,
    ttl: int,
    *,
    include_parent: bool = False,
) -> int:
    """Refresh TTL on all session-scoped keys via a single Redis pipeline.

    Standalone async function (not on RedisKeyBuilder, which is pure sync).
    Called from ContextLifecycle.ingest_batch() on every new turn.

    Returns count of keys that existed (EXPIRE returns 1 if key exists).
    """
    key_list = [
        keys.session_context(sk, sid),
        keys.session_messages(sk, sid),
        keys.session_goals(sk),
        keys.session_artifacts(sk, sid),
        keys.ws_snapshot(sk, sid),
        keys.compact_state(sk, sid),
        keys.compact_state_obj(sk, sid),
        keys.procedure_exec(sk, sid),
    ]
    key_list.append(keys.guard_history(sk, sid))
    key_list.append(keys.fact_domains(sk, sid))
    if include_parent:
        key_list.append(keys.session_parent(sk))

    pipe = redis.pipeline()
    for key in key_list:
        pipe.expire(key, ttl)
    results = await pipe.execute()

    # Touch parent's session_children set (needs GET — can't pipeline)
    if include_parent:
        parent_sk = await redis.get(keys.session_parent(sk))
        if parent_sk:
            await redis.expire(keys.session_children(parent_sk), ttl)

    return sum(1 for r in results if r)
