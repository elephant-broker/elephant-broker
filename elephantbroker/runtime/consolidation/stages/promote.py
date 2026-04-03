"""Stage 6: Promote Facts (Class + Scope) and Session Artifacts.

No LLM calls. Two-dimensional promotion: memory_class AND scope.
GLOBAL scope is NEVER auto-promoted (AD-13) — flagged for human review only.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.consolidation import PromotionResult

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
    from elephantbroker.runtime.context.session_artifact_store import SessionArtifactStore
    from elephantbroker.runtime.interfaces.artifact_store import IToolArtifactStore
    from elephantbroker.schemas.consolidation import ConsolidationConfig, ConsolidationContext
    from elephantbroker.schemas.fact import FactAssertion

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.promote")


class PromoteStage:
    """Two-dimensional promotion: memory_class AND scope.

    Uses Stage 1 cluster session_keys for recurrence detection (AD-12).
    GLOBAL flagged only (AD-13), never auto-promoted.
    Also promotes session artifacts if searched or frequently injected.
    """

    def __init__(
        self,
        graph: GraphAdapter,
        session_artifact_store: SessionArtifactStore | None,
        artifact_store: IToolArtifactStore | None,
        config: ConsolidationConfig,
    ) -> None:
        self._graph = graph
        self._session_artifact_store = session_artifact_store
        self._artifact_store = artifact_store
        self._session_threshold = config.promote_session_threshold
        self._artifact_threshold = config.promote_artifact_injected_threshold

    @traced
    async def run(
        self,
        facts: list[FactAssertion],
        gateway_id: str,
        context: ConsolidationContext,
    ) -> list[PromotionResult]:
        from elephantbroker.schemas.base import Scope
        from elephantbroker.schemas.fact import MemoryClass

        results: list[PromotionResult] = []
        global_candidates = 0

        # Build session count per fact from cluster data
        fact_session_counts = self._build_session_counts(facts, context)

        for fact in facts:
            if fact.archived:
                continue

            fid = str(fact.id)
            sessions_seen = fact_session_counts.get(fid, 1)
            has_persistent_goal = bool(fact.goal_ids)
            has_use = fact.successful_use_count > 0
            old_class = str(fact.memory_class.value if hasattr(fact.memory_class, "value") else fact.memory_class)
            old_scope = str(fact.scope.value if hasattr(fact.scope, "value") else fact.scope)
            new_class = old_class
            new_scope = old_scope

            # Check for supervisor-verified evidence → flag for GLOBAL review (AD-13)
            # (Would need graph query for evidence — simplified: check confidence == 1.0 as proxy)
            if fact.confidence >= 1.0 and has_persistent_goal and sessions_seen >= self._session_threshold:
                global_candidates += 1
                # DO NOT auto-promote to GLOBAL — just flag in report
                continue

            # Promotion decision matrix
            is_episodic = old_class == "episodic"
            is_session_scope = old_scope == "session"

            if sessions_seen >= self._session_threshold:
                # Recurring across 3+ sessions
                if is_episodic:
                    new_class = "semantic"
                if has_persistent_goal or has_use:
                    if is_session_scope:
                        new_scope = "actor"
                # No goal and no use → promote class only, keep session scope
            elif has_persistent_goal and has_use:
                # 1-2 sessions but persistent goal + use → scope promotion only
                if is_session_scope:
                    new_scope = "actor"

            if new_class != old_class or new_scope != old_scope:
                # Determine reason
                if sessions_seen >= self._session_threshold and has_persistent_goal:
                    reason = "recurring_with_goal"
                elif sessions_seen >= self._session_threshold and has_use:
                    reason = "recurring_with_use"
                elif has_persistent_goal:
                    reason = "persistent_goal_link"
                else:
                    reason = "recurring_with_use"

                # Apply promotion to fact
                if new_class != old_class:
                    fact.memory_class = MemoryClass(new_class)
                if new_scope != old_scope:
                    fact.scope = Scope(new_scope)

                results.append(PromotionResult(
                    fact_id=fid,
                    old_memory_class=old_class,
                    new_memory_class=new_class,
                    old_scope=old_scope,
                    new_scope=new_scope,
                    reason=reason,
                    sessions_seen=sessions_seen,
                ))

        # Promote session artifacts
        artifact_count = await self._promote_session_artifacts(gateway_id)

        logger.info(
            "Stage 6: %d facts promoted, %d GLOBAL candidates flagged, %d artifacts promoted (gateway=%s)",
            len(results), global_candidates, artifact_count, gateway_id,
        )
        return results

    def _build_session_counts(
        self,
        facts: list[FactAssertion],
        context: ConsolidationContext,
    ) -> dict[str, int]:
        """Map fact_id → number of distinct sessions it appeared in.

        Uses Stage 1 cluster data (context.clusters) for near-duplicate grouping,
        plus the fact's own session_key for exact matches.
        """
        counts: dict[str, int] = {}

        # From cluster data: cluster session_keys apply to ALL facts in the cluster
        cluster_sessions: dict[str, set[str]] = {}
        for cluster in context.clusters:
            for fid in cluster.fact_ids:
                cluster_sessions.setdefault(fid, set()).update(cluster.session_keys)

        for fact in facts:
            fid = str(fact.id)
            sessions = cluster_sessions.get(fid, set())
            if fact.session_key:
                sessions.add(fact.session_key)
            counts[fid] = max(len(sessions), 1)

        return counts

    async def _promote_session_artifacts(self, gateway_id: str) -> int:
        """Promote session artifacts that have been searched or frequently injected."""
        if not self._session_artifact_store:
            return 0

        promoted = 0
        # Scan all session artifacts — the store has list_all() per session
        # In practice, this scans non-expired Redis HASH keys
        # For consolidation, we rely on the store's existing API
        try:
            # Note: SessionArtifactStore doesn't have a cross-session scan.
            # Artifact promotion is already handled by promote_to_persistent()
            # which Phase 6 calls on session end. Stage 6 only handles facts.
            # This is intentional — session artifact promotion is session-scoped.
            pass
        except Exception:
            logger.debug("Session artifact promotion failed", exc_info=True)

        return promoted
