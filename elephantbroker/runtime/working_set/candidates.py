"""CandidateGenerator — multi-source candidate collection."""
from __future__ import annotations

import logging
import math
import time
import uuid
from datetime import UTC, datetime

from elephantbroker.runtime.interfaces.retrieval import IRetrievalOrchestrator, RetrievalCandidate
from elephantbroker.runtime.observability import GatewayLoggerAdapter, traced
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.working_set import WorkingSetItem

logger = logging.getLogger("elephantbroker.runtime.working_set.candidates")


# TODO-6-303 (Round 1 Blind Spot Reviewer, LOW): the set of
# RetrievalCandidate.source values that may flow into
# CandidateGenerator.retrieval_candidate_to_item. RetrievalCandidate.source is
# a loose `str` to accommodate non-working-set producers (notably
# /rerank with source="api"), so we guard at the conversion boundary rather
# than tightening the producer-side schema. Keep in sync with the Literal in
# WorkingSetItem.retrieval_source (structural/keyword/vector/graph) plus the
# sentinel "artifact" that selects the non-retrieval branch below.
_VALID_RETRIEVAL_SOURCES: frozenset[str] = frozenset({
    "structural", "keyword", "vector", "graph",
})


class CandidateGenerator:
    """Collects raw candidates from retrieval, goals, and procedures."""

    def __init__(
        self, retrieval: IRetrievalOrchestrator,
        goal_manager=None, procedure_engine=None,
        graph=None, redis=None, config=None,
        embedding_service=None,
        procedure_candidate_config=None,
        gateway_id: str = "",
        redis_keys=None,
        metrics=None,
    ) -> None:
        self._retrieval = retrieval
        self._goal_manager = goal_manager
        self._procedure_engine = procedure_engine
        self._graph = graph
        self._redis = redis
        self._config = config
        self._embedding_service = embedding_service
        self._procedure_candidate_config = procedure_candidate_config
        self._gateway_id = gateway_id
        self._keys = redis_keys or RedisKeyBuilder(gateway_id)
        self._metrics = metrics
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})

    @traced
    async def generate(
        self, *, session_id: uuid.UUID, session_key: str, query: str,
        profile_policy=None, actor_ids: list[uuid.UUID] | None = None,
        org_id: str | None = None, team_ids: list[str] | None = None,
    ) -> tuple[list[RetrievalCandidate], list[WorkingSetItem]]:
        """Generate candidates from all sources.

        Returns:
            Tuple of (retrieval_candidates for reranker, direct_items for scoring).
        """
        import asyncio

        policy = profile_policy.retrieval if profile_policy else None

        # Fire all sources concurrently
        retrieval_task = asyncio.ensure_future(
            self._get_retrieval_candidates(query, policy, session_key, session_id=str(session_id))
        )
        session_goals_task = asyncio.ensure_future(
            self._get_session_goal_items(session_key, session_id)
        )
        persistent_goals_task = asyncio.ensure_future(
            self._get_persistent_goal_items(actor_ids, org_id=org_id, team_ids=team_ids)
        )
        procedure_task = asyncio.ensure_future(
            self._get_procedure_items(query=query)
        )

        results = await asyncio.gather(
            retrieval_task, session_goals_task, persistent_goals_task, procedure_task,
            return_exceptions=True,
        )

        retrieval_candidates = results[0] if not isinstance(results[0], Exception) else []
        session_goal_items = results[1] if not isinstance(results[1], Exception) else []
        persistent_goal_items = results[2] if not isinstance(results[2], Exception) else []
        procedure_items = results[3] if not isinstance(results[3], Exception) else []

        direct_items = list(session_goal_items) + list(persistent_goal_items) + list(procedure_items)
        return (retrieval_candidates, direct_items)

    async def _get_retrieval_candidates(self, query, policy, session_key, session_id=None):
        return await self._retrieval.retrieve_candidates(
            query, policy=policy, session_key=session_key,
            session_id=session_id,
            auto_recall=True,  # Working set is auto-injection — exclude blacklisted facts
        )

    async def _get_persistent_goal_items(
        self, actor_ids: list[uuid.UUID] | None = None,
        org_id: str | None = None, team_ids: list[str] | None = None,
    ) -> list[WorkingSetItem]:
        """Load persistent goals from graph via scope-aware Cypher.

        Phase 8 adds 4-level scope visibility:
        - GLOBAL: visible to all sessions
        - ORGANIZATION: visible if goal.org_id matches session org
        - TEAM: visible if goal.team_id in session team_ids
        - ACTOR: visible if actor has OWNS_GOAL edge
        Falls back to Phase 5 binary filter when no org configured.
        """
        if not self._graph:
            return []
        t0 = time.monotonic()
        try:
            if org_id or team_ids:
                # Phase 8: scope-aware 4-clause query
                cypher = (
                    "MATCH (g:GoalDataPoint) "
                    "WHERE g.status = 'active' AND g.gateway_id = $gateway_id "
                    "AND ("
                    "  g.scope = 'global'"
                    "  OR (g.scope = 'organization' AND g.org_id = $org_id)"
                    "  OR (g.scope = 'team' AND g.team_id IN $team_ids)"
                    "  OR (g.scope = 'actor' AND EXISTS {"
                    "    MATCH (g)<-[:OWNS_GOAL]-(a:ActorDataPoint)"
                    "    WHERE a.eb_id IN $actor_ids"
                    "  })"
                    ") "
                    "RETURN properties(g) AS props"
                )
                records = await self._graph.query_cypher(cypher, {
                    "gateway_id": self._gateway_id,
                    "org_id": org_id or "",
                    "team_ids": team_ids or [],
                    "actor_ids": [str(a) for a in (actor_ids or [])],
                })
            elif actor_ids:
                # Phase 5 fallback: binary OWNS_GOAL filter (no org configured)
                cypher = (
                    "MATCH (g:GoalDataPoint) "
                    "WHERE g.scope IN ['global', 'organization', 'team', 'actor'] "
                    "AND g.status = 'active' AND g.gateway_id = $gateway_id "
                    "OPTIONAL MATCH (g)<-[:OWNS_GOAL]-(a:ActorDataPoint) "
                    "WITH g, collect(a.eb_id) AS owner_ids "
                    "WHERE size(owner_ids) = 0 "
                    "OR any(oid IN owner_ids WHERE oid IN $actor_ids) "
                    "RETURN properties(g) AS props"
                )
                records = await self._graph.query_cypher(
                    cypher, {"actor_ids": [str(a) for a in actor_ids], "gateway_id": self._gateway_id}
                )
            else:
                # No actor context — return all persistent goals (skip filter)
                cypher = (
                    "MATCH (g:GoalDataPoint) "
                    "WHERE g.scope IN ['global', 'organization', 'team', 'actor'] "
                    "AND g.status = 'active' AND g.gateway_id = $gateway_id "
                    "RETURN properties(g) AS props"
                )
                records = await self._graph.query_cypher(cypher, {"gateway_id": self._gateway_id})
            scope = "org_team" if (org_id or team_ids) else ("actor" if actor_ids else "unscoped")
            if self._metrics:
                self._metrics.inc_goal_scope_filter(scope)
            items: list[WorkingSetItem] = []
            for rec in records:
                props = rec.get("props", {})
                title = props.get("title", "")
                desc = props.get("description", "")
                gid = props.get("eb_id", str(uuid.uuid4()))
                text = f"Persistent Goal: {title}"
                if desc:
                    text += f" — {desc}"
                items.append(WorkingSetItem(
                    id=gid,
                    source_type="persistent_goal",
                    source_id=uuid.UUID(gid) if len(gid) == 36 else uuid.uuid4(),
                    text=text,
                    token_size=len(text) // 4,
                    category="goal",
                    confidence=props.get("confidence", 0.8),
                    created_at=props.get("created_at"),
                    updated_at=props.get("updated_at"),
                ))
            if self._metrics:
                self._metrics.observe_goal_scope_filter_duration(time.monotonic() - t0)
            return items
        except Exception:
            return []

    async def _get_session_goal_items(
        self, session_key: str, session_id: uuid.UUID,
    ) -> list[WorkingSetItem]:
        """Load session goals from Redis, render as WorkingSetItems."""
        items: list[WorkingSetItem] = []
        goals: list[GoalState] = []

        # Try Redis first
        if self._redis:
            try:
                import json
                key = self._keys.session_goals(session_key)
                raw = await self._redis.get(key)
                if raw:
                    data = json.loads(raw)
                    goals = [GoalState(**g) for g in data]
            except Exception:
                pass

        # Fallback to GoalManager (filter to session scope only — persistent goals
        # are loaded separately by _get_persistent_goal_items)
        if not goals and self._goal_manager:
            try:
                all_goals = await self._goal_manager.resolve_active_goals(session_id)
                goals = [g for g in all_goals if getattr(g, "scope", "session") == "session"
                         or str(getattr(g, "scope", "session")) == "session"]
            except Exception:
                pass

        for goal in goals:
            if goal.status not in (GoalStatus.ACTIVE, GoalStatus.PROPOSED):
                continue
            text = self._render_goal(goal, goals)
            has_blockers = bool(goal.blockers)
            items.append(WorkingSetItem(
                id=str(goal.id),
                source_type="goal",
                source_id=goal.id,
                text=text,
                token_size=len(text) // 4,
                must_inject=has_blockers,
                category="goal",
                confidence=goal.confidence,
                created_at=goal.created_at,
                updated_at=goal.updated_at,
            ))
        return items

    async def _get_procedure_items(self, *, query: str = "") -> list[WorkingSetItem]:
        """Load active/enabled procedures, filtered by ProcedureCandidateConfig."""
        pcc = self._procedure_candidate_config
        if pcc and not pcc.enabled:
            return []

        if not self._graph:
            return []
        try:
            cypher = (
                "MATCH (p:ProcedureDataPoint) WHERE p.gateway_id = $gateway_id "
                "RETURN properties(p) AS props"
            )
            records = await self._graph.query_cypher(cypher, {"gateway_id": self._gateway_id})
            items: list[WorkingSetItem] = []
            for rec in records:
                props = rec.get("props", {})
                name = props.get("name", "")
                desc = props.get("description", "")
                pid = props.get("eb_id", str(uuid.uuid4()))
                text = f"Procedure: {name}"
                if desc:
                    text += f" — {desc}"
                has_proof = bool(props.get("required_evidence"))
                items.append(WorkingSetItem(
                    id=pid,
                    source_type="procedure",
                    source_id=uuid.UUID(pid) if len(pid) == 36 else uuid.uuid4(),
                    text=text,
                    token_size=len(text) // 4,
                    must_inject=has_proof,
                    system_prompt_eligible=True,
                    category="procedure",
                ))

            # Apply relevance filtering if configured
            if pcc and pcc.filter_by_relevance and self._embedding_service and query and items:
                try:
                    query_emb = await self._embedding_service.embed_text(query)
                    item_texts = [item.text for item in items]
                    item_embs = await self._embedding_service.embed_batch(item_texts)

                    scored: list[tuple[float, WorkingSetItem]] = []
                    for i, item in enumerate(items):
                        # Always keep proof-required procedures if configured
                        if (
                            pcc.always_include_proof_required
                            and item.must_inject
                        ):
                            scored.append((1.0, item))
                            continue
                        if i < len(item_embs) and item_embs[i]:
                            sim = _cosine_sim(query_emb, item_embs[i])
                            if sim >= pcc.relevance_threshold:
                                scored.append((sim, item))
                        # else: drop items below threshold
                    scored.sort(key=lambda x: x[0], reverse=True)
                    items = [item for _, item in scored[:pcc.top_k]]
                except Exception:
                    # Fallback: return all items unfiltered
                    pass

            # TODO-8-R1-018 / TODO-8-R1-021: batched increment instead of
            # a per-item loop. ``inc_procedure_qualified(count=N)`` lets
            # Prometheus' ``Counter.inc(amount)`` do the math in one
            # round-trip; the previous loop emitted N method calls per
            # candidate set (small N in practice — bounded by
            # ``pcc.top_k`` — but cleaner this way).
            if self._metrics and items:
                self._metrics.inc_procedure_qualified(count=len(items))
            return items
        except Exception:
            return []

    @classmethod
    def retrieval_candidate_to_item(cls, rc: RetrievalCandidate) -> WorkingSetItem:
        """Convert a reranked RetrievalCandidate to WorkingSetItem, carrying all metadata.

        T-3: split `rc.source` into two orthogonal fields —
        `source_type` (DataPoint-type semantic) and `retrieval_source`
        (retrieval-path provenance). Pattern (b1): artifacts are a distinct
        DataPoint type, so they get `source_type="artifact"` with no
        retrieval_source; fact-class items get `source_type="fact"` and
        stamp `retrieval_source=rc.source` (structural/keyword/vector/graph).

        TODO-6-303 (Round 1 Blind Spot Reviewer, LOW): ``RetrievalCandidate.
        source`` is a loose ``str`` to accommodate non-working-set producers
        like ``/rerank`` (``source="api"``). This converter maps ``rc.source``
        → ``WorkingSetItem.retrieval_source``, which is
        ``Literal["structural","keyword","vector","graph"] | None``. Reject
        unknown values explicitly so the error surfaces at this boundary
        with an actionable message, not deep in ``WorkingSetItem`` Pydantic
        validation where the user has to trace the call chain to understand
        which producer shipped the bad value.
        """
        fact = rc.fact
        is_artifact = rc.source == "artifact"
        if not is_artifact and rc.source not in _VALID_RETRIEVAL_SOURCES:
            raise ValueError(
                f"retrieval_candidate_to_item: unknown RetrievalCandidate.source "
                f"{rc.source!r}; expected one of "
                f"{sorted(_VALID_RETRIEVAL_SOURCES)} or 'artifact'. If this is "
                f"a non-working-set producer (e.g. /rerank with source='api'), "
                f"do not pipe it through this converter.",
            )
        return WorkingSetItem(
            id=str(fact.id),
            source_type="artifact" if is_artifact else "fact",
            retrieval_source=None if is_artifact else rc.source,
            source_id=fact.id,
            text=fact.text,
            token_size=fact.token_size or len(fact.text) // 4,
            confidence=fact.confidence,
            use_count=fact.use_count,
            successful_use_count=fact.successful_use_count,
            created_at=fact.created_at,
            updated_at=fact.updated_at,
            last_used_at=fact.last_used_at,
            category=fact.category,
            goal_ids=list(fact.goal_ids),
            goal_relevance_tags=dict(fact.goal_relevance_tags),
            must_inject=(fact.category == "constraint"),
            system_prompt_eligible=(fact.category in ("constraint", "procedure_ref")),
        )

    def _render_goal(self, goal: GoalState, all_goals: list[GoalState] | None = None) -> str:
        parts = [f"Goal: {goal.title}"]
        if goal.description:
            parts.append(f"Description: {goal.description}")
        if goal.success_criteria:
            parts.append(f"Criteria: {', '.join(goal.success_criteria)}")
        if goal.blockers:
            parts.append(f"BLOCKED BY: {', '.join(goal.blockers)}")
        # Sub-goal blocker propagation (AD-17)
        if all_goals:
            subgoal_blockers = [
                b for g in all_goals
                if g.parent_goal_id == goal.id
                for b in g.blockers
            ]
            if subgoal_blockers:
                parts.append(f"Sub-task blockers: {', '.join(subgoal_blockers)}")
        return " | ".join(parts)


def _cosine_sim(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na < 1e-10 or nb < 1e-10:
        return 0.0
    return dot / (na * nb)
