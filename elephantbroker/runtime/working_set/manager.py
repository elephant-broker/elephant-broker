"""Working set manager — full scoring pipeline orchestrator."""
from __future__ import annotations

import asyncio
import logging
import time
import uuid

from elephantbroker.runtime.interfaces.retrieval import IRetrievalOrchestrator
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.interfaces.working_set import IWorkingSetManager
from elephantbroker.runtime.observability import traced
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.runtime.working_set.candidates import CandidateGenerator
from elephantbroker.runtime.working_set.scoring import ScoringEngine
from elephantbroker.runtime.working_set.selector import BudgetSelector
from elephantbroker.runtime.working_set.session_goals import SessionGoalStore
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.goal import GoalState
from elephantbroker.schemas.trace import TraceEvent, TraceEventType
from elephantbroker.schemas.working_set import (
    ScoringContext,
    WorkingSetItem,
    WorkingSetSnapshot,
)

logger = logging.getLogger("elephantbroker.runtime.working_set.manager")


class WorkingSetManager(IWorkingSetManager):
    """Full scoring pipeline: candidates → rerank → score → select."""

    def __init__(
        self,
        retrieval: IRetrievalOrchestrator,
        trace_ledger: ITraceLedger,
        rerank=None,
        goal_manager=None,
        procedure_engine=None,
        embedding_service=None,
        scoring_tuner=None,
        profile_registry=None,
        graph=None,
        redis=None,
        config: ElephantBrokerConfig | None = None,
        gateway_id: str = "",
        redis_keys=None,
        metrics=None,
        scoring_ledger_store=None,
        session_goal_store: SessionGoalStore | None = None,
    ) -> None:
        self._retrieval = retrieval
        self._trace = trace_ledger
        self._rerank = rerank
        self._goal_manager = goal_manager
        self._procedure_engine = procedure_engine
        self._embeddings = embedding_service
        self._scoring_tuner = scoring_tuner
        self._profile_registry = profile_registry
        self._graph = graph
        self._redis = redis
        self._config = config or ElephantBrokerConfig()
        self._snapshots: dict[uuid.UUID, WorkingSetSnapshot] = {}
        self._gateway_id = gateway_id
        self._keys = redis_keys or RedisKeyBuilder(gateway_id)
        self._metrics = metrics
        self._scoring_ledger_store = scoring_ledger_store
        self._session_goals = session_goal_store

        # Internal components
        self._candidate_gen = CandidateGenerator(
            retrieval=retrieval,
            goal_manager=goal_manager,
            procedure_engine=procedure_engine,
            graph=graph,
            redis=redis,
            config=self._config.scoring if config else None,
            embedding_service=embedding_service,
            procedure_candidate_config=self._config.procedure_candidates if config else None,
            gateway_id=gateway_id,
            redis_keys=redis_keys,
            metrics=metrics,
        )
        self._scoring_engine = ScoringEngine()
        self._budget_selector = BudgetSelector()

    @traced
    async def build_working_set(
        self, *, session_id: uuid.UUID, session_key: str,
        profile_name: str, query: str,
        goal_ids: list[uuid.UUID] | None = None,
        org_id: str | None = None,
        actor_ids: list[uuid.UUID] | None = None,
        team_ids: list[str] | None = None,
        token_budget_override: int | None = None,
    ) -> WorkingSetSnapshot:
        """Full pipeline: candidates → rerank → score → select."""
        t0 = time.monotonic()

        # Step 1: Resolve profile
        profile = None
        if self._profile_registry:
            try:
                profile = await self._profile_registry.get_effective_policy(profile_name, org_id=org_id)
            except Exception:
                pass

        weights = profile.scoring_weights if profile else None
        if weights is None:
            from elephantbroker.schemas.working_set import ScoringWeights
            weights = ScoringWeights()
        budgets = profile.budgets if profile else None
        token_budget = token_budget_override if token_budget_override is not None else (budgets.max_prompt_tokens if budgets else 8000)

        # Step 2: Build ScoringContext (parallel pre-computation)
        ctx = await self._build_scoring_context(
            query=query, session_key=session_key, session_id=session_id,
            weights=weights, token_budget=token_budget,
        )

        # Step 3: CandidateGenerator.generate()
        retrieval_candidates, direct_items = await self._candidate_gen.generate(
            session_id=session_id, session_key=session_key,
            query=query, profile_policy=profile,
            actor_ids=actor_ids, org_id=org_id, team_ids=team_ids,
        )

        # Step 4: RerankOrchestrator.rerank()
        reranked = retrieval_candidates
        if self._rerank and retrieval_candidates:
            try:
                reranked = await self._rerank.rerank(
                    retrieval_candidates, query,
                    query_embedding=ctx.turn_embedding if ctx.turn_embedding else None,
                )
            except Exception as exc:
                logger.warning("Rerank failed, using original order: %s", exc)

        # Step 5: Convert reranked candidates → WorkingSetItem, merge with direct_items
        converted = [
            CandidateGenerator.retrieval_candidate_to_item(rc)
            for rc in reranked
        ]
        all_items = converted + direct_items

        # Populate evidence_ref_ids from pre-computed evidence IDs (B2-O07)
        if ctx.evidence_ids:
            for item in all_items:
                eids = ctx.evidence_ids.get(item.id, [])
                if eids:
                    item.evidence_ref_ids = [uuid.UUID(eid) for eid in eids if len(eid) == 36]

        # Metrics: observe candidate counts by source type
        # T-3 (Option C): prefer retrieval_source when present
        # (structural/keyword/vector/graph) so dashboards keep seeing
        # per-retrieval-path breakdowns, fall back to the new DataPoint-type
        # semantic source_type (fact/artifact/goal/…) for non-retrieval
        # items. Preserves cardinality and dashboard compatibility.
        if self._metrics:
            source_counts: dict[str, int] = {}
            for item in all_items:
                st = (
                    getattr(item, "retrieval_source", None)
                    or getattr(item, "source_type", None)
                    or "unknown"
                )
                source_counts[st] = source_counts.get(st, 0) + 1
            for st, cnt in source_counts.items():
                self._metrics.observe_candidates(st, cnt)
            # Count must-inject items
            for item in all_items:
                if getattr(item, "must_inject", False):
                    self._metrics.inc_must_inject()

        # Ensure all item embeddings are in ScoringContext
        if self._embeddings:
            missing_texts = []
            missing_ids = []
            for item in all_items:
                if item.id not in ctx.item_embeddings:
                    missing_texts.append(item.text)
                    missing_ids.append(item.id)
            if missing_texts:
                try:
                    embs = await self._embeddings.embed_batch(missing_texts)
                    for i, emb in enumerate(embs):
                        ctx.item_embeddings[missing_ids[i]] = emb
                except Exception:
                    pass

        # Step 6: ScoringEngine Pass 1
        for item in all_items:
            item.scores = self._scoring_engine.score_independent(item, ctx)

        # Step 7: BudgetSelector Pass 2
        snapshot = self._budget_selector.select(
            scored_items=all_items,
            ctx=ctx,
            token_budget=token_budget,
            session_id=session_id,
            scoring_engine=self._scoring_engine,
        )

        # Stamp gateway_id on snapshot (B2-O01)
        snapshot.gateway_id = snapshot.gateway_id or self._gateway_id

        # Step 8: Cache snapshot
        self._snapshots[session_id] = snapshot
        if self._redis:
            try:
                cache_key = self._keys.ws_snapshot(session_key, str(session_id))
                ttl = self._config.scoring.snapshot_ttl_seconds
                await self._redis.setex(
                    cache_key, ttl, snapshot.model_dump_json(),
                )
            except Exception:
                pass

        # Phase 9: Write scoring ledger for Stage 9 correlation (BS-4)
        if self._scoring_ledger_store:
            try:
                import json as _json
                from elephantbroker.schemas.scoring import ScoringDimension
                selected_ids = {str(item.id) for item in snapshot.items}
                ledger_entries = []
                for item in all_items:
                    dim_scores = {}
                    if item.scores:
                        for dim in ScoringDimension:
                            dim_scores[dim.value] = getattr(item.scores, dim.value, 0.0)
                    ledger_entries.append({
                        "fact_id": str(item.id),
                        "session_id": str(session_id),
                        "session_key": session_key,
                        "gateway_id": self._gateway_id,
                        "profile_id": profile_name,
                        "dim_scores_json": _json.dumps(dim_scores),
                        "was_selected": str(item.id) in selected_ids,
                        "successful_use_count_at_scoring": getattr(item, "successful_use_count", 0),
                    })
                await self._scoring_ledger_store.write_batch(ledger_entries)
            except Exception:
                logger.warning("Scoring ledger write failed — non-fatal")

        # Metrics: observe build outcome
        if self._metrics:
            elapsed = time.monotonic() - t0
            self._metrics.inc_working_set_build(profile_name, "ok")
            self._metrics.observe_working_set_duration(profile_name, elapsed)
            self._metrics.observe_selected(len(snapshot.items))
            self._metrics.observe_tokens_used(snapshot.tokens_used)

        # Emit trace event (B2-O02: include agent identity)
        await self._trace.append_event(TraceEvent(
            event_type=TraceEventType.SCORING_COMPLETED,
            session_id=session_id,
            session_key=session_key,
            gateway_id=self._gateway_id,
            payload={
                "candidates": len(all_items),
                "selected": len(snapshot.items),
                "tokens_used": snapshot.tokens_used,
                "token_budget": token_budget,
                "profile": profile_name,
                "session_key": session_key,
            },
        ))

        return snapshot

    async def get_working_set(self, session_id: uuid.UUID) -> WorkingSetSnapshot | None:
        # Try in-memory first
        if session_id in self._snapshots:
            return self._snapshots[session_id]
        # Try Redis
        if self._redis:
            try:
                # Scan for matching key
                pattern = self._keys.ws_snapshot_scan_pattern(str(session_id))
                keys = []
                async for key in self._redis.scan_iter(match=pattern, count=10):
                    keys.append(key)
                if keys:
                    raw = await self._redis.get(keys[0])
                    if raw:
                        return WorkingSetSnapshot.model_validate_json(raw)
            except Exception:
                pass
        return None

    @traced
    async def _build_scoring_context(
        self, *, query: str, session_key: str, session_id: uuid.UUID,
        weights, token_budget: int,
    ) -> ScoringContext:
        """Build ScoringContext with parallel pre-computation."""

        # Parallel async tasks
        tasks: dict[str, asyncio.Task] = {}

        # PC-5: Turn embedding
        if self._embeddings:
            tasks["turn_emb"] = asyncio.ensure_future(
                self._embeddings.embed_text(query)
            )

        # PC-1: Evidence counts
        if self._graph:
            tasks["evidence"] = asyncio.ensure_future(
                self._query_evidence_index()
            )

        # PC-1b: Evidence IDs (B2-O07)
        if self._graph:
            tasks["evidence_ids"] = asyncio.ensure_future(
                self._query_evidence_ids()
            )

        # PC-2: Verification statuses
        if self._graph:
            tasks["verification"] = asyncio.ensure_future(
                self._query_verification_index()
            )

        # PC-3: Conflict pairs
        if self._graph:
            tasks["conflicts"] = asyncio.ensure_future(
                self._query_conflict_pairs()
            )

        # Session goals (Redis primary) — delegate to SessionGoalStore
        if self._session_goals:
            tasks["session_goals"] = asyncio.ensure_future(
                self._session_goals.get_goals(session_key, session_id)
            )

        # Persistent goals (Cypher with OWNS_GOAL filtering)
        if self._graph:
            tasks["persistent_goals"] = asyncio.ensure_future(
                self._get_persistent_goals_from_graph()
            )

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        result_map: dict[str, object] = {}
        for key, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                logger.warning("Pre-computation %s failed: %s", key, result)
                result_map[key] = None
            else:
                result_map[key] = result

        turn_embedding = result_map.get("turn_emb") or []
        evidence_index = result_map.get("evidence") or {}
        evidence_ids: dict[str, list[str]] = result_map.get("evidence_ids") or {}
        verification_index = result_map.get("verification") or {}
        conflict_data = result_map.get("conflicts") or (set(), {})
        if isinstance(conflict_data, tuple):
            conflict_pairs, conflict_edge_types = conflict_data
        else:
            conflict_pairs, conflict_edge_types = set(), {}
        session_goals = result_map.get("session_goals") or []
        persistent_goals = result_map.get("persistent_goals") or []

        # Embed goals for goal relevance scoring (sequential — depends on goal loading)
        goal_embeddings: dict[str, list[float]] = {}
        all_goals = list(session_goals) + list(persistent_goals)
        if all_goals and self._embeddings:
            try:
                goal_texts = [g.title for g in all_goals if hasattr(g, "title")]
                if goal_texts:
                    embs = await self._embeddings.embed_batch(goal_texts)
                    for i, goal in enumerate(all_goals):
                        if i < len(embs):
                            goal_embeddings[str(goal.id)] = embs[i]
            except Exception:
                pass

        return ScoringContext(
            turn_text=query,
            turn_embedding=turn_embedding if isinstance(turn_embedding, list) else [],
            session_goals=session_goals if isinstance(session_goals, list) else [],
            global_goals=persistent_goals if isinstance(persistent_goals, list) else [],
            goal_embeddings=goal_embeddings,
            compact_state_ids=set(),
            weights=weights,
            token_budget=token_budget,
            evidence_index=evidence_index if isinstance(evidence_index, dict) else {},
            verification_index=verification_index if isinstance(verification_index, dict) else {},
            conflict_pairs=conflict_pairs if isinstance(conflict_pairs, set) else set(),
            conflict_edge_types=conflict_edge_types if isinstance(conflict_edge_types, dict) else {},
            item_embeddings={},
            verification_multipliers=self._config.verification_multipliers,
            conflict_config=self._config.conflict_detection,
            scoring_config=self._config.scoring,
            evidence_ids=evidence_ids,
        )

    @traced
    async def _query_evidence_index(self) -> dict[str, int]:
        """PC-1: Count evidence supporting each fact."""
        try:
            cypher = (
                "MATCH (e:EvidenceDataPoint)-[:SUPPORTS]->(c:ClaimDataPoint)-[:SUPPORTS]->(f:FactDataPoint) "
                "WHERE f.gateway_id = $gateway_id "
                "RETURN f.eb_id AS fid, count(e) AS cnt"
            )
            records = await self._graph.query_cypher(cypher, {"gateway_id": self._gateway_id})
            return {r["fid"]: r["cnt"] for r in records if r.get("fid")}
        except Exception:
            return {}

    @traced
    async def _query_evidence_ids(self) -> dict[str, list[str]]:
        """PC-1b: Collect evidence IDs supporting each fact (B2-O07)."""
        try:
            cypher = (
                "MATCH (e:EvidenceDataPoint)-[:SUPPORTS]->(c:ClaimDataPoint)-[:SUPPORTS]->(f:FactDataPoint) "
                "WHERE f.gateway_id = $gateway_id "
                "RETURN f.eb_id AS fid, collect(e.eb_id) AS eids"
            )
            records = await self._graph.query_cypher(cypher, {"gateway_id": self._gateway_id})
            return {r["fid"]: r["eids"] for r in records if r.get("fid") and r.get("eids")}
        except Exception:
            return {}

    @traced
    async def _query_verification_index(self) -> dict[str, str]:
        """PC-2: Verification statuses for facts."""
        try:
            cypher = (
                "MATCH (c:ClaimDataPoint) WHERE c.status IS NOT NULL AND c.gateway_id = $gateway_id "
                "OPTIONAL MATCH (c)-[:SUPPORTS]->(f:FactDataPoint) "
                "WHERE f.gateway_id = $gateway_id "
                "RETURN f.eb_id AS fid, c.status AS status"
            )
            records = await self._graph.query_cypher(cypher, {"gateway_id": self._gateway_id})
            return {r["fid"]: r["status"] for r in records if r.get("fid") and r.get("status")}
        except Exception:
            return {}

    @traced
    async def _query_conflict_pairs(self) -> tuple[set[tuple[str, str]], dict[tuple[str, str], str]]:
        """PC-3: SUPERSEDES and CONTRADICTS edges between facts."""
        pairs: set[tuple[str, str]] = set()
        edge_types: dict[tuple[str, str], str] = {}
        try:
            cypher = (
                "MATCH (f1:FactDataPoint)-[r:SUPERSEDES|CONTRADICTS]->(f2:FactDataPoint) "
                "WHERE f1.gateway_id = $gateway_id "
                "RETURN f1.eb_id AS src, f2.eb_id AS tgt, type(r) AS rel"
            )
            records = await self._graph.query_cypher(cypher, {"gateway_id": self._gateway_id})
            for r in records:
                if r.get("src") and r.get("tgt"):
                    pair = (r["src"], r["tgt"])
                    pairs.add(pair)
                    edge_types[pair] = r.get("rel", "SUPERSEDES")
        except Exception:
            pass
        return pairs, edge_types

    async def _get_persistent_goals_from_graph(self) -> list[GoalState]:
        """Load persistent (global/org/team/actor) goals from Cognee graph."""
        try:
            from elephantbroker.runtime.adapters.cognee.datapoints import GoalDataPoint
            from elephantbroker.runtime.graph_utils import clean_graph_props
            cypher = (
                "MATCH (g:GoalDataPoint) "
                "WHERE g.scope IN ['global', 'organization', 'team', 'actor'] "
                "AND g.status = 'active' AND g.gateway_id = $gateway_id "
                "RETURN properties(g) AS props"
            )
            records = await self._graph.query_cypher(cypher, {"gateway_id": self._gateway_id})
            goals: list[GoalState] = []
            for rec in records:
                props = clean_graph_props(rec.get("props", {}))
                dp = GoalDataPoint(**props)
                goals.append(dp.to_schema())
            return goals
        except Exception:
            return []

