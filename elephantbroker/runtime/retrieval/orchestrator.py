"""Retrieval orchestrator — 5-source hybrid search."""
from __future__ import annotations

import asyncio
import logging

import cognee
from cognee.modules.search.types import SearchType

from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
from elephantbroker.runtime.adapters.cognee.embeddings import EmbeddingService
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
from elephantbroker.runtime.graph_utils import clean_graph_props
from elephantbroker.runtime.interfaces.retrieval import IRetrievalOrchestrator, RetrievalCandidate
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.fact import FactAssertion, MemoryClass
from elephantbroker.schemas.profile import IsolationLevel, IsolationScope, RetrievalPolicy
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.retrieval.orchestrator")

_FACTS_COLLECTION = "FactDataPoint_text"


class RetrievalOrchestrator(IRetrievalOrchestrator):

    def __init__(
        self,
        vector: VectorAdapter,
        graph: GraphAdapter,
        embeddings: EmbeddingService,
        trace_ledger: ITraceLedger,
        dataset_name: str = "elephantbroker",
        gateway_id: str = "",
    ) -> None:
        self._vector = vector
        self._graph = graph
        self._embeddings = embeddings
        self._trace = trace_ledger
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id

    @traced
    async def retrieve_candidates(
        self, query: str, *,
        policy: RetrievalPolicy | None = None,
        scope: str | None = None,
        actor_id: str | None = None,
        memory_class: MemoryClass | None = None,
        session_key: str | None = None,
        session_id: str | None = None,
        auto_recall: bool = False,
        caller_gateway_id: str = "",
    ) -> list[RetrievalCandidate]:
        if policy is None:
            policy = RetrievalPolicy()

        sk = self._dataset_name
        is_strict = policy.isolation_level == IsolationLevel.STRICT

        # Build concurrent tasks for enabled sources
        tasks: dict[str, asyncio.Task] = {}
        if policy.structural_enabled:
            tasks["structural"] = asyncio.ensure_future(
                self.get_structural_hits(
                    scope=scope, actor_id=actor_id, memory_class=memory_class,
                    session_key=session_key, limit=policy.structural_fetch_k,
                    auto_recall=auto_recall,
                    caller_gateway_id=caller_gateway_id,
                )
            )
        if policy.keyword_enabled and not is_strict:
            tasks["keyword"] = asyncio.ensure_future(
                self.get_keyword_hits(query, sk, policy.keyword_fetch_k)
            )
        if policy.vector_enabled:
            if is_strict:
                tasks["vector"] = asyncio.ensure_future(
                    self._get_direct_vector_hits(query, session_key, policy.vector_fetch_k)
                )
            else:
                tasks["vector"] = asyncio.ensure_future(
                    self.get_semantic_hits_cognee(query, sk, policy.vector_fetch_k)
                )
        if policy.graph_expansion_enabled:
            tasks["graph"] = asyncio.ensure_future(
                self.get_graph_neighbors(query, sk, policy.graph_max_depth)
            )
        if policy.artifact_enabled:
            tasks["artifact"] = asyncio.ensure_future(
                self._get_artifact_hits(query, sk, policy.artifact_fetch_k)
            )

        # Await all
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        source_names = list(tasks.keys())

        # Weight map
        weight_map = {
            "structural": policy.structural_weight,
            "keyword": policy.keyword_weight,
            "vector": policy.vector_weight,
            "graph": policy.graph_expansion_weight,
            "artifact": policy.vector_weight,  # Artifacts use vector weight
        }

        # Also track which sources were disabled for trace completeness
        all_source_enabled = {
            "structural": policy.structural_enabled,
            "keyword": policy.keyword_enabled and not is_strict,
            "vector": policy.vector_enabled,
            "graph": policy.graph_expansion_enabled,
            "artifact": policy.artifact_enabled,
        }

        # Collect candidates with weighted scores
        all_candidates: list[RetrievalCandidate] = []
        for i, source_name in enumerate(source_names):
            result = results[i]
            _trace_gw = caller_gateway_id or self._gateway_id
            if isinstance(result, Exception):
                logger.warning("Retrieval source %s failed: %s", source_name, result)
                if self._trace:
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.RETRIEVAL_SOURCE_RESULT,
                        session_key=session_key or "",
                        gateway_id=_trace_gw,
                        payload={"source_type": source_name, "result_count": 0,
                                 "enabled": True, "error": str(result)},
                    ))
                continue
            weight = weight_map.get(source_name, 0.3)
            for candidate in result:
                candidate.score = candidate.score * weight
                all_candidates.append(candidate)
            if self._trace:
                await self._trace.append_event(TraceEvent(
                    event_type=TraceEventType.RETRIEVAL_SOURCE_RESULT,
                    session_key=session_key or "",
                    gateway_id=_trace_gw,
                    payload={"source_type": source_name, "result_count": len(result),
                             "enabled": True},
                ))

        # Emit trace for disabled sources
        if self._trace:
            _trace_gw = caller_gateway_id or self._gateway_id
            for src, enabled in all_source_enabled.items():
                if not enabled:
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.RETRIEVAL_SOURCE_RESULT,
                        session_key=session_key or "",
                        gateway_id=_trace_gw,
                        payload={"source_type": src, "result_count": 0, "enabled": False},
                    ))

        # ID-based dedup — keep highest score
        best: dict[str, RetrievalCandidate] = {}
        for c in all_candidates:
            key = str(c.fact.id)
            if key not in best or c.score > best[key].score:
                best[key] = c

        # Post-retrieval isolation filter. TD-61 semantics: auto_recall
        # bypasses isolation-scope filters symmetrically for both
        # SESSION_KEY and ACTOR scopes — explicit-search enforces, auto
        # recall pulls cross-session/actor candidates.
        filtered = list(best.values())
        if policy.isolation_scope == IsolationScope.SESSION_KEY and session_key and not auto_recall:
            filtered = [c for c in filtered if c.fact.session_key == session_key or c.fact.session_key is None]
        elif policy.isolation_scope == IsolationScope.ACTOR and actor_id and not auto_recall:
            filtered = [c for c in filtered
                        if (c.fact.source_actor_id and str(c.fact.source_actor_id) == actor_id)
                        or any(str(t) == actor_id for t in c.fact.target_actor_ids)]

        # Sort by score descending, cap at root_top_k
        filtered.sort(key=lambda c: c.score, reverse=True)
        capped = filtered[:policy.root_top_k]

        logger.info("Retrieved %d candidates from %s", len(capped), source_names)

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.RETRIEVAL_PERFORMED,
                session_id=session_id,
                session_key=session_key,
                payload={
                    "action": "retrieve_candidates", "query": query[:100],
                    "sources": source_names, "results": len(capped),
                    "auto_recall": auto_recall,
                },
            )
        )
        return capped

    # --- Stage 0: Structural (direct Cypher) ---

    async def get_structural_hits(
        self, *, scope: str | None = None, actor_id: str | None = None,
        goal_ids: list[str] | None = None,
        memory_class: MemoryClass | None = None, session_key: str | None = None,
        limit: int = 20,
        auto_recall: bool = False,
        caller_gateway_id: str = "",
    ) -> list[RetrievalCandidate]:
        effective_gw = caller_gateway_id or self._gateway_id
        conditions: list[str] = ["f.gateway_id = $gateway_id"]
        params: dict = {"limit": limit, "gateway_id": effective_gw}
        # Phase 9: exclude archived facts (always)
        conditions.append("(f.archived IS NULL OR f.archived = false)")
        # Phase 9: exclude autorecall-blacklisted facts when auto_recall=True
        if auto_recall:
            conditions.append("(f.autorecall_blacklisted IS NULL OR f.autorecall_blacklisted = false)")
        if scope:
            conditions.append("f.scope = $scope")
            params["scope"] = scope
        # TD-61 symmetry: session_key and actor_id are isolation-scope
        # pre-filters. When auto_recall=True the caller wants cross-session
        # / cross-actor candidates, so the pre-filter must bypass in lockstep
        # with the post-retrieval isolation filter. Content selectors
        # (scope, memory_class, goal_ids) are not isolation filters and
        # remain applied regardless of auto_recall.
        if actor_id and not auto_recall:
            conditions.append("f.source_actor_id = $actor_id")
            params["actor_id"] = actor_id
        if goal_ids:
            conditions.append("ANY(gid IN f.goal_ids WHERE gid IN $goal_ids)")
            params["goal_ids"] = goal_ids
        if memory_class:
            conditions.append("f.memory_class = $memory_class")
            params["memory_class"] = memory_class.value if hasattr(memory_class, "value") else str(memory_class)
        if session_key and not auto_recall:
            conditions.append("f.session_key = $session_key")
            params["session_key"] = session_key

        where = " AND ".join(conditions)
        cypher = (
            f"MATCH (f:FactDataPoint) WHERE {where} "
            "OPTIONAL MATCH (f)-[r]->(target) "
            "RETURN properties(f) AS props, collect({type: type(r), target: properties(target)}) AS relations "
            "LIMIT $limit"
        )
        records = await self._graph.query_cypher(cypher, params)
        candidates: list[RetrievalCandidate] = []
        for rec in records:
            props = clean_graph_props(rec["props"])
            try:
                dp = FactDataPoint(**props)
                fact = dp.to_schema()
                relations = rec.get("relations", [])
                candidates.append(RetrievalCandidate(
                    fact=fact, source="structural", score=1.0,
                    relations=relations if isinstance(relations, list) else [],
                ))
            except Exception:
                continue
        return candidates

    # --- Stage 1: Keyword (Cognee CHUNKS_LEXICAL) ---

    async def get_keyword_hits(self, query: str, dataset: str, limit: int = 20) -> list[RetrievalCandidate]:
        try:
            hits = await cognee.search(
                query_type=SearchType.CHUNKS_LEXICAL,
                query_text=query,
                datasets=[dataset],
            )
            return self._cognee_hits_to_candidates(hits, "keyword")
        except Exception as exc:
            logger.warning("Keyword search failed: %s", exc)
            return []

    # --- Stage 2: Semantic (Cognee CHUNKS) ---

    async def get_semantic_hits_cognee(self, query: str, dataset: str, limit: int = 20) -> list[RetrievalCandidate]:
        try:
            hits = await cognee.search(
                query_type=SearchType.CHUNKS,
                query_text=query,
                datasets=[dataset],
            )
            candidates = self._cognee_hits_to_candidates(hits, "vector")
            # Fallback: Cognee CHUNKS returns DocumentChunk, not FactDataPoint —
            # conversion silently drops them. Fall back to direct Qdrant search.
            if not candidates:
                candidates = await self._get_direct_vector_hits(query, None, limit)
            return candidates
        except Exception as exc:
            logger.warning("Semantic search via Cognee failed, falling back to direct vector: %s", exc)
            return await self._get_direct_vector_hits(query, None, limit)

    # --- Stage 3: Graph neighbors (Cognee GRAPH_COMPLETION) ---

    async def get_graph_neighbors(self, query: str, dataset: str, depth: int = 1) -> list[RetrievalCandidate]:
        try:
            hits = await cognee.search(
                query_type=SearchType.GRAPH_COMPLETION,
                query_text=query,
                only_context=True,
                datasets=[dataset],
            )
            return self._cognee_hits_to_candidates(hits, "graph")
        except Exception as exc:
            logger.warning("Graph completion failed, falling back to BFS: %s", exc)
            return []

    # --- Stage 4: Artifact search ---

    async def _get_artifact_hits(self, query: str, dataset: str, limit: int = 10) -> list[RetrievalCandidate]:
        try:
            hits = await cognee.search(
                query_type=SearchType.CHUNKS,
                query_text=query,
                datasets=[f"{dataset}__artifacts"],
            )
            return self._cognee_hits_to_candidates(hits, "artifact")
        except Exception:
            return []

    # --- Direct Qdrant fallback (for STRICT isolation or Cognee failure) ---

    async def _get_direct_vector_hits(
        self, query: str, session_key: str | None, limit: int = 20,
    ) -> list[RetrievalCandidate]:
        try:
            embedding = await self._embeddings.embed_text(query)
            results = await self._vector.search_similar(
                collection=_FACTS_COLLECTION,
                query_embedding=embedding,
                top_k=limit,
            )
            candidates: list[RetrievalCandidate] = []
            for r in results:
                payload = r.payload if hasattr(r, "payload") else (r if isinstance(r, dict) else {})
                eb_id = payload.get("eb_id") or (r.id if hasattr(r, "id") else None)
                if not eb_id:
                    continue
                entity = await self._graph.get_entity(eb_id)
                if entity is None:
                    continue
                props = clean_graph_props(entity)
                dp = FactDataPoint(**props)
                fact = dp.to_schema()
                score = r.score if hasattr(r, "score") else payload.get("score", 0.5)
                candidates.append(RetrievalCandidate(fact=fact, source="vector", score=score))
            return candidates
        except Exception as exc:
            logger.warning("Direct vector search failed: %s", exc)
            return []

    # --- Helpers ---

    def _cognee_hits_to_candidates(self, hits: list, source: str) -> list[RetrievalCandidate]:
        candidates: list[RetrievalCandidate] = []
        if not hits:
            return candidates
        for item in hits:
            try:
                if isinstance(item, dict):
                    eb_id = item.get("eb_id") or item.get("id")
                    if eb_id:
                        props = clean_graph_props(item)
                        dp = FactDataPoint(**props)
                        candidates.append(RetrievalCandidate(
                            fact=dp.to_schema(), source=source, score=0.8,
                        ))
            except Exception:
                continue
        return candidates

    # --- Backward compatibility ---

    async def get_exact_hits(self, query: str, max_results: int = 20) -> list[FactAssertion]:
        """Legacy: keyword + structural hits as FactAssertions."""
        structural = await self.get_structural_hits(limit=max_results)
        keyword = await self.get_keyword_hits(query, self._dataset_name, max_results)
        seen: set[str] = set()
        facts: list[FactAssertion] = []
        for c in structural + keyword:
            key = str(c.fact.id)
            if key not in seen:
                seen.add(key)
                facts.append(c.fact)
        return facts[:max_results]

    async def get_semantic_hits(self, query: str, max_results: int = 20) -> list[FactAssertion]:
        """Legacy: semantic hits as FactAssertions."""
        candidates = await self.get_semantic_hits_cognee(query, self._dataset_name, max_results)
        return [c.fact for c in candidates][:max_results]
