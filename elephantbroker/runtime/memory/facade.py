"""Memory store facade — unified fact storage via Cognee + structural queries."""
from __future__ import annotations

import asyncio
import logging
import math
import uuid
from datetime import UTC, datetime

import cognee
from cognee.modules.search.types import SearchType
from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
from elephantbroker.runtime.adapters.cognee.embeddings import EmbeddingService
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
from elephantbroker.runtime.graph_utils import clean_graph_props
from elephantbroker.runtime.interfaces.memory_store import IMemoryStoreFacade
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.observability import traced
from elephantbroker.runtime.utils.tokens import count_tokens
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass
from elephantbroker.runtime.metrics import MetricsContext, inc_dedup, inc_edge, inc_gdpr_delete, inc_store
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.memory.facade")

_FACTS_COLLECTION = "FactDataPoint_text"


class DedupSkipped(Exception):
    """Raised when a store is skipped due to near-duplicate detection."""
    def __init__(self, existing_fact_id: str, similarity: float):
        self.existing_fact_id = existing_fact_id
        self.similarity = similarity
        super().__init__(f"Near-duplicate detected (id={existing_fact_id}, score={similarity:.3f})")
_DEFAULT_DEDUP_THRESHOLD = 0.95


class MemoryStoreFacade(IMemoryStoreFacade):

    def __init__(
        self,
        graph: GraphAdapter,
        vector: VectorAdapter,
        embeddings: EmbeddingService,
        trace_ledger: ITraceLedger,
        dataset_name: str = "elephantbroker",
        gateway_id: str = "",
        metrics=None,
    ) -> None:
        self._graph = graph
        self._vector = vector
        self._embeddings = embeddings
        self._trace = trace_ledger
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id
        self._metrics = metrics

    @traced
    async def store(
        self, fact: FactAssertion, *,
        dedup_threshold: float | None = None,
        precomputed_embedding: list[float] | None = None,
    ) -> FactAssertion:
        # Token size + gateway stamp
        fact.gateway_id = fact.gateway_id or self._gateway_id
        fact.token_size = count_tokens(fact.text)
        fact.embedding_ref = f"FactDataPoint_text:{fact.id}"

        # Dedup check — use caller-supplied threshold or fall back to default
        effective_threshold = dedup_threshold if dedup_threshold is not None else _DEFAULT_DEDUP_THRESHOLD
        if effective_threshold is not None:
            embedding = precomputed_embedding or await self._embeddings.embed_text(fact.text)
            try:
                hits = await self._vector.search_similar(_FACTS_COLLECTION, embedding, top_k=1)
                if hits and hits[0].score > effective_threshold:
                    logger.info("Dedup: skipping near-duplicate for fact %s (score=%.3f)", fact.id, hits[0].score)
                    if self._metrics:
                        self._metrics.inc_dedup("skipped")
                    else:
                        inc_dedup("skipped")
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.DEDUP_TRIGGERED,
                        session_key=fact.session_key,
                        session_id=fact.session_id,
                        payload={
                            "fact_text": fact.text[:50], "similarity": hits[0].score,
                            "threshold": effective_threshold, "action": "skipped",
                            "existing_fact_id": hits[0].id,
                        },
                    ))
                    raise DedupSkipped(hits[0].id, hits[0].score)
                if self._metrics:
                    self._metrics.inc_dedup("stored")
                else:
                    inc_dedup("stored")
            except DedupSkipped:
                raise
            except Exception as exc:
                logger.warning("Dedup check failed, proceeding with store: %s", exc)

        # Store via Cognee
        dp = FactDataPoint.from_schema(fact)
        await add_data_points([dp])
        await cognee.add(fact.text, dataset_name=self._dataset_name)

        # Graph edges (best-effort)
        edges_created = 0
        if fact.source_actor_id:
            edges_created += await self._try_add_edge(str(fact.id), str(fact.source_actor_id), "CREATED_BY")
        for target_id in fact.target_actor_ids:
            edges_created += await self._try_add_edge(str(fact.id), str(target_id), "ABOUT_ACTOR")
        for goal_id in fact.goal_ids:
            edges_created += await self._try_add_edge(str(fact.id), str(goal_id), "SERVES_GOAL")

        if self._metrics:
            self._metrics.inc_store("store", "success")
        else:
            inc_store("store", "success")
        logger.info("Stored fact %s (%s, %d tokens)", fact.id, fact.memory_class, fact.token_size or 0)

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.INPUT_RECEIVED,
                payload={"action": "store_fact", "fact_id": str(fact.id), "text": fact.text[:50]},
            )
        )
        return fact

    async def _try_add_edge(self, source: str, target: str, rel_type: str) -> int:
        try:
            await self._graph.add_relation(source, target, rel_type)
            if self._metrics:
                self._metrics.inc_edge(rel_type, True)
            else:
                inc_edge(rel_type, True)
            return 1
        except Exception as exc:
            if self._metrics:
                self._metrics.inc_edge(rel_type, False)
            else:
                inc_edge(rel_type, False)
            logger.warning("Edge creation failed (%s %s→%s): %s", rel_type, source[:8], target[:8], exc)
            return 0

    @traced
    async def search(
        self, query: str, max_results: int = 20, min_score: float = 0.0,
        scope: Scope | None = None, actor_id: str | None = None,
        memory_class: MemoryClass | None = None, session_key: str | None = None,
        profile_name: str = "default", auto_recall: bool = False,
        caller_gateway_id: str = "",
    ) -> list[FactAssertion]:
        results: dict[str, FactAssertion] = {}

        # Stage 1: Semantic — Cognee graph-aware search
        try:
            cognee_hits = await cognee.search(
                query_type=SearchType.GRAPH_COMPLETION,
                query_text=query,
                only_context=True,
                datasets=[self._dataset_name],
            )
            for fact in self._parse_graph_completion_to_facts(cognee_hits):
                results[str(fact.id)] = fact
        except Exception:
            pass

        # Stage 2: Structural — property-filtered Cypher
        cypher, params = self._build_structural_query(
            scope=scope, actor_id=actor_id, memory_class=memory_class,
            session_key=session_key, limit=max_results,
            caller_gateway_id=caller_gateway_id,
        )
        if cypher:
            records = await self._graph.query_cypher(cypher, params)
            for rec in records:
                props = clean_graph_props(rec["props"])
                try:
                    dp = FactDataPoint(**props)
                    fact = dp.to_schema()
                    if str(fact.id) not in results:
                        results[str(fact.id)] = fact
                except Exception:
                    continue

        # Compute freshness scores
        now = datetime.now(UTC)
        for fact in results.values():
            hours_since = (now - fact.updated_at).total_seconds() / 3600
            fact.freshness_score = math.exp(-0.01 * hours_since)

        # Fire-and-forget use_count update
        fact_list = list(results.values())[:max_results]
        if fact_list:
            asyncio.create_task(self._update_use_counts(fact_list))

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.RETRIEVAL_PERFORMED,
                payload={"action": "search", "query": query[:100], "results": len(fact_list)},
            )
        )
        if self._metrics:
            self._metrics.inc_retrieval(auto_recall=str(auto_recall).lower(), profile_name=profile_name)
        return fact_list

    async def _update_use_counts(self, facts: list[FactAssertion]) -> None:
        """Fire-and-forget: increment use_count and last_used_at."""
        try:
            now = datetime.now(UTC)
            dps = []
            for fact in facts:
                fact.use_count += 1
                fact.last_used_at = now
                dps.append(FactDataPoint.from_schema(fact))
            await add_data_points(dps)
        except Exception as exc:
            logger.warning("Failed to update use counts: %s", exc)

    def _build_structural_query(
        self, scope: Scope | None = None, actor_id: str | None = None,
        memory_class: MemoryClass | None = None, session_key: str | None = None,
        limit: int = 100, caller_gateway_id: str = "",
    ) -> tuple[str | None, dict]:
        """Build Cypher for property-filtered structural lookup."""
        effective_gw = caller_gateway_id or self._gateway_id
        conditions: list[str] = ["f.gateway_id = $gateway_id"]
        params: dict = {"limit": limit, "gateway_id": effective_gw}
        if scope:
            conditions.append("f.scope = $scope")
            params["scope"] = scope.value if hasattr(scope, "value") else str(scope)
        if actor_id:
            conditions.append("f.source_actor_id = $actor_id")
            params["actor_id"] = actor_id
        if memory_class:
            conditions.append("f.memory_class = $memory_class")
            params["memory_class"] = memory_class.value if hasattr(memory_class, "value") else str(memory_class)
        if session_key:
            conditions.append("f.session_key = $session_key")
            params["session_key"] = session_key
        where = " AND ".join(conditions)
        cypher = (
            f"MATCH (f:FactDataPoint) WHERE {where} "
            "OPTIONAL MATCH (f)-[r]->(target) "
            "RETURN properties(f) AS props, collect({type: type(r), target: properties(target)}) AS relations "
            "LIMIT $limit"
        )
        return cypher, params

    def _parse_graph_completion_to_facts(self, cognee_hits: list) -> list[FactAssertion]:
        """Extract FactAssertions from GRAPH_COMPLETION results."""
        facts: list[FactAssertion] = []
        if not cognee_hits:
            return facts
        for item in cognee_hits:
            try:
                if isinstance(item, dict):
                    eb_id = item.get("eb_id") or item.get("id")
                    if eb_id:
                        props = clean_graph_props(item)
                        dp = FactDataPoint(**props)
                        facts.append(dp.to_schema())
                elif isinstance(item, str):
                    continue
            except Exception:
                continue
        return facts

    @traced
    async def promote_scope(self, fact_id: uuid.UUID, to_scope: Scope) -> FactAssertion:
        entity = await self._graph.get_entity(str(fact_id))
        if entity is None:
            raise KeyError(f"Fact not found: {fact_id}")

        props = clean_graph_props(entity)
        dp = FactDataPoint(**props)
        fact = dp.to_schema()
        fact.scope = to_scope
        fact.updated_at = datetime.now(UTC)
        fact.gateway_id = fact.gateway_id or self._gateway_id

        updated_dp = FactDataPoint.from_schema(fact)
        await add_data_points([updated_dp])
        return fact

    # Keep old name as alias
    async def promote(self, fact_id: uuid.UUID, to_scope: Scope) -> FactAssertion:
        return await self.promote_scope(fact_id, to_scope)

    @traced
    async def promote_class(self, fact_id: uuid.UUID, to_class: MemoryClass) -> FactAssertion:
        entity = await self._graph.get_entity(str(fact_id))
        if entity is None:
            raise KeyError(f"Fact not found: {fact_id}")

        props = clean_graph_props(entity)
        dp = FactDataPoint(**props)
        fact = dp.to_schema()
        fact.memory_class = to_class
        fact.updated_at = datetime.now(UTC)
        fact.gateway_id = fact.gateway_id or self._gateway_id

        updated_dp = FactDataPoint.from_schema(fact)
        await add_data_points([updated_dp])
        return fact

    @traced
    async def get_by_id(self, fact_id: uuid.UUID) -> FactAssertion | None:
        try:
            entity = await self._graph.get_entity(str(fact_id))
        except Exception:
            return None
        if entity is None:
            return None
        props = clean_graph_props(entity)
        dp = FactDataPoint(**props)
        return dp.to_schema()

    @traced
    async def update(self, fact_id: uuid.UUID, updates: dict) -> FactAssertion:
        entity = await self._graph.get_entity(str(fact_id))
        if entity is None:
            raise KeyError(f"Fact not found: {fact_id}")

        props = clean_graph_props(entity)
        dp = FactDataPoint(**props)
        fact = dp.to_schema()

        # Apply updates
        text_changed = "text" in updates
        for key, value in updates.items():
            if key in ("id", "created_at", "source_actor_id", "gateway_id"):
                continue  # Immutable
            if hasattr(fact, key):
                setattr(fact, key, value)
        fact.updated_at = datetime.now(UTC)

        if text_changed:
            fact.token_size = count_tokens(fact.text)
            fact.embedding_ref = f"FactDataPoint_text:{fact.id}"
            await self._embeddings.embed_text(fact.text)
            await cognee.add(fact.text, dataset_name=self._dataset_name)

        updated_dp = FactDataPoint.from_schema(fact)
        await add_data_points([updated_dp])

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.INPUT_RECEIVED,
                payload={"action": "update_fact", "fact_id": str(fact_id), "fields": list(updates.keys())},
            )
        )
        logger.info("Updated fact %s: %s", fact_id, list(updates.keys()))
        return fact

    @traced
    async def delete(self, fact_id: uuid.UUID, *, caller_gateway_id: str = "") -> None:
        entity = await self._graph.get_entity(str(fact_id))
        if entity is None:
            raise KeyError(f"Fact not found: {fact_id}")

        # GDPR pre-check: verify gateway ownership
        # Use caller-supplied gateway_id (from request headers) if available,
        # otherwise fall back to module's configured gateway_id.
        effective_gw = caller_gateway_id or self._gateway_id
        entity_gw = entity.get("gateway_id", "")
        if entity_gw and entity_gw != effective_gw:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.AUTHORITY_CHECK_FAILED,
                payload={"fact_id": str(fact_id), "owner_gateway": entity_gw, "caller_gateway": effective_gw},
            ))
            raise PermissionError(f"Fact {fact_id} belongs to gateway {entity_gw}, not {effective_gw}")

        # Remove from Neo4j (DETACH DELETE removes node + all edges)
        await self._graph.delete_entity(str(fact_id))

        # Remove from Qdrant (best-effort)
        try:
            await self._vector.delete_embedding(_FACTS_COLLECTION, str(fact_id))
        except Exception as exc:
            logger.warning("Qdrant delete failed for fact %s: %s", fact_id, exc)

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.GDPR_DELETE,
                payload={"fact_id": str(fact_id)},
            )
        )
        if self._metrics:
            self._metrics.inc_gdpr_delete()
            self._metrics.inc_store("delete", "success")
        else:
            inc_gdpr_delete()
            inc_store("delete", "success")
        logger.info("GDPR delete: fact %s", fact_id)

    @traced
    async def decay(self, fact_id: uuid.UUID, factor: float) -> FactAssertion:
        entity = await self._graph.get_entity(str(fact_id))
        if entity is None:
            raise KeyError(f"Fact not found: {fact_id}")

        props = clean_graph_props(entity)
        dp = FactDataPoint(**props)
        fact = dp.to_schema()
        fact.confidence = max(0.0, min(1.0, fact.confidence * factor))
        fact.updated_at = datetime.now(UTC)
        fact.gateway_id = fact.gateway_id or self._gateway_id

        updated_dp = FactDataPoint.from_schema(fact)
        await add_data_points([updated_dp])
        return fact

    @traced
    async def get_by_scope(
        self, scope: Scope, limit: int = 100,
        memory_class: MemoryClass | None = None,
    ) -> list[FactAssertion]:
        conditions = ["f.scope = $scope", "f.gateway_id = $gateway_id"]
        params: dict = {"scope": scope.value, "limit": limit, "gateway_id": self._gateway_id}
        if memory_class:
            conditions.append("f.memory_class = $memory_class")
            params["memory_class"] = memory_class.value
        where = " AND ".join(conditions)
        cypher = f"MATCH (f:FactDataPoint) WHERE {where} RETURN properties(f) AS props LIMIT $limit"
        records = await self._graph.query_cypher(cypher, params)
        facts: list[FactAssertion] = []
        for rec in records:
            props = clean_graph_props(rec["props"])
            try:
                dp = FactDataPoint(**props)
                facts.append(dp.to_schema())
            except Exception:
                continue
        return facts
