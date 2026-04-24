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
from elephantbroker.runtime.interfaces.scrub_buffer import IScrubBuffer
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.memory.cascade_helper import CascadeStatus, cascade_cognee_data
from elephantbroker.runtime.observability import GatewayLoggerAdapter, traced
from elephantbroker.runtime.utils.tokens import count_tokens
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass
from elephantbroker.runtime.metrics import (
    MetricsContext, inc_cognee_capture_failure, inc_dedup, inc_edge,
    inc_fact_delete_cascade_failure, inc_gdpr_delete,
    inc_recent_facts_scrubbed, inc_search_stage_failure, inc_store,
)
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
        ingest_buffer: IScrubBuffer | None = None,
    ) -> None:
        self._graph = graph
        self._vector = vector
        self._embeddings = embeddings
        self._trace = trace_ledger
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id
        self._metrics = metrics
        self._ingest_buffer = ingest_buffer
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})

    @traced
    async def store(
        self, fact: FactAssertion, *,
        dedup_threshold: float | None = None,
        precomputed_embedding: list[float] | None = None,
    ) -> FactAssertion:
        try:
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

            # Store via Cognee. Call cognee.add() first to capture the data_id,
            # then persist the FactDataPoint with the captured id in a single
            # add_data_points() MERGE. Rationale:
            #   (a) avoids a double MERGE on the store hot path, and
            #   (b) eliminates the partial-failure window where the graph node
            #       existed with cognee_data_id=None — such a fact would
            #       permanently orphan the cognee-owned artifacts on delete
            #       because the cascade call had no data_id to pass.
            cognee_add_result = await cognee.add(fact.text, dataset_name=self._dataset_name)
            # TODO-5-003 / TODO-5-211: explicit UUID coercion at capture. A
            # malformed data_id must surface as a capture failure here so it
            # never reaches the cascade parse (TODO-5-109) as a poisoned row.
            # The except tuple includes ValueError so every shape AND every
            # non-UUID-parseable value routes through _emit_capture_failure —
            # one metric + DEGRADED_OPERATION trace regardless of which part
            # of the capture failed.
            # TODO-5-307: the captured id lives as a local string passed to
            # FactDataPoint.from_schema(cognee_data_id=...) below — not on
            # FactAssertion, which is pure semantic schema per layer hygiene.
            captured_data_id: str | None = None
            try:
                raw_data_id = cognee_add_result.data_ingestion_info[0]["data_id"]
                coerced = (
                    raw_data_id if isinstance(raw_data_id, uuid.UUID)
                    else uuid.UUID(str(raw_data_id))
                )
                captured_data_id = str(coerced)
            except (AttributeError, IndexError, KeyError, TypeError, ValueError) as exc:
                await self._emit_capture_failure(
                    operation="store", fact_id=fact.id, exc=exc,
                    session_key=fact.session_key, session_id=fact.session_id,
                )

            dp = FactDataPoint.from_schema(fact, cognee_data_id=captured_data_id)
            await add_data_points([dp])

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
        except DedupSkipped:
            # Dedup is a legitimate skip, not a failure — already observed via
            # inc_dedup("skipped") above. Surface it to the caller unchanged
            # and do NOT increment eb_memory_store_total{status="failure"}.
            raise
        except Exception:
            # Everything else — cognee.add / add_data_points / graph edges /
            # trace append — is a genuine store failure. Emit the failure
            # status BEFORE re-raising so Prometheus sees the outcome even
            # though the API layer will translate this to 5xx for the client.
            if self._metrics:
                self._metrics.inc_store("store", "failure")
            else:
                inc_store("store", "failure")
            raise

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
        session_id: str | None = None,
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
        except Exception as exc:
            # 5-205: downgrade Stage 1 failure to partial results (Stage 2
            # structural may still produce hits) but emit log + metric +
            # DEGRADED_OPERATION trace so the silent failure is visible.
            # TODO-5-510: route through the GatewayLoggerAdapter so the log
            # carries the gateway_id prefix — C18 migration missed this site.
            exc_type = type(exc).__name__
            self._log.warning(
                "facade.search Stage 1 (semantic) failed — downgrading to "
                "structural-only results (query=%r, exc=%s: %s)",
                query[:80], exc_type, exc,
            )
            if self._metrics:
                self._metrics.inc_search_stage_failure("semantic", exc_type)
            else:
                inc_search_stage_failure("semantic", exc_type, gateway_id=self._gateway_id)
            await self._trace.append_event(
                TraceEvent(
                    event_type=TraceEventType.DEGRADED_OPERATION,
                    session_key=session_key,
                    session_id=session_id,
                    payload={
                        "component": "memory_facade",
                        "operation": "search",
                        "failure": "stage_exception",
                        "stage": "semantic",
                        "exception_type": exc_type,
                        "exception": str(exc),
                    },
                )
            )

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
                session_id=session_id,
                session_key=session_key,
                payload={
                    "action": "search", "query": query[:100],
                    "results": len(fact_list), "auto_recall": auto_recall,
                },
            )
        )
        if self._metrics:
            self._metrics.inc_retrieval(auto_recall=str(auto_recall).lower(), profile_name=profile_name)
        return fact_list

    async def _fetch_cognee_data_ids(
        self, fact_ids: list[uuid.UUID | str],
    ) -> dict[str, str | None]:
        """Batch-fetch cognee_data_id per fact from the graph, gateway-scoped.

        TODO-5-008: FactDataPoint.from_schema() defaults cognee_data_id=None.
        Any MERGE-by-ID call site that omits the kwarg wipes the existing
        graph property, re-orphaning TD-50 cascades on a later delete. Call
        sites that don't hold a DP with the value in scope (e.g. post-C21
        fire-and-forget paths whose input is a FactAssertion list — the
        schema layer no longer carries the storage-backend id) must
        round-trip the graph before MERGE.

        Returns a map of eb_id (str) → cognee_data_id (str or None).
        Missing or read-failing ids simply do not appear in the returned
        map; callers that pass an unknown id get None via dict.get().
        """
        if not fact_ids:
            return {}
        ids_as_str = [str(fid) for fid in fact_ids]
        try:
            records = await self._graph.query_cypher(
                "MATCH (f:FactDataPoint) "
                "WHERE f.eb_id IN $ids AND f.gateway_id = $gw "
                "RETURN f.eb_id AS eb_id, f.cognee_data_id AS cognee_data_id",
                {"ids": ids_as_str, "gw": self._gateway_id},
            )
            return {rec["eb_id"]: rec.get("cognee_data_id") for rec in records}
        except Exception as exc:
            self._log.warning(
                "Batch fetch of cognee_data_ids failed (count=%d): %s",
                len(ids_as_str), exc,
            )
            return {}

    async def _update_use_counts(self, facts: list[FactAssertion]) -> None:
        """Fire-and-forget: increment use_count and last_used_at.

        TODO-5-008: batch-fetch cognee_data_ids BEFORE MERGE. FactAssertion
        does not carry the storage-backend id (schema/storage hygiene per
        C21), so passing fact directly to FactDataPoint.from_schema()
        without the kwarg would default cognee_data_id=None and wipe the
        graph property on MERGE — re-orphaning TD-50 cascades for any
        searched-then-deleted fact.
        """
        try:
            now = datetime.now(UTC)
            data_id_map = await self._fetch_cognee_data_ids(
                [f.id for f in facts]
            )
            dps = []
            for fact in facts:
                fact.use_count += 1
                fact.last_used_at = now
                dps.append(FactDataPoint.from_schema(
                    fact, cognee_data_id=data_id_map.get(str(fact.id)),
                ))
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
    async def promote_scope(
        self, fact_id: uuid.UUID, to_scope: Scope, *, caller_gateway_id: str = "",
    ) -> FactAssertion:
        entity = await self._graph.get_entity(str(fact_id))
        if entity is None:
            raise KeyError(f"Fact not found: {fact_id}")

        # Gateway-ownership pre-check (#1168 RESOLVED) — mirrors the update()
        # pattern at facade.py:480-503. Without this, POST /memory/promote-scope
        # was a cross-tenant mutation vector: any caller with a valid session
        # could promote facts owned by another gateway. Empty stored
        # gateway_id passes through (pre-Gateway-Identity facts must remain
        # mutable by their owning runtime). 403 here matches the delete()/
        # update() pattern — the caller already proved id knowledge via POST
        # intent, so hiding the existence oracle adds no security.
        effective_gw = caller_gateway_id or self._gateway_id
        entity_gw = entity.get("gateway_id", "")
        if entity_gw and entity_gw != effective_gw:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.AUTHORITY_CHECK_FAILED,
                payload={
                    "action": "promote_scope",
                    "fact_id": str(fact_id),
                    "owner_gateway": entity_gw,
                    "caller_gateway": effective_gw,
                },
            ))
            raise PermissionError(
                f"Fact {fact_id} belongs to gateway {entity_gw}, not {effective_gw}"
            )

        props = clean_graph_props(entity)
        dp = FactDataPoint(**props)
        fact = dp.to_schema()
        fact.scope = to_scope
        fact.updated_at = datetime.now(UTC)
        fact.gateway_id = fact.gateway_id or self._gateway_id

        # TODO-5-008: carry cognee_data_id through the to_schema/from_schema
        # round-trip. to_schema() drops it (schema/storage separation, C21)
        # so we forward the value from the in-scope `dp` — otherwise MERGE
        # wipes the graph pointer and re-orphans TD-50 cascades.
        updated_dp = FactDataPoint.from_schema(
            fact, cognee_data_id=dp.cognee_data_id,
        )
        await add_data_points([updated_dp])
        return fact

    # Keep old name as alias
    async def promote(
        self, fact_id: uuid.UUID, to_scope: Scope, *, caller_gateway_id: str = "",
    ) -> FactAssertion:
        return await self.promote_scope(fact_id, to_scope, caller_gateway_id=caller_gateway_id)

    @traced
    async def promote_class(
        self, fact_id: uuid.UUID, to_class: MemoryClass, *, caller_gateway_id: str = "",
    ) -> FactAssertion:
        entity = await self._graph.get_entity(str(fact_id))
        if entity is None:
            raise KeyError(f"Fact not found: {fact_id}")

        # Gateway-ownership pre-check (#1169 RESOLVED) — same pattern as
        # promote_scope. POST /memory/promote-class was the third cross-tenant
        # mutation surface; update() was the only one guarded before this
        # commit.
        effective_gw = caller_gateway_id or self._gateway_id
        entity_gw = entity.get("gateway_id", "")
        if entity_gw and entity_gw != effective_gw:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.AUTHORITY_CHECK_FAILED,
                payload={
                    "action": "promote_class",
                    "fact_id": str(fact_id),
                    "owner_gateway": entity_gw,
                    "caller_gateway": effective_gw,
                },
            ))
            raise PermissionError(
                f"Fact {fact_id} belongs to gateway {entity_gw}, not {effective_gw}"
            )

        props = clean_graph_props(entity)
        dp = FactDataPoint(**props)
        fact = dp.to_schema()
        fact.memory_class = to_class
        fact.updated_at = datetime.now(UTC)
        fact.gateway_id = fact.gateway_id or self._gateway_id

        # TODO-5-008: see promote_scope for rationale.
        updated_dp = FactDataPoint.from_schema(
            fact, cognee_data_id=dp.cognee_data_id,
        )
        await add_data_points([updated_dp])
        return fact

    @traced
    async def get_by_id(
        self, fact_id: uuid.UUID, *, caller_gateway_id: str = "",
    ) -> FactAssertion | None:
        try:
            entity = await self._graph.get_entity(str(fact_id))
        except Exception:
            return None
        if entity is None:
            return None

        # Gateway-ownership pre-check (#1167 RESOLVED) — cross-gateway read
        # returns None (not PermissionError). 404 semantic hides the
        # existence oracle: a caller attempting GET /memory/{id} for
        # another tenant's fact cannot distinguish "does not exist" from
        # "exists but not yours", so there is no enumeration side-channel.
        # Mutation paths (update/promote_*/delete) use 403 instead, because
        # the caller has already proved id knowledge via the PATCH/POST/
        # DELETE intent. Empty stored gateway_id passes through for legacy
        # facts (pre-Gateway-Identity).
        effective_gw = caller_gateway_id or self._gateway_id
        entity_gw = entity.get("gateway_id", "")
        if entity_gw and entity_gw != effective_gw:
            return None

        props = clean_graph_props(entity)
        dp = FactDataPoint(**props)
        return dp.to_schema()

    @traced
    async def update(
        self, fact_id: uuid.UUID, updates: dict, *, caller_gateway_id: str = "",
    ) -> FactAssertion:
        try:
            entity = await self._graph.get_entity(str(fact_id))
            if entity is None:
                raise KeyError(f"Fact not found: {fact_id}")

            # Gateway-ownership pre-check — mirrors the delete() pattern.
            # Without this, PATCH /memory/{fact_id} was a cross-tenant mutation
            # vector: any caller with a valid session could modify facts owned
            # by another gateway. We compare the stored gateway_id against the
            # caller-supplied value (from the X-EB-Gateway-ID header via
            # request.state), falling back to the module's configured gateway
            # for in-process callers. Empty stored gateway_id passes through —
            # pre-Gateway-Identity facts exist in the wild and must remain
            # mutable by their owning runtime.
            effective_gw = caller_gateway_id or self._gateway_id
            entity_gw = entity.get("gateway_id", "")
            if entity_gw and entity_gw != effective_gw:
                await self._trace.append_event(TraceEvent(
                    event_type=TraceEventType.AUTHORITY_CHECK_FAILED,
                    payload={
                        "action": "update",
                        "fact_id": str(fact_id),
                        "owner_gateway": entity_gw,
                        "caller_gateway": effective_gw,
                    },
                ))
                raise PermissionError(
                    f"Fact {fact_id} belongs to gateway {entity_gw}, not {effective_gw}"
                )

            props = clean_graph_props(entity)
            dp = FactDataPoint(**props)
            fact = dp.to_schema()
            # TODO-5-307: the existing cognee_data_id lives on the FactDataPoint
            # (storage layer), not FactAssertion (pure semantic). Read it from
            # the DP — same shape delete() uses when it reads the graph entity
            # dict directly. Coerce the stored string to uuid.UUID so the
            # cascade call site receives the exact type it received pre-refactor.
            old_cognee_data_id: uuid.UUID | None = None
            if dp.cognee_data_id:
                try:
                    old_cognee_data_id = uuid.UUID(dp.cognee_data_id)
                except (ValueError, TypeError):
                    # Legacy corrupted value; TODO-5-109 cascade will skip.
                    old_cognee_data_id = None

            text_changed = "text" in updates
            for key, value in updates.items():
                if key in ("id", "created_at", "source_actor_id", "gateway_id"):
                    continue  # Immutable
                if hasattr(fact, key):
                    setattr(fact, key, value)
            fact.updated_at = datetime.now(UTC)

            # Default: preserve existing id (metadata-only update must not
            # null the Cognee-side pointer on MERGE).
            new_cognee_data_id: uuid.UUID | None = old_cognee_data_id

            if text_changed:
                fact.token_size = count_tokens(fact.text)
                fact.embedding_ref = f"FactDataPoint_text:{fact.id}"
                # TODO-5-612 / TODO-5-701: no standalone embed_text() call on
                # the update path — the subsequent cognee.add() re-embeds
                # internally via Cognee's ingest pipeline, and the update
                # path (unlike store()) has no dedup pre-check to consume
                # an external embedding. A second embed_text() here was
                # pure waste.
                # Re-ingest the new text into Cognee and refresh the data_id
                # BEFORE persisting. Without this, the graph node keeps
                # pointing at the pre-update document and the cognee-side
                # artifacts for the NEW text become permanent orphans that
                # delete() cannot reach — the TD-50 regression in update().
                cognee_add_result = await cognee.add(fact.text, dataset_name=self._dataset_name)
                # TODO-5-003 / TODO-5-211: same UUID coercion shape as store().
                # See the store-path comment for the full rationale.
                try:
                    raw_data_id = cognee_add_result.data_ingestion_info[0]["data_id"]
                    new_cognee_data_id = (
                        raw_data_id if isinstance(raw_data_id, uuid.UUID)
                        else uuid.UUID(str(raw_data_id))
                    )
                except (AttributeError, IndexError, KeyError, TypeError, ValueError) as exc:
                    await self._emit_capture_failure(
                        operation="update", fact_id=fact.id, exc=exc,
                        session_key=fact.session_key, session_id=fact.session_id,
                    )
                    new_cognee_data_id = None

            updated_dp = FactDataPoint.from_schema(
                fact,
                cognee_data_id=str(new_cognee_data_id) if new_cognee_data_id else None,
            )
            await add_data_points([updated_dp])

            # Cascade the superseded cognee doc only after the graph node points
            # at the new one — so an observer never sees the fact referencing a
            # half-deleted doc. Metadata-only updates (no text change) never
            # refresh the data_id and must not cascade.
            # TODO-5-110: mirror delete()'s emit-on-failure pattern so a
            # cascade "failed" status on the update path also produces the
            # observability trio (warn log inside helper + metric +
            # DEGRADED_OPERATION trace). Pre-fix, update-path cascade
            # failures were silent — the superseded Cognee doc would leak
            # without any dashboard signal.
            if (
                text_changed
                and old_cognee_data_id
                and old_cognee_data_id != new_cognee_data_id
            ):
                update_cascade_status = await self._cascade_cognee_data(
                    old_cognee_data_id, fact_id=fact_id, context="update_text_change",
                )
                if update_cascade_status == "failed":
                    await self._emit_cascade_failure(
                        step="cognee_data", fact_id=fact_id,
                        exc=RuntimeError(
                            f"cognee.datasets.delete_data failed for data_id={old_cognee_data_id}"
                        ),
                        session_key=fact.session_key, session_id=fact.session_id,
                        operation="update",
                    )

            await self._trace.append_event(
                TraceEvent(
                    event_type=TraceEventType.INPUT_RECEIVED,
                    payload={"action": "update_fact", "fact_id": str(fact_id), "fields": list(updates.keys())},
                )
            )
            if self._metrics:
                self._metrics.inc_store("update", "success")
            else:
                inc_store("update", "success")
            logger.info("Updated fact %s: %s", fact_id, list(updates.keys()))
            return fact
        except Exception:
            # KeyError (fact not found), PermissionError (cross-tenant), or any
            # failure from cognee.add / add_data_points / cascade / trace.
            # Emit eb_memory_store_total{operation="update", status="failure"}
            # BEFORE re-raising so Prometheus sees the outcome; the route layer
            # translates KeyError → 404, PermissionError → 403, other → 5xx.
            if self._metrics:
                self._metrics.inc_store("update", "failure")
            else:
                inc_store("update", "failure")
            raise

    async def _emit_cascade_failure(
        self, *, step: str, fact_id: uuid.UUID, exc: Exception,
        session_key: str | None = None, session_id: uuid.UUID | None = None,
        operation: str = "delete",
    ) -> None:
        """Observability for a failed TD-50 cascade step.

        Fires when one of the cascade layers (graph, vector, cognee_data)
        raises during delete — or when update()'s post-text-change cascade
        of the superseded cognee doc fails (TODO-5-110). The EB-layer
        operation continues on each failure so the eventual GDPR_DELETE /
        updated-fact trace is emitted even on partial cascade failure —
        but we still need a per-step signal so dashboards can distinguish
        "clean" from "acknowledged but one layer is lagging". Emits a
        metric + DEGRADED_OPERATION trace identifying which step failed +
        the fact id; existing WARNING logs at the call site are preserved.

        `operation` tags the parent op ("delete" | "update") in the trace
        payload so a single metric + trace type covers both code paths
        while keeping them distinguishable in audit.
        """
        if self._metrics:
            self._metrics.inc_fact_delete_cascade_failure(step, operation=operation)
        else:
            inc_fact_delete_cascade_failure(
                step, operation=operation, gateway_id=self._gateway_id,
            )
        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.DEGRADED_OPERATION,
                session_key=session_key,
                session_id=session_id,
                payload={
                    "component": "memory_facade",
                    "operation": operation,
                    "failure": "cascade_step",
                    "step": step,
                    "fact_id": str(fact_id),
                    "exception_type": type(exc).__name__,
                    "exception": str(exc),
                },
            )
        )

    async def _emit_capture_failure(
        self, *, operation: str, fact_id: uuid.UUID, exc: Exception,
        session_key: str | None = None, session_id: uuid.UUID | None = None,
    ) -> None:
        """Observability for TD-50 silent-failure path.

        Fires when cognee.add() returns a shape we cannot extract a data_id
        from. The fact is still persisted with cognee_data_id=None, but the
        delete cascade will not be able to reach the Cognee-owned document —
        which is exactly the class of orphan TD-50 exists to prevent. We
        emit a metric + DEGRADED_OPERATION trace so the silent degradation
        is visible to the observability stack; the existing WARNING log is
        retained for operator eyeballs.
        """
        if self._metrics:
            self._metrics.inc_cognee_capture_failure(operation)
        else:
            inc_cognee_capture_failure(operation, gateway_id=self._gateway_id)
        self._log.warning(
            "Could not capture cognee_data_id for fact %s on %s "
            "(delete cascade will skip cognee cleanup): %s",
            fact_id, operation, exc,
        )
        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.DEGRADED_OPERATION,
                session_key=session_key,
                session_id=session_id,
                payload={
                    "component": "memory_facade",
                    "operation": operation,
                    "failure": "cognee_data_id_capture",
                    "fact_id": str(fact_id),
                    "exception_type": type(exc).__name__,
                    "exception": str(exc),
                },
            )
        )

    async def _cascade_cognee_data(
        self, cognee_data_id, *, fact_id: uuid.UUID, context: str,
    ) -> CascadeStatus:
        """Thin wrapper over `memory.cascade_helper.cascade_cognee_data`.

        TODO-5-314: shared with canonicalize's superseded-doc cleanup. See
        `elephantbroker/runtime/memory/cascade_helper.py` for the pin-
        invariant docstring (TODO-5-006), status-code contract, and
        TD-Cognee-Qdrant-404 recovery rationale.

        TODO-5-410: return-type narrowed from bare `str` to the
        `CascadeStatus` Literal alias so mypy/pyright catch typo'd status
        comparisons (`== "okay"`) at the call sites that consume the
        return value (`facade.update`, `facade.delete`).
        """
        return await cascade_cognee_data(
            cognee_data_id,
            dataset_name=self._dataset_name,
            fact_id=fact_id,
            context=context,
            log=self._log,
        )

    @traced
    async def delete(self, fact_id: uuid.UUID, *, caller_gateway_id: str = "") -> None:
        try:
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

            # Extract session fields for enriched trace payloads + TraceEvent
            # routing. Stored session_id is a string on the graph node; parse to
            # UUID for the TraceEvent field (payload keeps the raw string).
            session_key_val: str | None = entity.get("session_key") or None
            session_id_raw = entity.get("session_id")
            session_id_val: uuid.UUID | None = None
            if session_id_raw:
                try:
                    session_id_val = uuid.UUID(str(session_id_raw))
                except (ValueError, TypeError):
                    session_id_val = None

            # 5-210: Scrub recent_facts BEFORE the graph delete, not after.
            # Rationale — the previous order (scrub after cascade) left a
            # window between graph.delete_entity and scrub_fact_from_recent
            # where a concurrent turn-ingest cycle could observe the
            # still-cached entry and keep the deleted fact's text alive in
            # the extraction-context window. Scrubbing first closes that
            # window: after the graph delete, the recent_facts window is
            # already clean. A narrow residual race remains if an ingest was
            # already mid-flight before scrub began, but the simple pre-order
            # removes the common case without needing a lock pattern.
            if self._ingest_buffer is not None and session_key_val:
                try:
                    removed = await self._ingest_buffer.scrub_fact_from_recent(
                        session_key_val, str(fact_id),
                    )
                    scrub_status = "scrubbed" if removed else "noop"
                except Exception as exc:
                    self._log.warning("recent_facts scrub failed for fact %s: %s", fact_id, exc)
                    scrub_status = "failure"
                if self._metrics:
                    self._metrics.inc_recent_facts_scrubbed(scrub_status)
                else:
                    inc_recent_facts_scrubbed(scrub_status, gateway_id=self._gateway_id)

            # Three-step cascade. Each step runs independently — a failure in
            # any one layer must not short-circuit the remaining layers (TD-50
            # + 5-607). Per-step failures emit DEGRADED_OPERATION + metric via
            # _emit_cascade_failure; the aggregate cascade_status is stamped
            # onto GDPR_DELETE so auditors can tell clean-delete from
            # partial-failure without cross-referencing the degraded-ops stream.

            # Step 1 — Neo4j (DETACH DELETE removes node + all edges)
            try:
                await self._graph.delete_entity(str(fact_id))
                graph_status = "ok"
            except Exception as exc:
                self._log.warning("Neo4j delete failed for fact %s: %s", fact_id, exc)
                graph_status = "failed"
                await self._emit_cascade_failure(
                    step="graph", fact_id=fact_id, exc=exc,
                    session_key=session_key_val, session_id=session_id_val,
                )

            # Step 2 — Qdrant (best-effort)
            try:
                await self._vector.delete_embedding(_FACTS_COLLECTION, str(fact_id))
                vector_status = "ok"
            except Exception as exc:
                self._log.warning("Qdrant delete failed for fact %s: %s", fact_id, exc)
                vector_status = "failed"
                await self._emit_cascade_failure(
                    step="vector", fact_id=fact_id, exc=exc,
                    session_key=session_key_val, session_id=session_id_val,
                )

            # Step 3 — Cascade Cognee-owned artifacts (TD-50). cognee.datasets.
            # delete_data removes chunks/documents/summaries in Neo4j, chunk
            # points across Qdrant collections, SQLite rows, and the
            # .data_storage file. _cascade_cognee_data captures its own
            # exceptions and returns a status string; we re-emit DEGRADED_OP at
            # this call site so the per-step metric carries step=cognee_data.
            cognee_data_id = entity.get("cognee_data_id") if isinstance(entity, dict) else None
            if cognee_data_id:
                cognee_data_status = await self._cascade_cognee_data(
                    cognee_data_id, fact_id=fact_id, context="delete",
                )
                if cognee_data_status == "failed":
                    await self._emit_cascade_failure(
                        step="cognee_data", fact_id=fact_id,
                        exc=RuntimeError(
                            f"cognee.datasets.delete_data failed for data_id={cognee_data_id}"
                        ),
                        session_key=session_key_val, session_id=session_id_val,
                    )
            else:
                self._log.info(
                    "TD-50 cascade skipped: fact %s has no cognee_data_id (pre-TD-50 fact)",
                    fact_id,
                )
                cognee_data_status = "skipped_no_data_id"

            # GDPR_DELETE audit event — emitted on every delete that reached
            # this point, INCLUDING partial-cascade failures. cascade_status
            # records the per-step outcome so downstream auditors can reason
            # about the completeness of the delete without stitching together
            # the degraded-operation stream. session_key/session_id are
            # promoted to first-class TraceEvent fields so SessionTimeline and
            # the /trace search surface can filter by the originating session
            # (the merge report's GDPR flow claim depends on this — without
            # the session fields a /trace?session_key=... query would miss
            # delete events).
            await self._trace.append_event(
                TraceEvent(
                    event_type=TraceEventType.GDPR_DELETE,
                    session_key=session_key_val,
                    session_id=session_id_val,
                    payload={
                        "fact_id": str(fact_id),
                        "session_key": session_key_val,
                        "cascade_status": {
                            "graph": graph_status,
                            "vector": vector_status,
                            "cognee_data": cognee_data_status,
                        },
                    },
                )
            )
            if self._metrics:
                self._metrics.inc_gdpr_delete()
                self._metrics.inc_store("delete", "success")
            else:
                inc_gdpr_delete()
                inc_store("delete", "success")
            logger.info("GDPR delete: fact %s", fact_id)
        except Exception:
            # KeyError (fact not found), PermissionError (cross-tenant), or
            # any unhandled fall-through from the cascade / scrub / trace
            # path. The individual cascade steps already self-capture their
            # own exceptions and emit per-step cascade-failure metrics, so
            # anything surfacing here is a pre-cascade or post-cascade
            # failure (e.g., trace ledger unavailable). Emit
            # eb_memory_store_total{operation="delete", status="failure"}
            # BEFORE re-raising so Prometheus sees the aggregate outcome
            # alongside the per-step observability.
            if self._metrics:
                self._metrics.inc_store("delete", "failure")
            else:
                inc_store("delete", "failure")
            raise

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

        # TODO-5-008: see promote_scope for rationale.
        updated_dp = FactDataPoint.from_schema(
            fact, cognee_data_id=dp.cognee_data_id,
        )
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
