"""Stage 2: Canonicalize Stable Facts — LLM smart merge for non-identical clusters.

Creates NEW canonical fact per cluster, archives ALL originals (AD-3).
LLM calls bounded by context.llm_calls_cap.
"""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from elephantbroker.runtime.metrics import (
    inc_cognee_capture_failure,
    inc_fact_delete_cascade_failure,
)
from elephantbroker.runtime.observability import GatewayLoggerAdapter, traced
from elephantbroker.schemas.consolidation import CanonicalResult
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.cognee.cached_embeddings import CachedEmbeddingService
    from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
    from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
    from elephantbroker.runtime.adapters.llm.client import LLMClient
    from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
    # TODO-5-410: Literal alias imported under TYPE_CHECKING so static
    # checkers resolve the return-type annotation on the thin wrapper
    # below without adding a runtime import dependency — matches the
    # existing function-local `cascade_cognee_data` import pattern.
    from elephantbroker.runtime.memory.cascade_helper import CascadeStatus
    from elephantbroker.runtime.metrics import MetricsContext
    from elephantbroker.schemas.consolidation import (
        ConsolidationConfig,
        ConsolidationContext,
        DuplicateCluster,
    )
    from elephantbroker.schemas.fact import FactAssertion

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.canonicalize")

_MERGE_PROMPT = """These facts describe the same thing. Synthesize a single, precise, \
canonical statement that captures the best information from all versions.
Preserve specific details (names, numbers, versions) over vague ones.
Return ONLY the synthesized statement text, nothing else.

Facts:
{fact_texts}"""


class CanonicalizationStage:
    """LLM-powered smart merge for duplicate clusters.

    For each cluster:
    - If all texts identical → deterministic merge (no LLM)
    - Otherwise → LLM synthesizes canonical text
    - Creates NEW FactAssertion with merged fields
    - Archives ALL originals (archived=True, confidence=0.0, Qdrant embedding deleted)
    - Creates SUPERSEDED_BY edges from originals → canonical
    """

    def __init__(
        self,
        graph: GraphAdapter,
        vector: VectorAdapter,
        llm_client: LLMClient | None,
        embedding_service: CachedEmbeddingService,
        config: ConsolidationConfig,
        dataset_name: str,
        trace_ledger: ITraceLedger | None = None,
        metrics: MetricsContext | None = None,
        gateway_id: str = "",
    ) -> None:
        self._graph = graph
        self._vector = vector
        self._llm = llm_client
        self._embeddings = embedding_service
        self._config = config
        self._dataset_name = dataset_name
        self._trace = trace_ledger
        self._metrics = metrics
        # TODO-5-509: hold gateway_id so the bare-function metric paths
        # (used when self._metrics is None — e.g. unit tests) can label
        # the counter correctly. The wrapped MetricsContext path already
        # carries gateway_id internally; the bare path did not.
        self._gateway_id = gateway_id
        # TODO-5-902: GatewayLoggerAdapter so the cascade_helper call site
        # below emits `[gateway_id]`-prefixed logs (parity with facade's
        # cascade path). Module logger is still used for non-cascade
        # INFO/WARN lines in this stage.
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})

    @traced
    async def run(
        self,
        clusters: list[DuplicateCluster],
        facts: list[FactAssertion],
        gateway_id: str,
        context: ConsolidationContext,
    ) -> list[CanonicalResult]:

        facts_by_id: dict[str, FactAssertion] = {str(f.id): f for f in facts}
        results: list[CanonicalResult] = []

        for cluster in clusters:
            members = [facts_by_id[fid] for fid in cluster.fact_ids if fid in facts_by_id]
            if len(members) < 2:
                continue

            # Check LLM call budget
            texts_unique = {m.text.strip() for m in members}
            needs_llm = len(texts_unique) > 1
            if needs_llm and (not self._llm or context.llm_calls_used >= context.llm_calls_cap):
                logger.warning("LLM cap reached or no LLM — skipping non-identical cluster %s", cluster.cluster_id)
                continue

            try:
                result = await self._canonicalize_cluster(
                    cluster, members, gateway_id, context, needs_llm,
                )
                if result:
                    results.append(result)
            except Exception:
                logger.warning("Failed to canonicalize cluster %s", cluster.cluster_id, exc_info=True)

        logger.info(
            "Stage 2: %d clusters → %d canonicalized (gateway=%s)",
            len(clusters), len(results), gateway_id,
        )
        return results

    async def _canonicalize_cluster(
        self,
        cluster: DuplicateCluster,
        members: list[FactAssertion],
        gateway_id: str,
        context: ConsolidationContext,
        needs_llm: bool,
    ) -> CanonicalResult | None:
        from cognee.tasks.storage import add_data_points

        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        from elephantbroker.schemas.base import Scope
        from elephantbroker.schemas.fact import FactAssertion as FactModel
        from elephantbroker.schemas.fact import MemoryClass

        # Determine canonical text
        if needs_llm:
            fact_texts = "\n".join(f"- {m.text}" for m in members)
            prompt = _MERGE_PROMPT.format(fact_texts=fact_texts)
            try:
                canonical_text = await self._llm.complete(
                    system_prompt="You are a knowledge synthesizer.",
                    user_prompt=prompt,
                    max_tokens=500,
                )
                context.llm_calls_used += 1
            except Exception:
                logger.warning("LLM merge failed for cluster %s", cluster.cluster_id, exc_info=True)
                return None
        else:
            # All identical text — use highest-confidence member's text
            canonical_text = max(members, key=lambda m: m.confidence).text

        # Determine best values from all members
        best = max(members, key=lambda m: (m.confidence, m.updated_at))

        # Scope ordering for "broadest"
        scope_order = {
            Scope.SESSION: 0, Scope.ACTOR: 1, Scope.TEAM: 2,
            Scope.ORGANIZATION: 3, Scope.GLOBAL: 4,
        }
        broadest_scope = max(
            members,
            key=lambda m: scope_order.get(m.scope, 0),
        ).scope

        # Memory class ordering for "most promoted"
        class_order = {
            MemoryClass.WORKING_MEMORY: 0, MemoryClass.EPISODIC: 1,
            MemoryClass.SEMANTIC: 2, MemoryClass.PROCEDURAL: 3, MemoryClass.POLICY: 4,
        }
        most_promoted_class = max(
            members,
            key=lambda m: class_order.get(m.memory_class, 0),
        ).memory_class

        # Create NEW canonical fact
        new_id = uuid.uuid4()
        merged_provenance = list({ref for m in members for ref in m.provenance_refs})
        merged_goal_ids_str = list({str(gid) for m in members for gid in m.goal_ids})
        merged_use_count = sum(m.use_count for m in members)
        merged_suc = sum(m.successful_use_count for m in members)

        new_fact = FactModel(
            id=new_id,
            text=canonical_text.strip(),
            category=best.category,
            scope=broadest_scope,
            confidence=max(m.confidence for m in members),
            memory_class=most_promoted_class,
            session_key=None,  # Cross-session canonical
            session_id=None,
            source_actor_id=best.source_actor_id,
            target_actor_ids=list({uid for m in members for uid in m.target_actor_ids}),
            goal_ids=[uuid.UUID(g) for g in merged_goal_ids_str],
            created_at=min(m.created_at for m in members),
            updated_at=datetime.now(UTC),
            use_count=merged_use_count,
            successful_use_count=merged_suc,
            provenance_refs=merged_provenance,
            gateway_id=gateway_id,
            decision_domain=best.decision_domain,
        )

        # TD-50 / BS-10: Cognee-first to capture data_id on the canonical
        # fact BEFORE the graph MERGE. Mirrors facade.store() (C1). Without
        # this, the canonical graph node is persisted with
        # cognee_data_id=None and a future delete cascade silently
        # orphans the Cognee-owned document — same class of bug the
        # facade fix closed.
        # TODO-5-313: dataset_name is threaded from ConsolidationEngine →
        # container (which composes it from gateway_id + EB_DEFAULT_DATASET).
        # Do NOT hardcode "elephantbroker" here — a deployment with a custom
        # EB_DEFAULT_DATASET would silently canonicalize into the wrong
        # Cognee dataset, leaving the intended one without canonicals.
        import cognee
        cognee_add_result = None
        try:
            cognee_add_result = await cognee.add(
                canonical_text.strip(), dataset_name=self._dataset_name,
            )
        except Exception:
            logger.debug("cognee.add failed for canonical — non-fatal", exc_info=True)

        # TODO-5-307: canonical data_id is captured as a local string and
        # passed to FactDataPoint.from_schema(cognee_data_id=...) below.
        # FactAssertion no longer carries the storage-backend identifier.
        canonical_data_id: str | None = None
        if cognee_add_result is not None:
            # TODO-5-003 / TODO-5-211: explicit UUID coercion at capture
            # (ValueError now in the except tuple). Mirrors the two sites
            # in facade.py — see that file's store-path block for the full
            # rationale. A malformed data_id must surface as a capture
            # failure here so it never reaches cascade parse as a poisoned
            # row.
            try:
                raw_data_id = cognee_add_result.data_ingestion_info[0]["data_id"]
                coerced = (
                    raw_data_id if isinstance(raw_data_id, uuid.UUID)
                    else uuid.UUID(str(raw_data_id))
                )
                canonical_data_id = str(coerced)
            except (AttributeError, IndexError, KeyError, TypeError, ValueError) as exc:
                await self._emit_capture_failure(fact_id=new_fact.id, exc=exc)

        # Store canonical via Cognee-first dual write (BS-10)
        dp = FactDataPoint.from_schema(new_fact, cognee_data_id=canonical_data_id)
        try:
            await add_data_points([dp])
        except Exception:
            logger.warning("add_data_points failed for canonical %s", new_id, exc_info=True)
            return None

        # TODO-5-307: fetch each member's current cognee_data_id from the
        # graph before archival. The in-memory `members` list carries
        # FactAssertion (pure semantic) which no longer exposes this
        # storage-backend identifier. Reading from the graph entity dict
        # is the same shape facade.delete() uses, so the archive MERGE
        # below can pass the existing data_id through and avoid nulling
        # the graph property on write-back.
        member_cognee_data_ids: dict[uuid.UUID, str | None] = {}
        for member in members:
            try:
                member_entity = await self._graph.get_entity(str(member.id))
                if isinstance(member_entity, dict):
                    raw = member_entity.get("cognee_data_id")
                    member_cognee_data_ids[member.id] = str(raw) if raw else None
                else:
                    member_cognee_data_ids[member.id] = None
            except Exception as exc:
                # TODO-5-113: a preload failure here silently strands the
                # member's superseded cognee_data_id — the cascade loop at
                # line 308 skips None entries, so the Cognee document
                # becomes an orphan with no dashboard signal. Upgrade the
                # silent logger.debug to the observability trio (WARNING +
                # metric + DEGRADED_OPERATION trace) used by every other
                # TD-50 silent-failure site. operation="canonicalize_preload"
                # keeps this distinguishable from the canonicalize capture
                # failure at line 338 so dashboards can split the two.
                await self._emit_preload_failure(fact_id=member.id, exc=exc)
                member_cognee_data_ids[member.id] = None

        # Collect the superseded facts' cognee_data_ids. Each old id points
        # at a Cognee document that is now dead weight — the canonical's
        # new document supersedes it. These must be cascaded after the
        # graph nodes are re-MERGEd as archived, so an observer never sees
        # an archived fact whose cognee_data_id already points at a
        # half-deleted document.
        superseded_data_ids: list[tuple[uuid.UUID, object]] = [
            (fid, cdi)
            for fid, cdi in member_cognee_data_ids.items()
            if cdi
        ]

        # Archive ALL originals (AD-3)
        archived_ids: list[str] = []
        for member in members:
            member.archived = True
            member.confidence = 0.0
            archived_dp = FactDataPoint.from_schema(
                member, cognee_data_id=member_cognee_data_ids.get(member.id),
            )
            try:
                await add_data_points([archived_dp])
                archived_ids.append(str(member.id))
            except Exception:
                logger.warning("Failed to archive fact %s", member.id, exc_info=True)

            # Delete Qdrant embedding
            try:
                await self._vector.delete_embedding("FactDataPoint_text", str(member.id))
            except Exception:
                logger.debug("Qdrant delete failed for %s — non-fatal", member.id)

            # Create SUPERSEDED_BY edge
            try:
                await self._graph.add_relation(
                    str(member.id), str(new_id), "SUPERSEDED_BY",
                )
            except Exception:
                logger.debug("SUPERSEDED_BY edge failed for %s", member.id)

        # Cascade superseded Cognee documents. Best-effort per-item — a
        # failure for one superseded id does not block the others.
        # TODO-5-901: consume the CascadeStatus return value and emit the
        # observability trio (metric + DEGRADED_OPERATION trace) on
        # "failed", mirroring facade.delete / facade.update. Pre-fix the
        # status was discarded — a failed superseded-doc cleanup left a
        # Cognee orphan with no dashboard signal.
        for member_id, old_data_id in superseded_data_ids:
            cascade_status = await self._cascade_superseded_data_id(
                old_data_id, fact_id=member_id,
            )
            if cascade_status == "failed":
                await self._emit_superseded_cascade_failure(
                    fact_id=member_id, cognee_data_id=old_data_id,
                )

        return CanonicalResult(
            cluster_id=cluster.cluster_id,
            new_canonical_fact_id=str(new_id),
            canonical_text=canonical_text.strip(),
            archived_fact_ids=archived_ids,
            merged_provenance=merged_provenance,
            merged_use_count=merged_use_count,
            merged_successful_use_count=merged_suc,
            merged_goal_ids=merged_goal_ids_str,
            llm_used=needs_llm,
        )

    async def _emit_capture_failure(
        self, *, fact_id: uuid.UUID, exc: Exception,
    ) -> None:
        """TD-50 silent-failure observability for the canonicalize path.

        Fires when cognee.add() returns a shape we cannot extract a
        data_id from. The canonical fact is still persisted with
        cognee_data_id=None, but the delete cascade will not be able to
        reach the Cognee-owned document — the orphan TD-50 exists to
        prevent. Emits the same metric + DEGRADED_OPERATION trace shape
        as facade.store()/update(), with operation="canonicalize" so
        dashboards can split capture failures by call site.
        """
        if self._metrics:
            self._metrics.inc_cognee_capture_failure("canonicalize")
        else:
            # TODO-5-509: thread gateway_id through the bare-function
            # path so the metric label matches the MetricsContext path.
            # Before fix: label was the empty-string default.
            inc_cognee_capture_failure("canonicalize", gateway_id=self._gateway_id)
        logger.warning(
            "Could not capture cognee_data_id for canonical fact %s "
            "(delete cascade will skip cognee cleanup): %s",
            fact_id, exc,
        )
        if self._trace is not None:
            await self._trace.append_event(
                TraceEvent(
                    event_type=TraceEventType.DEGRADED_OPERATION,
                    payload={
                        "component": "consolidation_canonicalize",
                        "operation": "canonicalize",
                        "failure": "cognee_data_id_capture",
                        "fact_id": str(fact_id),
                        "exception_type": type(exc).__name__,
                        "exception": str(exc),
                    },
                )
            )

    async def _emit_preload_failure(
        self, *, fact_id: uuid.UUID, exc: Exception,
    ) -> None:
        """TODO-5-113: observability trio for a failed graph preload.

        Fires when `self._graph.get_entity` raises while the canonicalize
        stage is preloading the superseded members' cognee_data_ids. The
        preload's fallback value is None, which downstream elides the
        cascade for that member — so without a signal the Cognee document
        silently becomes an orphan. Emits the same trio as
        `_emit_capture_failure` (WARNING log + metric + DEGRADED_OPERATION
        trace) but with `operation="canonicalize_preload"` so dashboards
        can split the preload-miss failure mode from the capture-miss
        failure mode. `gateway_id` is threaded through the bare-function
        metric path (TODO-5-509) so the label is always populated.
        """
        if self._metrics:
            self._metrics.inc_cognee_capture_failure("canonicalize_preload")
        else:
            inc_cognee_capture_failure(
                "canonicalize_preload", gateway_id=self._gateway_id,
            )
        logger.warning(
            "Preload of cognee_data_id failed for member %s "
            "(superseded-cascade will skip this member): %s",
            fact_id, exc,
        )
        if self._trace is not None:
            await self._trace.append_event(
                TraceEvent(
                    event_type=TraceEventType.DEGRADED_OPERATION,
                    payload={
                        "component": "consolidation_canonicalize",
                        "operation": "canonicalize_preload",
                        "failure": "get_entity_exception",
                        "fact_id": str(fact_id),
                        "exception_type": type(exc).__name__,
                        "exception": str(exc),
                    },
                )
            )

    async def _cascade_superseded_data_id(
        self, cognee_data_id, *, fact_id: uuid.UUID,
    ) -> CascadeStatus:
        """Thin wrapper over `memory.cascade_helper.cascade_cognee_data`.

        TODO-5-314: shared with the memory facade's delete/update cascade.
        See `elephantbroker/runtime/memory/cascade_helper.py` for the
        pin-invariant docstring (TODO-5-006), status-code contract, and
        TD-Cognee-Qdrant-404 recovery rationale. Prior to this extraction
        canonicalize carried a near-copy that was MISSING the 404 recovery
        branch — a cluster canonicalized against a never-cognify()'d
        member would 404 mid-cascade and leave the Data↔Dataset
        association orphaned. Best-effort: the helper returns a status
        string and does not raise, so one dead document does not block
        the remaining cascades in the enclosing loop.

        TODO-5-410: return-type narrowed from `None` to the `CascadeStatus`
        Literal alias — the one caller currently discards the value, but
        propagating the typed status keeps the wrapper truthful and
        available to a future observability extension (e.g. emitting a
        DEGRADED_OPERATION trace here when status == "failed", mirroring
        the facade.delete / facade.update pattern).
        """
        from elephantbroker.runtime.memory.cascade_helper import (
            cascade_cognee_data,
        )
        return await cascade_cognee_data(
            cognee_data_id,
            dataset_name=self._dataset_name,
            fact_id=fact_id,
            context="consolidation_canonicalize",
            log=self._log,
        )

    async def _emit_superseded_cascade_failure(
        self, *, fact_id: uuid.UUID, cognee_data_id,
    ) -> None:
        """TODO-5-901: observability for a failed superseded-doc cascade.

        Canonicalize's superseded-member loop calls the shared cascade
        helper per archived fact; a "failed" status leaves the Cognee
        document orphaned. Emits the same trio as facade._emit_cascade_
        failure (WARNING log + `eb_fact_delete_cascade_failures_total`
        metric + DEGRADED_OPERATION trace), tagged with
        `operation="canonicalize"` so the per-op metric split (TODO-5-
        511) separates canonicalize-path cascade failures from the
        delete-path and update-path cascades. Best-effort: the cascade
        helper already captured the underlying exception; this fires on
        the returned status and never re-raises.
        """
        step = "cognee_data"
        operation = "canonicalize"
        if self._metrics:
            self._metrics.inc_fact_delete_cascade_failure(step, operation=operation)
        else:
            inc_fact_delete_cascade_failure(
                step, operation=operation, gateway_id=self._gateway_id,
            )
        self._log.warning(
            "Canonicalize superseded-cascade failed (fact_id=%s, data_id=%s) "
            "— Cognee-side document may be orphaned",
            fact_id, cognee_data_id,
        )
        if self._trace is not None:
            await self._trace.append_event(
                TraceEvent(
                    event_type=TraceEventType.DEGRADED_OPERATION,
                    payload={
                        "component": "consolidation_canonicalize",
                        "operation": operation,
                        "failure": "cascade_step",
                        "step": step,
                        "fact_id": str(fact_id),
                        "cognee_data_id": str(cognee_data_id)
                            if cognee_data_id is not None else None,
                    },
                )
            )
