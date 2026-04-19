"""Stage 2: Canonicalize Stable Facts — LLM smart merge for non-identical clusters.

Creates NEW canonical fact per cluster, archives ALL originals (AD-3).
LLM calls bounded by context.llm_calls_cap.
"""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from elephantbroker.runtime.metrics import inc_cognee_capture_failure
from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.consolidation import CanonicalResult
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.cognee.cached_embeddings import CachedEmbeddingService
    from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
    from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
    from elephantbroker.runtime.adapters.llm.client import LLMClient
    from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
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
        trace_ledger: ITraceLedger | None = None,
        metrics: MetricsContext | None = None,
    ) -> None:
        self._graph = graph
        self._vector = vector
        self._llm = llm_client
        self._embeddings = embedding_service
        self._config = config
        self._trace = trace_ledger
        self._metrics = metrics

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
        import cognee
        dataset_name = f"{gateway_id}__elephantbroker"
        cognee_add_result = None
        try:
            cognee_add_result = await cognee.add(
                canonical_text.strip(), dataset_name=dataset_name,
            )
        except Exception:
            logger.debug("cognee.add failed for canonical — non-fatal", exc_info=True)

        if cognee_add_result is not None:
            try:
                new_fact.cognee_data_id = cognee_add_result.data_ingestion_info[0]["data_id"]
            except (AttributeError, IndexError, KeyError, TypeError) as exc:
                await self._emit_capture_failure(fact_id=new_fact.id, exc=exc)

        # Store canonical via Cognee-first dual write (BS-10)
        dp = FactDataPoint.from_schema(new_fact)
        try:
            await add_data_points([dp])
        except Exception:
            logger.warning("add_data_points failed for canonical %s", new_id, exc_info=True)
            return None

        # Collect the superseded facts' cognee_data_ids BEFORE archival
        # mutates member state. Each old id points at a Cognee document
        # that is now dead weight — the canonical's new document
        # supersedes it. These must be cascaded after the graph nodes
        # are re-MERGEd as archived, so an observer never sees an
        # archived fact whose cognee_data_id already points at a
        # half-deleted document.
        superseded_data_ids: list[tuple[uuid.UUID, object]] = [
            (member.id, member.cognee_data_id)
            for member in members
            if member.cognee_data_id
        ]

        # Archive ALL originals (AD-3)
        archived_ids: list[str] = []
        for member in members:
            member.archived = True
            member.confidence = 0.0
            archived_dp = FactDataPoint.from_schema(member)
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
        for member_id, old_data_id in superseded_data_ids:
            await self._cascade_superseded_data_id(
                old_data_id, fact_id=member_id, dataset_name=dataset_name,
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
            inc_cognee_capture_failure("canonicalize")
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

    async def _cascade_superseded_data_id(
        self, cognee_data_id, *, fact_id: uuid.UUID, dataset_name: str,
    ) -> None:
        """Remove the Cognee-side document for a superseded fact.

        Called once per archived original that had a cognee_data_id.
        Same shape as facade._cascade_cognee_data (intentionally
        duplicated rather than imported — keeping the consolidation
        stage decoupled from the memory facade is worth the few lines).
        Best-effort: failures are logged and swallowed so one dead
        document does not block the remaining cascades.
        """
        import cognee
        try:
            from uuid import UUID
            from cognee.modules.data.methods import get_datasets_by_name
            from cognee.modules.users.methods import get_default_user
            user = await get_default_user()
            datasets = await get_datasets_by_name([dataset_name], user.id)
            if not datasets:
                logger.debug(
                    "Superseded-cascade skipped: dataset %s not found for fact %s",
                    dataset_name, fact_id,
                )
                return
            data_id_uuid = UUID(str(cognee_data_id))
            await cognee.datasets.delete_data(
                dataset_id=datasets[0].id,
                data_id=data_id_uuid,
                mode="soft",
                delete_dataset_if_empty=False,
            )
        except Exception as exc:
            logger.warning(
                "Superseded-cascade failed (fact_id=%s, data_id=%s): %r",
                fact_id, cognee_data_id, exc,
            )
