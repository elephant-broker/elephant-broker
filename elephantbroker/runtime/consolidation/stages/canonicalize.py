"""Stage 2: Canonicalize Stable Facts — LLM smart merge for non-identical clusters.

Creates NEW canonical fact per cluster, archives ALL originals (AD-3).
LLM calls bounded by context.llm_calls_cap.
"""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.consolidation import CanonicalResult

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.cognee.cached_embeddings import CachedEmbeddingService
    from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
    from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
    from elephantbroker.runtime.adapters.llm.client import LLMClient
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
    ) -> None:
        self._graph = graph
        self._vector = vector
        self._llm = llm_client
        self._embeddings = embedding_service
        self._config = config

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

        # Store canonical via Cognee-first dual write (BS-10)
        dp = FactDataPoint.from_schema(new_fact)
        try:
            await add_data_points([dp])
        except Exception:
            logger.warning("add_data_points failed for canonical %s", new_id, exc_info=True)
            return None

        try:
            import cognee
            dataset_name = f"{gateway_id}__elephantbroker"
            await cognee.add(canonical_text.strip(), dataset_name=dataset_name)
        except Exception:
            logger.debug("cognee.add failed for canonical — non-fatal", exc_info=True)

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
