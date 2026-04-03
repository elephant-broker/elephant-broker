"""Stage 1: Cluster Near-Duplicates — embedding-based clustering with union-find.

No LLM calls. No graph mutations. Pure read-only analysis.
"""
from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.consolidation import DuplicateCluster

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.cognee.cached_embeddings import CachedEmbeddingService
    from elephantbroker.schemas.consolidation import ConsolidationConfig
    from elephantbroker.schemas.fact import FactAssertion

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.cluster_duplicates")


def _cosine_sim(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two embedding vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na < 1e-10 or nb < 1e-10:
        return 0.0
    return dot / (na * nb)


class ClusterDuplicatesStage:
    """Embedding-based clustering of near-duplicate facts.

    Algorithm:
    1. Batch-embed all fact texts via CachedEmbeddingService
    2. For each fact, find top-K most similar via cosine similarity
    3. If similarity >= threshold (0.92), add to same cluster (union-find)
    4. For each cluster, select canonical candidate (highest confidence, then most recent)
    5. Record distinct session_keys per cluster for Stage 6 promotion
    """

    def __init__(
        self,
        embedding_service: CachedEmbeddingService,
        config: ConsolidationConfig,
    ) -> None:
        self._embeddings = embedding_service
        self._threshold = config.cluster_similarity_threshold

    @traced
    async def run(
        self, facts: list[FactAssertion], gateway_id: str,
    ) -> list[DuplicateCluster]:
        if len(facts) < 2:
            return []

        # 1. Batch embed all fact texts
        texts = [f.text for f in facts]
        try:
            embeddings = await self._embeddings.embed_batch(texts)
        except Exception:
            logger.warning("Embedding batch failed in Stage 1, skipping clustering")
            return []

        if len(embeddings) != len(facts):
            logger.warning("Embedding count mismatch: %d vs %d facts", len(embeddings), len(facts))
            return []

        # 2-3. Union-find clustering based on pairwise cosine similarity
        parent = list(range(len(facts)))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]  # path compression
                x = parent[x]
            return x

        def union(x: int, y: int) -> None:
            rx, ry = find(x), find(y)
            if rx != ry:
                parent[rx] = ry

        for i in range(len(facts)):
            for j in range(i + 1, len(facts)):
                sim = _cosine_sim(embeddings[i], embeddings[j])
                if sim >= self._threshold:
                    union(i, j)

        # Group facts by cluster root
        clusters_map: dict[int, list[int]] = {}
        for i in range(len(facts)):
            root = find(i)
            clusters_map.setdefault(root, []).append(i)

        # 4-5. Build DuplicateCluster objects for clusters with 2+ members
        result: list[DuplicateCluster] = []
        for indices in clusters_map.values():
            if len(indices) < 2:
                continue

            cluster_facts = [facts[i] for i in indices]
            fact_ids = [str(f.id) for f in cluster_facts]

            # Canonical candidate: highest confidence, then most recent updated_at
            canonical = max(
                cluster_facts,
                key=lambda f: (f.confidence, f.updated_at),
            )

            # Average pairwise similarity within cluster
            sims = []
            for a_idx in range(len(indices)):
                for b_idx in range(a_idx + 1, len(indices)):
                    sims.append(_cosine_sim(
                        embeddings[indices[a_idx]],
                        embeddings[indices[b_idx]],
                    ))
            avg_sim = sum(sims) / len(sims) if sims else 0.0

            # Distinct session keys
            session_keys = list({
                f.session_key for f in cluster_facts if f.session_key
            })

            result.append(DuplicateCluster(
                fact_ids=fact_ids,
                canonical_candidate_id=str(canonical.id),
                avg_similarity=avg_sim,
                session_keys=session_keys,
            ))

        logger.info(
            "Stage 1: %d facts → %d clusters (threshold=%.2f, gateway=%s)",
            len(facts), len(result), self._threshold, gateway_id,
        )
        return result
