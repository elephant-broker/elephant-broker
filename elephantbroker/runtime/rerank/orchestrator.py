"""Rerank orchestrator — 4-stage pipeline: cheap_prune → semantic → cross-encoder → merge."""
from __future__ import annotations

import logging
import math
import re
import time

import httpx

from elephantbroker.runtime.interfaces.rerank import IRerankOrchestrator
from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.config import RerankerConfig, ScoringConfig
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.runtime.rerank.orchestrator")

_WORD_RE = re.compile(r"\w+")


class RerankOrchestrator(IRerankOrchestrator):
    """4-stage reranking pipeline for retrieval candidates."""

    def __init__(
        self, trace_ledger: ITraceLedger,
        embedding_service=None,
        reranker_config: RerankerConfig | None = None,
        scoring_config: ScoringConfig | None = None,
        metrics=None,
    ) -> None:
        self._trace = trace_ledger
        self._embeddings = embedding_service
        self._reranker_config = reranker_config or RerankerConfig()
        self._scoring_config = scoring_config or ScoringConfig()
        self._http_client: httpx.AsyncClient | None = None
        self._metrics = metrics

    async def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            headers = {"Content-Type": "application/json"}
            if self._reranker_config.api_key:
                headers["Authorization"] = f"Bearer {self._reranker_config.api_key}"
            self._http_client = httpx.AsyncClient(
                headers=headers,
                timeout=self._reranker_config.timeout_seconds,
            )
        return self._http_client

    @traced
    async def rerank(
        self, candidates: list[RetrievalCandidate], query: str, *,
        query_embedding: list[float] | None = None,
        top_n: int | None = None,
    ) -> list[RetrievalCandidate]:
        """Full 4-stage reranking pipeline."""
        if not candidates:
            return []

        t0 = time.monotonic()

        # Stage 1: Cheap prune
        pruned = await self.cheap_prune(
            candidates, query,
            max_candidates=self._scoring_config.cheap_prune_max_candidates,
        )

        # Stage 2: Semantic rerank (if embeddings available)
        if query_embedding and self._embeddings:
            pruned = await self._semantic_rerank(pruned, query_embedding)

        # Stage 3: Cross-encoder rerank
        pruned = await self.cross_encoder_rerank(
            pruned, query, top_n=top_n or self._reranker_config.top_n,
        )

        # Stage 4: Merge duplicates
        merged = await self.merge_duplicates(pruned)

        if self._metrics:
            self._metrics.observe_rerank_candidates_in(len(candidates))
            self._metrics.observe_rerank_candidates_out(len(merged))
            self._metrics.inc_rerank_call("ok")
            self._metrics.observe_rerank_duration("full_pipeline", time.monotonic() - t0)

        logger.info("Rerank pipeline: %d → %d candidates", len(candidates), len(merged))
        return merged

    async def cheap_prune(
        self, candidates: list[RetrievalCandidate], query: str, *,
        max_candidates: int = 80,
    ) -> list[RetrievalCandidate]:
        """Stage 1: Quick prune via token overlap + retrieval score blend."""
        if len(candidates) <= max_candidates:
            return list(candidates)

        query_tokens = set(_WORD_RE.findall(query.lower()))
        if not query_tokens:
            candidates.sort(key=lambda c: c.score, reverse=True)
            return candidates[:max_candidates]

        scored: list[tuple[float, RetrievalCandidate]] = []
        for c in candidates:
            item_tokens = set(_WORD_RE.findall(c.fact.text.lower()))
            overlap = len(query_tokens & item_tokens) / max(len(query_tokens), 1)
            blended = overlap * 0.5 + c.score * 0.5
            scored.append((blended, c))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored[:max_candidates]]

    async def _semantic_rerank(
        self, candidates: list[RetrievalCandidate],
        query_embedding: list[float],
    ) -> list[RetrievalCandidate]:
        """Stage 2: Semantic reranking by cosine similarity blend."""
        blend_weight = self._scoring_config.semantic_blend_weight

        # Get embeddings for all candidate texts
        texts = [c.fact.text for c in candidates]
        try:
            embeddings = await self._embeddings.embed_batch(texts)
        except Exception as exc:
            logger.warning("Semantic rerank embedding failed: %s", exc)
            return candidates

        scored: list[tuple[float, RetrievalCandidate]] = []
        for i, c in enumerate(candidates):
            if i < len(embeddings) and embeddings[i]:
                semantic = _cosine_sim(query_embedding, embeddings[i])
                blended = blend_weight * semantic + (1 - blend_weight) * c.score
            else:
                blended = c.score
            scored.append((blended, c))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored]

    @traced
    async def cross_encoder_rerank(
        self, candidates: list[RetrievalCandidate], query: str, *,
        top_n: int | None = None,
    ) -> list[RetrievalCandidate]:
        """Stage 3: Cross-encoder reranking via external API."""
        if not self._reranker_config.enabled or not candidates:
            return list(candidates)

        # Truncate to max_documents
        truncated = candidates[:self._reranker_config.max_documents]
        documents = [c.fact.text for c in truncated]

        try:
            client = await self._get_http_client()
            payload: dict = {
                "model": self._reranker_config.model,
                "query": query,
                "documents": documents,
            }
            if top_n is not None:
                payload["top_n"] = top_n
            elif self._reranker_config.top_n is not None:
                payload["top_n"] = self._reranker_config.top_n

            response = await client.post(
                f"{self._reranker_config.endpoint}/v1/rerank",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

            # Parse results — raw scores need sigmoid normalization
            results = data.get("results", [])
            score_map: dict[int, float] = {}
            for r in results:
                idx = r.get("index", 0)
                raw = r.get("relevance_score", 0.0)
                score_map[idx] = _sigmoid(raw)

            # Sort by normalized score
            scored = [(score_map.get(i, 0.0), c) for i, c in enumerate(truncated)]
            scored.sort(key=lambda x: x[0], reverse=True)
            return [c for _, c in scored]

        except Exception as exc:
            if self._reranker_config.fallback_on_error:
                logger.warning("Cross-encoder failed, falling back: %s", exc)
                if self._metrics:
                    self._metrics.inc_rerank_fallback()
                await self._trace.append_event(TraceEvent(
                    event_type=TraceEventType.DEGRADED_OPERATION,
                    payload={"component": "reranker", "error": str(exc)[:200]},
                ))
                return list(candidates)
            raise

    async def merge_duplicates(
        self, candidates: list[RetrievalCandidate],
    ) -> list[RetrievalCandidate]:
        """Stage 4: Merge near-duplicate candidates by embedding similarity."""
        if len(candidates) <= 1 or not self._embeddings:
            return list(candidates)

        threshold = self._scoring_config.merge_similarity_threshold
        texts = [c.fact.text for c in candidates]

        try:
            embeddings = await self._embeddings.embed_batch(texts)
        except Exception:
            return list(candidates)

        n = len(candidates)
        parent = list(range(n))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        # Pairwise comparison
        for i in range(n):
            for j in range(i + 1, n):
                if i < len(embeddings) and j < len(embeddings):
                    sim = _cosine_sim(embeddings[i], embeddings[j])
                    if sim > threshold:
                        union(i, j)

        # Group and keep highest scored
        groups: dict[int, list[int]] = {}
        for i in range(n):
            root = find(i)
            groups.setdefault(root, []).append(i)

        result: list[RetrievalCandidate] = []
        for members in groups.values():
            best_idx = max(members, key=lambda i: candidates[i].score)
            best = candidates[best_idx]
            # Union relations from merged candidates
            merged_relations = list(best.relations)
            for idx in members:
                if idx != best_idx:
                    merged_relations.extend(candidates[idx].relations)
            result.append(RetrievalCandidate(
                fact=best.fact, source=best.source, score=best.score,
                relations=merged_relations,
            ))
        return result

    async def dedup_safe(
        self, candidates: list[RetrievalCandidate],
    ) -> list[RetrievalCandidate]:
        """Remove exact ID duplicates without merging."""
        seen: set[str] = set()
        deduped: list[RetrievalCandidate] = []
        for c in candidates:
            key = str(c.fact.id)
            if key not in seen:
                seen.add(key)
                deduped.append(c)
        return deduped

    async def close(self) -> None:
        """Close the httpx client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None


def _cosine_sim(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na < 1e-10 or nb < 1e-10:
        return 0.0
    return dot / (na * nb)


def _sigmoid(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0
