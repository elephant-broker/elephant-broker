"""Rerank orchestrator interface."""
from __future__ import annotations

from abc import ABC, abstractmethod

from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate


class IRerankOrchestrator(ABC):
    """Reranks and deduplicates retrieval candidates."""

    @abstractmethod
    async def rerank(
        self, candidates: list[RetrievalCandidate], query: str, *,
        query_embedding: list[float] | None = None,
        top_n: int | None = None,
    ) -> list[RetrievalCandidate]:
        """Rerank candidates by relevance to the query."""
        ...

    @abstractmethod
    async def cheap_prune(
        self, candidates: list[RetrievalCandidate], query: str, *,
        max_candidates: int = 80,
    ) -> list[RetrievalCandidate]:
        """Quick prune via token overlap + retrieval score blend."""
        ...

    @abstractmethod
    async def cross_encoder_rerank(
        self, candidates: list[RetrievalCandidate], query: str, *,
        top_n: int | None = None,
    ) -> list[RetrievalCandidate]:
        """Rerank via external cross-encoder model."""
        ...

    @abstractmethod
    async def merge_duplicates(self, candidates: list[RetrievalCandidate]) -> list[RetrievalCandidate]:
        """Merge near-duplicate candidates, keeping the highest-quality version."""
        ...

    @abstractmethod
    async def dedup_safe(self, candidates: list[RetrievalCandidate]) -> list[RetrievalCandidate]:
        """Remove exact duplicates without merging."""
        ...
