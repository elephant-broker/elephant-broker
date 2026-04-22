"""Retrieval orchestrator interface."""
from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from elephantbroker.schemas.fact import FactAssertion, MemoryClass
from elephantbroker.schemas.profile import RetrievalPolicy


class RetrievalCandidate(BaseModel):
    """A single retrieval result with source attribution and score.

    ``source`` is intentionally untyped (``str``) to accommodate producers
    outside the working-set pipeline. Known producers and their ``source``
    values:

    - ``RetrievalOrchestrator`` (working-set path): "structural", "keyword",
      "vector", "graph", "artifact" — the 5-source hybrid retrieval.
    - ``/rerank`` API route (``elephantbroker/api/routes/rerank.py``):
      ``source="api"`` — the reranker endpoint constructs ``RetrievalCandidate``
      purely as a transport shape for its input documents; these candidates
      never flow into ``retrieval_candidate_to_item``.

    Consumers that pipe ``RetrievalCandidate`` into ``WorkingSetItem`` via
    ``elephantbroker.runtime.working_set.candidates.retrieval_candidate_to_item``
    MUST restrict to ``{structural, keyword, vector, graph, artifact}``. The
    converter rejects unknown values explicitly (TODO-6-303) rather than
    silently constructing a ``WorkingSetItem`` whose ``retrieval_source``
    fails the Pydantic Literal validation at a less-obvious call site.
    """
    fact: FactAssertion
    source: str
    score: float = 0.0
    relations: list[dict] = Field(default_factory=list)


class IRetrievalOrchestrator(ABC):
    """Orchestrates candidate retrieval from graph and vector stores."""

    @abstractmethod
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
        """Retrieve candidate facts using 5-source hybrid search."""
        ...

    @abstractmethod
    async def get_exact_hits(self, query: str, max_results: int = 20) -> list[FactAssertion]:
        """Get exact/keyword matches (backward compat)."""
        ...

    @abstractmethod
    async def get_semantic_hits(self, query: str, max_results: int = 20) -> list[FactAssertion]:
        """Get semantic similarity matches (backward compat)."""
        ...
