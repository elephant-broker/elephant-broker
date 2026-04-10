"""Retrieval orchestrator interface."""
from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from elephantbroker.schemas.fact import FactAssertion, MemoryClass
from elephantbroker.schemas.profile import RetrievalPolicy


class RetrievalCandidate(BaseModel):
    """A single retrieval result with source attribution and score."""
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
