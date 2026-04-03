"""Memory store facade interface."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass


class IMemoryStoreFacade(ABC):
    """Unified facade for storing, searching, and managing memory facts."""

    @abstractmethod
    async def store(
        self, fact: FactAssertion, *,
        dedup_threshold: float | None = None,
        precomputed_embedding: list[float] | None = None,
    ) -> FactAssertion:
        """Store a new fact in memory. Raises DedupSkipped if near-duplicate detected."""
        ...

    @abstractmethod
    async def search(
        self, query: str, max_results: int = 20, min_score: float = 0.0,
        scope: Scope | None = None, actor_id: str | None = None,
        memory_class: MemoryClass | None = None, session_key: str | None = None,
        profile_name: str = "default", auto_recall: bool = False,
        caller_gateway_id: str = "",
    ) -> list[FactAssertion]:
        """Search memory for facts matching the query."""
        ...

    @abstractmethod
    async def promote_scope(self, fact_id: uuid.UUID, to_scope: Scope) -> FactAssertion:
        """Promote a fact to a wider scope."""
        ...

    @abstractmethod
    async def promote_class(self, fact_id: uuid.UUID, to_class: MemoryClass) -> FactAssertion:
        """Promote a fact to a higher memory class."""
        ...

    @abstractmethod
    async def decay(self, fact_id: uuid.UUID, factor: float) -> FactAssertion:
        """Apply decay to a fact's confidence/relevance."""
        ...

    @abstractmethod
    async def get_by_id(self, fact_id: uuid.UUID) -> FactAssertion | None:
        """Retrieve a single fact by ID."""
        ...

    @abstractmethod
    async def update(self, fact_id: uuid.UUID, updates: dict) -> FactAssertion:
        """Update fact fields. Re-embeds if text changes."""
        ...

    @abstractmethod
    async def delete(self, fact_id: uuid.UUID, *, caller_gateway_id: str = "") -> None:
        """GDPR-compliant deletion from all stores."""
        ...

    @abstractmethod
    async def get_by_scope(
        self, scope: Scope, limit: int = 100,
        memory_class: MemoryClass | None = None,
    ) -> list[FactAssertion]:
        """Retrieve facts filtered by scope."""
        ...
