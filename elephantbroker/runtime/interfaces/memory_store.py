"""Memory store facade interface."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass


class IMemoryStoreFacade(ABC):
    """Unified facade for storing, searching, and managing memory facts.

    Security: methods accepting ``caller_gateway_id`` enforce gateway
    ownership — the concrete ``MemoryStoreFacade`` raises ``PermissionError``
    (HTTP 403 via error middleware) on cross-gateway access. Test stubs that
    omit this check skip the security boundary.
    """

    @abstractmethod
    async def store(
        self, fact: FactAssertion, *,
        dedup_threshold: float | None = None,
        precomputed_embedding: list[float] | None = None,
        profile_name: str | None = None,
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
    async def promote_scope(
        self, fact_id: uuid.UUID, to_scope: Scope, *, caller_gateway_id: str = "",
    ) -> FactAssertion:
        """Promote a fact to a wider scope. Raises PermissionError when
        the stored ``gateway_id`` does not match ``caller_gateway_id`` (or
        the facade's configured gateway_id as fallback)."""
        ...

    @abstractmethod
    async def promote_class(
        self, fact_id: uuid.UUID, to_class: MemoryClass, *, caller_gateway_id: str = "",
    ) -> FactAssertion:
        """Promote a fact to a higher memory class. Raises PermissionError
        when the stored ``gateway_id`` does not match ``caller_gateway_id``
        (or the facade's configured gateway_id as fallback)."""
        ...

    @abstractmethod
    async def decay(self, fact_id: uuid.UUID, factor: float) -> FactAssertion:
        """Apply decay to a fact's confidence/relevance."""
        ...

    @abstractmethod
    async def get_by_id(
        self, fact_id: uuid.UUID, *, caller_gateway_id: str = "",
    ) -> FactAssertion | None:
        """Retrieve a single fact by ID. Returns ``None`` for cross-gateway
        reads (404-semantic: hides existence oracle) when ``caller_gateway_id``
        (or facade default) does not match the stored ``gateway_id``."""
        ...

    @abstractmethod
    async def update(
        self, fact_id: uuid.UUID, updates: dict, *, caller_gateway_id: str = "",
    ) -> FactAssertion:
        """Update fact fields. Re-embeds if text changes. Raises
        PermissionError when the stored ``gateway_id`` does not match
        ``caller_gateway_id`` (or the facade's configured gateway_id)."""
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
