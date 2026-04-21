"""IngestBuffer interface — Redis-backed message/fact window contract.

Declared to decouple consumers (MemoryStoreFacade, TurnIngest pipeline) from
the concrete IngestBuffer. Notably, facade.delete() depends on
scrub_fact_from_recent() to purge GDPR-deleted facts from the extraction
context window (see Phase 4 TD #2) — making that dependency a declared ABC
method rather than a duck-typed attribute prevents accidental skew.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class IIngestBuffer(ABC):
    """Contract for the turn-ingest Redis buffer and recent-facts window."""

    @abstractmethod
    async def add_messages(
        self,
        session_key: str,
        messages: list[dict],
        *,
        effective_batch_size: int | None = None,
    ) -> bool:
        """Append messages to the per-session buffer; return True if batch full.

        ``effective_batch_size`` is an optional per-call override for the flush
        threshold (and its 3x overflow guard). When ``None``, implementations
        must fall back to their configured default so that existing deployments
        see no behavior change.
        """
        ...

    @abstractmethod
    async def flush(self, session_key: str) -> list[dict]:
        """Atomically drain the buffer for a session."""
        ...

    @abstractmethod
    async def force_flush(self, session_key: str) -> list[dict]:
        """Force a flush regardless of batch size or timeout."""
        ...

    @abstractmethod
    async def check_timeout_flush(self, session_key: str) -> bool:
        """Return True when the batch-timeout window has elapsed."""
        ...

    @abstractmethod
    async def load_recent_facts(self, session_key: str) -> list[dict]:
        """Load the recently-extracted-facts window for extraction context."""
        ...

    @abstractmethod
    async def update_recent_facts(
        self, session_key: str, new_facts: list[dict], max_count: int = 20,
    ) -> None:
        """Update the recent-facts window, keeping only the last max_count."""
        ...

    @abstractmethod
    async def scrub_fact_from_recent(self, session_key: str, fact_id: str) -> int:
        """Remove a fact from the recent-facts window on GDPR delete.

        Returns the number of entries removed (0 if key missing or id not
        present). Required contract — facade.delete() relies on this to
        prevent re-extraction of deleted facts inside the TTL window.
        """
        ...
