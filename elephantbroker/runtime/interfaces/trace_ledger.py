"""Trace ledger interface."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.trace import TraceEvent, TraceQuery


class ITraceLedger(ABC):
    """Append-only audit trail for all system events."""

    @abstractmethod
    async def append_event(self, event: TraceEvent) -> TraceEvent:
        """Append a new event to the trace ledger."""
        ...

    @abstractmethod
    async def query_trace(self, query: TraceQuery) -> list[TraceEvent]:
        """Query the trace ledger."""
        ...

    @abstractmethod
    async def get_evidence_chain(self, target_id: uuid.UUID) -> list[TraceEvent]:
        """Get all trace events related to a specific target."""
        ...
