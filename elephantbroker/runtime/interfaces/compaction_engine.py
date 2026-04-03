"""Compaction engine interface."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.context import CompactResult, CompactionContext, SessionCompactState


class ICompactionEngine(ABC):
    """Continuous, goal-aware context compaction."""

    @abstractmethod
    async def compact(
        self,
        session_id: uuid.UUID,
        token_budget: int,
        force: bool = False,
    ) -> CompactResult:
        """Run compaction for a session within the token budget."""
        ...

    @abstractmethod
    async def get_compact_state(self, session_id: uuid.UUID) -> CompactResult:
        """Get the current compaction state for a session."""
        ...

    @abstractmethod
    async def merge_overlapping(self, session_id: uuid.UUID) -> int:
        """Merge overlapping compacted segments, returning the number of merges."""
        ...

    # Phase 6 additions

    async def compact_with_context(self, context: CompactionContext) -> CompactResult:
        """Run compaction with full context (goals, messages, profile)."""
        return CompactResult(ok=True, compacted=False, reason="not implemented")

    async def get_session_compact_state(
        self, sk: str, sid: str
    ) -> SessionCompactState | None:
        """Get structured compact state for a session."""
        return None
