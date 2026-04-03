"""Red-line guard engine interface (Phase 7 — 5 methods)."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.context import AgentMessage
from elephantbroker.schemas.guards import GuardEvent, GuardResult


class IRedLineGuardEngine(ABC):
    """Cheap-first pipeline for red-line constraint enforcement."""

    @abstractmethod
    async def preflight_check(self, session_id: uuid.UUID, messages: list[AgentMessage]) -> GuardResult:
        """Run preflight guard checks, returning a structured GuardResult."""
        ...

    @abstractmethod
    async def reinject_constraints(self, session_id: uuid.UUID) -> list[str]:
        """Get constraints that must be reinjected into the context."""
        ...

    @abstractmethod
    async def get_guard_history(self, session_id: uuid.UUID) -> list[GuardEvent]:
        """Get the history of guard checks and their results for a session."""
        ...

    @abstractmethod
    async def load_session_rules(
        self,
        session_id: uuid.UUID,
        profile_name: str,
        active_procedure_ids: list[uuid.UUID] | None = None,
        *,
        session_key: str = "",
        agent_id: str = "",
    ) -> None:
        """Load guard rules for a session from profile + active procedures."""
        ...

    @abstractmethod
    async def unload_session(self, session_id: uuid.UUID) -> None:
        """Clean up guard state for a session."""
        ...
