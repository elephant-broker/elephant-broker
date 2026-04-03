"""Working set manager interface."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.working_set import WorkingSetSnapshot


class IWorkingSetManager(ABC):
    """Manages the budgeted working-set competition for context assembly."""

    @abstractmethod
    async def build_working_set(
        self, *, session_id: uuid.UUID, session_key: str,
        profile_name: str, query: str,
        goal_ids: list[uuid.UUID] | None = None,
    ) -> WorkingSetSnapshot:
        """Build the full working set: candidates → rerank → score → select."""
        ...

    @abstractmethod
    async def get_working_set(self, session_id: uuid.UUID) -> WorkingSetSnapshot | None:
        """Get the current working set for a session."""
        ...
