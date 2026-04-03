"""Goal manager interface."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.goal import GoalHierarchy, GoalState, GoalStatus


class IGoalManager(ABC):
    """Manages goal lifecycle: creation, status updates, hierarchy resolution."""

    @abstractmethod
    async def set_goal(self, goal: GoalState) -> GoalState:
        """Create or update a goal."""
        ...

    @abstractmethod
    async def resolve_active_goals(self, session_id: uuid.UUID) -> list[GoalState]:
        """Return all active goals for a session."""
        ...

    @abstractmethod
    async def get_goal_hierarchy(self, root_goal_id: uuid.UUID) -> GoalHierarchy:
        """Get the full hierarchy rooted at the given goal."""
        ...

    @abstractmethod
    async def update_goal_status(self, goal_id: uuid.UUID, status: GoalStatus) -> GoalState:
        """Update the status of a goal."""
        ...
