"""Goal tracking schemas."""
from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field

from elephantbroker.schemas.base import Scope


class GoalStatus(StrEnum):
    """Status of a goal."""
    PROPOSED = "proposed"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ABANDONED = "abandoned"


class GoalState(BaseModel):
    """A single goal with its current state."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    title: str = Field(min_length=1)
    description: str = ""
    status: GoalStatus = GoalStatus.ACTIVE
    scope: Scope = Scope.SESSION
    parent_goal_id: uuid.UUID | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    owner_actor_ids: list[uuid.UUID] = Field(default_factory=list)
    success_criteria: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)
    gateway_id: str = ""
    # Phase 8: org/team scoping for persistent goal visibility
    org_id: uuid.UUID | None = None
    team_id: uuid.UUID | None = None
    # Phase 7: metadata for auto-goal tracking (source_type, source_system, resolved_by_runtime, etc.)
    metadata: dict[str, str] = Field(default_factory=dict)


class GoalHierarchy(BaseModel):
    """Tree of goals with parent-child relationships."""
    root_goals: list[GoalState] = Field(default_factory=list)
    children: dict[str, list[GoalState]] = Field(default_factory=dict)

    def all_goals(self) -> list[GoalState]:
        """Return all goals in the hierarchy (roots + children)."""
        result = list(self.root_goals)
        for child_list in self.children.values():
            result.extend(child_list)
        return result

    def find_by_id(self, goal_id: uuid.UUID) -> GoalState | None:
        """Find a goal by its ID."""
        for goal in self.all_goals():
            if goal.id == goal_id:
                return goal
        return None

    def depth_first(self) -> Iterator[GoalState]:
        """Iterate goals depth-first starting from roots."""
        for root in self.root_goals:
            yield from self._visit(root)

    def _visit(self, goal: GoalState) -> Iterator[GoalState]:
        yield goal
        key = str(goal.id)
        for child in self.children.get(key, []):
            yield from self._visit(child)
