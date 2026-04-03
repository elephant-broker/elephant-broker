"""Tests for goal schemas."""
import uuid

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.goal import GoalHierarchy, GoalState, GoalStatus


class TestGoalStatus:
    def test_all_statuses(self):
        assert len(GoalStatus) == 5
        assert GoalStatus.PROPOSED == "proposed"
        assert GoalStatus.ACTIVE == "active"
        assert GoalStatus.PAUSED == "paused"
        assert GoalStatus.COMPLETED == "completed"
        assert GoalStatus.ABANDONED == "abandoned"


class TestGoalState:
    def test_valid_creation(self):
        goal = GoalState(title="Build the thing")
        assert goal.status == GoalStatus.ACTIVE
        assert isinstance(goal.id, uuid.UUID)

    def test_title_required(self):
        with pytest.raises(ValidationError):
            GoalState(title="")

    def test_description_optional(self):
        goal = GoalState(title="x")
        assert goal.description == ""

    def test_scope_default(self):
        goal = GoalState(title="x")
        assert goal.scope == Scope.SESSION

    def test_json_round_trip(self):
        goal = GoalState(title="test goal", status=GoalStatus.PAUSED)
        data = goal.model_dump(mode="json")
        restored = GoalState.model_validate(data)
        assert restored.title == goal.title
        assert restored.status == GoalStatus.PAUSED

    def test_optional_fields_default(self):
        goal = GoalState(title="x")
        assert goal.parent_goal_id is None
        assert goal.owner_actor_ids == []
        assert goal.success_criteria == []
        assert goal.blockers == []
        assert goal.confidence == 0.8

    def test_confidence_bounds(self):
        goal = GoalState(title="x", confidence=0.5)
        assert goal.confidence == 0.5
        with pytest.raises(ValidationError):
            GoalState(title="x", confidence=1.5)
        with pytest.raises(ValidationError):
            GoalState(title="x", confidence=-0.1)

    def test_multiple_owners(self):
        ids = [uuid.uuid4(), uuid.uuid4()]
        goal = GoalState(title="shared", owner_actor_ids=ids)
        assert len(goal.owner_actor_ids) == 2


class TestGoalHierarchy:
    def test_empty_hierarchy(self):
        h = GoalHierarchy()
        assert h.root_goals == []
        assert h.children == {}

    def test_all_goals(self):
        root = GoalState(title="root")
        child = GoalState(title="child", parent_goal_id=root.id)
        h = GoalHierarchy(
            root_goals=[root],
            children={str(root.id): [child]},
        )
        assert len(h.all_goals()) == 2

    def test_find_by_id(self):
        root = GoalState(title="root")
        child = GoalState(title="child", parent_goal_id=root.id)
        h = GoalHierarchy(
            root_goals=[root],
            children={str(root.id): [child]},
        )
        assert h.find_by_id(root.id) is not None
        assert h.find_by_id(child.id) is not None
        assert h.find_by_id(uuid.uuid4()) is None

    def test_depth_first(self):
        root = GoalState(title="root")
        child = GoalState(title="child", parent_goal_id=root.id)
        h = GoalHierarchy(
            root_goals=[root],
            children={str(root.id): [child]},
        )
        goals = list(h.depth_first())
        assert len(goals) == 2
        assert goals[0].title == "root"
        assert goals[1].title == "child"

    def test_depth_first_empty(self):
        h = GoalHierarchy()
        assert list(h.depth_first()) == []


class TestGoalStateMetadata:
    """Phase 7: metadata field for auto-goal tracking."""

    def test_metadata_default_empty(self):
        g = GoalState(title="test")
        assert g.metadata == {}

    def test_metadata_round_trip(self):
        g = GoalState(title="auto goal", metadata={
            "source_type": "auto",
            "source_system": "procedure",
            "source_id": "abc123",
            "resolved_by_runtime": "false",
        })
        data = g.model_dump(mode="json")
        restored = GoalState.model_validate(data)
        assert restored.metadata["source_type"] == "auto"
        assert restored.metadata["resolved_by_runtime"] == "false"

    def test_metadata_json_serialization(self):
        g = GoalState(title="test", metadata={"key": "value"})
        json_str = g.model_dump_json()
        restored = GoalState.model_validate_json(json_str)
        assert restored.metadata == {"key": "value"}
