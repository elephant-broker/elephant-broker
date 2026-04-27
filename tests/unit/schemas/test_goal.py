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
    def _make_complex_hierarchy(self):
        """G1: Builds root1 + [c1a, c1b]; c1b + [grandchild]; root2 standalone."""
        root1 = GoalState(title="root1")
        root2 = GoalState(title="root2")
        c1a = GoalState(title="c1a", parent_goal_id=root1.id)
        c1b = GoalState(title="c1b", parent_goal_id=root1.id)
        grandchild = GoalState(title="grandchild", parent_goal_id=c1b.id)
        h = GoalHierarchy(
            root_goals=[root1, root2],
            children={
                str(root1.id): [c1a, c1b],
                str(c1b.id): [grandchild],
            },
        )
        return h, root1, root2, c1a, c1b, grandchild

    def test_empty_hierarchy(self):
        h = GoalHierarchy()
        assert h.root_goals == []
        assert h.children == {}
        assert h.all_goals() == []  # G8: empty hierarchy all_goals returns []

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

    def test_all_goals_includes_grandchildren_and_standalone(self):
        """G2: all_goals() returns roots + every child list (grandchildren + standalone roots)."""
        h, _root1, _root2, _c1a, _c1b, _grandchild = self._make_complex_hierarchy()
        assert len(h.all_goals()) == 5
        assert {g.title for g in h.all_goals()} == {"root1", "root2", "c1a", "c1b", "grandchild"}

    def test_find_by_id_finds_grandchild(self):
        """G3: find_by_id() reaches grandchildren via the flat all_goals() scan."""
        h, _, _, _, _, grandchild = self._make_complex_hierarchy()
        found = h.find_by_id(grandchild.id)
        assert found is not None
        assert found.title == "grandchild"

    def test_depth_first_dfs_ordering(self):
        """G4: depth_first() yields root1 -> c1a -> c1b -> grandchild -> root2.

        Python 3.7+ guarantees dict insertion order, so c1a precedes c1b within
        children[str(root1.id)], and grandchild follows c1b because _visit(c1b)
        recurses before returning to root1's sibling list.
        """
        h, _root1, _root2, _c1a, _c1b, _grandchild = self._make_complex_hierarchy()
        assert [g.title for g in h.depth_first()] == ["root1", "c1a", "c1b", "grandchild", "root2"]

    def test_children_dict_requires_str_uuid_keys(self):
        """G5: depth_first() looks up children by str(goal.id); dict MUST use str keys.

        Correct form (str(root.id) key) yields the child. A hierarchy keyed by raw
        UUID (bypassing validation via model_construct so the UUID key survives verbatim)
        does NOT yield the child because _visit() does a str-typed lookup.
        """
        root = GoalState(title="root")
        child = GoalState(title="child", parent_goal_id=root.id)
        h_correct = GoalHierarchy(root_goals=[root], children={str(root.id): [child]})
        assert child in list(h_correct.depth_first())
        # Bypass validation to preserve the wrong key type (UUID, not str).
        h_wrong = GoalHierarchy.model_construct(
            root_goals=[root],
            children={root.id: [child]},
        )
        assert child not in list(h_wrong.depth_first())

    def test_depth_first_leaf_yields_only_self(self):
        """G6: A leaf root with no children entry yields exactly itself, no error."""
        root = GoalState(title="root")
        h = GoalHierarchy(root_goals=[root])  # no children entry at all
        assert list(h.depth_first()) == [root]

    def test_orphaned_children_in_all_goals_not_in_depth_first(self):
        """G7: Orphaned children (key not matching any root id) appear in all_goals() but NOT depth_first().

        This pins the documented asymmetry: all_goals() flattens every children value
        regardless of key, while depth_first() only descends via str(goal.id) lookups.
        """
        root = GoalState(title="root")
        orphan = GoalState(title="orphan")
        h = GoalHierarchy(
            root_goals=[root],
            children={"non-matching-key": [orphan]},
        )
        assert orphan in h.all_goals()
        assert orphan not in list(h.depth_first())

    def test_status_transitions_unrestricted(self):
        """G9: GoalState.status has no schema-level transition enforcement; any state is assignable."""
        g = GoalState(title="x", status=GoalStatus.COMPLETED)
        assert g.status == GoalStatus.COMPLETED
        g.status = GoalStatus.ACTIVE
        assert g.status == GoalStatus.ACTIVE
        g.status = GoalStatus.PROPOSED
        assert g.status == GoalStatus.PROPOSED


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
