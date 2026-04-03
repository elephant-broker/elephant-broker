"""Integration tests for GoalManager with real Neo4j."""
from __future__ import annotations

import pytest

from elephantbroker.schemas.goal import GoalStatus
from tests.fixtures.factories import make_goal_state


@pytest.mark.integration
class TestGoalManagerIntegration:
    async def test_create_and_retrieve_goal_via_neo4j(self, goal_manager):
        goal = make_goal_state()
        await goal_manager.set_goal(goal)
        hierarchy = await goal_manager.get_goal_hierarchy(goal.id)
        assert len(hierarchy.root_goals) == 1
        assert hierarchy.root_goals[0].title == goal.title

    async def test_goal_hierarchy_parent_child(self, goal_manager):
        parent = make_goal_state(title="Parent goal")
        child = make_goal_state(title="Child goal", parent_goal_id=parent.id)
        await goal_manager.set_goal(parent)
        await goal_manager.set_goal(child)
        hierarchy = await goal_manager.get_goal_hierarchy(parent.id)
        assert len(hierarchy.root_goals) == 1
        children = hierarchy.children.get(str(parent.id), [])
        assert len(children) == 1
        assert children[0].title == "Child goal"

    async def test_update_goal_status(self, goal_manager):
        goal = make_goal_state()
        await goal_manager.set_goal(goal)
        updated = await goal_manager.update_goal_status(goal.id, GoalStatus.COMPLETED)
        assert updated.status == GoalStatus.COMPLETED
