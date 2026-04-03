"""Scenario: Goal-Driven — goals, goal-relevant facts, working set scoring."""
from __future__ import annotations

from tests.scenarios.base import Scenario
from tests.scenarios.runner import register


@register
class GoalDrivenScenario(Scenario):
    """Goals -> goal-relevant facts -> working set scoring."""

    name = "goal_driven"
    required_phase = 5
    required_amendment_6_2 = False

    async def run(self):
        self.expect_trace("scoring_completed", min_count=1)

        goal = await self.sim.simulate_session_goals_create("Implement authentication module")
        goal_id = goal.get("id") or goal.get("goal_id")
        self.step("goal_created", passed=goal_id is not None, message=f"Goal: {goal_id}")

        await self.sim.simulate_tool_memory_store("JWT tokens expire after 1 hour", "auth")
        await self.sim.simulate_tool_memory_store("The weather is sunny today", "general")
        await self.sim.simulate_tool_memory_store("Auth middleware validates every request", "auth")

        ws = await self.sim.simulate_build_working_set("authentication implementation")
        items = ws.get("items", [])
        self.step("working_set_has_items", passed=len(items) > 0,
                  message=f"Got {len(items)} working set items")

        if goal_id:
            await self.sim.simulate_session_goals_progress(goal_id, "Started JWT implementation")
            goals = await self.sim.simulate_session_goals_list()
            self.step("goal_progress_recorded", passed=len(goals) > 0)
