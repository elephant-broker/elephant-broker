"""Scenario: Guard Check — exercise guard pipeline, near-miss or guard pass."""
from __future__ import annotations

from tests.scenarios.base import Scenario
from tests.scenarios.runner import register


@register
class GuardCheckScenario(Scenario):
    """Exercise guard pipeline: trigger a near-miss or guard pass."""

    name = "guard_check"
    required_phase = 7
    required_amendment_6_2 = False

    async def run(self):
        self.expect_trace("guard_passed", min_count=0)
        self.expect_trace("constraint_reinjected", min_count=0)

        await self.sim.simulate_tool_memory_store(
            "User wants to execute shell commands on the server", "technical")

        r = await self.sim.client.get("/guards/status", params={
            "session_key": self.sim.session_key,
            "session_id": str(self.sim.session_id),
        })
        self.step("guard_status_accessible", passed=r.status_code == 200)

        r = await self.sim.client.get("/guards/list", params={
            "session_key": self.sim.session_key,
        })
        self.step("constraints_listed", passed=r.status_code == 200,
                  message=f"Constraints: {len(r.json()) if r.status_code == 200 else 'N/A'}")
