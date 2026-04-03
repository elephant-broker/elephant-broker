"""Scenario: Context Lifecycle — bootstrap, ingest, assemble, compact, after-turn."""
from __future__ import annotations

from tests.scenarios.base import Scenario
from tests.scenarios.runner import register


@register
class ContextLifecycleScenario(Scenario):
    """Bootstrap -> ingest -> assemble -> compact -> after-turn."""

    name = "context_lifecycle"
    required_phase = 6
    required_amendment_6_2 = True

    async def setup(self):
        await self.sim.simulate_session_start()
        await self.sim.simulate_context_bootstrap()

    async def run(self):
        self.expect_trace("bootstrap_completed", min_count=1, max_count=1)
        self.expect_trace("context_assembled", min_count=1)
        self.expect_trace("after_turn_completed", min_count=1)

        for i in range(3):
            result = await self.sim.simulate_full_lifecycle_turn(
                user_msg=f"Turn {i}: explain the architecture of component {i}",
                assistant_msg=f"Component {i} uses a layered architecture with...",
                token_budget=4000)
            self.step(f"turn_{i}", passed="assembly" in result,
                      message=f"Turn {i} completed")

        assembly = await self.sim.simulate_context_assemble(token_budget=4000)
        tokens = assembly.get("total_tokens", 0)
        self.step("assembly_within_budget", passed=tokens <= 4000,
                  message=f"Assembly used {tokens} tokens (budget: 4000)")

        compact_result = await self.sim.simulate_context_compact()
        self.step("compaction_ran", passed=compact_result is not None)
        self.expect_trace("compaction_action", min_count=0)
