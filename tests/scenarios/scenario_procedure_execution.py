"""Scenario: Procedure Execution — create, activate, step through, complete."""
from __future__ import annotations

from tests.scenarios.base import Scenario
from tests.scenarios.runner import register


@register
class ProcedureExecutionScenario(Scenario):
    """Procedure create -> activate -> step through -> complete."""

    name = "procedure_execution"
    required_phase = 7
    required_amendment_6_2 = False

    async def run(self):
        self.expect_trace("procedure_step_passed", min_count=1)

        proc = await self.sim.simulate_procedure_create("deploy_service", steps=[
            {"name": "run_tests", "description": "Run test suite"},
            {"name": "deploy", "description": "Deploy to staging"},
        ])
        proc_id = proc.get("id") or proc.get("procedure_id")
        self.step("procedure_created", passed=proc_id is not None)

        if proc_id:
            activation = await self.sim.simulate_procedure_activate(proc_id)
            exec_id = activation.get("execution_id")
            self.step("procedure_activated", passed=exec_id is not None)

            if exec_id:
                steps = activation.get("steps", [])
                for step in steps:
                    step_id = step.get("id") or step.get("step_id")
                    if step_id:
                        await self.sim.simulate_procedure_complete_step(
                            exec_id, step_id, proof_value="Tests passed")
                        self.step(f"step_{step_id}_completed", passed=True)

        status = await self.sim.simulate_procedure_status()
        self.step("procedure_status_available", passed=status is not None)
