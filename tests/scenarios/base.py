from __future__ import annotations
import time
import uuid
from pydantic import BaseModel
from tests.e2e.gateway_simulator.simulator import OpenClawGatewaySimulator


class StepResult(BaseModel):
    name: str
    passed: bool
    message: str = ""
    data: dict = {}


class TraceAssertion(BaseModel):
    event_type: str
    min_count: int = 1
    max_count: int | None = None
    actual_count: int = 0
    passed: bool = False


class ScenarioResult(BaseModel):
    name: str
    passed: bool
    steps: list[StepResult]
    trace_summary: dict
    trace_assertions: list[TraceAssertion]
    duration_ms: int
    errors: list[str]
    reward_score: float  # 0.0-1.0


class Scenario:
    """Base class for runnable test scenarios."""
    name: str = "unnamed"
    required_phase: int = 6
    required_amendment_6_2: bool = False

    def __init__(self, base_url: str = "http://localhost:8420",
                 gateway_id: str = "local"):
        self.base_url = base_url
        self.gateway_id = gateway_id
        instance_id = str(uuid.uuid4())[:8]
        session_key = f"scenario:{self.name}:{instance_id}"
        self.sim = OpenClawGatewaySimulator(base_url, session_key=session_key)
        self._steps: list[StepResult] = []
        self._errors: list[str] = []
        self._trace_assertions: list[TraceAssertion] = []

    async def setup(self) -> None:
        await self.sim.simulate_session_start()

    async def run(self) -> None:
        raise NotImplementedError

    async def teardown(self) -> None:
        await self.sim.simulate_session_end()

    async def execute(self) -> ScenarioResult:
        start = time.monotonic()
        try:
            await self.setup()
            await self.run()
        except Exception as e:
            self._errors.append(f"Execution error: {e}")
        finally:
            try:
                await self.teardown()
            except Exception as e:
                self._errors.append(f"Teardown error: {e}")

        # Collect trace summary
        try:
            summary = await self.sim.get_session_summary()
        except Exception:
            summary = {}

        # Verify trace assertions
        for assertion in self._trace_assertions:
            assertion.actual_count = summary.get("event_counts", {}).get(
                assertion.event_type, 0)
            assertion.passed = (
                assertion.actual_count >= assertion.min_count
                and (assertion.max_count is None
                     or assertion.actual_count <= assertion.max_count)
            )

        duration_ms = int((time.monotonic() - start) * 1000)

        step_score = (sum(1 for s in self._steps if s.passed) / max(len(self._steps), 1))
        trace_score = (sum(1 for a in self._trace_assertions if a.passed)
                       / max(len(self._trace_assertions), 1))
        error_penalty = min(len(self._errors) * 0.1, 0.5)
        reward = max(0.0, (step_score * 0.6 + trace_score * 0.4) - error_penalty)

        all_passed = (
            all(s.passed for s in self._steps)
            and all(a.passed for a in self._trace_assertions)
            and len(self._errors) == 0
        )

        return ScenarioResult(
            name=self.name,
            passed=all_passed,
            steps=list(self._steps),
            trace_summary=summary,
            trace_assertions=list(self._trace_assertions),
            duration_ms=duration_ms,
            errors=list(self._errors),
            reward_score=reward,
        )

    def step(self, name: str, passed: bool, message: str = "", **data):
        self._steps.append(StepResult(name=name, passed=passed, message=message, data=data))

    def expect_trace(self, event_type: str, min_count: int = 1, max_count: int | None = None):
        self._trace_assertions.append(TraceAssertion(
            event_type=event_type, min_count=min_count, max_count=max_count))
