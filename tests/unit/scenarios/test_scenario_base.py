"""Tests for the scenario base class and models."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from tests.scenarios.base import Scenario, StepResult, ScenarioResult, TraceAssertion


# ---------------------------------------------------------------------------
# StepResult model tests
# ---------------------------------------------------------------------------


class TestStepResult:
    def test_create_with_defaults(self):
        s = StepResult(name="test", passed=True)
        assert s.message == ""
        assert s.data == {}

    def test_create_with_all_fields(self):
        s = StepResult(name="test", passed=False, message="failed", data={"key": "val"})
        assert not s.passed
        assert s.data["key"] == "val"

    def test_name_required(self):
        with pytest.raises(Exception):
            StepResult(passed=True)

    def test_passed_required(self):
        with pytest.raises(Exception):
            StepResult(name="test")


# ---------------------------------------------------------------------------
# TraceAssertion model tests
# ---------------------------------------------------------------------------


class TestTraceAssertion:
    def test_defaults(self):
        ta = TraceAssertion(event_type="fact_extracted")
        assert ta.min_count == 1
        assert ta.max_count is None
        assert ta.actual_count == 0
        assert not ta.passed

    def test_with_bounds(self):
        ta = TraceAssertion(event_type="guard_triggered", min_count=0, max_count=5)
        assert ta.max_count == 5

    def test_event_type_required(self):
        with pytest.raises(Exception):
            TraceAssertion()


# ---------------------------------------------------------------------------
# ScenarioResult model tests
# ---------------------------------------------------------------------------


class TestScenarioResult:
    def test_reward_score_stored(self):
        r = ScenarioResult(
            name="test", passed=True, steps=[], trace_summary={},
            trace_assertions=[], duration_ms=100, errors=[], reward_score=0.85,
        )
        assert r.reward_score == 0.85

    def test_all_fields_set(self):
        step = StepResult(name="s1", passed=True)
        ta = TraceAssertion(event_type="evt", passed=True, actual_count=1)
        r = ScenarioResult(
            name="scenario_x", passed=False, steps=[step],
            trace_summary={"event_counts": {"evt": 1}},
            trace_assertions=[ta], duration_ms=250,
            errors=["oops"], reward_score=0.5,
        )
        assert r.name == "scenario_x"
        assert not r.passed
        assert len(r.steps) == 1
        assert len(r.errors) == 1


# ---------------------------------------------------------------------------
# Scenario class tests
# ---------------------------------------------------------------------------


class TestScenario:
    @pytest.mark.asyncio
    async def test_step_records_result(self):
        """Calling step() adds to internal list."""
        with patch("tests.scenarios.base.OpenClawGatewaySimulator"):
            s = Scenario(base_url="http://test:8420")
            s.step("my_step", True, "it worked", extra="data")
            assert len(s._steps) == 1
            assert s._steps[0].name == "my_step"
            assert s._steps[0].passed
            assert s._steps[0].data == {"extra": "data"}

    @pytest.mark.asyncio
    async def test_step_failure(self):
        """step() can record a failure."""
        with patch("tests.scenarios.base.OpenClawGatewaySimulator"):
            s = Scenario(base_url="http://test:8420")
            s.step("bad_step", False, "went wrong")
            assert not s._steps[0].passed
            assert s._steps[0].message == "went wrong"

    @pytest.mark.asyncio
    async def test_expect_trace_records_assertion(self):
        with patch("tests.scenarios.base.OpenClawGatewaySimulator"):
            s = Scenario(base_url="http://test:8420")
            s.expect_trace("fact_extracted", min_count=2, max_count=10)
            assert len(s._trace_assertions) == 1
            assert s._trace_assertions[0].event_type == "fact_extracted"
            assert s._trace_assertions[0].min_count == 2
            assert s._trace_assertions[0].max_count == 10

    @pytest.mark.asyncio
    async def test_expect_trace_defaults(self):
        with patch("tests.scenarios.base.OpenClawGatewaySimulator"):
            s = Scenario(base_url="http://test:8420")
            s.expect_trace("some_event")
            assert s._trace_assertions[0].min_count == 1
            assert s._trace_assertions[0].max_count is None

    @pytest.mark.asyncio
    async def test_execute_catches_run_error(self):
        """If run() raises, error is captured, not re-raised."""
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            mock_sim = MockSim.return_value
            mock_sim.simulate_session_start = AsyncMock()
            mock_sim.simulate_session_end = AsyncMock()
            mock_sim.get_session_summary = AsyncMock(return_value={})

            class FailScenario(Scenario):
                name = "fail_test"
                async def run(self):
                    raise RuntimeError("boom")

            s = FailScenario(base_url="http://test:8420")
            result = await s.execute()
            assert not result.passed
            assert any("boom" in e for e in result.errors)

    @pytest.mark.asyncio
    async def test_execute_catches_teardown_error(self):
        """If teardown raises, error is captured."""
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            mock_sim = MockSim.return_value
            mock_sim.simulate_session_start = AsyncMock()
            mock_sim.simulate_session_end = AsyncMock(side_effect=RuntimeError("teardown fail"))
            mock_sim.get_session_summary = AsyncMock(return_value={})

            class OKScenario(Scenario):
                name = "ok_test"
                async def run(self):
                    self.step("s1", True)

            s = OKScenario(base_url="http://test:8420")
            result = await s.execute()
            assert not result.passed  # teardown error makes it fail
            assert any("teardown" in e.lower() for e in result.errors)

    @pytest.mark.asyncio
    async def test_reward_score_all_passing(self):
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            mock_sim = MockSim.return_value
            mock_sim.simulate_session_start = AsyncMock()
            mock_sim.simulate_session_end = AsyncMock()
            mock_sim.get_session_summary = AsyncMock(return_value={
                "event_counts": {"fact_extracted": 3}
            })

            class GoodScenario(Scenario):
                name = "good_test"
                async def run(self):
                    self.step("s1", True)
                    self.step("s2", True)
                    self.expect_trace("fact_extracted", min_count=1)

            s = GoodScenario(base_url="http://test:8420")
            result = await s.execute()
            assert result.passed
            # reward = (1.0 * 0.6 + 1.0 * 0.4) - 0 = 1.0
            assert result.reward_score == 1.0

    @pytest.mark.asyncio
    async def test_reward_score_with_failures(self):
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            mock_sim = MockSim.return_value
            mock_sim.simulate_session_start = AsyncMock()
            mock_sim.simulate_session_end = AsyncMock()
            mock_sim.get_session_summary = AsyncMock(return_value={"event_counts": {}})

            class MixedScenario(Scenario):
                name = "mixed_test"
                async def run(self):
                    self.step("pass", True)
                    self.step("fail", False)
                    self.expect_trace("missing_event", min_count=1)

            s = MixedScenario(base_url="http://test:8420")
            result = await s.execute()
            assert not result.passed
            # step_score = 1/2 = 0.5, trace_score = 0/1 = 0.0
            # reward = 0.5 * 0.6 + 0.0 * 0.4 = 0.3
            assert result.reward_score == pytest.approx(0.3)

    @pytest.mark.asyncio
    async def test_reward_score_with_error_penalty(self):
        """Errors impose a penalty on the reward score."""
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            mock_sim = MockSim.return_value
            mock_sim.simulate_session_start = AsyncMock()
            mock_sim.simulate_session_end = AsyncMock()
            mock_sim.get_session_summary = AsyncMock(return_value={
                "event_counts": {"evt": 1}
            })

            class ErrorScenario(Scenario):
                name = "error_test"
                async def run(self):
                    self.step("s1", True)
                    self.expect_trace("evt", min_count=1)
                    raise RuntimeError("oops")

            s = ErrorScenario(base_url="http://test:8420")
            result = await s.execute()
            # Would be 1.0 without penalty; 1 error -> -0.1
            assert result.reward_score == pytest.approx(0.9)
            assert not result.passed

    @pytest.mark.asyncio
    async def test_reward_score_never_negative(self):
        """Reward is clamped to 0.0 minimum."""
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            mock_sim = MockSim.return_value
            mock_sim.simulate_session_start = AsyncMock()
            mock_sim.simulate_session_end = AsyncMock()
            mock_sim.get_session_summary = AsyncMock(return_value={"event_counts": {}})

            class BadScenario(Scenario):
                name = "bad_test"
                async def run(self):
                    self.step("f1", False)
                    self.step("f2", False)
                    self.expect_trace("x", min_count=100)
                    raise RuntimeError("fail")

            s = BadScenario(base_url="http://test:8420")
            result = await s.execute()
            assert result.reward_score >= 0.0

    @pytest.mark.asyncio
    async def test_unique_session_keys(self):
        """Each Scenario instance gets a unique session key via uuid."""
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            Scenario(base_url="http://test:8420")
            Scenario(base_url="http://test:8420")
            calls = MockSim.call_args_list
            # session_key is the second positional or keyword arg
            key1 = calls[0].kwargs.get("session_key", calls[0].args[1] if len(calls[0].args) > 1 else None)
            key2 = calls[1].kwargs.get("session_key", calls[1].args[1] if len(calls[1].args) > 1 else None)
            assert key1 is not None
            assert key2 is not None
            assert key1 != key2
            assert key1.startswith("scenario:")
            assert key2.startswith("scenario:")

    @pytest.mark.asyncio
    async def test_trace_assertion_max_count_exceeded(self):
        """Trace assertion fails if actual exceeds max_count."""
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            mock_sim = MockSim.return_value
            mock_sim.simulate_session_start = AsyncMock()
            mock_sim.simulate_session_end = AsyncMock()
            mock_sim.get_session_summary = AsyncMock(return_value={
                "event_counts": {"evt": 20}
            })

            class BoundedScenario(Scenario):
                name = "bounded_test"
                async def run(self):
                    self.expect_trace("evt", min_count=1, max_count=5)

            s = BoundedScenario(base_url="http://test:8420")
            result = await s.execute()
            assert not result.passed
            assert result.trace_assertions[0].actual_count == 20
            assert not result.trace_assertions[0].passed

    @pytest.mark.asyncio
    async def test_default_class_attributes(self):
        """Scenario has expected class-level defaults."""
        assert Scenario.name == "unnamed"
        assert Scenario.required_phase == 6
        assert Scenario.required_amendment_6_2 is False

    @pytest.mark.asyncio
    async def test_no_steps_no_traces_passes(self):
        """A scenario with no steps and no trace assertions passes (vacuously)."""
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            mock_sim = MockSim.return_value
            mock_sim.simulate_session_start = AsyncMock()
            mock_sim.simulate_session_end = AsyncMock()
            mock_sim.get_session_summary = AsyncMock(return_value={})

            class EmptyScenario(Scenario):
                name = "empty_test"
                async def run(self):
                    pass

            s = EmptyScenario(base_url="http://test:8420")
            result = await s.execute()
            assert result.passed
            # step_score = 0/max(0,1) = 0, trace_score = 0/max(0,1) = 0
            # But all() on empty list is True, so passed=True
            # reward = (0*0.6 + 0*0.4) - 0 = 0.0... wait
            # Actually: sum(1 for s in [] if s.passed) / max(0,1) = 0/1 = 0.0
            # But that's fine; it passes but reward is 0.
            assert result.reward_score == 0.0

    @pytest.mark.asyncio
    async def test_duration_is_positive(self):
        with patch("tests.scenarios.base.OpenClawGatewaySimulator") as MockSim:
            mock_sim = MockSim.return_value
            mock_sim.simulate_session_start = AsyncMock()
            mock_sim.simulate_session_end = AsyncMock()
            mock_sim.get_session_summary = AsyncMock(return_value={})

            class QuickScenario(Scenario):
                name = "quick_test"
                async def run(self):
                    pass

            s = QuickScenario(base_url="http://test:8420")
            result = await s.execute()
            assert result.duration_ms >= 0
