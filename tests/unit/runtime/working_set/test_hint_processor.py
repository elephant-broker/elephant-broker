"""Tests for GoalHintProcessor edge cases — invalid goal_index handling
and Tier 2 exception logging."""
from __future__ import annotations

import logging
import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.working_set.goal_refinement import GoalRefinementTask
from elephantbroker.runtime.working_set.hint_processor import GoalHintProcessor
from elephantbroker.schemas.config import GoalRefinementConfig
from tests.fixtures.factories import make_goal_state


def _make_mock_store():
    store = AsyncMock()
    store.update_goal = AsyncMock()
    store.add_goal = AsyncMock()
    return store


def _make_processor(*, store=None, task=None, config=None) -> GoalHintProcessor:
    return GoalHintProcessor(
        session_goal_store=store or _make_mock_store(),
        goal_refinement_task=task or GoalRefinementTask(
            config=config or GoalRefinementConfig(),
        ),
        config=config or GoalRefinementConfig(),
    )


class TestHintProcessorInvalidGoalIndex:
    """TF-05-008 #525: invalid ``goal_index`` values are silently
    skipped, never raised.

    Pins the guard at ``hint_processor.py:69``:
    ``if goal_index is None or goal_index < 0 or goal_index >= len(session_goals): continue``

    Three branches matter — None, negative, out-of-range. A future
    regression that loosens any branch (e.g. drops the negative check
    and raises an IndexError on -1) would break the contract that
    extraction-supplied bad data degrades gracefully.
    """

    @pytest.mark.asyncio
    async def test_invalid_goal_index_skipped(self):
        store = _make_mock_store()
        config = GoalRefinementConfig(run_refinement_async=False)
        processor = _make_processor(store=store, config=config)

        goal = make_goal_state()
        # Three invalid goal_index values — none should raise, none should
        # cause a store mutation.
        hints = [
            {"goal_index": None, "hint": "completed", "evidence": "none-idx"},
            {"goal_index": -1, "hint": "completed", "evidence": "neg-idx"},
            {"goal_index": 99, "hint": "completed", "evidence": "oob-idx"},
        ]
        await processor.process_hints(
            hints, [goal],
            session_key="agent:main:main",
            session_id=uuid.uuid4(),
        )
        store.update_goal.assert_not_awaited()
        store.add_goal.assert_not_awaited()


class TestHintProcessorTier2Exception:
    """TF-05-008 #1328: Tier 2 hint failures are logged at WARNING
    even though no trace event is emitted.

    Pins ``hint_processor.py:169-170``:
    ``except Exception as exc: self._log.warning("Tier 2 hint processing failed: %s", exc)``

    The failure path is intentionally silent on the trace ledger (Tier 2
    runs fire-and-forget on the async path; emitting a trace would
    cross-contaminate the caller's session). The compensating signal is
    a WARNING log carrying the exception. A future regression that
    drops the log (e.g. the bare ``except Exception: pass`` shape we've
    eliminated elsewhere in this PR) would erase the only operator
    breadcrumb for failed Tier 2 hints.
    """

    @pytest.mark.asyncio
    async def test_tier2_exception_not_swallowed(self, caplog):
        store = _make_mock_store()
        # GoalRefinementTask whose process_hint raises — simulates an
        # internal Tier 2 failure (LLM crash, dedup explosion, etc).
        broken_task = GoalRefinementTask(config=GoalRefinementConfig())
        broken_task.process_hint = AsyncMock(
            side_effect=RuntimeError("simulated tier2 crash"),
        )
        # run_refinement_async=False so the Tier 2 path runs inline and
        # the exception is reachable from the test (the async path would
        # park the failure on the event loop and bypass our caplog).
        config = GoalRefinementConfig(run_refinement_async=False)
        processor = _make_processor(store=store, task=broken_task, config=config)

        goal = make_goal_state()
        hints = [{"goal_index": 0, "hint": "refined", "evidence": "trigger"}]
        with caplog.at_level(
            logging.WARNING,
            logger="elephantbroker.runtime.working_set.hint_processor",
        ):
            # Must NOT raise — the exception is swallowed by the Tier 2
            # guard, but the WARNING log is the compensating signal.
            await processor.process_hints(
                hints, [goal],
                session_key="agent:main:main",
                session_id=uuid.uuid4(),
            )
        # No store mutation (the failure short-circuits before update/add).
        store.update_goal.assert_not_awaited()
        store.add_goal.assert_not_awaited()
        # WARNING log fires with the exception message — pins both the
        # log level and the inclusion of the underlying error text so a
        # future regression that drops either is caught.
        assert any(
            rec.levelno == logging.WARNING
            and "Tier 2 hint processing failed" in rec.getMessage()
            and "simulated tier2 crash" in rec.getMessage()
            for rec in caplog.records
        )
