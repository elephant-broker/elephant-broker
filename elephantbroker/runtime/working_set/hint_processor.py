"""GoalHintProcessor — dispatches extraction hints to GoalRefinementTask."""
from __future__ import annotations

import asyncio
import logging
import uuid

from elephantbroker.runtime.observability import GatewayLoggerAdapter, traced
from elephantbroker.runtime.working_set.goal_refinement import GoalRefinementTask
from elephantbroker.schemas.config import GoalRefinementConfig
from elephantbroker.schemas.goal import GoalState
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.runtime.working_set.hint_processor")

# Tier 1 hints (direct, no LLM)
_TIER1_HINTS = {"completed", "abandoned", "blocked", "progressed"}


class GoalHintProcessor:
    """Dispatches goal status hints from extraction to GoalRefinementTask."""

    def __init__(self, session_goal_store, goal_refinement_task: GoalRefinementTask,
                 config: GoalRefinementConfig | None = None,
                 trace_ledger=None, metrics=None, gateway_id: str = "") -> None:
        self._store = session_goal_store
        self._refinement = goal_refinement_task
        self._config = config or GoalRefinementConfig()
        self._trace = trace_ledger
        self._metrics = metrics
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})

    @traced
    async def process_hints(
        self, hints: list[dict], session_goals: list[GoalState],
        session_key: str, session_id: uuid.UUID,
        recent_messages: list[dict] | None = None,
    ) -> None:
        """Process all hints from extraction output.

        TD-39 Issue F (correlation): before dispatching hints, group them by
        goal_index and look up paired hints. Specifically, when a
        `new_subgoal` hint shares a goal_index with a `blocked` hint in the
        same batch (per the TD-39 prompt pairing rule), pass the
        `blocked.evidence` (the obstacle text) as `obstacle_hint` to
        `_create_subgoal` so the LLM sees both the problem and is asked to
        propose the unblocking work.
        """
        if not self._config.hints_enabled or not hints:
            return

        # Pre-pass: build a per-goal_index map of hint_type -> hint_dict.
        # Later hints of the same type for the same goal_index overwrite
        # earlier ones (the LLM shouldn't emit duplicates, but if it does
        # the last one wins — same semantics as the pre-correlation loop).
        hints_by_goal: dict[int, dict[str, dict]] = {}
        for h in hints:
            gi = h.get("goal_index")
            ht = h.get("hint", "")
            if not isinstance(gi, int) or not ht:
                continue
            hints_by_goal.setdefault(gi, {})[ht] = h

        for hint_data in hints:
            goal_index = hint_data.get("goal_index")
            hint_type = hint_data.get("hint", "")
            evidence = hint_data.get("evidence", "")

            if goal_index is None or goal_index < 0 or goal_index >= len(session_goals):
                continue

            goal = session_goals[goal_index]

            if hint_type in _TIER1_HINTS:
                # Tier 1: direct update (pass session_goals for parent confidence update on completion)
                updated = await self._refinement.process_hint(
                    goal, hint_type, evidence,
                    session_goals=session_goals,
                    session_key=session_key, session_id=session_id,
                )
                if updated:
                    await self._store.update_goal(
                        session_key, session_id, goal.id,
                        updated.model_dump(exclude_unset=False),
                    )
                    if self._trace:
                        await self._trace.append_event(TraceEvent(
                            event_type=TraceEventType.SESSION_GOAL_UPDATED,
                            session_id=session_id,
                            session_key=session_key,
                            goal_ids=[goal.id],
                            payload={
                                "hint_type": hint_type,
                                "tier": "1",
                                "session_key": session_key,
                            },
                        ))
            else:
                # Tier 2: LLM-powered, fire-and-forget if async.
                # For new_subgoal, look up a paired blocked hint for the
                # same goal_index and pass its evidence as obstacle_hint.
                obstacle_hint: str | None = None
                if hint_type == "new_subgoal":
                    paired_blocked = hints_by_goal.get(goal_index, {}).get("blocked")
                    if paired_blocked:
                        ob = paired_blocked.get("evidence", "")
                        if ob:
                            obstacle_hint = str(ob)

                if self._config.run_refinement_async:
                    asyncio.ensure_future(self._process_tier2(
                        goal, hint_type, evidence, session_goals,
                        session_key, session_id, recent_messages,
                        obstacle_hint,
                    ))
                else:
                    await self._process_tier2(
                        goal, hint_type, evidence, session_goals,
                        session_key, session_id, recent_messages,
                        obstacle_hint,
                    )

    async def _process_tier2(
        self, goal, hint_type, evidence, session_goals,
        session_key, session_id, recent_messages,
        obstacle_hint: str | None = None,
    ) -> None:
        try:
            result = await self._refinement.process_hint(
                goal, hint_type, evidence,
                recent_messages=recent_messages,
                session_goals=session_goals,
                session_key=session_key,
                session_id=session_id,
                obstacle_hint=obstacle_hint,
            )
            if result and hint_type == "new_subgoal":
                await self._store.add_goal(session_key, session_id, result)
                if self._trace:
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.SESSION_GOAL_CREATED,
                        session_id=session_id,
                        session_key=session_key,
                        goal_ids=[result.id],
                        payload={
                            "hint_type": hint_type,
                            "tier": "2",
                            "parent_goal_id": str(goal.id),
                            "session_key": session_key,
                        },
                    ))
            elif result:
                await self._store.update_goal(
                    session_key, session_id, goal.id,
                    result.model_dump(exclude_unset=False),
                )
                if self._trace:
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.SESSION_GOAL_UPDATED,
                        session_id=session_id,
                        session_key=session_key,
                        goal_ids=[goal.id],
                        payload={
                            "hint_type": hint_type,
                            "tier": "2",
                            "session_key": session_key,
                        },
                    ))
        except Exception as exc:
            self._log.warning("Tier 2 hint processing failed: %s", exc)
