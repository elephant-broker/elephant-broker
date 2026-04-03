"""Goal refinement task — processes hints and refines goals."""
from __future__ import annotations

import logging
import time
import uuid
from datetime import UTC, datetime

from elephantbroker.runtime.observability import GatewayLoggerAdapter, traced
from elephantbroker.schemas.config import GoalRefinementConfig
from elephantbroker.schemas.goal import GoalState, GoalStatus

logger = logging.getLogger("elephantbroker.runtime.working_set.goal_refinement")


class GoalRefinementTask:
    """Processes goal hints — Tier 1 (direct) and Tier 2 (LLM-powered)."""

    def __init__(self, llm_client=None, config: GoalRefinementConfig | None = None,
                 trace_ledger=None, metrics=None, gateway_id: str = "local") -> None:
        self._llm = llm_client
        self._config = config or GoalRefinementConfig()
        self._trace = trace_ledger
        self._metrics = metrics
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})

    @traced
    async def process_hint(
        self, goal: GoalState, hint: str, evidence: str,
        recent_messages: list[dict] | None = None,
        session_goals: list[GoalState] | None = None,
        session_key: str = "", session_id: uuid.UUID | None = None,
        session_goal_store=None,
    ) -> GoalState | None:
        """Process a goal status hint. Returns updated goal or new sub-goal."""
        if not self._config.hints_enabled:
            return None

        if self._metrics:
            self._metrics.inc_goal_hint(hint)

        # Tier 1: Direct Redis updates (no LLM)
        if hint == "completed":
            goal.status = GoalStatus.COMPLETED
            goal.updated_at = datetime.now(UTC)
            if evidence and evidence not in goal.success_criteria:
                goal.success_criteria.append(evidence)
            # Update parent confidence from sub-goal completion ratio
            if goal.parent_goal_id and session_goals:
                parent = next((g for g in session_goals if g.id == goal.parent_goal_id), None)
                if parent:
                    subgoals = [g for g in session_goals if g.parent_goal_id == parent.id]
                    if subgoals:
                        completed_ratio = sum(
                            1 for g in subgoals if g.status == GoalStatus.COMPLETED
                        ) / len(subgoals)
                        parent.confidence = max(parent.confidence, completed_ratio)
                        parent.updated_at = datetime.now(UTC)
            return goal

        elif hint == "abandoned":
            goal.status = GoalStatus.ABANDONED
            goal.updated_at = datetime.now(UTC)
            return goal

        elif hint == "blocked":
            if evidence and evidence not in goal.blockers:
                goal.blockers.append(evidence)
            goal.updated_at = datetime.now(UTC)
            return goal

        elif hint == "progressed":
            delta = self._config.progress_confidence_delta
            goal.confidence = min(1.0, goal.confidence + delta)
            goal.updated_at = datetime.now(UTC)
            return goal

        # Tier 2: LLM-powered (fire-and-forget if async)
        elif hint == "refined" and self._config.refinement_task_enabled:
            return await self._refine_goal(goal, evidence, recent_messages or [])

        elif hint == "new_subgoal" and self._config.refinement_task_enabled:
            return await self._create_subgoal(
                goal, evidence, recent_messages or [],
                session_goals or [],
            )

        return None

    async def _refine_goal(self, goal: GoalState, evidence: str,
                           messages: list[dict]) -> GoalState:
        """LLM-powered goal refinement — rewrites title/description/criteria."""
        if not self._llm:
            return goal

        if self._metrics:
            self._metrics.inc_goal_refinement_call()

        prompt = (
            f"Refine this goal based on new evidence:\n"
            f"Current: {goal.title} — {goal.description}\n"
            f"Evidence: {evidence}\n"
            f"Return JSON: {{\"title\": ..., \"description\": ..., \"success_criteria\": [...]}}"
        )
        t0 = time.monotonic()
        try:
            result = await self._llm.complete_json(
                "You refine goal definitions.", prompt,
                max_tokens=500,
            )
            if isinstance(result, dict):
                if "title" in result:
                    goal.title = result["title"]
                if "description" in result:
                    goal.description = result["description"]
                if "success_criteria" in result:
                    goal.success_criteria = result["success_criteria"]
            goal.updated_at = datetime.now(UTC)
        except Exception as exc:
            self._log.warning("Goal refinement LLM call failed: %s", exc)
        finally:
            if self._metrics:
                self._metrics.observe_goal_refinement_duration(time.monotonic() - t0)
        return goal

    async def _create_subgoal(
        self, parent: GoalState, evidence: str, messages: list[dict],
        session_goals: list[GoalState],
    ) -> GoalState | None:
        """Create a sub-goal via LLM."""
        # Count ALL subgoals in the session, not just per-parent
        subgoal_count = sum(1 for g in session_goals if g.parent_goal_id is not None)
        if subgoal_count >= self._config.max_subgoals_per_session:
            self._log.info("Subgoal limit reached for goal %s", parent.id)
            return None

        if not self._llm:
            # Without LLM, create a simple subgoal from evidence
            subgoal = GoalState(
                title=evidence[:100] if evidence else "Sub-task",
                parent_goal_id=parent.id,
            )
            if self._metrics:
                self._metrics.inc_subgoals_created()
            return subgoal

        if self._metrics:
            self._metrics.inc_goal_refinement_call()

        prompt = (
            f"Create a sub-goal for:\n"
            f"Parent: {parent.title}\n"
            f"Evidence: {evidence}\n"
            f"Return JSON: {{\"title\": ..., \"description\": ..., \"success_criteria\": [...]}}"
        )
        t0 = time.monotonic()
        try:
            result = await self._llm.complete_json(
                "You create sub-goals.", prompt, max_tokens=500,
            )
            if isinstance(result, dict) and "title" in result:
                # Dedup check
                if self._should_create_subgoal(parent.id, result["title"], session_goals):
                    subgoal = GoalState(
                        title=result["title"],
                        description=result.get("description", ""),
                        success_criteria=result.get("success_criteria", []),
                        parent_goal_id=parent.id,
                    )
                    if self._metrics:
                        self._metrics.inc_subgoals_created()
                    return subgoal
                else:
                    if self._metrics:
                        self._metrics.inc_subgoals_dedup_skipped()
        except Exception as exc:
            self._log.warning("Subgoal creation LLM call failed: %s", exc)
        finally:
            if self._metrics:
                self._metrics.observe_goal_refinement_duration(time.monotonic() - t0)
        return None

    def _should_create_subgoal(
        self, parent_id: uuid.UUID, new_title: str,
        session_goals: list[GoalState],
    ) -> bool:
        """Dedup via Jaccard similarity on sibling titles."""
        threshold = self._config.subgoal_dedup_threshold
        new_tokens = set(new_title.lower().split())
        for g in session_goals:
            if g.parent_goal_id == parent_id:
                existing_tokens = set(g.title.lower().split())
                if not new_tokens or not existing_tokens:
                    continue
                intersection = new_tokens & existing_tokens
                union = new_tokens | existing_tokens
                jaccard = len(intersection) / len(union) if union else 0
                if jaccard >= threshold:
                    return False
        return True
