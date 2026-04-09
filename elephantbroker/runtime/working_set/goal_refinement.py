"""Goal refinement task — processes hints and refines goals."""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import UTC, datetime

from elephantbroker.runtime.adapters.llm.util import strip_markdown_fences
from elephantbroker.runtime.observability import GatewayLoggerAdapter, traced
from elephantbroker.schemas.config import GoalRefinementConfig, LLMConfig
from elephantbroker.schemas.goal import GoalState, GoalStatus

logger = logging.getLogger("elephantbroker.runtime.working_set.goal_refinement")


class GoalRefinementTask:
    """Processes goal hints — Tier 1 (direct) and Tier 2 (LLM-powered)."""

    def __init__(self, llm_client=None, config: GoalRefinementConfig | None = None,
                 trace_ledger=None, metrics=None, gateway_id: str = "",
                 llm_config: LLMConfig | None = None) -> None:
        self._llm = llm_client
        self._config = config or GoalRefinementConfig()
        self._trace = trace_ledger
        self._metrics = metrics
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})

        # TD-39 Issue F: Tier 2 LLM calls should use the cheap model declared
        # in GoalRefinementConfig.model (default gemini/gemini-2.5-flash-lite),
        # not the main self._llm which is pinned to the expensive EB_LLM_MODEL at
        # init. Instantiate a dedicated httpx.AsyncClient bound to
        # goal_refinement.model + main LLM endpoint + main LLM api_key.
        # TODO(TD-39 long-term): replace this dedicated client with a per-call
        # model override on LLMClient (step 4 Option (a)) once LLMClient grows
        # that API. Until then, this is Option (b) — mirrors the
        # BlockerExtractionTask pattern but points at the runtime's main LLM
        # endpoint rather than a separate RT-2 endpoint.
        self._cheap_client = None
        if llm_config is not None and self._config.refinement_task_enabled:
            import httpx
            headers = {}
            api_key = llm_config.api_key
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
            self._cheap_client = httpx.AsyncClient(
                base_url=llm_config.endpoint,
                headers=headers,
                timeout=30.0,
            )

    async def close(self) -> None:
        """Release the dedicated cheap-model httpx client."""
        if self._cheap_client is not None:
            await self._cheap_client.aclose()
            self._cheap_client = None

    async def _call_cheap_model(self, system: str, user: str, max_tokens: int = 500) -> dict | None:
        """Invoke the cheap model via the dedicated httpx client.

        Returns the parsed JSON dict on success, None on any failure (network,
        HTTP, parse). Caller handles the None case by falling through to its
        own error path.

        Three bugs fixed in the TD-39 hotfix:
        - Wrong model name: the default is now gemini-2.5-flash-lite (see
          GoalRefinementConfig.model comment); the stale flash alias returned
          HTTP 404 with a "model not found" body that the original error
          handling swallowed.
        - Markdown fence wrapping: LiteLLM's Gemini backends wrap JSON output
          in ```json ... ``` fences even with response_format=json_object. We
          strip fences via strip_markdown_fences before json.loads().
        - Non-200 surfacing: previously response.raise_for_status() bubbled up
          as an HTTPStatusError whose repr did not include the body, and on
          some paths the code fell through to json.loads('') and raised a
          cryptic JSONDecodeError. Now we check status_code first and raise a
          RuntimeError whose message includes the real HTTP code, model name,
          and a truncated response body so journalctl shows the root cause.
        """
        if self._cheap_client is None:
            return None
        try:
            response = await self._cheap_client.post(
                "/chat/completions",
                json={
                    "model": self._config.model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    "max_tokens": max_tokens,
                    "temperature": 0.1,
                    "response_format": {"type": "json_object"},
                },
            )
            if response.status_code != 200:
                body = response.text[:500]
                raise RuntimeError(
                    f"cheap-model call failed: HTTP {response.status_code} "
                    f"from /chat/completions model={self._config.model} body={body}"
                )
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            content = strip_markdown_fences(content)
            parsed = json.loads(content)
            return parsed if isinstance(parsed, dict) else None
        except Exception as exc:
            self._log.warning("Cheap-model LLM call failed: %s", exc)
            return None

    @staticmethod
    def _format_conversation_slice(messages: list[dict], n: int) -> str:
        """Slice the last N messages and format as role: content lines.

        Returns an empty string if messages is empty. Content is truncated to
        500 chars per message (mirrors the BlockerExtractionTask pattern so
        prompt budgets stay comparable).
        """
        if not messages:
            return ""
        recent = messages[-n:]
        lines: list[str] = []
        for msg in recent:
            role = msg.get("role", "unknown")
            content = str(msg.get("content", ""))[:500]
            lines.append(f"{role}: {content}")
        return "\n".join(lines)

    @traced
    async def process_hint(
        self, goal: GoalState, hint: str, evidence: str,
        recent_messages: list[dict] | None = None,
        session_goals: list[GoalState] | None = None,
        session_key: str = "", session_id: uuid.UUID | None = None,
        session_goal_store=None,
        obstacle_hint: str | None = None,
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
            # TD-39 Sketch D part 1 (decision Q6 = both): also append to
            # goal.evidence as an audit-log entry so the per-goal event log
            # is symmetric across all 4 Tier 1 hints. success_criteria keeps
            # the "checkable claim" semantics; goal.evidence carries the
            # "completion happened" audit trail.
            if evidence:
                entry = f"completed: {evidence}"
                if entry not in goal.evidence:
                    goal.evidence.append(entry)
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
            # TD-39 Sketch D part 1: capture the abandonment reason (previously
            # silently discarded). goal.evidence becomes the per-goal audit log.
            if evidence:
                entry = f"abandoned: {evidence}"
                if entry not in goal.evidence:
                    goal.evidence.append(entry)
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
            # TD-39 Sketch D part 1: capture the progress description
            # (previously silently discarded).
            if evidence:
                entry = f"progressed: {evidence}"
                if entry not in goal.evidence:
                    goal.evidence.append(entry)
            return goal

        # Tier 2: LLM-powered (fire-and-forget if async)
        elif hint == "refined" and self._config.refinement_task_enabled:
            return await self._refine_goal(goal, evidence, recent_messages or [])

        elif hint == "new_subgoal" and self._config.refinement_task_enabled:
            return await self._create_subgoal(
                goal, evidence, recent_messages or [],
                session_goals or [],
                obstacle_hint=obstacle_hint,
            )

        return None

    async def _refine_goal(self, goal: GoalState, evidence: str,
                           messages: list[dict]) -> GoalState:
        """LLM-powered goal refinement — rewrites title/description/criteria.

        TD-39 Sketch D part 2: consume the `messages` parameter (previously
        dead — declared but never read). Slice to config.feed_recent_messages
        and include as a "RECENT CONVERSATION" section so the refinement LLM
        finally has the context that motivated the `refined` hint.
        """
        if self._metrics:
            self._metrics.inc_goal_refinement_call()

        conversation = self._format_conversation_slice(
            messages, self._config.feed_recent_messages
        )

        criteria_list = "\n".join(f"- {c}" for c in goal.success_criteria) or "(none)"
        parts = [
            "Refine this goal based on new evidence and the recent conversation.",
            "",
            "CURRENT GOAL:",
            f"  Title: {goal.title}",
            f"  Description: {goal.description or '(empty)'}",
            "  Success criteria:",
            criteria_list,
            "",
            f"NEW EVIDENCE: {evidence}",
        ]
        if conversation:
            parts.extend(["", "RECENT CONVERSATION:", conversation])
        parts.extend([
            "",
            "Rewrite title, description, and success_criteria to reflect the deepened understanding.",
            "Keep the scope aligned with the original intent — this is a rewording/refinement, not a pivot.",
            'Return JSON: {"title": "...", "description": "...", "success_criteria": ["..."]}',
        ])
        prompt = "\n".join(parts)

        t0 = time.monotonic()
        try:
            # Prefer cheap-model client; fall back to self._llm only if the
            # cheap client isn't wired (tests, legacy callers).
            result: dict | None = None
            if self._cheap_client is not None:
                result = await self._call_cheap_model(
                    "You refine goal definitions.", prompt, max_tokens=500,
                )
            if result is None and self._llm is not None:
                try:
                    result = await self._llm.complete_json(
                        "You refine goal definitions.", prompt, max_tokens=500,
                    )
                except Exception as exc:
                    self._log.warning("Goal refinement LLM call failed: %s", exc)
                    result = None
            if isinstance(result, dict):
                if "title" in result:
                    goal.title = result["title"]
                if "description" in result:
                    goal.description = result["description"]
                if "success_criteria" in result:
                    goal.success_criteria = result["success_criteria"]
            goal.updated_at = datetime.now(UTC)
        finally:
            if self._metrics:
                self._metrics.observe_goal_refinement_duration(time.monotonic() - t0)
        return goal

    async def _create_subgoal(
        self, parent: GoalState, evidence: str, messages: list[dict],
        session_goals: list[GoalState],
        obstacle_hint: str | None = None,
    ) -> GoalState | None:
        """Create a sub-goal via the cheap-model LLM.

        TD-39 Issue F + Sketch D part 2 + TD-48 quality-rule migration:
        - Uses the dedicated cheap-model httpx client (self._cheap_client),
          bound to GoalRefinementConfig.model (default gemini/gemini-2.5-flash-lite),
          instead of the expensive self._llm which is pinned to EB_LLM_MODEL.
        - Consumes the `messages` parameter (previously dead — declared but
          never read). Slices to config.feed_recent_messages and includes as
          a "RECENT CONVERSATION" section.
        - Accepts `obstacle_hint`: when the caller is HintProcessor dispatching
          a `new_subgoal` that was paired with a `blocked` hint on the same
          goal, the blocked.evidence is passed here as the obstacle text so
          the LLM sees both the problem and is asked for the proposed work.
        - Carries the RT-2 anti-false-positive quality rules verbatim from
          BlockerExtractionTask._BLOCKER_PROMPT (TD-48 prerequisite — these
          rules must live here before RT-2 can be deleted).
        """
        # Count ALL subgoals in the session, not just per-parent
        subgoal_count = sum(1 for g in session_goals if g.parent_goal_id is not None)
        if subgoal_count >= self._config.max_subgoals_per_session:
            self._log.info("Subgoal limit reached for goal %s", parent.id)
            return None

        if self._cheap_client is None and self._llm is None:
            # Without any LLM client, create a simple subgoal from evidence.
            # Preserved for tests / legacy callers that construct
            # GoalRefinementTask without llm_client or llm_config.
            subgoal = GoalState(
                title=evidence[:100] if evidence else "Sub-task",
                parent_goal_id=parent.id,
            )
            if self._metrics:
                self._metrics.inc_subgoals_created()
            return subgoal

        if self._metrics:
            self._metrics.inc_goal_refinement_call()

        # Build sibling list for cross-reference (LLM dedup hint; Jaccard
        # dedup also runs below).
        siblings = [g for g in session_goals if g.parent_goal_id == parent.id]
        sibling_lines = (
            "\n".join(f"- {g.title}" for g in siblings) if siblings else "(none)"
        )

        criteria_list = "\n".join(f"- {c}" for c in parent.success_criteria) or "(none)"
        conversation = self._format_conversation_slice(
            messages, self._config.feed_recent_messages
        )

        parts = [
            "Create a concrete sub-goal that advances or unblocks the parent goal.",
            "",
            "PARENT GOAL:",
            f"  Title: {parent.title}",
            f"  Description: {parent.description or '(empty)'}",
            "  Success criteria:",
            criteria_list,
            "",
            f"PROPOSED SUB-GOAL HINT: {evidence}",
        ]
        if obstacle_hint:
            parts.extend([
                "",
                f"OBSTACLE (paired `blocked` hint for the same parent goal): {obstacle_hint}",
                "Transform this obstacle into the minimum next action that would unblock the parent.",
            ])
        if conversation:
            parts.extend(["", "RECENT CONVERSATION:", conversation])
        parts.extend([
            "",
            "EXISTING SIBLING SUB-GOALS (do NOT duplicate):",
            sibling_lines,
            "",
            "Quality rules:",
            "- A sub-goal is a CONCRETE next action that advances or unblocks the parent.",
            "- Do NOT restate the obstacle; propose the WORK that resolves it.",
            "- Do NOT propose a sub-goal if the obstacle was already resolved earlier in the conversation.",
            "- Do NOT duplicate any of the existing sibling sub-goals listed above.",
            "- Only propose a sub-goal you are confident about.",
            "",
            'Return JSON: {"title": "...", "description": "...", "success_criteria": ["..."]}',
        ])
        prompt = "\n".join(parts)

        t0 = time.monotonic()
        try:
            result: dict | None = None
            if self._cheap_client is not None:
                result = await self._call_cheap_model(
                    "You create concrete sub-goals that unblock or advance a parent goal.",
                    prompt,
                    max_tokens=500,
                )
            if result is None and self._llm is not None:
                try:
                    result = await self._llm.complete_json(
                        "You create concrete sub-goals that unblock or advance a parent goal.",
                        prompt,
                        max_tokens=500,
                    )
                except Exception as exc:
                    self._log.warning("Subgoal creation LLM call failed: %s", exc)
                    result = None
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
