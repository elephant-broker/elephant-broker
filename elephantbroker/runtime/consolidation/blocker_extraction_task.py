"""RT-2: Blocker Extraction Task — automatic LLM-based blocker detection for session goals.

Off by default (BlockerExtractionConfig.enabled=False). Fires as background
asyncio.create_task from ContextLifecycle.after_turn() every run_every_n_turns.

Goals with non-empty blockers get must_inject=True in the scoring pipeline,
ensuring the agent ALWAYS sees blocked goals.
"""
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elephantbroker.runtime.working_set.session_goals import SessionGoalStore
    from elephantbroker.schemas.config import BlockerExtractionConfig
    from elephantbroker.schemas.goal import GoalState

logger = logging.getLogger("elephantbroker.runtime.consolidation.blocker_extraction_task")

_BLOCKER_PROMPT = """Analyze this conversation for blockers on the listed goals.

GOALS:
{goal_list}

RECENT CONVERSATION:
{messages}

Rules:
- A blocker is a CONCRETE obstacle preventing progress on a specific goal.
- Do NOT report vague concerns, future risks, or general difficulties.
- Do NOT report something that has already been resolved in the conversation.
- Do NOT repeat existing blockers already listed above.
- Only report blockers you are confident about.
- If no blockers are found, return an empty array.

Return JSON: [{{"goal_index": int, "blocker_text": "description of the blocker"}}]"""


class BlockerExtractionTask:
    """Automatic LLM-based blocker extraction for session goals."""

    def __init__(
        self,
        config: BlockerExtractionConfig,
        session_goal_store: SessionGoalStore,
    ) -> None:
        self._config = config
        self._goal_store = session_goal_store
        self._client = None
        if config.enabled:
            import httpx
            headers = {}
            api_key = config.api_key
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
            self._client = httpx.AsyncClient(
                base_url=config.endpoint,
                headers=headers,
                timeout=30.0,
            )

    async def extract(
        self,
        session_key: str,
        session_id: str,
        gateway_id: str,
        messages: list[dict],
        goals: list[GoalState],
    ) -> list[dict]:
        """Analyze recent messages for blockers on active goals.

        Returns list of {goal_index, blocker_text} dicts.
        Appends detected blockers to goal.blockers in Redis.
        """
        if not self._client or not goals or not messages:
            return []

        # Build prompt with existing blockers shown (LLM handles dedup)
        goal_lines = []
        for i, goal in enumerate(goals):
            line = f"[{i}] {goal.title}"
            if goal.description:
                line += f" — {goal.description}"
            if goal.blockers:
                line += f"\n    Existing blockers: {'; '.join(goal.blockers)}"
            goal_lines.append(line)

        recent = messages[-self._config.recent_messages_window :]
        msg_text = ""
        for msg in recent:
            role = msg.get("role", "unknown")
            content = msg.get("content", "")[:500]
            msg_text += f"{role}: {content}\n"

        prompt = _BLOCKER_PROMPT.format(
            goal_list="\n".join(goal_lines),
            messages=msg_text[:4000],
        )

        # Call LLM
        try:
            response = await self._client.post(
                "/chat/completions",
                json={
                    "model": self._config.model,
                    "messages": [
                        {"role": "system", "content": "You are a project blocker analyzer."},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": 500,
                    "temperature": 0.1,
                },
            )
            response.raise_for_status()
            data = response.json()
            content = data["choices"][0]["message"]["content"]
        except Exception:
            logger.warning("RT-2 LLM call failed", exc_info=True)
            return []

        # Parse response
        try:
            blockers = json.loads(content)
            if not isinstance(blockers, list):
                return []
        except (json.JSONDecodeError, TypeError):
            logger.warning("RT-2 JSON parse failed: %s", content[:200])
            return []

        # Apply blockers to goals in Redis
        applied: list[dict] = []
        for b in blockers:
            goal_idx = b.get("goal_index")
            text = b.get("blocker_text", "")
            if not isinstance(goal_idx, int) or goal_idx < 0 or goal_idx >= len(goals) or not text:
                continue

            goal = goals[goal_idx]
            if text not in goal.blockers:
                goal.blockers.append(text)
                try:
                    await self._goal_store.update_goal(
                        session_key, session_id, str(goal.id),
                        {"blockers": goal.blockers},
                    )
                    applied.append({"goal_index": goal_idx, "blocker_text": text})
                except Exception:
                    logger.warning("RT-2 goal update failed for %s", goal.id, exc_info=True)

        if applied:
            logger.info(
                "RT-2: extracted %d blockers across %d goals (gateway=%s)",
                len(applied), len(goals), gateway_id,
            )
        return applied

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
