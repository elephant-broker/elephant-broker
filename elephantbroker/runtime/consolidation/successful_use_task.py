"""RT-1: LLM-Based Successful Use Reasoning — batch evaluation of injected fact usefulness.

Off by default (SuccessfulUseConfig.enabled=False). Fires as background asyncio.create_task
from ContextLifecycle.after_turn() every batch_size turns or batch_timeout_seconds.

LLM result OVERRIDES Phase 6 heuristic attribution for evaluated turns.
"""
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elephantbroker.runtime.interfaces.memory_store import IMemoryStoreFacade
    from elephantbroker.schemas.config import SuccessfulUseConfig
    from elephantbroker.schemas.fact import FactAssertion
    from elephantbroker.schemas.goal import GoalState

logger = logging.getLogger("elephantbroker.runtime.consolidation.successful_use_task")

_EVAL_PROMPT = """You are evaluating whether injected knowledge was useful to the agent.

INJECTED FACTS:
{fact_list}

SESSION GOALS:
{goal_list}

CONVERSATION (last {turn_count} turns):
{conversation}

For each fact, determine if it:
a) Was directly referenced or acted upon by the agent
b) Led to progress toward a session goal
c) Received positive user feedback (user accepted the agent's action)

Return a JSON object: {{"used_fact_indices": [int], "reasoning": "brief explanation"}}
Only include facts you are confident (>70%) actually contributed.
Do NOT mark a fact as used if the agent would have done the same without it."""


class SuccessfulUseReasoningTask:
    """Batch LLM evaluation of injected fact usefulness."""

    def __init__(
        self,
        config: SuccessfulUseConfig,
        memory_store: IMemoryStoreFacade,
    ) -> None:
        self._config = config
        self._memory = memory_store
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

    async def evaluate_batch(
        self,
        injected_facts: list[FactAssertion],
        turn_messages: list[list[dict]],
        session_goals: list[GoalState],
        gateway_id: str,
    ) -> list[str]:
        """Evaluate which injected facts contributed across a batch of turns.

        Returns list of fact IDs that were successfully used.
        Increments successful_use_count on each confirmed fact.
        """
        if not self._client or not injected_facts:
            return []

        facts_window = injected_facts[: self._config.feed_last_facts]

        # Build prompt
        fact_list = "\n".join(f"[{i}] {f.text}" for i, f in enumerate(facts_window))
        goal_list = "\n".join(f"- {g.title}: {g.description or ''}" for g in session_goals) or "(none)"
        conversation = ""
        for turn_idx, msgs in enumerate(turn_messages):
            for msg in msgs:
                role = msg.get("role", "unknown")
                content = msg.get("content", "")[:500]
                conversation += f"[Turn {turn_idx + 1}] {role}: {content}\n"

        prompt = _EVAL_PROMPT.format(
            fact_list=fact_list,
            goal_list=goal_list,
            turn_count=len(turn_messages),
            conversation=conversation[:4000],
        )

        # Call LLM
        try:
            response = await self._client.post(
                "/chat/completions",
                json={
                    "model": self._config.model,
                    "messages": [
                        {"role": "system", "content": "You are a knowledge evaluation assistant."},
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
            logger.warning("RT-1 LLM call failed", exc_info=True)
            return []

        # Parse response
        try:
            result = json.loads(content)
            used_indices = result.get("used_fact_indices", [])
        except (json.JSONDecodeError, TypeError, KeyError):
            logger.warning("RT-1 JSON parse failed: %s", content[:200])
            return []

        # Update successful_use_count for confirmed facts
        used_ids: list[str] = []
        for idx in used_indices:
            if not isinstance(idx, int) or idx < 0 or idx >= len(facts_window):
                continue
            fact = facts_window[idx]
            try:
                await self._memory.update(
                    fact.id,
                    {"successful_use_count": fact.successful_use_count + 1},
                )
                used_ids.append(str(fact.id))
            except Exception:
                logger.warning("RT-1 fact update failed for %s", fact.id, exc_info=True)

        logger.info(
            "RT-1: evaluated %d facts across %d turns, %d confirmed used (gateway=%s)",
            len(facts_window), len(turn_messages), len(used_ids), gateway_id,
        )
        return used_ids

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
