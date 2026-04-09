"""Task: extract facts from conversation messages via LLM."""
from __future__ import annotations

import json
import logging
from typing import Any

from elephantbroker.runtime.observability import traced
from elephantbroker.runtime.utils.tokens import count_tokens
from elephantbroker.schemas.fact import BUILTIN_CATEGORIES
from elephantbroker.schemas.guards import DecisionDomain

logger = logging.getLogger("elephantbroker.tasks.extract_facts")

_SYSTEM_PROMPT_TEMPLATE = """\
You are a fact extraction engine for a {profile_name} agent.
Your task is to extract discrete, atomic facts from the NEW MESSAGES below.

{focus_section}
{goal_section}

TOP-LEVEL RESPONSE FIELDS (siblings at the root of the JSON object):
- "facts": array of fact objects (see per-fact schema below)
- "goal_status_hints": array of hint objects reporting session-goal status changes detected in the new messages (see GOAL STATUS HINTS section below). This is a TOP-LEVEL field, NOT nested inside each fact.

Each fact MUST have:
- "text": a clean, atomic fact statement (one sentence)
- "category": one of the valid categories listed below
- "source_turns": list of message indices (0-based) that support this fact
- "supersedes_index": index into PREVIOUSLY EXTRACTED FACTS if this fact replaces an older one, or -1
- "contradicts_index": index into PREVIOUSLY EXTRACTED FACTS if this fact contradicts (but does not replace) an older one, or -1
- "goal_relevance": array tagging which goals each fact is relevant to (session goals only)

VALID CATEGORIES: {valid_categories}

DECISION DOMAIN TAXONOMY (set decision_domain when category is "decision"):
financial, data_access, communication, code_change, scope_change, resource, info_share, delegation, record_mutation, uncategorized
When a fact has category "decision", also set decision_domain to classify the decision area.

INSTRUCTIONS:
- Extract discrete, atomic facts from the NEW MESSAGES only
- Use FOCUS AREAS to prioritize what is worth extracting
- Resolve contradictions within the new messages (extract the resolved version, not both sides)
- If a new fact replaces/updates a PREVIOUSLY EXTRACTED FACT, set supersedes_index to that fact's index
- Set supersedes_index to -1 if the fact does not supersede any previous fact
- If a new fact contradicts a PREVIOUSLY EXTRACTED FACT (without replacing it), set contradicts_index to that fact's index
- Set contradicts_index to -1 if the fact does not contradict any previous fact
- For each fact, populate goal_relevance with the indices of relevant ACTIVE SESSION GOALS and strength (direct/indirect/none)
- Do NOT produce goal_relevance for PERSISTENT GOALS
- Emit goal_status_hints at the TOP LEVEL (sibling of facts), NOT inside any fact. Hints for PERSISTENT GOALS are invalid and will be dropped — only ACTIVE SESSION GOALS are eligible
- source_turns = indices of new messages that contribute to this fact
- Return at most {max_facts} facts. Return {{"facts": [], "goal_status_hints": []}} if nothing worth extracting.

Return ONLY valid JSON matching the schema. Do not add commentary.\
"""

_TOOL_SYSTEM_PROMPT_TEMPLATE = """\
You are a fact extraction engine for a {profile_name} agent.
Your task is to extract key findings, results, and errors from structured tool output.

{focus_section}
{goal_section}

TOP-LEVEL RESPONSE FIELDS (siblings at the root of the JSON object):
- "facts": array of fact objects (see per-fact schema below)
- "goal_status_hints": array of hint objects reporting session-goal status changes detected in the new messages (see GOAL STATUS HINTS section below). This is a TOP-LEVEL field, NOT nested inside each fact.

Each fact MUST have:
- "text": a clean, atomic fact statement (one sentence)
- "category": one of the valid categories listed below
- "source_turns": list of message indices (0-based) that produced this output
- "supersedes_index": index into PREVIOUSLY EXTRACTED FACTS if this fact replaces an older one, or -1
- "contradicts_index": index into PREVIOUSLY EXTRACTED FACTS if this fact contradicts (but does not replace) an older one, or -1
- "goal_relevance": array tagging which goals each fact is relevant to (session goals only)

VALID CATEGORIES: {valid_categories}

DECISION DOMAIN TAXONOMY (set decision_domain when category is "decision"):
financial, data_access, communication, code_change, scope_change, resource, info_share, delegation, record_mutation, uncategorized
When a fact has category "decision", also set decision_domain to classify the decision area.

Focus on extracting: key results, error messages, configuration values, tool outputs that represent decisions or state.
Emit goal_status_hints at the TOP LEVEL (sibling of facts), NOT inside any fact.
Return at most {max_facts} facts. Return {{"facts": [], "goal_status_hints": []}} if nothing worth extracting.

Return ONLY valid JSON matching the schema. Do not add commentary.\
"""

_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "facts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "Clean, atomic fact statement"},
                    "category": {"type": "string"},
                    "source_turns": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                    "supersedes_index": {"type": "integer", "description": "Index in PREVIOUSLY EXTRACTED FACTS, or -1"},
                    "contradicts_index": {"type": "integer", "description": "Index in PREVIOUSLY EXTRACTED FACTS if contradicted, or -1"},
                    "decision_domain": {
                        "type": "string",
                        "description": "For decision-category facts: financial, data_access, communication, code_change, scope_change, resource, info_share, delegation, record_mutation, uncategorized",
                    },
                    "goal_relevance": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "goal_index": {"type": "integer"},
                                "strength": {"type": "string", "enum": ["direct", "indirect", "none"]},
                            },
                        },
                    },
                },
                "required": ["text", "category", "source_turns", "supersedes_index"],
            },
        },
        "goal_status_hints": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "goal_index": {"type": "integer"},
                    "hint": {"type": "string", "enum": ["completed", "abandoned", "blocked", "progressed", "refined", "new_subgoal"]},
                    "evidence": {"type": "string"},
                },
            },
        },
    },
    "required": ["facts"],
}


def _short_fact_id(fact_id: str) -> str:
    """Return an 8-hex-char short display ID from a UUID string."""
    return str(fact_id).replace("-", "")[:8]


def _build_user_prompt(messages: list[dict], recent_facts: list[dict]) -> str:
    parts: list[str] = []
    if recent_facts:
        parts.append("PREVIOUSLY EXTRACTED FACTS (reference only — set supersedes_index or contradicts_index if a new fact replaces or contradicts one):")
        for i, rf in enumerate(recent_facts):
            cat = rf.get("category", "general")
            fid = _short_fact_id(rf.get("id", "?"))
            parts.append(f'[{i}] "{rf.get("text", "")}" ({cat}, id={fid})')
        parts.append("")
    parts.append("NEW MESSAGES (extract facts from these — resolve contradictions within the batch):")
    for i, msg in enumerate(messages):
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        ts = msg.get("timestamp", "")
        if ts:
            parts.append(f"[{i}] {role} ({ts}): {content}")
        else:
            parts.append(f"[{i}] {role}: {content}")
    return "\n".join(parts)


def _build_goal_section(
    active_session_goals: list[dict] | None,
    persistent_goals: list[dict] | None,
) -> str:
    """Build the goal injection section for the system prompt."""
    lines: list[str] = []

    if active_session_goals:
        lines.append("ACTIVE SESSION GOALS (tag which goals each fact is relevant to; report status changes as hints):")
        for i, goal in enumerate(active_session_goals):
            title = goal.get("title", "")
            lines.append(f'[{i}] "{title}"')
        lines.append("")

    if persistent_goals:
        lines.append("PERSISTENT GOALS (read-only context — do NOT produce hints for these):")
        for i, goal in enumerate(persistent_goals):
            title = goal.get("title", "")
            lines.append(f'[P{i}] "{title}"')
        lines.append("")

    if active_session_goals:
        lines.append("GOAL STATUS HINTS — emit at the TOP LEVEL of your response (sibling of facts, NOT inside any fact).")
        lines.append("For each ACTIVE SESSION GOAL whose status changed in these messages, emit one hint object:")
        lines.append('  {"goal_index": <int>, "hint": "<type>", "evidence": "<text>"}')
        lines.append("")
        lines.append("Valid hint types and their semantics (emit only when you are CONFIDENT the change occurred in these messages):")
        lines.append('- "completed": the goal has been accomplished. evidence = the success criterion that was met (short phrase, not a full quote).')
        lines.append('- "abandoned": the user or agent has dropped the goal or pivoted away from it. evidence = a brief reason or the pivot trigger.')
        lines.append('- "blocked": a CONCRETE obstacle preventing progress on this goal was detected. evidence = the obstacle text itself.')
        lines.append('    * A blocker is a CONCRETE obstacle, NOT a vague concern, a future risk, or a general difficulty.')
        lines.append('    * Do NOT report something that was already resolved earlier in the conversation.')
        lines.append('    * Only report blockers you are confident about.')
        lines.append('    * A "blocked" hint MUST be paired with a "new_subgoal" hint for the same goal_index in the same response, describing the minimum next action that would unblock the parent.')
        lines.append('- "progressed": meaningful partial progress was made toward the goal. evidence = what concretely changed.')
        lines.append('- "refined": the understanding of the goal deepened and it needs rewording. evidence = the new framing.')
        lines.append('- "new_subgoal": a sub-task or unblocking action emerged. evidence = the sub-task description (the WORK to do, not a restatement of the obstacle).')
        lines.append('    * A sub-goal is a CONCRETE next action that would unblock or advance the parent.')
        lines.append('    * Do NOT restate the obstacle as a sub-goal; propose the work that resolves it.')
        lines.append('    * Do NOT propose a sub-goal if the obstacle was already resolved earlier in the conversation.')
        lines.append('    * Do NOT duplicate existing sibling sub-goals visible under the parent.')
        lines.append('    * goal_index MUST be the parent goal\'s index (one of the ACTIVE SESSION GOALS listed above).')
        lines.append('    * Emit alongside a "blocked" hint for the same goal_index when proposing how to address an obstacle.')
        lines.append("")
        lines.append("Semantic distinction for paired emissions:")
        lines.append("- blocked.evidence = the PROBLEM (e.g. \"Database migration script is missing the rollback SQL\")")
        lines.append("- new_subgoal.evidence = the proposed WORK (e.g. \"Write rollback SQL for migration 0042\")")
        lines.append("Do NOT emit hints for PERSISTENT GOALS — only ACTIVE SESSION GOALS are eligible.")
        lines.append("")

    return "\n".join(lines)


def _is_tool_only_batch(messages: list[dict]) -> bool:
    """Check if all messages in the batch are tool-output messages."""
    return all(msg.get("role") == "tool" for msg in messages) and len(messages) > 0


@traced
async def extract_facts(
    messages: list[dict],
    recent_facts: list[dict],
    llm_client: Any,
    config: Any,
    extraction_focus: list[str] | None = None,
    custom_categories: list[str] | None = None,
    profile_name: str = "coding",
    active_session_goals: list[dict] | None = None,
    persistent_goals: list[dict] | None = None,
    goal_injection_config: Any | None = None,
) -> dict:
    """Extract fact assertions from conversation messages via LLM.

    Returns a dict with keys:
      - "facts": list of raw fact dicts with keys: text, category, source_turns,
        supersedes_index, contradicts_index, goal_relevance.
      - "goal_status_hints": list of goal status hint dicts from extraction.

    On any failure or empty input, returns {"facts": [], "goal_status_hints": []}.
    """
    _empty_result: dict = {"facts": [], "goal_status_hints": []}

    # Edge case: empty or trivially short batch
    total_chars = sum(len(msg.get("content", "")) for msg in messages)
    if total_chars < 10:
        logger.debug("Batch too short (%d chars), skipping extraction", total_chars)
        return _empty_result

    # Build valid categories
    valid_categories = list(BUILTIN_CATEGORIES)
    if custom_categories:
        valid_categories.extend(custom_categories)

    max_facts = getattr(config, "extraction_max_facts_per_batch", 10)

    # Build system prompt — variant for tool-only batches
    focus_section = ""
    if extraction_focus:
        focus_section = "FOCUS AREAS (prioritize extracting information about these topics):\n" + ", ".join(extraction_focus)

    if goal_injection_config and not getattr(goal_injection_config, 'enabled', True):
        goal_section = ""
    else:
        max_session = getattr(goal_injection_config, 'max_session_goals', 5) if goal_injection_config else 5
        max_persistent = getattr(goal_injection_config, 'max_persistent_goals', 3) if goal_injection_config else 3
        capped_session = (active_session_goals or [])[:max_session]
        capped_persistent = (persistent_goals or [])[:max_persistent]
        goal_section = _build_goal_section(capped_session, capped_persistent)

    if _is_tool_only_batch(messages):
        template = _TOOL_SYSTEM_PROMPT_TEMPLATE
    else:
        template = _SYSTEM_PROMPT_TEMPLATE

    system_prompt = template.format(
        profile_name=profile_name,
        focus_section=focus_section,
        goal_section=goal_section,
        valid_categories=", ".join(valid_categories),
        max_facts=max_facts,
    )

    # Build user prompt, truncated to max input tokens
    user_prompt = _build_user_prompt(messages, recent_facts)
    max_input_tokens = getattr(config, "extraction_max_input_tokens", 4000)
    prompt_tokens = count_tokens(user_prompt)
    if prompt_tokens > max_input_tokens:
        ratio = max_input_tokens / prompt_tokens
        user_prompt = user_prompt[: int(len(user_prompt) * ratio)]

    # LLM call
    max_output_tokens = getattr(config, "extraction_max_output_tokens", 16384)
    try:
        result = await llm_client.complete_json(
            system_prompt,
            user_prompt,
            max_tokens=max_output_tokens,
            json_schema=_RESPONSE_SCHEMA,
        )
    except Exception as exc:
        logger.warning("LLM extraction failed: %s", exc)
        return _empty_result

    # Parse and validate
    try:
        facts = result.get("facts", [])
        if not isinstance(facts, list):
            return _empty_result
    except (AttributeError, TypeError):
        return _empty_result

    # Number of active session goals (for validating goal_index bounds)
    num_session_goals = len(active_session_goals) if active_session_goals else 0

    # Validate each fact has required fields
    validated: list[dict] = []
    for f in facts:
        if not isinstance(f, dict):
            continue
        if "text" not in f or not f["text"]:
            continue
        if "category" not in f:
            f["category"] = "general"
        if "source_turns" not in f:
            f["source_turns"] = []
        if "supersedes_index" not in f:
            f["supersedes_index"] = -1
        # Validate contradicts_index
        con_idx = f.get("contradicts_index", -1)
        if not isinstance(con_idx, int) or con_idx < -1 or con_idx >= len(recent_facts):
            f["contradicts_index"] = -1
        else:
            f["contradicts_index"] = con_idx
        # Validate goal_relevance entries
        raw_gr = f.get("goal_relevance", [])
        if not isinstance(raw_gr, list):
            raw_gr = []
        valid_gr: list[dict] = []
        for gr in raw_gr:
            if not isinstance(gr, dict):
                continue
            gi = gr.get("goal_index")
            strength = gr.get("strength", "none")
            if isinstance(gi, int) and 0 <= gi < num_session_goals and strength in ("direct", "indirect", "none"):
                valid_gr.append({"goal_index": gi, "strength": strength})
        f["goal_relevance"] = valid_gr
        # Validate decision_domain (only meaningful for "decision" category)
        raw_domain = f.get("decision_domain")
        if f.get("category") == "decision" and raw_domain:
            valid_domains = {d.value for d in DecisionDomain}
            if raw_domain not in valid_domains:
                logger.warning("Invalid decision_domain %r for fact, defaulting to uncategorized", raw_domain)
                f["decision_domain"] = "uncategorized"
        elif f.get("category") != "decision":
            f.pop("decision_domain", None)
        validated.append(f)

    # Validate goal_status_hints (top-level, not per-fact)
    raw_hints = result.get("goal_status_hints", []) if isinstance(result, dict) else []
    if not isinstance(raw_hints, list):
        raw_hints = []
    valid_hints: list[dict] = []
    _valid_hint_types = {"completed", "abandoned", "blocked", "progressed", "refined", "new_subgoal"}
    for h in raw_hints:
        if not isinstance(h, dict):
            continue
        gi = h.get("goal_index")
        hint_type = h.get("hint", "")
        evidence = h.get("evidence", "")
        if isinstance(gi, int) and 0 <= gi < num_session_goals and hint_type in _valid_hint_types:
            valid_hints.append({"goal_index": gi, "hint": hint_type, "evidence": str(evidence)})

    # Cap at max facts per batch
    if len(validated) > max_facts:
        logger.debug("Capping facts from %d to %d", len(validated), max_facts)
        validated = validated[:max_facts]

    logger.info("Extracted %d facts from %d messages", len(validated), len(messages))
    return {"facts": validated, "goal_status_hints": valid_hints}
