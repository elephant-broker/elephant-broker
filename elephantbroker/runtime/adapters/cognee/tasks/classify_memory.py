"""Task: classify facts into memory tiers based on category."""
from __future__ import annotations

import logging
from typing import Any

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.fact import FactAssertion, MemoryClass

logger = logging.getLogger("elephantbroker.tasks.classify_memory")

# Rule table: category -> MemoryClass
_CATEGORY_MAP: dict[str, MemoryClass] = {
    "constraint": MemoryClass.POLICY,
    "procedure_ref": MemoryClass.POLICY,
    "preference": MemoryClass.SEMANTIC,
    "identity": MemoryClass.SEMANTIC,
    "trait": MemoryClass.SEMANTIC,
    "relationship": MemoryClass.SEMANTIC,
    "project": MemoryClass.SEMANTIC,
    "system": MemoryClass.SEMANTIC,
    "event": MemoryClass.EPISODIC,
    "decision": MemoryClass.EPISODIC,
    "verification": MemoryClass.EPISODIC,
}


async def _llm_classify(fact: FactAssertion, llm_client: Any) -> MemoryClass:
    """Use LLM to classify a fact with unknown or general category."""
    try:
        result = await llm_client.complete_json(
            "Classify the following fact into one of: episodic, semantic, policy. "
            "Return JSON: {\"memory_class\": \"...\"}",
            f"Fact: {fact.text}\nCategory: {fact.category}",
            max_tokens=50,
            json_schema={
                "type": "object",
                "properties": {"memory_class": {"type": "string", "enum": ["episodic", "semantic", "policy"]}},
                "required": ["memory_class"],
            },
        )
        mc_str = result.get("memory_class", "episodic")
        return MemoryClass(mc_str)
    except Exception as exc:
        logger.warning("LLM classification failed for fact '%s': %s", fact.text[:50], exc)
        return MemoryClass.EPISODIC


@traced
async def classify_memory(
    facts: list[FactAssertion],
    llm_client: Any = None,
) -> list[tuple[FactAssertion, MemoryClass]]:
    """Classify facts into memory classes based on their category.

    Rule table:
    - constraint, procedure_ref -> POLICY
    - preference, identity, trait, relationship, project, system -> SEMANTIC
    - event, decision, verification -> EPISODIC
    - general or unknown -> LLM fallback if available, else EPISODIC
    - PROCEDURAL is NOT assigned here

    Returns list of (FactAssertion, MemoryClass) tuples.
    """
    results: list[tuple[FactAssertion, MemoryClass]] = []

    for fact in facts:
        category = fact.category.lower() if fact.category else "general"

        if category in _CATEGORY_MAP:
            mc = _CATEGORY_MAP[category]
        elif category == "general" or category not in _CATEGORY_MAP:
            # LLM fallback for general or unknown custom categories
            if llm_client is not None:
                mc = await _llm_classify(fact, llm_client)
            else:
                mc = MemoryClass.EPISODIC
        else:
            mc = MemoryClass.EPISODIC

        results.append((fact, mc))

    logger.debug("Classified %d facts", len(results))
    return results
