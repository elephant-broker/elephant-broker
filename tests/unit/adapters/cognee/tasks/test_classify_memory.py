"""Unit tests for classify_memory task."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.adapters.cognee.tasks.classify_memory import (
    _CATEGORY_MAP,
    classify_memory,
)
from elephantbroker.schemas.fact import BUILTIN_CATEGORIES, FactAssertion, FactCategory, MemoryClass


class TestClassifyMemory:
    async def test_constraint_maps_to_policy(self):
        facts = [FactAssertion(text="Never delete production data", category="constraint")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.POLICY

    async def test_procedure_ref_maps_to_policy(self):
        facts = [FactAssertion(text="Follow the deploy checklist", category="procedure_ref")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.POLICY

    async def test_preference_maps_to_semantic(self):
        facts = [FactAssertion(text="User prefers dark mode", category="preference")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.SEMANTIC

    async def test_identity_maps_to_semantic(self):
        facts = [FactAssertion(text="User is Jane", category="identity")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.SEMANTIC

    async def test_trait_maps_to_semantic(self):
        facts = [FactAssertion(text="User is detail-oriented", category="trait")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.SEMANTIC

    async def test_relationship_maps_to_semantic(self):
        facts = [FactAssertion(text="Alice works with Bob", category="relationship")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.SEMANTIC

    async def test_project_maps_to_semantic(self):
        facts = [FactAssertion(text="The project uses Neo4j", category="project")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.SEMANTIC

    async def test_system_maps_to_semantic(self):
        facts = [FactAssertion(text="System runs on Kubernetes", category="system")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.SEMANTIC

    async def test_event_maps_to_episodic(self):
        facts = [FactAssertion(text="Deployed v2.0 today", category="event")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.EPISODIC

    async def test_decision_maps_to_episodic(self):
        facts = [FactAssertion(text="Decided to use Postgres", category="decision")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.EPISODIC

    async def test_verification_maps_to_episodic(self):
        facts = [FactAssertion(text="Tests passed on CI", category="verification")]
        result = await classify_memory(facts)
        assert result[0][1] == MemoryClass.EPISODIC

    async def test_general_defaults_to_episodic_without_llm(self):
        facts = [FactAssertion(text="Some general information", category="general")]
        result = await classify_memory(facts, llm_client=None)
        assert result[0][1] == MemoryClass.EPISODIC

    async def test_general_uses_llm_when_available(self):
        llm = MagicMock()
        llm.complete_json = AsyncMock(return_value={"memory_class": "semantic"})
        facts = [FactAssertion(text="General fact", category="general")]
        result = await classify_memory(facts, llm_client=llm)
        assert result[0][1] == MemoryClass.SEMANTIC

    async def test_llm_fallback_default_episodic(self):
        """When the LLM raises, classify_memory falls back to EPISODIC on the general-category path."""
        llm = MagicMock()
        llm.complete_json = AsyncMock(side_effect=RuntimeError("LLM provider unavailable"))
        facts = [FactAssertion(text="Some general information", category="general")]
        result = await classify_memory(facts, llm_client=llm)
        assert result[0][1] == MemoryClass.EPISODIC

    async def test_unknown_category_defaults_episodic_without_llm(self):
        facts = [FactAssertion(text="Custom stuff", category="my_custom_cat")]
        result = await classify_memory(facts, llm_client=None)
        assert result[0][1] == MemoryClass.EPISODIC

    async def test_empty_input_returns_empty(self):
        result = await classify_memory([])
        assert result == []

    async def test_preserves_fact_in_tuple(self):
        fact = FactAssertion(text="preserved", category="event")
        result = await classify_memory([fact])
        assert result[0][0] is fact

    async def test_all_builtin_categories_mapped(self):
        """Every builtin category should be either in the rule table or handled as general."""
        for cat in BUILTIN_CATEGORIES:
            fact = FactAssertion(text=f"test {cat}", category=cat)
            result = await classify_memory([fact])
            assert len(result) == 1
            _, mc = result[0]
            assert isinstance(mc, MemoryClass)

    async def test_procedural_not_assigned(self):
        """PROCEDURAL should never be assigned by classify_memory."""
        for cat in BUILTIN_CATEGORIES:
            fact = FactAssertion(text=f"test {cat}", category=cat)
            result = await classify_memory([fact])
            _, mc = result[0]
            assert mc != MemoryClass.PROCEDURAL
