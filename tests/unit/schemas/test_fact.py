"""Tests for fact schemas."""
import uuid

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, FactCategory, FactConflict


class TestFactCategory:
    def test_all_categories(self):
        assert len(FactCategory) == 12

    def test_spec_categories_present(self):
        expected = {
            "IDENTITY", "PREFERENCE", "EVENT", "DECISION", "SYSTEM", "RELATIONSHIP",
            "TRAIT", "PROJECT", "GENERAL", "CONSTRAINT", "PROCEDURE_REF", "VERIFICATION",
        }
        assert {c.name for c in FactCategory} == expected


class TestFactAssertion:
    def test_valid_creation(self):
        fact = FactAssertion(text="Python is great", category=FactCategory.GENERAL)
        assert fact.confidence == 1.0
        assert fact.scope == Scope.SESSION

    def test_empty_text_rejected(self):
        with pytest.raises(ValidationError):
            FactAssertion(text="", category=FactCategory.GENERAL)

    def test_confidence_bounds(self):
        fact = FactAssertion(text="x", category=FactCategory.PREFERENCE, confidence=0.5)
        assert fact.confidence == 0.5
        with pytest.raises(ValidationError):
            FactAssertion(text="x", category=FactCategory.PREFERENCE, confidence=1.5)
        with pytest.raises(ValidationError):
            FactAssertion(text="x", category=FactCategory.PREFERENCE, confidence=-0.1)

    def test_json_round_trip(self):
        fact = FactAssertion(text="test", category=FactCategory.DECISION)
        data = fact.model_dump(mode="json")
        restored = FactAssertion.model_validate(data)
        assert restored.text == fact.text
        assert restored.category == fact.category

    def test_optional_fields_default(self):
        fact = FactAssertion(text="x", category=FactCategory.EVENT)
        assert fact.source_actor_id is None
        assert fact.target_actor_ids == []
        assert fact.goal_ids == []
        assert fact.use_count == 0
        assert fact.successful_use_count == 0
        assert fact.last_used_at is None
        assert fact.freshness_score is None
        assert fact.provenance_refs == []
        assert fact.embedding_ref is None
        assert fact.token_size is None

    def test_target_actor_ids_list(self):
        ids = [uuid.uuid4(), uuid.uuid4()]
        fact = FactAssertion(text="x", category=FactCategory.RELATIONSHIP, target_actor_ids=ids)
        assert len(fact.target_actor_ids) == 2

    def test_successful_use_count_non_negative(self):
        with pytest.raises(ValidationError):
            FactAssertion(text="x", category=FactCategory.GENERAL, successful_use_count=-1)


class TestFactConflict:
    def test_valid_creation(self):
        c = FactConflict(fact_a_id=uuid.uuid4(), fact_b_id=uuid.uuid4(), description="contradicts")
        assert c.resolved is False
        assert c.resolution is None
        assert c.conflict_type == ""

    def test_conflict_type(self):
        c = FactConflict(
            fact_a_id=uuid.uuid4(), fact_b_id=uuid.uuid4(),
            description="contradicts", conflict_type="semantic",
        )
        assert c.conflict_type == "semantic"

    def test_json_round_trip(self):
        c = FactConflict(fact_a_id=uuid.uuid4(), fact_b_id=uuid.uuid4(), description="conflict")
        data = c.model_dump(mode="json")
        restored = FactConflict.model_validate(data)
        assert restored.description == c.description


class TestMemoryClass:
    def test_enum_values(self):
        from elephantbroker.schemas.fact import MemoryClass
        assert MemoryClass.EPISODIC == "episodic"
        assert MemoryClass.SEMANTIC == "semantic"
        assert MemoryClass.PROCEDURAL == "procedural"
        assert MemoryClass.POLICY == "policy"
        assert MemoryClass.WORKING_MEMORY == "working_memory"
        assert len(MemoryClass) == 5

    def test_default_on_fact_assertion(self):
        from elephantbroker.schemas.fact import FactAssertion, MemoryClass
        f = FactAssertion(text="test", category="general")
        assert f.memory_class == MemoryClass.EPISODIC

    def test_session_key_field(self):
        from elephantbroker.schemas.fact import FactAssertion
        f = FactAssertion(text="test", category="general", session_key="agent:main:main")
        assert f.session_key == "agent:main:main"

    def test_session_id_field(self):
        import uuid
        from elephantbroker.schemas.fact import FactAssertion
        sid = uuid.uuid4()
        f = FactAssertion(text="test", category="general", session_id=sid)
        assert f.session_id == sid

    def test_category_accepts_builtin_string(self):
        from elephantbroker.schemas.fact import FactAssertion
        f = FactAssertion(text="test", category="preference")
        assert f.category == "preference"

    def test_category_accepts_custom_string(self):
        from elephantbroker.schemas.fact import FactAssertion
        f = FactAssertion(text="test", category="code_decision")
        assert f.category == "code_decision"

    def test_category_accepts_fact_category_enum(self):
        from elephantbroker.schemas.fact import FactAssertion, FactCategory
        f = FactAssertion(text="test", category=FactCategory.IDENTITY)
        assert f.category == "identity"

    def test_builtin_categories_constant(self):
        from elephantbroker.schemas.fact import BUILTIN_CATEGORIES, FactCategory
        assert len(BUILTIN_CATEGORIES) == len(FactCategory)
        for cat in FactCategory:
            assert cat.value in BUILTIN_CATEGORIES
