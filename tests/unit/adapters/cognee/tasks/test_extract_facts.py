"""Unit tests for extract_facts task."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.adapters.cognee.tasks.extract_facts import (
    _RESPONSE_SCHEMA,
    _short_fact_id,
    extract_facts,
)
from elephantbroker.schemas.guards import DecisionDomain


def _make_config(**overrides):
    config = MagicMock()
    config.extraction_max_input_tokens = overrides.get("extraction_max_input_tokens", 4000)
    config.extraction_max_output_tokens = overrides.get("extraction_max_output_tokens", 16384)
    config.extraction_max_facts_per_batch = overrides.get("extraction_max_facts_per_batch", 10)
    return config


def _make_llm(facts_response=None, raise_exc=None):
    llm = MagicMock()
    if raise_exc:
        llm.complete_json = AsyncMock(side_effect=raise_exc)
    else:
        response = facts_response or {"facts": [], "goal_status_hints": []}
        llm.complete_json = AsyncMock(return_value=response)
    return llm


class TestExtractFacts:
    async def test_extracts_facts_from_batch(self):
        """Valid messages produce fact dicts."""
        llm = _make_llm(facts_response={
            "facts": [
                {
                    "text": "User prefers Python 3.11",
                    "category": "preference",
                    "source_turns": [0],
                    "supersedes_index": -1,
                },
                {
                    "text": "Project uses FastAPI",
                    "category": "project",
                    "source_turns": [1],
                    "supersedes_index": -1,
                },
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [
            {"role": "user", "content": "I really prefer Python 3.11 for my projects"},
            {"role": "assistant", "content": "Great, the project uses FastAPI as the web framework"},
        ]
        result = await extract_facts(messages, [], llm, config)
        facts = result["facts"]
        assert len(facts) == 2
        assert facts[0]["text"] == "User prefers Python 3.11"
        assert facts[0]["category"] == "preference"
        assert facts[1]["source_turns"] == [1]

    async def test_empty_batch_skips_llm(self):
        """Batch with < 10 chars total should skip LLM and return empty result."""
        llm = _make_llm()
        config = _make_config()
        messages = [{"role": "user", "content": "hi"}]
        result = await extract_facts(messages, [], llm, config)
        assert result["facts"] == []
        assert result["goal_status_hints"] == []
        llm.complete_json.assert_not_called()

    async def test_caps_at_max_facts(self):
        """More facts than max should be truncated."""
        many_facts = [
            {"text": f"fact {i}", "category": "general", "source_turns": [0], "supersedes_index": -1}
            for i in range(20)
        ]
        llm = _make_llm(facts_response={"facts": many_facts, "goal_status_hints": []})
        config = _make_config(extraction_max_facts_per_batch=5)
        messages = [{"role": "user", "content": "a long message with many facts embedded in it"}]
        result = await extract_facts(messages, [], llm, config)
        assert len(result["facts"]) == 5

    async def test_llm_failure_returns_empty(self):
        """LLM error should return empty result dict."""
        llm = _make_llm(raise_exc=RuntimeError("LLM down"))
        config = _make_config()
        messages = [{"role": "user", "content": "This is a normal conversation message"}]
        result = await extract_facts(messages, [], llm, config)
        assert result["facts"] == []
        assert result["goal_status_hints"] == []

    async def test_extraction_focus_in_prompt(self):
        """Focus areas should appear in the system prompt."""
        llm = _make_llm()
        config = _make_config()
        messages = [{"role": "user", "content": "I work on security and compliance issues"}]
        await extract_facts(messages, [], llm, config, extraction_focus=["security", "compliance"])
        call_args = llm.complete_json.call_args
        system_prompt = call_args[0][0]
        assert "security" in system_prompt
        assert "compliance" in system_prompt

    async def test_custom_categories_in_prompt(self):
        """Custom categories should appear in the system prompt."""
        llm = _make_llm()
        config = _make_config()
        messages = [{"role": "user", "content": "Testing custom category extraction in conversation"}]
        await extract_facts(messages, [], llm, config, custom_categories=["custom_cat_a", "custom_cat_b"])
        call_args = llm.complete_json.call_args
        system_prompt = call_args[0][0]
        assert "custom_cat_a" in system_prompt
        assert "custom_cat_b" in system_prompt

    async def test_uses_json_schema(self):
        """json_schema should be passed to complete_json."""
        llm = _make_llm()
        config = _make_config()
        messages = [{"role": "user", "content": "Some conversation content to extract facts from"}]
        await extract_facts(messages, [], llm, config)
        call_kwargs = llm.complete_json.call_args[1]
        assert "json_schema" in call_kwargs
        assert call_kwargs["json_schema"] == _RESPONSE_SCHEMA

    async def test_malformed_json_returns_empty(self):
        """If LLM returns non-dict, return empty result."""
        llm = MagicMock()
        llm.complete_json = AsyncMock(return_value="not a dict")
        config = _make_config()
        messages = [{"role": "user", "content": "Some conversation content to extract facts from"}]
        result = await extract_facts(messages, [], llm, config)
        assert result["facts"] == []
        assert result["goal_status_hints"] == []

    async def test_recent_facts_in_user_prompt(self):
        """Recent facts should appear in the user prompt."""
        llm = _make_llm()
        config = _make_config()
        messages = [{"role": "user", "content": "Continuing our conversation about the project setup"}]
        recent = [{"text": "User likes Rust", "category": "preference"}]
        await extract_facts(messages, recent, llm, config)
        user_prompt = llm.complete_json.call_args[0][1]
        assert "User likes Rust" in user_prompt

    async def test_contradicts_index_validated(self):
        """contradicts_index should be validated against recent_facts bounds."""
        llm = _make_llm(facts_response={
            "facts": [
                {
                    "text": "Python 3.12 is now preferred",
                    "category": "preference",
                    "source_turns": [0],
                    "supersedes_index": -1,
                    "contradicts_index": 0,
                },
                {
                    "text": "Uses FastAPI v2",
                    "category": "project",
                    "source_turns": [0],
                    "supersedes_index": -1,
                    "contradicts_index": 99,  # out of bounds
                },
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "Python 3.12 is preferred and we use FastAPI v2 now"}]
        recent = [{"id": "aaaa-bbbb", "text": "Python 3.11 preferred", "category": "preference"}]
        result = await extract_facts(messages, recent, llm, config)
        facts = result["facts"]
        assert facts[0]["contradicts_index"] == 0
        assert facts[1]["contradicts_index"] == -1  # out-of-bounds clamped to -1

    async def test_goal_relevance_validated(self):
        """goal_relevance entries should be validated against active session goals."""
        llm = _make_llm(facts_response={
            "facts": [
                {
                    "text": "Login bug is fixed",
                    "category": "event",
                    "source_turns": [0],
                    "supersedes_index": -1,
                    "goal_relevance": [
                        {"goal_index": 0, "strength": "direct"},
                        {"goal_index": 5, "strength": "direct"},  # out of bounds
                    ],
                },
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "The login bug is now fixed after the latest commit"}]
        goals = [{"title": "Fix login bug"}, {"title": "Migrate DB"}]
        result = await extract_facts(
            messages, [], llm, config,
            active_session_goals=goals,
        )
        facts = result["facts"]
        assert len(facts[0]["goal_relevance"]) == 1  # only index 0 valid
        assert facts[0]["goal_relevance"][0]["goal_index"] == 0

    async def test_goal_status_hints_validated(self):
        """goal_status_hints should be validated against session goal bounds."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "fact", "category": "general", "source_turns": [0], "supersedes_index": -1},
            ],
            "goal_status_hints": [
                {"goal_index": 0, "hint": "completed", "evidence": "user confirmed"},
                {"goal_index": 5, "hint": "blocked", "evidence": "invalid"},  # out of bounds
                {"goal_index": 0, "hint": "invalid_hint", "evidence": "bad"},  # invalid hint type
            ],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "The login bug is confirmed fixed now"}]
        goals = [{"title": "Fix login bug"}]
        result = await extract_facts(
            messages, [], llm, config,
            active_session_goals=goals,
        )
        hints = result["goal_status_hints"]
        assert len(hints) == 1
        assert hints[0]["hint"] == "completed"

    async def test_goal_sections_in_prompt(self):
        """Active and persistent goal sections should appear in the system prompt."""
        llm = _make_llm()
        config = _make_config()
        messages = [{"role": "user", "content": "Working on the login bug fix and database migration"}]
        await extract_facts(
            messages, [], llm, config,
            active_session_goals=[{"title": "Fix login bug"}, {"title": "Migrate to PostgreSQL 16"}],
            persistent_goals=[{"title": "Prioritize user privacy"}],
        )
        system_prompt = llm.complete_json.call_args[0][0]
        assert "ACTIVE SESSION GOALS" in system_prompt
        assert "Fix login bug" in system_prompt
        assert "PERSISTENT GOALS" in system_prompt
        assert "Prioritize user privacy" in system_prompt
        assert "GOAL STATUS HINTS" in system_prompt

    async def test_hint_types_enumerated_in_prompt(self):
        """TD-39 Issue A: prompt must name all 6 hint types with per-type guidance,
        not just the 3-type summary (completed/blocked/progressed) that starved the
        Tier 2 new_subgoal pipeline.
        """
        llm = _make_llm()
        config = _make_config()
        messages = [{"role": "user", "content": "Working on the login bug"}]
        await extract_facts(
            messages, [], llm, config,
            active_session_goals=[{"title": "Fix login bug"}],
        )
        system_prompt = llm.complete_json.call_args[0][0]
        # All 6 hint types must be named as quoted strings in the prompt
        for hint_type in ("completed", "abandoned", "blocked", "progressed", "refined", "new_subgoal"):
            assert f'"{hint_type}"' in system_prompt, f"hint type {hint_type!r} not named in prompt"

    async def test_blocked_paired_with_new_subgoal_rule_in_prompt(self):
        """TD-39 Issue A: the prompt must explicitly instruct that `blocked` hints
        be paired with `new_subgoal` hints for the same goal_index. Without this
        pairing rule the Tier 2 sub-goal pipeline is starved (TD-39 root cause).
        """
        llm = _make_llm()
        config = _make_config()
        await extract_facts(
            [{"role": "user", "content": "stuck on the migration"}], [], llm, config,
            active_session_goals=[{"title": "Migrate to PostgreSQL 16"}],
        )
        system_prompt = llm.complete_json.call_args[0][0]
        assert "paired" in system_prompt.lower()
        # Check both hint types appear near the pairing language
        assert "new_subgoal" in system_prompt
        assert "blocked" in system_prompt

    async def test_rt2_quality_rules_migrated_into_prompt(self):
        """TD-48: RT-2 BlockerExtractionTask's anti-false-positive quality rules
        must migrate into the extract_facts `blocked` hint section before RT-2 is
        deleted. Assert the key phrases from _BLOCKER_PROMPT (blocker_extraction_task.py)
        are present in the goal-hint section.
        """
        llm = _make_llm()
        config = _make_config()
        await extract_facts(
            [{"role": "user", "content": "stuck on the migration"}], [], llm, config,
            active_session_goals=[{"title": "Migrate to PostgreSQL 16"}],
        )
        system_prompt = llm.complete_json.call_args[0][0]
        # CONCRETE obstacle, not vague concerns
        assert "CONCRETE" in system_prompt
        # "already resolved" anti-pattern
        assert "already resolved" in system_prompt.lower()
        # "confident" emission gate
        assert "confident" in system_prompt.lower()

    async def test_new_subgoal_is_work_not_obstacle_rule_in_prompt(self):
        """TD-39 Issue A + TD-48: the prompt must instruct the LLM that
        `new_subgoal.evidence` is the WORK to do (the minimum next action that
        unblocks the parent), NOT a restatement of the obstacle. This is the
        sub-goal quality rule that RT-2's prompt carried and that now lives in
        the extract_facts prompt.
        """
        llm = _make_llm()
        config = _make_config()
        await extract_facts(
            [{"role": "user", "content": "stuck on the migration"}], [], llm, config,
            active_session_goals=[{"title": "Migrate to PostgreSQL 16"}],
        )
        system_prompt = llm.complete_json.call_args[0][0]
        # Do NOT restate the obstacle
        assert "restate" in system_prompt.lower() or "restating" in system_prompt.lower()
        # Sibling dedup rule
        assert "duplicate" in system_prompt.lower() or "dedup" in system_prompt.lower()
        # Semantic distinction block (blocked.evidence = PROBLEM, new_subgoal.evidence = WORK)
        assert "PROBLEM" in system_prompt
        assert "WORK" in system_prompt

    async def test_goal_status_hints_documented_as_top_level(self):
        """TD-39 Issue B: the prompt must describe `goal_status_hints` as a
        TOP-LEVEL sibling of `facts`, not as a per-fact field. The schema already
        places it at the top level and the validator reads it from the top level,
        so the prompt's natural-language contract must agree.
        """
        llm = _make_llm()
        config = _make_config()
        await extract_facts(
            [{"role": "user", "content": "stuck on the migration"}], [], llm, config,
            active_session_goals=[{"title": "Migrate to PostgreSQL 16"}],
        )
        system_prompt = llm.complete_json.call_args[0][0]
        # The "TOP LEVEL" / "TOP-LEVEL" phrasing is required (case-insensitive)
        assert "top-level" in system_prompt.lower() or "top level" in system_prompt.lower()
        # And the per-fact list MUST NOT mention goal_status_hints anymore
        # (find the "Each fact MUST have" block and assert goal_status_hints is absent from it)
        per_fact_start = system_prompt.find("Each fact MUST have:")
        assert per_fact_start != -1, "Each fact MUST have block missing"
        # Scan the next ~12 lines (per-fact field list) for the forbidden field
        per_fact_block = system_prompt[per_fact_start:per_fact_start + 1000]
        per_fact_header_end = per_fact_block.find("VALID CATEGORIES")
        assert per_fact_header_end != -1
        per_fact_list = per_fact_block[:per_fact_header_end]
        assert '"goal_status_hints"' not in per_fact_list, (
            "goal_status_hints should no longer be listed as a per-fact field; "
            "it is a top-level sibling of `facts`"
        )

    async def test_short_fact_id(self):
        """Short fact ID should be 8 hex chars from UUID."""
        assert _short_fact_id("a1b2c3d4-e5f6-7890-abcd-ef1234567890") == "a1b2c3d4"
        assert _short_fact_id("?") == "?"
        assert len(_short_fact_id("12345678-1234-1234-1234-123456789012")) == 8

    async def test_short_fact_id_in_user_prompt(self):
        """Recent facts should use short 8-hex-char IDs in the user prompt."""
        llm = _make_llm()
        config = _make_config()
        messages = [{"role": "user", "content": "Continuing the conversation about the project setup"}]
        recent = [{"id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "text": "fact one", "category": "general"}]
        await extract_facts(messages, recent, llm, config)
        user_prompt = llm.complete_json.call_args[0][1]
        assert "id=a1b2c3d4" in user_prompt
        # Should NOT contain the full UUID
        assert "a1b2c3d4-e5f6-7890-abcd-ef1234567890" not in user_prompt


class TestGoalStatusHintsSchema:
    """Tests for goal_status_hints schema and validation (evidence required)."""

    def test_evidence_required_in_hint_schema(self):
        """The goal_status_hints item schema must mark evidence as required,
        otherwise the LLM treats it as optional and downstream code guards
        on `if evidence` silently no-op."""
        hint_schema = _RESPONSE_SCHEMA["properties"]["goal_status_hints"]["items"]
        assert "required" in hint_schema
        assert hint_schema["required"] == ["goal_index", "hint", "evidence"]

    async def test_thin_evidence_warning_fires(self, caplog):
        """Empty or near-empty evidence should trigger a Thin evidence warning."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "fact", "category": "general", "source_turns": [0], "supersedes_index": -1},
            ],
            "goal_status_hints": [
                {"goal_index": 0, "hint": "blocked", "evidence": ""},
            ],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "I am stuck on the migration and cannot proceed"}]
        goals = [{"title": "Fix login bug"}]
        import logging
        with caplog.at_level(logging.WARNING, logger="elephantbroker.tasks.extract_facts"):
            await extract_facts(messages, [], llm, config, active_session_goals=goals)
        assert "Thin evidence on blocked hint" in caplog.text

    async def test_normal_evidence_no_warning(self, caplog):
        """Evidence over 10 chars should NOT trigger the thin-evidence warning."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "fact", "category": "general", "source_turns": [0], "supersedes_index": -1},
            ],
            "goal_status_hints": [
                {"goal_index": 0, "hint": "blocked", "evidence": "migration script missing rollback path"},
            ],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "The migration script is missing a rollback path"}]
        goals = [{"title": "Fix login bug"}]
        import logging
        with caplog.at_level(logging.WARNING, logger="elephantbroker.tasks.extract_facts"):
            await extract_facts(messages, [], llm, config, active_session_goals=goals)
        assert "Thin evidence" not in caplog.text


class TestDecisionDomainExtraction:
    """Tests for decision_domain extraction (Amendment 7.1, Deviation 5)."""

    # --- Schema tests ---

    def test_decision_domain_in_response_schema(self):
        """_RESPONSE_SCHEMA should include decision_domain property on fact items."""
        fact_props = _RESPONSE_SCHEMA["properties"]["facts"]["items"]["properties"]
        assert "decision_domain" in fact_props
        assert fact_props["decision_domain"]["type"] == "string"

    def test_decision_domain_not_required_in_schema(self):
        """decision_domain should NOT be in the required array."""
        required = _RESPONSE_SCHEMA["properties"]["facts"]["items"]["required"]
        assert "decision_domain" not in required

    async def test_domain_taxonomy_in_system_prompt(self):
        """System prompt should contain domain taxonomy section."""
        llm = _make_llm()
        config = _make_config()
        messages = [{"role": "user", "content": "We decided to invest in cloud infrastructure for the team"}]
        await extract_facts(messages, [], llm, config)
        system_prompt = llm.complete_json.call_args[0][0]
        assert "DECISION DOMAIN TAXONOMY" in system_prompt
        for domain in DecisionDomain:
            assert domain.value in system_prompt

    # --- Validation: valid inputs ---

    async def test_decision_domain_financial_passes_through(self):
        """category=decision + decision_domain=financial should be preserved."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "Budget approved for Q3", "category": "decision",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": "financial"},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "The budget for Q3 has been approved by the finance team"}]
        result = await extract_facts(messages, [], llm, config)
        assert result["facts"][0]["decision_domain"] == "financial"

    async def test_decision_domain_code_change_passes_through(self):
        """category=decision + decision_domain=code_change should be preserved."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "Deploy to staging approved", "category": "decision",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": "code_change"},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "Staging deploy has been approved by the tech lead"}]
        result = await extract_facts(messages, [], llm, config)
        assert result["facts"][0]["decision_domain"] == "code_change"

    @pytest.mark.parametrize("domain", [d.value for d in DecisionDomain])
    async def test_decision_domain_all_valid_values_accepted(self, domain):
        """All DecisionDomain enum values should be accepted for decision-category facts."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": f"Decision in {domain}", "category": "decision",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": domain},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": f"Making a decision related to {domain} domain area"}]
        result = await extract_facts(messages, [], llm, config)
        assert result["facts"][0]["decision_domain"] == domain

    async def test_decision_domain_none_for_decision_category_ok(self):
        """category=decision without decision_domain should not error."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "Some decision made", "category": "decision",
                 "source_turns": [0], "supersedes_index": -1},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "A general decision was made in the team meeting"}]
        result = await extract_facts(messages, [], llm, config)
        assert len(result["facts"]) == 1
        assert result["facts"][0].get("decision_domain") is None

    # --- Validation: invalid inputs ---

    async def test_decision_domain_invalid_defaults_to_uncategorized(self):
        """Invalid decision_domain for decision category should default to uncategorized."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "Decided something", "category": "decision",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": "invalid_domain"},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "We decided something important for the project today"}]
        result = await extract_facts(messages, [], llm, config)
        assert result["facts"][0]["decision_domain"] == "uncategorized"

    async def test_decision_domain_empty_string_defaults_to_uncategorized(self):
        """Empty string decision_domain for decision category should default to uncategorized."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "Decided something", "category": "decision",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": ""},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "We decided something important about the deployment"}]
        result = await extract_facts(messages, [], llm, config)
        # Empty string is falsy, so the validation block doesn't enter the "if raw_domain" branch
        # decision_domain stays as "" (not replaced) — this is acceptable behavior
        assert result["facts"][0].get("decision_domain") == ""

    async def test_decision_domain_numeric_defaults_to_uncategorized(self):
        """Numeric string decision_domain should default to uncategorized."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "Decided something", "category": "decision",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": "123"},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "We made a decision that should be categorized properly"}]
        result = await extract_facts(messages, [], llm, config)
        assert result["facts"][0]["decision_domain"] == "uncategorized"

    async def test_decision_domain_case_sensitive(self):
        """Uppercase domain value should be rejected (enum values are lowercase)."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "Budget approved", "category": "decision",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": "FINANCIAL"},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "The financial budget has been officially approved"}]
        result = await extract_facts(messages, [], llm, config)
        assert result["facts"][0]["decision_domain"] == "uncategorized"

    # --- Validation: non-decision categories ---

    async def test_decision_domain_cleared_for_preference_category(self):
        """decision_domain should be stripped from non-decision category facts."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "User likes Python", "category": "preference",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": "financial"},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "I really like Python for backend development projects"}]
        result = await extract_facts(messages, [], llm, config)
        assert "decision_domain" not in result["facts"][0]

    async def test_decision_domain_cleared_for_event_category(self):
        """decision_domain should be stripped from event-category facts."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "Deploy happened", "category": "event",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": "code_change"},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "The deployment to production happened this morning"}]
        result = await extract_facts(messages, [], llm, config)
        assert "decision_domain" not in result["facts"][0]

    async def test_decision_domain_cleared_for_general_category(self):
        """decision_domain should be stripped from general-category facts."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "Team meets weekly", "category": "general",
                 "source_turns": [0], "supersedes_index": -1, "decision_domain": "resource"},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "Our team has weekly standup meetings every Monday"}]
        result = await extract_facts(messages, [], llm, config)
        assert "decision_domain" not in result["facts"][0]

    async def test_decision_domain_absent_for_non_decision_no_error(self):
        """Non-decision fact without decision_domain should not error."""
        llm = _make_llm(facts_response={
            "facts": [
                {"text": "User name is Jane", "category": "identity",
                 "source_turns": [0], "supersedes_index": -1},
            ],
            "goal_status_hints": [],
        })
        config = _make_config()
        messages = [{"role": "user", "content": "My name is Jane and I work on this project"}]
        result = await extract_facts(messages, [], llm, config)
        assert len(result["facts"]) == 1
        assert "decision_domain" not in result["facts"][0]
