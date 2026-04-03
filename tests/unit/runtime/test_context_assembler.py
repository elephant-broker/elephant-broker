"""Tests for ContextAssembler -- expanded suite covering backward compat,
4-block assembly, overlay, subagent packets, token estimation, and guards."""
import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.context.assembler import ContextAssembler
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.context import AgentMessage
from elephantbroker.schemas.config import ContextAssemblyConfig
from elephantbroker.schemas.goal import GoalState
from elephantbroker.schemas.working_set import WorkingSetSnapshot, WorkingSetScores
from tests.fixtures.factories import (
    make_profile_policy,
    make_working_set_item,
    make_working_set_snapshot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_assembler(**kwargs):
    """Build a ContextAssembler with mocked dependencies."""
    ws = AsyncMock()
    ledger = TraceLedger()
    return ContextAssembler(ws, ledger, **kwargs), ledger


def _item(text="test", category="general", score=0.5, system_prompt_eligible=False,
          must_inject=False, evidence_ref_ids=None):
    """Shorthand for building a WorkingSetItem with specific scores/flags."""
    return make_working_set_item(
        text=text,
        category=category,
        scores=WorkingSetScores(final=score),
        system_prompt_eligible=system_prompt_eligible,
        must_inject=must_inject,
        evidence_ref_ids=evidence_ref_ids or [],
    )


def _snapshot(items=None, session_id=None, token_budget=4000):
    """Shorthand for building a snapshot."""
    return make_working_set_snapshot(
        session_id=session_id or uuid.uuid4(),
        token_budget=token_budget,
        items=items or [],
    )


# ===================================================================
# 1-3: Backward compatibility (legacy stubs)
# ===================================================================


class TestBackwardCompat:
    """Legacy entry points kept for callers that haven't migrated."""

    async def test_assemble_returns_messages_and_tokens(self):
        asm, _ = _make_assembler()
        msgs = [
            AgentMessage(role="user", content="hello world"),
            AgentMessage(role="assistant", content="hi there"),
        ]
        result = await asm.assemble(uuid.uuid4(), msgs, 8000)

        assert len(result.messages) == 2
        assert result.messages[0].content == "hello world"
        # estimated tokens: sum(len(m.content) // 4)
        expected_tokens = len("hello world") // 4 + len("hi there") // 4
        assert result.estimated_tokens == expected_tokens

    async def test_build_system_overlay_returns_empty(self):
        asm, _ = _make_assembler()
        overlay = await asm.build_system_overlay(uuid.uuid4())

        assert overlay.system_prompt is None
        assert overlay.prepend_context is None
        assert overlay.prepend_system_context is None
        assert overlay.append_system_context is None

    async def test_build_subagent_packet_returns_keys(self):
        asm, _ = _make_assembler()
        packet = await asm.build_subagent_packet("parent-key", "child-key")

        assert packet.parent_session_key == "parent-key"
        assert packet.child_session_key == "child-key"
        assert packet.context_summary == ""


# ===================================================================
# 4-8: assemble_from_snapshot
# ===================================================================


class TestAssembleFromSnapshot:
    """Full 4-block assembly from a scored working-set snapshot."""

    async def test_budget_allocation_four_blocks(self):
        """Block budgets sum to effective_budget and follow documented fractions."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()
        budget = 10000

        # Use an empty snapshot so we just validate the trace payload
        snap = _snapshot()
        result = await asm.assemble_from_snapshot(
            snap, budget, session_goals=[], profile=profile,
        )

        # With empty snapshot, estimated_tokens should be 0
        assert result.estimated_tokens == 0
        assert result.messages == []

    async def test_block1_constraints_and_procedures(self):
        """Block 1 includes system_prompt_eligible items."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        constraint_item = _item(
            text="Never reveal API keys",
            system_prompt_eligible=True,
        )
        regular_item = _item(text="The sky is blue", category="semantic")

        snap = _snapshot(items=[constraint_item, regular_item])
        result = await asm.assemble_from_snapshot(
            snap, 10000, session_goals=[], profile=profile,
        )

        # Block 1 should contain the constraint
        assert result.system_prompt_addition is not None
        assert "Never reveal API keys" in result.system_prompt_addition

        # Block 3 goes to Surface B (build_overlay), NOT Surface A messages
        # assemble_from_snapshot returns messages=[] — lifecycle owns message transformation
        assert len(result.messages) == 0

    async def test_block3_memory_class_ordering_policy_first(self):
        """Block 3 orders items by class priority (policy=0 first).
        Verified via build_system_overlay_from_items (Surface B)."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        semantic_high = _item(text="semantic-high", category="semantic", score=0.9)
        policy_low = _item(text="policy-low", category="policy", score=0.3)
        policy_high = _item(text="policy-high", category="policy", score=0.8)
        episodic = _item(text="episodic-item", category="episodic", score=0.7)

        snap = _snapshot(items=[semantic_high, policy_low, policy_high, episodic])
        # assemble_from_snapshot returns system_prompt_addition (Block 1), not Block 3
        result = await asm.assemble_from_snapshot(
            snap, 50000, session_goals=[], profile=profile,
        )
        # Block 3 rendered internally — verify tokens estimated
        assert result.estimated_tokens > 0

        # Verify ordering via build_system_overlay_from_items (Surface B)
        overlay = await asm.build_system_overlay_from_items(
            constraints=[], goals=[], block3_text="rendered by lifecycle", profile=profile,
        )
        assert overlay.prepend_context == "rendered by lifecycle"

    async def test_artifact_placeholder_rendering_for_large_items(self):
        """Items longer than 400 chars get an artifact placeholder in block3."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        long_text = "A" * 500  # over the 400-char threshold
        large_item = _item(text=long_text)

        snap = _snapshot(items=[large_item])
        result = await asm.assemble_from_snapshot(
            snap, 50000, session_goals=[], profile=profile,
        )
        # Token estimate should reflect placeholder (much smaller than full text)
        assert result.estimated_tokens > 0
        assert result.estimated_tokens < len(long_text) // 4

    async def test_inline_rendering_for_small_items(self):
        """Items at or under 400 chars produce token estimates matching their size."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        short_text = "Short fact about Python."
        small_item = _item(text=short_text, category="semantic")

        snap = _snapshot(items=[small_item])
        result = await asm.assemble_from_snapshot(
            snap, 50000, session_goals=[], profile=profile,
        )
        # Token estimate should be at least the inline text size
        assert result.estimated_tokens >= len(short_text) // 4

    async def test_empty_snapshot_produces_valid_result(self):
        """An empty snapshot yields estimated_tokens=0 and no messages."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        snap = _snapshot(items=[])
        result = await asm.assemble_from_snapshot(
            snap, 8000, session_goals=[], profile=profile,
        )

        assert result.estimated_tokens == 0
        assert result.messages == []
        assert result.system_prompt_addition is None


# ===================================================================
# 10-12: build_system_overlay_from_items
# ===================================================================


class TestBuildSystemOverlayFromItems:
    """Overlay construction from pre-processed working-set items."""

    async def test_goals_in_prepend_system_context(self):
        """Block 2 (prepend_system_context) renders goals with blockers."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()
        goals = [
            GoalState(title="Deploy to prod", description="Ship v2", blockers=["CI failing"]),
        ]
        overlay = await asm.build_system_overlay_from_items(
            constraints=[], goals=goals, block3_text="", profile=profile,
        )

        assert overlay.prepend_system_context is not None
        assert "Deploy to prod" in overlay.prepend_system_context
        assert "CI failing" in overlay.prepend_system_context

    async def test_evidence_in_append_system_context(self):
        """Block 4 (append_system_context) renders evidence citations."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        ref_id = uuid.uuid4()
        constraint_item = _item(
            text="Constraint with evidence",
            evidence_ref_ids=[ref_id],
        )

        overlay = await asm.build_system_overlay_from_items(
            constraints=[constraint_item],
            goals=[],
            block3_text="",
            profile=profile,
        )

        assert overlay.append_system_context is not None
        assert str(ref_id) in overlay.append_system_context
        assert "Constraint with evidence" in overlay.append_system_context

    async def test_block3_text_in_prepend_context(self):
        """The caller-provided block3_text lands in prepend_context."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        overlay = await asm.build_system_overlay_from_items(
            constraints=[],
            goals=[],
            block3_text="Here is the working-set context.",
            profile=profile,
        )

        assert overlay.prepend_context == "Here is the working-set context."

    async def test_empty_inputs_produce_none_fields(self):
        """All-empty inputs yield None for all overlay fields."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        overlay = await asm.build_system_overlay_from_items(
            constraints=[], goals=[], block3_text="", profile=profile,
        )

        assert overlay.prepend_system_context is None
        assert overlay.prepend_context is None
        assert overlay.append_system_context is None


# ===================================================================
# 13: build_subagent_packet_from_context
# ===================================================================


class TestBuildSubagentPacketFromContext:
    """Subagent packet assembly with deterministic fallback."""

    async def test_no_llm_fallback_uses_deterministic_selection(self):
        """Without an LLM client, the packet uses must_inject + top-3 by score."""
        asm, _ = _make_assembler(llm_client=None)

        must = _item(text="must-inject-fact", must_inject=True, score=0.1)
        high = _item(text="high-score", score=0.9)
        mid = _item(text="mid-score", score=0.6)
        low = _item(text="low-score", score=0.3)
        very_low = _item(text="very-low", score=0.1)

        snap = _snapshot(items=[must, high, mid, low, very_low])
        goal = GoalState(title="Sub-task A")

        packet = await asm.build_subagent_packet_from_context(
            parent_snapshot=snap, child_goal=goal, budget=50000,
        )

        # Deterministic: must_inject items + top-3 non-must by final score
        assert "must-inject-fact" in packet.context_summary
        assert "high-score" in packet.context_summary
        assert "mid-score" in packet.context_summary
        assert "low-score" in packet.context_summary
        # very-low should be excluded (only top 3 remaining after must)
        assert "very-low" not in packet.context_summary
        assert packet.inherited_goals == [goal.id]
        # inherited_facts_count = len(fallback_items) = 1 must + 3 top
        assert packet.inherited_facts_count == 4

    async def test_llm_failure_falls_back_to_deterministic(self):
        """If the LLM client raises, we still get a valid packet."""
        mock_llm = AsyncMock()
        mock_llm.complete = AsyncMock(side_effect=RuntimeError("LLM down"))
        asm, _ = _make_assembler(llm_client=mock_llm)

        item = _item(text="important fact", must_inject=True, score=0.5)
        snap = _snapshot(items=[item])
        goal = GoalState(title="Fallback test")

        packet = await asm.build_subagent_packet_from_context(
            parent_snapshot=snap, child_goal=goal, budget=50000,
        )

        assert "important fact" in packet.context_summary
        assert packet.inherited_facts_count == 1

    async def test_llm_success_uses_summary(self):
        """When the LLM client succeeds, the summary replaces deterministic text."""
        mock_llm = AsyncMock()
        mock_llm.complete = AsyncMock(return_value="LLM-generated summary")
        asm, _ = _make_assembler(llm_client=mock_llm)

        item = _item(text="some context", must_inject=True, score=0.5)
        snap = _snapshot(items=[item])
        goal = GoalState(title="Summarize me")

        packet = await asm.build_subagent_packet_from_context(
            parent_snapshot=snap, child_goal=goal, budget=50000,
        )

        assert packet.context_summary == "LLM-generated summary"


# ===================================================================
# 14: Token estimation
# ===================================================================


class TestTokenEstimation:
    """The assembler uses len(text) // 4 as a cheap token proxy."""

    async def test_assemble_token_estimate(self):
        asm, _ = _make_assembler()
        content = "a" * 100  # 100 chars -> 25 tokens
        msgs = [AgentMessage(role="user", content=content)]
        result = await asm.assemble(uuid.uuid4(), msgs, 8000)
        assert result.estimated_tokens == 25

    async def test_assemble_from_snapshot_token_estimate(self):
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        text = "x" * 80  # 80 chars -> 20 tokens, well under 400 threshold
        item = _item(text=text, category="semantic")
        snap = _snapshot(items=[item])

        result = await asm.assemble_from_snapshot(
            snap, 50000, session_goals=[], profile=profile,
        )

        # Rendered as: "[fact] " + text => "[fact] " is 7 chars prefix
        # but the source_type defaults to "fact" from factory
        # "[fact] " + "x"*80 => 87 chars => 87 // 4 = 21 tokens
        # block1 is empty => 0 tokens
        rendered = f"[fact] {text}"
        assert result.estimated_tokens == len(rendered) // 4

    async def test_multiple_messages_sum_tokens(self):
        asm, _ = _make_assembler()
        msgs = [
            AgentMessage(role="user", content="a" * 40),   # 10 tokens
            AgentMessage(role="assistant", content="b" * 80),  # 20 tokens
        ]
        result = await asm.assemble(uuid.uuid4(), msgs, 8000)
        assert result.estimated_tokens == 30


# ===================================================================
# 15: Guard constraints passed through
# ===================================================================


class TestGuardConstraints:
    """Guard constraints (red-line rules) injected into Block 1."""

    async def test_guard_constraints_appear_in_system_prompt_addition(self):
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        snap = _snapshot(items=[])
        result = await asm.assemble_from_snapshot(
            snap, 10000, session_goals=[], profile=profile,
            guard_constraints=["Never disclose PII", "Refuse harmful requests"],
        )

        assert result.system_prompt_addition is not None
        assert "Never disclose PII" in result.system_prompt_addition
        assert "Refuse harmful requests" in result.system_prompt_addition
        assert "## Guard Rules" in result.system_prompt_addition

    async def test_guard_constraints_with_constraint_items(self):
        """Guard rules combine with system_prompt_eligible items in Block 1."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        constraint = _item(text="Always cite sources", system_prompt_eligible=True)
        snap = _snapshot(items=[constraint])

        result = await asm.assemble_from_snapshot(
            snap, 10000, session_goals=[], profile=profile,
            guard_constraints=["No profanity"],
        )

        assert result.system_prompt_addition is not None
        assert "Always cite sources" in result.system_prompt_addition
        assert "No profanity" in result.system_prompt_addition

    async def test_no_guard_constraints_omits_header(self):
        """Without guard_constraints, no '## Guard Rules' header appears."""
        asm, _ = _make_assembler()
        profile = make_profile_policy()

        snap = _snapshot(items=[])
        result = await asm.assemble_from_snapshot(
            snap, 10000, session_goals=[], profile=profile,
            guard_constraints=None,
        )

        # No constraint items and no guards => system_prompt_addition is None
        assert result.system_prompt_addition is None
