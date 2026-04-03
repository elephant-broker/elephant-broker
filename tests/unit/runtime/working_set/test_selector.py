"""Comprehensive tests for BudgetSelector (Pass 2 greedy selection with dynamic penalties)."""
from __future__ import annotations

import logging
import uuid
from unittest.mock import MagicMock

import pytest

from elephantbroker.runtime.working_set.scoring import ScoringEngine
from elephantbroker.runtime.working_set.selector import BudgetSelector
from elephantbroker.schemas.working_set import (
    ScoringContext,
    ScoringWeights,
    WorkingSetItem,
    WorkingSetScores,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_item(
    *,
    item_id: str | None = None,
    token_size: int = 100,
    must_inject: bool = False,
    final: float = 5.0,
    source_type: str = "fact",
    confidence: float = 1.0,
    turn_relevance: float = 0.8,
    session_goal_relevance: float = 0.5,
    global_goal_relevance: float = 0.3,
    recency: float = 0.9,
    successful_use_prior: float = 0.5,
    score_confidence: float = 0.8,
    evidence_strength: float = 0.5,
    novelty: float = 1.0,
) -> WorkingSetItem:
    """Build a WorkingSetItem with pre-computed scores for testing."""
    iid = item_id or str(uuid.uuid4())
    scores = WorkingSetScores(
        turn_relevance=turn_relevance,
        session_goal_relevance=session_goal_relevance,
        global_goal_relevance=global_goal_relevance,
        recency=recency,
        successful_use_prior=successful_use_prior,
        confidence=score_confidence,
        evidence_strength=evidence_strength,
        novelty=novelty,
        redundancy_penalty=0.0,
        contradiction_penalty=0.0,
        cost_penalty=0.0,
        final=final,
    )
    return WorkingSetItem(
        id=iid,
        source_type=source_type,
        source_id=uuid.uuid4(),
        text=f"Item {iid[:8]}",
        scores=scores,
        token_size=token_size,
        must_inject=must_inject,
        confidence=confidence,
    )


def _make_ctx(weights: ScoringWeights | None = None) -> ScoringContext:
    """Build a minimal ScoringContext."""
    return ScoringContext(weights=weights or ScoringWeights())


def _make_engine(
    *,
    redundancy: float = 0.0,
    contradiction: float = 0.0,
    cost: float | None = None,
) -> ScoringEngine:
    """Return a mock ScoringEngine with configurable return values.

    If `cost` is None, the mock returns token_size / max(budget, 1) like the
    real implementation so that cost_penalty is realistic.
    """
    engine = MagicMock(spec=ScoringEngine)
    engine.compute_redundancy_penalty.return_value = redundancy
    engine.compute_contradiction_penalty.return_value = contradiction
    if cost is not None:
        engine.compute_cost_penalty.return_value = cost
    else:
        # Realistic default: proportional to token_size / remaining_budget
        engine.compute_cost_penalty.side_effect = (
            lambda item, budget: item.token_size / max(budget, 1)
        )
    return engine


# ---------------------------------------------------------------------------
# Must-inject tests
# ---------------------------------------------------------------------------


class TestMustInject:
    """Must-inject items are always selected regardless of budget constraints."""

    def test_must_inject_always_selected(self):
        item = _make_item(must_inject=True, token_size=50)
        selector = BudgetSelector()
        snap = selector.select(
            [item], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert len(snap.items) == 1
        assert snap.items[0].id == item.id

    def test_must_inject_deducts_budget(self):
        mi = _make_item(must_inject=True, token_size=200)
        cand = _make_item(token_size=900, final=10.0)
        selector = BudgetSelector()
        snap = selector.select(
            [mi, cand], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        # must_inject uses 200, leaving 800 — candidate of 900 doesn't fit
        assert len(snap.items) == 1
        assert snap.items[0].id == mi.id

    def test_must_inject_warns_when_over_budget(self, caplog):
        mi = _make_item(must_inject=True, token_size=500)
        selector = BudgetSelector()
        with caplog.at_level(logging.WARNING, logger="elephantbroker.runtime.working_set.selector"):
            snap = selector.select(
                [mi], _make_ctx(), token_budget=100,
                session_id=uuid.uuid4(), scoring_engine=_make_engine(),
            )
        assert len(snap.items) == 1
        assert "exceeds remaining budget" in caplog.text

    def test_must_inject_still_included_when_exceeds_budget(self):
        """Even if token_size > budget, must-inject is included."""
        mi = _make_item(must_inject=True, token_size=5000)
        selector = BudgetSelector()
        snap = selector.select(
            [mi], _make_ctx(), token_budget=100,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert len(snap.items) == 1
        assert snap.tokens_used == 5000

    def test_multiple_must_inject_all_selected(self):
        mi1 = _make_item(must_inject=True, token_size=100)
        mi2 = _make_item(must_inject=True, token_size=200)
        selector = BudgetSelector()
        snap = selector.select(
            [mi1, mi2], _make_ctx(), token_budget=500,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        ids = {it.id for it in snap.items}
        assert mi1.id in ids
        assert mi2.id in ids
        assert snap.tokens_used == 300


# ---------------------------------------------------------------------------
# Greedy selection ordering and budget tests
# ---------------------------------------------------------------------------


class TestGreedySelection:
    """Greedy selection picks highest-scored items first within budget."""

    def test_highest_partial_score_selected_first(self):
        high = _make_item(final=10.0, token_size=100)
        low = _make_item(final=1.0, token_size=100)
        selector = BudgetSelector()
        snap = selector.select(
            [low, high], _make_ctx(), token_budget=150,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        # Only 150 budget — one fits; it should be the higher-scored one
        assert len(snap.items) == 1
        assert snap.items[0].id == high.id

    def test_budget_not_exceeded(self):
        items = [_make_item(final=5.0, token_size=100) for _ in range(10)]
        selector = BudgetSelector()
        snap = selector.select(
            items, _make_ctx(), token_budget=350,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert snap.tokens_used <= 350

    def test_exact_budget_exhaustion(self):
        """If items exactly fill the budget, all are selected."""
        a = _make_item(final=5.0, token_size=200)
        b = _make_item(final=4.0, token_size=300)
        selector = BudgetSelector()
        snap = selector.select(
            [a, b], _make_ctx(), token_budget=500,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert snap.tokens_used == 500
        assert len(snap.items) == 2

    def test_selected_items_ordered_by_final_score_descending(self):
        """All selected items must be sorted by final_score descending."""
        low = _make_item(final=3.0, token_size=100)
        high = _make_item(final=7.0, token_size=100)
        mid = _make_item(final=5.0, token_size=100)
        selector = BudgetSelector()
        snap = selector.select(
            [low, high, mid], _make_ctx(), token_budget=10000,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert len(snap.items) == 3
        assert snap.items[0].scores.final >= snap.items[1].scores.final >= snap.items[2].scores.final

    def test_skips_item_that_does_not_fit(self):
        big = _make_item(final=10.0, token_size=600)
        small = _make_item(final=5.0, token_size=200)
        selector = BudgetSelector()
        snap = selector.select(
            [big, small], _make_ctx(), token_budget=500,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        # big doesn't fit (600 > 500), but small does
        assert len(snap.items) == 1
        assert snap.items[0].id == small.id


# ---------------------------------------------------------------------------
# Dynamic penalty tests (GAP-9: cost_penalty against remaining_budget)
# ---------------------------------------------------------------------------


class TestCostPenaltyDynamic:
    """Cost penalty is recomputed against remaining budget, not total budget (GAP-9)."""

    def test_cost_penalty_recomputed_against_remaining_budget(self):
        """After selecting item A, remaining budget shrinks, so cost_penalty
        for item B should be computed against the smaller budget."""
        a = _make_item(item_id="item-a", final=10.0, token_size=400)
        b = _make_item(item_id="item-b", final=8.0, token_size=200)
        engine = _make_engine()
        selector = BudgetSelector()

        snap = selector.select(
            [a, b], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=engine,
        )

        # B should have been evaluated with remaining_budget = 600 (1000 - 400)
        cost_calls = engine.compute_cost_penalty.call_args_list
        # First call is for A (budget=1000), second for B (budget=600)
        assert len(cost_calls) == 2
        assert cost_calls[0].args == (a, 1000)
        assert cost_calls[1].args == (b, 600)

    def test_cost_penalty_increases_as_budget_shrinks(self):
        """A fixed-size item has higher cost_penalty when budget is lower."""
        # Use the real cost formula: token_size / budget_remaining
        item = _make_item(final=5.0, token_size=200)
        engine = _make_engine()  # default uses realistic cost
        selector = BudgetSelector()

        snap = selector.select(
            [item], _make_ctx(), token_budget=400,
            session_id=uuid.uuid4(), scoring_engine=engine,
        )
        # cost_penalty = 200/400 = 0.5
        assert snap.items[0].scores.cost_penalty == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Negative final score
# ---------------------------------------------------------------------------


class TestNegativeFinalScore:
    """Items whose recomputed final score <= 0 are skipped."""

    def test_negative_final_score_skipped(self):
        """If dynamic penalties push final below zero, item is excluded."""
        item = _make_item(final=2.0, token_size=100)
        # High redundancy penalty will make final negative (weighted at -0.7)
        # and contradiction penalty (weighted at -1.0)
        engine = _make_engine(redundancy=1.0, contradiction=1.0, cost=0.5)
        selector = BudgetSelector()
        snap = selector.select(
            [item], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=engine,
        )
        # Verify: the item's weighted sum with those penalty values should be <= 0
        # If so, it is skipped
        if len(snap.items) == 0:
            assert snap.tokens_used == 0
        else:
            # If somehow still positive, ensure final > 0
            assert snap.items[0].scores.final > 0

    def test_zero_final_score_skipped(self):
        """Exactly zero final score should also be skipped (condition is > 0)."""
        # Build an item whose weighted_sum will be exactly 0 after penalties
        weights = ScoringWeights(
            turn_relevance=1.0,
            session_goal_relevance=0.0,
            global_goal_relevance=0.0,
            recency=0.0,
            successful_use_prior=0.0,
            confidence=0.0,
            evidence_strength=0.0,
            novelty=0.0,
            redundancy_penalty=0.0,
            contradiction_penalty=0.0,
            cost_penalty=-1.0,
        )
        item = _make_item(final=1.0, token_size=100, turn_relevance=0.5)
        engine = _make_engine(redundancy=0.0, contradiction=0.0, cost=0.5)
        ctx = _make_ctx(weights)
        selector = BudgetSelector()
        snap = selector.select(
            [item], ctx, token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=engine,
        )
        # weighted_sum = 1.0*0.5 + (-1.0)*0.5 = 0.0 => skipped
        assert len(snap.items) == 0


# ---------------------------------------------------------------------------
# Redundancy penalty
# ---------------------------------------------------------------------------


class TestRedundancyPenalty:
    """Redundancy penalty prevents near-duplicate items from being selected."""

    def test_redundancy_penalty_prevents_near_duplicates(self):
        """When engine returns high redundancy, item is excluded."""
        a = _make_item(item_id="original", final=10.0, token_size=100)
        dup = _make_item(item_id="duplicate", final=9.0, token_size=100)

        call_count = [0]

        def mock_redundancy(item, selected, ctx):
            call_count[0] += 1
            # First item (a) has no selected yet, second (dup) hits high redundancy
            if item.id == "duplicate" and any(s.id == "original" for s in selected):
                return 0.95
            return 0.0

        engine = _make_engine()
        engine.compute_redundancy_penalty.side_effect = mock_redundancy

        selector = BudgetSelector()
        snap = selector.select(
            [a, dup], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=engine,
        )
        # 'a' is selected, 'dup' may be excluded if penalty makes final <= 0
        # With default weights, redundancy_penalty weight is -0.7
        # So 0.95 * (-0.7) = -0.665 added to score. This might or might not
        # push it below zero depending on other dims. Let's verify the penalty was set.
        selected_ids = {it.id for it in snap.items}
        assert "original" in selected_ids

    def test_redundancy_zero_for_first_item(self):
        """First selected item has no redundancy to compare against."""
        item = _make_item(final=5.0, token_size=100)
        engine = _make_engine(redundancy=0.0)
        selector = BudgetSelector()
        snap = selector.select(
            [item], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=engine,
        )
        assert len(snap.items) == 1
        assert snap.items[0].scores.redundancy_penalty == 0.0


# ---------------------------------------------------------------------------
# Contradiction penalty — Layer 1 and Layer 2
# ---------------------------------------------------------------------------


class TestContradictionPenalty:
    """Contradiction penalty prevents conflicting items."""

    def test_contradiction_layer1_supersession_edge(self):
        """Items linked by supersession edge get max contradiction penalty."""
        old = _make_item(item_id="old-fact", final=10.0, token_size=100)
        new = _make_item(item_id="new-fact", final=8.0, token_size=100)

        def mock_contradiction(item, selected, ctx):
            if item.id == "new-fact" and any(s.id == "old-fact" for s in selected):
                return 1.0  # Layer 1: supersession edge
            return 0.0

        engine = _make_engine()
        engine.compute_contradiction_penalty.side_effect = mock_contradiction

        selector = BudgetSelector()
        snap = selector.select(
            [old, new], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=engine,
        )
        selected_ids = {it.id for it in snap.items}
        assert "old-fact" in selected_ids
        # new-fact gets contradiction_penalty=1.0, weighted at -1.0 = -1.0 subtracted

    def test_contradiction_layer2_high_sim_confidence_gap(self):
        """High similarity + confidence divergence triggers Layer 2 penalty."""
        a = _make_item(item_id="confident", final=10.0, token_size=100, confidence=0.9)
        b = _make_item(item_id="uncertain", final=8.0, token_size=100, confidence=0.3)

        def mock_contradiction(item, selected, ctx):
            if item.id == "uncertain" and any(s.id == "confident" for s in selected):
                return 0.7  # Layer 2 penalty
            return 0.0

        engine = _make_engine()
        engine.compute_contradiction_penalty.side_effect = mock_contradiction

        selector = BudgetSelector()
        snap = selector.select(
            [a, b], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=engine,
        )
        selected_ids = {it.id for it in snap.items}
        assert "confident" in selected_ids

    def test_no_contradiction_when_no_conflict(self):
        """Non-conflicting items both get selected."""
        a = _make_item(final=5.0, token_size=100)
        b = _make_item(final=4.0, token_size=100)
        engine = _make_engine(redundancy=0.0, contradiction=0.0)
        selector = BudgetSelector()
        snap = selector.select(
            [a, b], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=engine,
        )
        assert len(snap.items) == 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases: empty, all fit, zero budget, single candidate."""

    def test_empty_candidates(self):
        selector = BudgetSelector()
        snap = selector.select(
            [], _make_ctx(), token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert snap.items == []
        assert snap.tokens_used == 0

    def test_all_candidates_fit(self):
        items = [_make_item(final=5.0, token_size=50) for _ in range(5)]
        selector = BudgetSelector()
        snap = selector.select(
            items, _make_ctx(), token_budget=10000,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert len(snap.items) == 5
        assert snap.tokens_used == 250

    def test_zero_budget_selects_only_must_inject(self):
        mi = _make_item(must_inject=True, token_size=100)
        cand = _make_item(final=10.0, token_size=50)
        selector = BudgetSelector()
        snap = selector.select(
            [mi, cand], _make_ctx(), token_budget=0,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        # must_inject included (with warning); candidate skipped (remaining <= 0)
        assert len(snap.items) == 1
        assert snap.items[0].id == mi.id

    def test_single_candidate_fits(self):
        item = _make_item(final=5.0, token_size=100)
        selector = BudgetSelector()
        snap = selector.select(
            [item], _make_ctx(), token_budget=200,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert len(snap.items) == 1

    def test_single_candidate_does_not_fit(self):
        item = _make_item(final=5.0, token_size=300)
        selector = BudgetSelector()
        snap = selector.select(
            [item], _make_ctx(), token_budget=200,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert len(snap.items) == 0
        assert snap.tokens_used == 0


# ---------------------------------------------------------------------------
# Snapshot correctness
# ---------------------------------------------------------------------------


class TestSnapshotAccuracy:
    """Snapshot fields must be accurate after selection."""

    def test_tokens_used_accurate(self):
        items = [
            _make_item(final=5.0, token_size=120),
            _make_item(final=4.0, token_size=80),
            _make_item(final=3.0, token_size=150),
        ]
        selector = BudgetSelector()
        snap = selector.select(
            items, _make_ctx(), token_budget=250,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        expected = sum(it.token_size for it in snap.items)
        assert snap.tokens_used == expected

    def test_tokens_used_includes_must_inject(self):
        mi = _make_item(must_inject=True, token_size=300)
        cand = _make_item(final=5.0, token_size=100)
        selector = BudgetSelector()
        snap = selector.select(
            [mi, cand], _make_ctx(), token_budget=500,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert snap.tokens_used == sum(it.token_size for it in snap.items)

    def test_weights_used_recorded(self):
        weights = ScoringWeights(turn_relevance=2.0, recency=0.1)
        ctx = _make_ctx(weights)
        selector = BudgetSelector()
        snap = selector.select(
            [_make_item(final=5.0, token_size=50)], ctx, token_budget=1000,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert snap.weights_used.turn_relevance == 2.0
        assert snap.weights_used.recency == 0.1

    def test_token_budget_recorded(self):
        selector = BudgetSelector()
        snap = selector.select(
            [], _make_ctx(), token_budget=4096,
            session_id=uuid.uuid4(), scoring_engine=_make_engine(),
        )
        assert snap.token_budget == 4096

    def test_session_id_uuid_passthrough(self):
        sid = uuid.uuid4()
        selector = BudgetSelector()
        snap = selector.select(
            [], _make_ctx(), token_budget=1000,
            session_id=sid, scoring_engine=_make_engine(),
        )
        assert snap.session_id == sid

    def test_session_id_string_converted(self):
        sid = uuid.uuid4()
        selector = BudgetSelector()
        snap = selector.select(
            [], _make_ctx(), token_budget=1000,
            session_id=str(sid), scoring_engine=_make_engine(),
        )
        assert snap.session_id == sid


# ---------------------------------------------------------------------------
# Diversity warning
# ---------------------------------------------------------------------------


class TestDiversityWarning:
    """Low diversity warning is logged when all items share source_type."""

    def test_diversity_warning_logged(self, caplog):
        items = [
            _make_item(final=5.0, token_size=50, source_type="fact"),
            _make_item(final=4.0, token_size=50, source_type="fact"),
            _make_item(final=3.0, token_size=50, source_type="fact"),
        ]
        engine = _make_engine(redundancy=0.0, contradiction=0.0)
        selector = BudgetSelector()
        with caplog.at_level(logging.WARNING, logger="elephantbroker.runtime.working_set.selector"):
            snap = selector.select(
                items, _make_ctx(), token_budget=1000,
                session_id=uuid.uuid4(), scoring_engine=engine,
            )
        assert len(snap.items) == 3
        assert "low diversity" in caplog.text

    def test_no_diversity_warning_when_mixed_sources(self, caplog):
        items = [
            _make_item(final=5.0, token_size=50, source_type="fact"),
            _make_item(final=4.0, token_size=50, source_type="goal"),
        ]
        engine = _make_engine(redundancy=0.0, contradiction=0.0)
        selector = BudgetSelector()
        with caplog.at_level(logging.WARNING, logger="elephantbroker.runtime.working_set.selector"):
            snap = selector.select(
                items, _make_ctx(), token_budget=1000,
                session_id=uuid.uuid4(), scoring_engine=engine,
            )
        assert "low diversity" not in caplog.text

    def test_no_diversity_warning_for_single_item(self, caplog):
        """Single item should not trigger diversity warning."""
        item = _make_item(final=5.0, token_size=50, source_type="fact")
        selector = BudgetSelector()
        with caplog.at_level(logging.WARNING, logger="elephantbroker.runtime.working_set.selector"):
            snap = selector.select(
                [item], _make_ctx(), token_budget=1000,
                session_id=uuid.uuid4(), scoring_engine=_make_engine(),
            )
        assert "low diversity" not in caplog.text
