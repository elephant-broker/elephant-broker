"""Tests for lifecycle state machine."""
import pytest

from elephantbroker.schemas.lifecycle import (
    VALID_TRANSITIONS,
    InvalidLifecycleTransitionError,
    LifecyclePhase,
    LifecycleStateMachine,
)


class TestLifecyclePhase:
    def test_all_phases(self):
        assert len(LifecyclePhase) == 8


class TestLifecycleTransitions:
    def test_bootstrap_must_be_first(self):
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        assert sm.current_phase == LifecyclePhase.BOOTSTRAP

    def test_ingest_before_bootstrap_raises(self):
        sm = LifecycleStateMachine()
        with pytest.raises(InvalidLifecycleTransitionError):
            sm.transition(LifecyclePhase.INGEST)

    def test_assemble_requires_prior_ingest(self):
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        # Can't go directly to assemble from bootstrap
        with pytest.raises(InvalidLifecycleTransitionError):
            sm.transition(LifecyclePhase.ASSEMBLE)

    def test_valid_full_lifecycle(self):
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        sm.transition(LifecyclePhase.INGEST)
        sm.transition(LifecyclePhase.ASSEMBLE)
        sm.transition(LifecyclePhase.COMPACT)
        sm.transition(LifecyclePhase.AFTER_TURN)
        sm.transition(LifecyclePhase.INGEST)
        sm.transition(LifecyclePhase.DISPOSE)
        assert sm.current_phase == LifecyclePhase.DISPOSE

    def test_dispose_is_terminal(self):
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        sm.transition(LifecyclePhase.DISPOSE)
        with pytest.raises(InvalidLifecycleTransitionError):
            sm.transition(LifecyclePhase.INGEST)

    def test_can_transition_check(self):
        sm = LifecycleStateMachine()
        assert sm.can_transition(LifecyclePhase.BOOTSTRAP) is True
        assert sm.can_transition(LifecyclePhase.INGEST) is False

    def test_history_tracked(self):
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        sm.transition(LifecyclePhase.INGEST)
        sm.transition(LifecyclePhase.ASSEMBLE)
        assert sm.history == [LifecyclePhase.BOOTSTRAP, LifecyclePhase.INGEST]

    def test_subagent_lifecycle(self):
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        sm.transition(LifecyclePhase.INGEST)
        sm.transition(LifecyclePhase.PREPARE_SUBAGENT_SPAWN)
        sm.transition(LifecyclePhase.ON_SUBAGENT_ENDED)
        sm.transition(LifecyclePhase.AFTER_TURN)
        assert sm.current_phase == LifecyclePhase.AFTER_TURN


class TestLifecycleEdgeCases:
    """TF-FN-001 G1-G10 -- additive edge-case coverage for LifecycleStateMachine.

    Shipped semantic: transition() appends the LEAVING phase to history BEFORE
    mutating current_phase (see lifecycle.py:86-87). Thus history holds phases
    that were LEFT; current_phase holds the latest. Fresh state has
    current_phase=None and history=[]. Only BOOTSTRAP is valid from None.
    DISPOSE is terminal (VALID_TRANSITIONS[DISPOSE] == set()).
    """

    def test_fresh_sm_has_initial_state(self):
        """G1: Fresh state machine has current_phase=None and empty history."""
        sm = LifecycleStateMachine()
        assert sm.current_phase is None
        assert sm.history == []

    def test_self_loop_appends_to_history(self):
        """G2: INGEST->INGEST is a valid self-loop; each transition appends the LEFT phase."""
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        sm.transition(LifecyclePhase.INGEST)
        sm.transition(LifecyclePhase.INGEST)
        assert sm.history == [LifecyclePhase.BOOTSTRAP, LifecyclePhase.INGEST]
        assert sm.current_phase == LifecyclePhase.INGEST

    def test_dispose_self_loop_raises(self):
        """G3: DISPOSE is terminal; DISPOSE->DISPOSE raises."""
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        sm.transition(LifecyclePhase.DISPOSE)
        with pytest.raises(InvalidLifecycleTransitionError):
            sm.transition(LifecyclePhase.DISPOSE)

    @pytest.mark.parametrize(
        "phase",
        [
            LifecyclePhase.INGEST,
            LifecyclePhase.ASSEMBLE,
            LifecyclePhase.COMPACT,
            LifecyclePhase.AFTER_TURN,
            LifecyclePhase.PREPARE_SUBAGENT_SPAWN,
            LifecyclePhase.ON_SUBAGENT_ENDED,
            LifecyclePhase.DISPOSE,
        ],
    )
    def test_only_bootstrap_valid_from_none(self, phase):
        """G4: From None (fresh), only BOOTSTRAP is a valid first transition."""
        sm = LifecycleStateMachine()
        with pytest.raises(InvalidLifecycleTransitionError):
            sm.transition(phase)

    @pytest.mark.parametrize(
        "from_phase,to_phase",
        [
            (LifecyclePhase.BOOTSTRAP, LifecyclePhase.BOOTSTRAP),
            (LifecyclePhase.ASSEMBLE, LifecyclePhase.ASSEMBLE),
            (LifecyclePhase.AFTER_TURN, LifecyclePhase.ASSEMBLE),
            (LifecyclePhase.AFTER_TURN, LifecyclePhase.COMPACT),
        ],
    )
    def test_invalid_transitions_raise(self, from_phase, to_phase):
        """G5: Selected illegal transitions raise InvalidLifecycleTransitionError."""
        drive_paths = {
            LifecyclePhase.BOOTSTRAP: [LifecyclePhase.BOOTSTRAP],
            LifecyclePhase.ASSEMBLE: [
                LifecyclePhase.BOOTSTRAP,
                LifecyclePhase.INGEST,
                LifecyclePhase.ASSEMBLE,
            ],
            LifecyclePhase.AFTER_TURN: [
                LifecyclePhase.BOOTSTRAP,
                LifecyclePhase.INGEST,
                LifecyclePhase.AFTER_TURN,
            ],
        }
        sm = LifecycleStateMachine()
        for step in drive_paths[from_phase]:
            sm.transition(step)
        assert sm.current_phase == from_phase
        with pytest.raises(InvalidLifecycleTransitionError):
            sm.transition(to_phase)

    def test_subagent_round_trip_returns_to_ingest(self):
        """G6: BOOTSTRAP -> INGEST -> PREPARE_SUBAGENT_SPAWN -> ON_SUBAGENT_ENDED -> INGEST is a legal round trip."""
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        sm.transition(LifecyclePhase.INGEST)
        sm.transition(LifecyclePhase.PREPARE_SUBAGENT_SPAWN)
        sm.transition(LifecyclePhase.ON_SUBAGENT_ENDED)
        sm.transition(LifecyclePhase.INGEST)
        assert sm.current_phase == LifecyclePhase.INGEST

    def test_on_subagent_ended_requires_prepare(self):
        """G7: ON_SUBAGENT_ENDED cannot be reached without a prior PREPARE_SUBAGENT_SPAWN."""
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        sm.transition(LifecyclePhase.INGEST)
        with pytest.raises(InvalidLifecycleTransitionError):
            sm.transition(LifecyclePhase.ON_SUBAGENT_ENDED)

    def test_can_transition_does_not_mutate(self):
        """G8: can_transition() is a pure query; it does not change current_phase or history."""
        sm = LifecycleStateMachine()
        phase_before = sm.current_phase
        history_before = list(sm.history)
        assert sm.can_transition(LifecyclePhase.BOOTSTRAP) is True
        assert sm.can_transition(LifecyclePhase.INGEST) is False
        assert sm.current_phase == phase_before
        assert sm.history == history_before

    def test_invalid_transition_leaves_state_unchanged(self):
        """G9: A failed transition() call does not mutate current_phase or history."""
        sm = LifecycleStateMachine()
        sm.transition(LifecyclePhase.BOOTSTRAP)
        sm.transition(LifecyclePhase.INGEST)
        phase_before = sm.current_phase
        history_before = list(sm.history)
        with pytest.raises(InvalidLifecycleTransitionError):
            sm.transition(LifecyclePhase.ON_SUBAGENT_ENDED)
        assert sm.current_phase == phase_before
        assert sm.history == history_before

    def test_valid_transitions_has_9_keys(self):
        """G10: VALID_TRANSITIONS has exactly 9 keys -- None plus all 8 LifecyclePhase values."""
        assert len(VALID_TRANSITIONS) == 9
        assert None in VALID_TRANSITIONS
        for phase in LifecyclePhase:
            assert phase in VALID_TRANSITIONS
