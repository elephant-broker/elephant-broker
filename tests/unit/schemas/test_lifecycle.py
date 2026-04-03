"""Tests for lifecycle state machine."""
import pytest

from elephantbroker.schemas.lifecycle import InvalidLifecycleTransitionError, LifecyclePhase, LifecycleStateMachine


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
