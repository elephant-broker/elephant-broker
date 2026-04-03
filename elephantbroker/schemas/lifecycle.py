"""ContextEngine lifecycle state machine."""
from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class LifecyclePhase(StrEnum):
    """Phases in the ContextEngine lifecycle."""
    BOOTSTRAP = "bootstrap"
    INGEST = "ingest"
    ASSEMBLE = "assemble"
    COMPACT = "compact"
    AFTER_TURN = "after_turn"
    PREPARE_SUBAGENT_SPAWN = "prepare_subagent_spawn"
    ON_SUBAGENT_ENDED = "on_subagent_ended"
    DISPOSE = "dispose"


# Valid transitions: from_phase -> set of allowed next phases
VALID_TRANSITIONS: dict[LifecyclePhase | None, set[LifecyclePhase]] = {
    None: {LifecyclePhase.BOOTSTRAP},
    LifecyclePhase.BOOTSTRAP: {
        LifecyclePhase.INGEST,
        LifecyclePhase.DISPOSE,
    },
    LifecyclePhase.INGEST: {
        LifecyclePhase.INGEST,
        LifecyclePhase.ASSEMBLE,
        LifecyclePhase.COMPACT,
        LifecyclePhase.AFTER_TURN,
        LifecyclePhase.PREPARE_SUBAGENT_SPAWN,
        LifecyclePhase.DISPOSE,
    },
    LifecyclePhase.ASSEMBLE: {
        LifecyclePhase.INGEST,
        LifecyclePhase.COMPACT,
        LifecyclePhase.AFTER_TURN,
        LifecyclePhase.PREPARE_SUBAGENT_SPAWN,
        LifecyclePhase.DISPOSE,
    },
    LifecyclePhase.COMPACT: {
        LifecyclePhase.INGEST,
        LifecyclePhase.ASSEMBLE,
        LifecyclePhase.AFTER_TURN,
        LifecyclePhase.DISPOSE,
    },
    LifecyclePhase.AFTER_TURN: {
        LifecyclePhase.INGEST,
        LifecyclePhase.DISPOSE,
    },
    LifecyclePhase.PREPARE_SUBAGENT_SPAWN: {
        LifecyclePhase.ON_SUBAGENT_ENDED,
        LifecyclePhase.INGEST,
        LifecyclePhase.DISPOSE,
    },
    LifecyclePhase.ON_SUBAGENT_ENDED: {
        LifecyclePhase.INGEST,
        LifecyclePhase.AFTER_TURN,
        LifecyclePhase.DISPOSE,
    },
    LifecyclePhase.DISPOSE: set(),
}


class InvalidLifecycleTransitionError(Exception):
    """Raised when an invalid lifecycle transition is attempted."""

    def __init__(self, from_phase: LifecyclePhase | None, to_phase: LifecyclePhase) -> None:
        self.from_phase = from_phase
        self.to_phase = to_phase
        super().__init__(f"Invalid transition: {from_phase} -> {to_phase}")


class LifecycleStateMachine(BaseModel):
    """Tracks and enforces the ContextEngine lifecycle."""
    current_phase: LifecyclePhase | None = None
    history: list[LifecyclePhase] = Field(default_factory=list)

    def transition(self, to_phase: LifecyclePhase) -> None:
        """Transition to a new phase, raising InvalidLifecycleTransition if not allowed."""
        allowed = VALID_TRANSITIONS.get(self.current_phase, set())
        if to_phase not in allowed:
            raise InvalidLifecycleTransitionError(self.current_phase, to_phase)
        if self.current_phase is not None:
            self.history.append(self.current_phase)
        self.current_phase = to_phase

    def can_transition(self, to_phase: LifecyclePhase) -> bool:
        """Check if a transition is valid without performing it."""
        allowed = VALID_TRANSITIONS.get(self.current_phase, set())
        return to_phase in allowed
