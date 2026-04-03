"""Procedure engine interface (Phase 7 — extended with completion gate)."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.guards import CompletionCheckResult, StepCheckResult
from elephantbroker.schemas.procedure import ProcedureDefinition, ProcedureExecution


class IProcedureEngine(ABC):
    """Manages procedural memory: activation, step tracking, and completion."""

    @abstractmethod
    async def store_procedure(self, procedure: ProcedureDefinition) -> ProcedureDefinition:
        """Store a procedure definition via Cognee."""
        ...

    @abstractmethod
    async def activate(self, procedure_id: uuid.UUID, actor_id: uuid.UUID | None = None,
                       *, session_key: str = "", session_id: uuid.UUID | None = None) -> ProcedureExecution:
        """Activate a procedure for execution by an actor."""
        ...

    @abstractmethod
    async def check_step(self, activation_id: uuid.UUID, step_id: uuid.UUID) -> StepCheckResult:
        """Check if a step's proof requirements are met."""
        ...

    @abstractmethod
    async def validate_completion(self, activation_id: uuid.UUID) -> CompletionCheckResult:
        """Validate that all required steps are completed with evidence."""
        ...

    @abstractmethod
    async def get_active_execution_ids(self, session_key: str, session_id: uuid.UUID) -> list[uuid.UUID]:
        """Return procedure_ids for active (non-completed) executions in this session."""
        ...
