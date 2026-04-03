"""Procedural memory schemas."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field

from elephantbroker.schemas.base import Scope


class ProofType(StrEnum):
    """Types of proof that can satisfy an evidence requirement."""
    DIFF_HASH = "diff_hash"
    CHUNK_REF = "chunk_ref"
    RECEIPT = "receipt"
    VERSION_RECORD = "version_record"
    SUPERVISOR_SIGN_OFF = "supervisor_sign_off"


class ProofRequirement(BaseModel):
    """What evidence is needed to prove a step was completed."""
    description: str
    required: bool = True
    proof_type: ProofType = ProofType.CHUNK_REF


class ProcedureStep(BaseModel):
    """A single step in a procedure."""
    step_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    order: int = Field(ge=0)
    instruction: str = Field(min_length=1)
    required_evidence: list[ProofRequirement] = Field(default_factory=list)
    is_optional: bool = False


class ProcedureActivation(BaseModel):
    """Activation mode flags for a procedure."""
    manual: bool = False
    actor_default: bool = False
    trigger_word: str | None = None
    task_classifier: str | None = None
    goal_bound: bool = False
    supervisor_forced: bool = False


class ProcedureExecution(BaseModel):
    """An in-progress execution of a procedure."""
    execution_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    procedure_id: uuid.UUID
    current_step_index: int = Field(default=0, ge=0)
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_steps: list[uuid.UUID] = Field(default_factory=list)
    actor_id: uuid.UUID | None = None
    goal_id: uuid.UUID | None = None
    # Phase 7 additions
    session_key: str = ""
    session_id: uuid.UUID | None = None
    decision_domain: str | None = None


class ProcedureDefinition(BaseModel):
    """A stored procedure — a known sequence of steps for accomplishing a task."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    name: str = Field(min_length=1)
    description: str = ""
    scope: Scope = Scope.SESSION
    steps: list[ProcedureStep] = Field(default_factory=list)
    activation_modes: list[ProcedureActivation] = Field(default_factory=list)
    required_evidence: list[ProofRequirement] = Field(default_factory=list)
    red_line_bindings: list[str] = Field(default_factory=list)
    role_variants: dict[str, object] = Field(default_factory=dict)
    approval_requirements: list[str] = Field(default_factory=list)
    retry_patterns: list[str] = Field(default_factory=list)
    decision_domain: str | None = None
    enabled: bool = True
    source_actor_id: uuid.UUID | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    version: int = Field(default=1, ge=1)
    gateway_id: str = ""


class ProcedureSuggestion(BaseModel):
    """A draft procedure suggested by consolidation Stage 7."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    pattern_description: str  # Human-readable summary of the detected pattern
    tool_sequence: list[str]  # Ordered tool names forming the pattern
    sessions_observed: int    # How many sessions exhibited this pattern
    draft_procedure: ProcedureDefinition | None = None  # LLM-generated draft
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    approval_status: str = "pending"  # pending, approved, rejected
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    gateway_id: str = ""
