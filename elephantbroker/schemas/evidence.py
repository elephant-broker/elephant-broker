"""Claims and evidence schemas."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class ClaimStatus(StrEnum):
    """Status of a claim's verification."""
    UNVERIFIED = "unverified"
    SELF_SUPPORTED = "self_supported"
    TOOL_SUPPORTED = "tool_supported"
    SUPERVISOR_VERIFIED = "supervisor_verified"
    REJECTED = "rejected"


class EvidenceRef(BaseModel):
    """Reference to a piece of evidence supporting a claim."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    type: str  # chunk_ref, tool_output, supervisor_sign_off, external_link
    ref_value: str
    content_hash: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    created_by_actor_id: uuid.UUID | None = None
    gateway_id: str = ""


class ClaimRecord(BaseModel):
    """A claim that requires evidence."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    claim_text: str = Field(min_length=1)
    claim_type: str = ""
    status: ClaimStatus = ClaimStatus.UNVERIFIED
    evidence_refs: list[EvidenceRef] = Field(default_factory=list)
    procedure_id: uuid.UUID | None = None
    step_id: uuid.UUID | None = None
    goal_id: uuid.UUID | None = None
    actor_id: uuid.UUID | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    gateway_id: str = ""


class VerificationState(BaseModel):
    """Per-claim verification state."""
    claim_id: uuid.UUID
    status: ClaimStatus = ClaimStatus.UNVERIFIED
    evidence_refs: list[EvidenceRef] = Field(default_factory=list)
    verifier_actor_id: uuid.UUID | None = None
    verified_at: datetime | None = None
    rejection_reason: str | None = None


class VerificationSummary(BaseModel):
    """Aggregate verification state for a set of claims."""
    total_claims: int = Field(default=0, ge=0)
    verified: int = Field(default=0, ge=0)
    pending: int = Field(default=0, ge=0)
    disputed: int = Field(default=0, ge=0)
    retracted: int = Field(default=0, ge=0)
    coverage: float = Field(default=0.0, ge=0.0, le=1.0)


class VerificationGap(BaseModel):
    """A gap found by consolidation Stage 8."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    claim_id: uuid.UUID
    claim_text: str
    procedure_id: uuid.UUID | None = None
    step_id: str | None = None
    missing_proof_type: str  # ProofType value that's missing
    missing_proof_description: str
    severity: str = "medium"  # low, medium, high
    suggested_action: str = "request_evidence"
    # "request_evidence", "escalate_to_supervisor", "request_retry"
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    gateway_id: str = ""
