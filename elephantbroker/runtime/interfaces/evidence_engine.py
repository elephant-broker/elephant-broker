"""Evidence and verification engine interface (Phase 7 — extended)."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.evidence import (
    ClaimRecord,
    EvidenceRef,
    VerificationState,
    VerificationSummary,
)
from elephantbroker.schemas.guards import CompletionCheckResult


class IEvidenceAndVerificationEngine(ABC):
    """Manages claims, evidence attachment, and verification state."""

    @abstractmethod
    async def record_claim(self, claim: ClaimRecord, *,
                           session_id: "uuid.UUID | None" = None) -> ClaimRecord:
        """Record a new claim for verification tracking.

        Args:
            claim: The claim to record.
            session_id: Optional session association for filtering in get_verification_state.
        """
        ...

    @abstractmethod
    async def attach_evidence(self, claim_id: uuid.UUID, evidence: EvidenceRef) -> ClaimRecord:
        """Attach evidence to a claim."""
        ...

    @abstractmethod
    async def verify(self, claim_id: uuid.UUID) -> ClaimRecord:
        """Attempt to verify a claim based on attached evidence."""
        ...

    @abstractmethod
    async def get_verification_state(self, session_id: uuid.UUID) -> VerificationSummary:
        """Get aggregate verification state for a session."""
        ...

    @abstractmethod
    async def get_claim_verification(self, claim_id: uuid.UUID) -> VerificationState:
        """Get per-claim verification state."""
        ...

    @abstractmethod
    async def check_completion_requirements(self, procedure_id: uuid.UUID) -> CompletionCheckResult:
        """Check if all ProofRequirements for a procedure are satisfied."""
        ...

    @abstractmethod
    async def get_claims_for_procedure(self, procedure_id: uuid.UUID) -> list:
        """Return all claims for a given procedure."""
        ...

    @abstractmethod
    async def reject(self, claim_id: uuid.UUID, reason: str,
                     rejector_actor_id: uuid.UUID | None = None) -> ClaimRecord:
        """Explicitly reject a claim."""
        ...
