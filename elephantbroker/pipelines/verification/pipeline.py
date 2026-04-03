"""Verification pipeline — batch claim verification (Phase 7 — §7.10)."""
from __future__ import annotations

import logging
import random
import uuid

from pydantic import BaseModel

from elephantbroker.schemas.evidence import ClaimStatus
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger(__name__)


class VerificationPipelineResult(BaseModel):
    """Result of a batch verification run."""
    total_claims: int = 0
    auto_verified: int = 0
    flagged_no_evidence: int = 0
    queued_for_review: int = 0
    already_verified: int = 0
    rejected: int = 0


class VerificationPipeline:
    """Batch verification of claims for a session or procedure."""

    def __init__(self, evidence_engine, graph=None, trace_ledger=None,
                 pipeline_runner=None, metrics=None, sampling_rate: float = 0.0) -> None:
        self._evidence_engine = evidence_engine
        self._graph = graph
        self._trace = trace_ledger
        self._pipeline_runner = pipeline_runner
        self._metrics = metrics
        self._sampling_rate = sampling_rate

    async def run(
        self,
        *,
        session_id: uuid.UUID | None = None,
        procedure_id: uuid.UUID | None = None,
    ) -> VerificationPipelineResult:
        """Batch verification. At least one param required."""
        if not session_id and not procedure_id:
            raise ValueError("At least one of session_id or procedure_id required")

        if procedure_id:
            claims = await self._evidence_engine.get_claims_for_procedure(procedure_id)
        else:
            claims = list(self._evidence_engine._claims.values())  # fallback for session-only filter

        auto_verified = 0
        flagged = 0
        queued = 0
        already_verified = 0
        rejected = 0

        for claim in claims:
            if claim.status in (ClaimStatus.TOOL_SUPPORTED, ClaimStatus.SUPERVISOR_VERIFIED):
                already_verified += 1
                continue
            if claim.status == ClaimStatus.REJECTED:
                rejected += 1
                continue

            has_tool_output = any(ev.type == "tool_output" for ev in claim.evidence_refs)
            if has_tool_output and claim.status == ClaimStatus.SELF_SUPPORTED:
                await self._evidence_engine.verify(claim.id)
                auto_verified += 1
                continue

            if not claim.evidence_refs:
                flagged += 1
                continue

            if self._sampling_rate > 0 and random.random() < self._sampling_rate:
                queued += 1

        result = VerificationPipelineResult(
            total_claims=len(claims),
            auto_verified=auto_verified,
            flagged_no_evidence=flagged,
            queued_for_review=queued,
            already_verified=already_verified,
            rejected=rejected,
        )

        if self._metrics:
            self._metrics.inc_verification_check("complete")
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.PROCEDURE_COMPLETION_CHECKED,
                payload={"total": len(claims), "auto_verified": auto_verified, "flagged": flagged},
            ))

        logger.info("Verification pipeline: %d claims, %d auto-verified, %d flagged",
                     len(claims), auto_verified, flagged)
        return result
