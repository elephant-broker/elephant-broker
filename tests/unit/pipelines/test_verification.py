"""Tests for VerificationPipeline (Phase 7 — §7.10)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.pipelines.verification.pipeline import VerificationPipeline
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus, EvidenceRef


def _make_evidence_engine():
    engine = AsyncMock()
    engine._claims = {}
    engine.verify = AsyncMock()
    return engine


class TestVerificationPipeline:
    async def test_requires_at_least_one_param(self):
        pipeline = VerificationPipeline(evidence_engine=_make_evidence_engine())
        with pytest.raises(ValueError, match="At least one"):
            await pipeline.run()

    async def test_auto_verifies_tool_supported(self):
        engine = _make_evidence_engine()
        claim = ClaimRecord(claim_text="Done", status=ClaimStatus.SELF_SUPPORTED,
                            evidence_refs=[EvidenceRef(type="tool_output", ref_value="log.txt")])
        engine._claims[claim.id] = claim
        pipeline = VerificationPipeline(evidence_engine=engine, trace_ledger=TraceLedger())
        result = await pipeline.run(session_id=uuid.uuid4())
        assert result.auto_verified == 1
        engine.verify.assert_called_once_with(claim.id)

    async def test_flags_no_evidence_claims(self):
        engine = _make_evidence_engine()
        claim = ClaimRecord(claim_text="Unproven", status=ClaimStatus.UNVERIFIED, evidence_refs=[])
        engine._claims[claim.id] = claim
        pipeline = VerificationPipeline(evidence_engine=engine, trace_ledger=TraceLedger())
        result = await pipeline.run(session_id=uuid.uuid4())
        assert result.flagged_no_evidence == 1

    async def test_already_verified_skipped(self):
        engine = _make_evidence_engine()
        claim = ClaimRecord(claim_text="Done", status=ClaimStatus.SUPERVISOR_VERIFIED)
        engine._claims[claim.id] = claim
        pipeline = VerificationPipeline(evidence_engine=engine, trace_ledger=TraceLedger())
        result = await pipeline.run(session_id=uuid.uuid4())
        assert result.already_verified == 1
        engine.verify.assert_not_called()

    async def test_rejected_skipped(self):
        engine = _make_evidence_engine()
        claim = ClaimRecord(claim_text="Bad", status=ClaimStatus.REJECTED)
        engine._claims[claim.id] = claim
        pipeline = VerificationPipeline(evidence_engine=engine, trace_ledger=TraceLedger())
        result = await pipeline.run(session_id=uuid.uuid4())
        assert result.rejected == 1

    async def test_filters_by_procedure(self):
        engine = _make_evidence_engine()
        proc_id = uuid.uuid4()
        claim1 = ClaimRecord(claim_text="Match", procedure_id=proc_id, status=ClaimStatus.UNVERIFIED)
        claim2 = ClaimRecord(claim_text="Other", procedure_id=uuid.uuid4(), status=ClaimStatus.UNVERIFIED)
        engine._claims[claim1.id] = claim1
        engine._claims[claim2.id] = claim2
        # get_claims_for_procedure returns only matching claims
        engine.get_claims_for_procedure = AsyncMock(return_value=[claim1])
        pipeline = VerificationPipeline(evidence_engine=engine, trace_ledger=TraceLedger())
        result = await pipeline.run(procedure_id=proc_id)
        assert result.total_claims == 1
        engine.get_claims_for_procedure.assert_called_once_with(proc_id)

    async def test_emits_trace_event(self):
        engine = _make_evidence_engine()
        ledger = TraceLedger()
        pipeline = VerificationPipeline(evidence_engine=engine, trace_ledger=ledger)
        await pipeline.run(session_id=uuid.uuid4())
        from elephantbroker.schemas.trace import TraceQuery
        events = await ledger.query_trace(TraceQuery())
        assert len(events) >= 1
