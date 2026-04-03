"""Integration tests for EvidenceAndVerificationEngine with real Neo4j."""
from __future__ import annotations

import pytest

from elephantbroker.schemas.evidence import ClaimStatus
from tests.fixtures.factories import make_claim_record, make_evidence_ref


@pytest.mark.integration
class TestEvidenceEngineIntegration:
    async def test_record_claim_and_attach_evidence(self, evidence_engine):
        claim = make_claim_record()
        await evidence_engine.record_claim(claim)
        ev = make_evidence_ref(type="tool_output")
        result = await evidence_engine.attach_evidence(claim.id, ev)
        assert len(result.evidence_refs) == 1
        assert result.status == ClaimStatus.SELF_SUPPORTED

    async def test_claim_state_transitions(self, evidence_engine):
        claim = make_claim_record()
        await evidence_engine.record_claim(claim)

        # Attach tool evidence
        ev = make_evidence_ref(type="tool_output")
        await evidence_engine.attach_evidence(claim.id, ev)

        # Verify should transition to TOOL_SUPPORTED
        verified = await evidence_engine.verify(claim.id)
        assert verified.status == ClaimStatus.TOOL_SUPPORTED
