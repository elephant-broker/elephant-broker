"""Unit tests for compute_evidence task."""
from __future__ import annotations

from elephantbroker.runtime.adapters.cognee.tasks.compute_evidence import compute_evidence
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus


class TestComputeEvidence:
    async def test_returns_claims_unchanged(self):
        claims = [
            ClaimRecord(claim_text="claim 1"),
            ClaimRecord(claim_text="claim 2", status=ClaimStatus.TOOL_SUPPORTED),
        ]
        result = await compute_evidence(claims)
        assert len(result) == 2
        assert result[0].claim_text == "claim 1"
        assert result[1].status == ClaimStatus.TOOL_SUPPORTED

    async def test_empty_input(self):
        result = await compute_evidence([])
        assert result == []

    async def test_preserves_identity(self):
        claim = ClaimRecord(claim_text="same")
        result = await compute_evidence([claim])
        assert result[0] is claim
