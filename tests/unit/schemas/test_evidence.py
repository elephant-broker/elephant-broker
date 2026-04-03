"""Tests for evidence schemas."""
import uuid

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.evidence import (
    ClaimRecord,
    ClaimStatus,
    EvidenceRef,
    VerificationState,
    VerificationSummary,
)


class TestClaimStatus:
    def test_all_statuses(self):
        assert len(ClaimStatus) == 5

    def test_spec_statuses_present(self):
        expected = {"UNVERIFIED", "SELF_SUPPORTED", "TOOL_SUPPORTED", "SUPERVISOR_VERIFIED", "REJECTED"}
        assert {s.name for s in ClaimStatus} == expected


class TestEvidenceRef:
    def test_valid_creation(self):
        ev = EvidenceRef(type="tool_output", ref_value="pytest")
        assert isinstance(ev.id, uuid.UUID)
        assert ev.content_hash is None

    def test_json_round_trip(self):
        ev = EvidenceRef(type="chunk_ref", ref_value="doc.md")
        data = ev.model_dump(mode="json")
        restored = EvidenceRef.model_validate(data)
        assert restored.ref_value == ev.ref_value

    def test_created_by_actor_id(self):
        aid = uuid.uuid4()
        ev = EvidenceRef(type="tool_output", ref_value="x", created_by_actor_id=aid)
        assert ev.created_by_actor_id == aid


class TestClaimRecord:
    def test_valid_creation(self):
        claim = ClaimRecord(claim_text="The sky is blue")
        assert claim.status == ClaimStatus.UNVERIFIED
        assert claim.evidence_refs == []
        assert claim.claim_type == ""

    def test_empty_claim_text_rejected(self):
        with pytest.raises(ValidationError):
            ClaimRecord(claim_text="")

    def test_json_round_trip(self):
        claim = ClaimRecord(claim_text="test claim", status=ClaimStatus.TOOL_SUPPORTED)
        data = claim.model_dump(mode="json")
        restored = ClaimRecord.model_validate(data)
        assert restored.status == ClaimStatus.TOOL_SUPPORTED

    def test_new_fields(self):
        claim = ClaimRecord(claim_text="x", claim_type="factual", procedure_id=uuid.uuid4())
        assert claim.claim_type == "factual"
        assert claim.procedure_id is not None
        assert claim.goal_id is None
        assert claim.actor_id is None


class TestVerificationState:
    def test_valid_creation(self):
        vs = VerificationState(claim_id=uuid.uuid4())
        assert vs.status == ClaimStatus.UNVERIFIED
        assert vs.evidence_refs == []
        assert vs.verifier_actor_id is None
        assert vs.verified_at is None
        assert vs.rejection_reason is None

    def test_json_round_trip(self):
        vs = VerificationState(claim_id=uuid.uuid4(), status=ClaimStatus.REJECTED, rejection_reason="invalid")
        data = vs.model_dump(mode="json")
        restored = VerificationState.model_validate(data)
        assert restored.status == ClaimStatus.REJECTED
        assert restored.rejection_reason == "invalid"


class TestVerificationSummary:
    def test_defaults(self):
        vs = VerificationSummary()
        assert vs.total_claims == 0
        assert vs.coverage == 0.0

    def test_coverage_bounds(self):
        with pytest.raises(ValidationError):
            VerificationSummary(coverage=1.5)

    def test_counts_non_negative(self):
        with pytest.raises(ValidationError):
            VerificationSummary(total_claims=-1)
