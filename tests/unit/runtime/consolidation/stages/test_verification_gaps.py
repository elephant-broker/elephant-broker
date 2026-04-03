"""Tests for Stage 8: Identify Verification Gaps."""
from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock

from elephantbroker.runtime.consolidation.stages.verification_gaps import VerificationGapsStage


def _make_stage(claim_rows=None, proc_rows=None, evidence_rows=None):
    evidence_engine = AsyncMock()
    procedure_engine = AsyncMock()
    graph = AsyncMock()

    call_count = [0]
    async def mock_cypher(cypher, params):
        call_count[0] += 1
        # Evidence query contains BOTH EvidenceDataPoint AND ClaimDataPoint (via SUPPORTS edge)
        # Check evidence first to avoid matching claim query
        if "EvidenceDataPoint" in cypher and "SUPPORTS" in cypher:
            return evidence_rows or []
        if "ClaimDataPoint" in cypher:
            return claim_rows or []
        if "ProcedureDataPoint" in cypher:
            return proc_rows or []
        return []

    graph.query_cypher = mock_cypher
    return VerificationGapsStage(evidence_engine, procedure_engine, graph)


class TestVerificationGaps:
    async def test_finds_claim_missing_evidence(self):
        claim_rows = [{"props": {
            "eb_id": str(uuid.uuid4()), "claim_text": "claim A",
            "procedure_id": str(uuid.uuid4()), "gateway_id": "gw",
        }}]
        proc_rows = [{"props": {
            "steps_json": json.dumps([{"step_id": "s1", "required_evidence": [{"proof_type": "diff_hash", "description": "need hash"}]}]),
        }}]
        evidence_rows = []  # No evidence
        stage = _make_stage(claim_rows, proc_rows, evidence_rows)
        gaps = await stage.run("gw")
        assert len(gaps) >= 1
        assert gaps[0].missing_proof_type == "diff_hash"

    async def test_no_gaps_when_evidence_complete(self):
        claim_id = str(uuid.uuid4())
        claim_rows = [{"props": {
            "eb_id": claim_id, "claim_text": "complete claim",
            "procedure_id": str(uuid.uuid4()), "gateway_id": "gw",
        }}]
        proc_rows = [{"props": {
            "steps_json": json.dumps([{"step_id": "s1", "required_evidence": [{"proof_type": "diff_hash", "description": "need hash"}]}]),
        }}]
        evidence_rows = [{"props": {"evidence_type": "diff_hash"}}]
        stage = _make_stage(claim_rows, proc_rows, evidence_rows)
        gaps = await stage.run("gw")
        assert len(gaps) == 0

    async def test_reports_missing_proof_type(self):
        claim_rows = [{"props": {
            "eb_id": str(uuid.uuid4()), "claim_text": "needs receipt",
            "procedure_id": str(uuid.uuid4()), "gateway_id": "gw",
        }}]
        proc_rows = [{"props": {
            "steps_json": json.dumps([{
                "step_id": "s1",
                "required_evidence": [
                    {"proof_type": "receipt", "description": "payment receipt"},
                    {"proof_type": "supervisor_sign_off", "description": "manager approval"},
                ],
            }]),
        }}]
        evidence_rows = [{"props": {"evidence_type": "receipt"}}]  # Only receipt, no sign-off
        stage = _make_stage(claim_rows, proc_rows, evidence_rows)
        gaps = await stage.run("gw")
        assert len(gaps) >= 1
        assert any(g.missing_proof_type == "supervisor_sign_off" for g in gaps)

    async def test_no_claims_returns_empty(self):
        stage = _make_stage(claim_rows=[])
        gaps = await stage.run("gw")
        assert gaps == []

    async def test_gateway_id_on_gaps(self):
        claim_rows = [{"props": {
            "eb_id": str(uuid.uuid4()), "claim_text": "test",
            "procedure_id": str(uuid.uuid4()), "gateway_id": "gw-42",
        }}]
        proc_rows = [{"props": {
            "steps_json": json.dumps([{"step_id": "s1", "required_evidence": [{"proof_type": "hash", "description": "h"}]}]),
        }}]
        stage = _make_stage(claim_rows, proc_rows, [])
        gaps = await stage.run("gw-42")
        for g in gaps:
            assert g.gateway_id == "gw-42"

    async def test_no_llm_calls(self):
        stage = _make_stage()
        gaps = await stage.run("gw")
        assert isinstance(gaps, list)
