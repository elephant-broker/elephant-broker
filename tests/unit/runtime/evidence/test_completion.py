"""Tests for evidence engine Phase 7 extensions — reject + completion requirements."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus, EvidenceRef


def _make():
    graph = AsyncMock()
    ledger = TraceLedger()
    engine = EvidenceAndVerificationEngine(graph, ledger, dataset_name="test", gateway_id="test")
    return engine, graph, ledger


class TestRejectClaim:
    async def test_reject_sets_status_to_rejected(self, monkeypatch, mock_add_data_points):
        engine, _, _ = _make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", AsyncMock())
        claim = ClaimRecord(claim_text="Test claim")
        await engine.record_claim(claim)
        result = await engine.reject(claim.id, "Invalid claim")
        assert result.status == ClaimStatus.REJECTED

    async def test_reject_requires_reason(self, monkeypatch, mock_add_data_points):
        engine, _, _ = _make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", AsyncMock())
        claim = ClaimRecord(claim_text="Test claim")
        await engine.record_claim(claim)
        with pytest.raises(ValueError, match="reason"):
            await engine.reject(claim.id, "")

    async def test_reject_empty_whitespace_reason(self, monkeypatch, mock_add_data_points):
        engine, _, _ = _make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", AsyncMock())
        claim = ClaimRecord(claim_text="Test claim")
        await engine.record_claim(claim)
        with pytest.raises(ValueError, match="reason"):
            await engine.reject(claim.id, "   ")

    async def test_reject_missing_claim_raises_key_error(self):
        engine, _, _ = _make()
        with pytest.raises(KeyError):
            await engine.reject(uuid.uuid4(), "reason")

    async def test_reject_emits_trace_event(self, monkeypatch, mock_add_data_points):
        engine, _, ledger = _make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", AsyncMock())
        claim = ClaimRecord(claim_text="Test claim")
        await engine.record_claim(claim)
        await engine.reject(claim.id, "bad claim")
        from elephantbroker.schemas.trace import TraceQuery, TraceEventType
        events = await ledger.query_trace(TraceQuery())
        reject_events = [e for e in events if e.payload.get("action") == "rejected"]
        assert len(reject_events) >= 1

    async def test_reject_persists_via_add_data_points(self, monkeypatch, mock_add_data_points):
        engine, _, _ = _make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", AsyncMock())
        claim = ClaimRecord(claim_text="Test claim")
        await engine.record_claim(claim)
        await engine.reject(claim.id, "reason")
        # add_data_points called for both record_claim and reject
        assert len(mock_add_data_points.calls) >= 2


class TestCheckCompletionRequirements:
    async def test_complete_when_no_graph_data_and_verified_claims(self, monkeypatch, mock_add_data_points):
        engine, graph, _ = _make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", AsyncMock())
        proc_id = uuid.uuid4()
        claim = ClaimRecord(claim_text="Done", procedure_id=proc_id, status=ClaimStatus.TOOL_SUPPORTED)
        await engine.record_claim(claim)
        graph.query_cypher = AsyncMock(return_value=[])  # No graph data
        result = await engine.check_completion_requirements(proc_id)
        assert result.complete is True

    async def test_incomplete_when_no_claims(self, monkeypatch, mock_add_data_points):
        engine, graph, _ = _make()
        graph.query_cypher = AsyncMock(return_value=[])
        proc_id = uuid.uuid4()
        result = await engine.check_completion_requirements(proc_id)
        assert result.complete is False

    async def test_reports_unverified_claims(self, monkeypatch, mock_add_data_points):
        engine, graph, _ = _make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", AsyncMock())
        proc_id = uuid.uuid4()
        claim = ClaimRecord(claim_text="Pending", procedure_id=proc_id, status=ClaimStatus.UNVERIFIED)
        await engine.record_claim(claim)
        graph.query_cypher = AsyncMock(return_value=[{"p": {
            "eb_id": str(proc_id), "gateway_id": "test",
            "steps": '[{"instruction": "Step 1", "is_optional": false, "required_evidence": [{"description": "proof", "required": true, "proof_type": "chunk_ref"}]}]',
            "approval_requirements": "[]",
        }}])
        result = await engine.check_completion_requirements(proc_id)
        assert result.complete is False
        assert len(result.unverified_claims) >= 1
