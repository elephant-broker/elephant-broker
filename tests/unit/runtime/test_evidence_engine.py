"""Tests for EvidenceAndVerificationEngine."""
import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.evidence import ClaimStatus
from elephantbroker.schemas.trace import TraceEventType
from tests.fixtures.factories import make_claim_record, make_evidence_ref


class TestEvidenceEngine:
    def _make(self):
        graph = AsyncMock()
        graph.add_relation = AsyncMock()
        ledger = TraceLedger()
        return EvidenceAndVerificationEngine(graph, ledger, dataset_name="test_ds"), graph, ledger

    async def test_record_claim(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        result = await engine.record_claim(claim)
        assert result.id == claim.id

    async def test_attach_evidence(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        ev = make_evidence_ref()
        result = await engine.attach_evidence(claim.id, ev)
        assert len(result.evidence_refs) == 1
        assert result.status == ClaimStatus.SELF_SUPPORTED

    async def test_verify_with_tool_output(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        ev = make_evidence_ref(type="tool_output")
        await engine.attach_evidence(claim.id, ev)
        result = await engine.verify(claim.id)
        assert result.status == ClaimStatus.TOOL_SUPPORTED

    async def test_verify_with_supervisor(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        ev = make_evidence_ref(type="supervisor_sign_off")
        await engine.attach_evidence(claim.id, ev)
        result = await engine.verify(claim.id)
        assert result.status == ClaimStatus.SUPERVISOR_VERIFIED

    async def test_get_verification_state(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        summary = await engine.get_verification_state(uuid.uuid4())
        assert summary.total_claims == 1
        assert summary.pending == 1

    async def test_get_claim_verification(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        state = await engine.get_claim_verification(claim.id)
        assert state.claim_id == claim.id

    async def test_record_claim_calls_add_data_points(self, monkeypatch, mock_add_data_points, mock_cognee):
        """CREATE: add_data_points called with ClaimDataPoint."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        assert len(mock_add_data_points.calls) == 1
        dp = mock_add_data_points.calls[0]["data_points"][0]
        assert dp.eb_id == str(claim.id)

    async def test_record_claim_calls_cognee_add(self, monkeypatch, mock_add_data_points, mock_cognee):
        """CREATE: cognee.add() called with claim_text."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record(claim_text="The sky is blue")
        await engine.record_claim(claim)
        mock_cognee.add.assert_called_once()
        text = mock_cognee.add.call_args[0][0]
        assert text == "The sky is blue"

    async def test_attach_evidence_calls_add_data_points_for_evidence(self, monkeypatch, mock_add_data_points, mock_cognee):
        """CREATE: add_data_points called with EvidenceDataPoint."""
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        mock_add_data_points.calls.clear()
        ev = make_evidence_ref()
        await engine.attach_evidence(claim.id, ev)
        # Should have 2 calls: evidence CREATE + claim UPDATE
        assert len(mock_add_data_points.calls) == 2

    async def test_attach_evidence_update_calls_add_data_points_not_cognee_add(self, monkeypatch, mock_add_data_points, mock_cognee):
        """UPDATE: claim status update uses add_data_points but not extra cognee.add()."""
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        mock_cognee.add.reset_mock()
        ev = make_evidence_ref()
        await engine.attach_evidence(claim.id, ev)
        # cognee.add called once for evidence CREATE, not for claim UPDATE
        assert mock_cognee.add.call_count == 1

    async def test_verify_calls_add_data_points_not_cognee_add(self, monkeypatch, mock_add_data_points, mock_cognee):
        """UPDATE: verify() uses add_data_points but not cognee.add()."""
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        ev = make_evidence_ref(type="tool_output")
        await engine.attach_evidence(claim.id, ev)
        mock_cognee.add.reset_mock()
        mock_add_data_points.calls.clear()
        await engine.verify(claim.id)
        assert len(mock_add_data_points.calls) == 1
        mock_cognee.add.assert_not_called()

    async def test_attach_evidence_raises_on_missing_claim(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        with pytest.raises(KeyError):
            await engine.attach_evidence(uuid.uuid4(), make_evidence_ref())

    async def test_verify_raises_on_missing_claim(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        with pytest.raises(KeyError):
            await engine.verify(uuid.uuid4())

    async def test_get_claim_verification_raises_on_missing(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        with pytest.raises(KeyError):
            await engine.get_claim_verification(uuid.uuid4())

    async def test_multiple_evidence_on_claim(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        await engine.attach_evidence(claim.id, make_evidence_ref(type="tool_output"))
        result = await engine.attach_evidence(claim.id, make_evidence_ref(type="chunk_ref"))
        assert len(result.evidence_refs) == 2

    async def test_mixed_evidence_priority(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        await engine.attach_evidence(claim.id, make_evidence_ref(type="tool_output"))
        await engine.attach_evidence(claim.id, make_evidence_ref(type="supervisor_sign_off"))
        result = await engine.verify(claim.id)
        assert result.status == ClaimStatus.SUPERVISOR_VERIFIED

    async def test_verify_no_evidence_stays_unverified(self, monkeypatch, mock_add_data_points, mock_cognee):
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        result = await engine.verify(claim.id)
        assert result.status == ClaimStatus.UNVERIFIED

    # --- Amendment 7.2 tests ---

    async def test_reject_with_valid_reason_sets_rejected(self, monkeypatch, mock_add_data_points, mock_cognee):
        """reject() with a valid reason sets claim status to REJECTED."""
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        result = await engine.reject(claim.id, reason="Contradicted by tool output")
        assert result.status == ClaimStatus.REJECTED

    async def test_reject_with_empty_reason_raises(self, monkeypatch, mock_add_data_points, mock_cognee):
        """reject() with empty reason raises ValueError."""
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        with pytest.raises(ValueError):
            await engine.reject(claim.id, reason="")

    async def test_reject_whitespace_only_reason_raises(self, monkeypatch, mock_add_data_points, mock_cognee):
        """reject() with whitespace-only reason raises ValueError."""
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        with pytest.raises(ValueError):
            await engine.reject(claim.id, reason="   ")

    async def test_reject_missing_claim_raises(self, monkeypatch, mock_add_data_points, mock_cognee):
        """reject() on nonexistent claim raises KeyError."""
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        with pytest.raises(KeyError):
            await engine.reject(uuid.uuid4(), reason="Gone")

    async def test_reject_emits_claim_verified_trace(self, monkeypatch, mock_add_data_points, mock_cognee):
        """reject() emits CLAIM_VERIFIED trace event with action=rejected."""
        engine, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        await engine.reject(claim.id, reason="Wrong")
        from elephantbroker.schemas.trace import TraceQuery
        events = await ledger.query_trace(TraceQuery())
        reject_events = [e for e in events if e.payload.get("action") == "rejected"]
        assert len(reject_events) == 1
        assert reject_events[0].event_type == TraceEventType.CLAIM_VERIFIED

    async def test_check_completion_requirements_all_proofs_satisfied(self, monkeypatch, mock_add_data_points, mock_cognee):
        """check_completion_requirements returns complete=True when all proofs are satisfied."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        proc_id = uuid.uuid4()
        # No graph data — fallback logic
        graph.query_cypher = AsyncMock(return_value=[])
        claim = make_claim_record(procedure_id=proc_id)
        await engine.record_claim(claim)
        ev = make_evidence_ref(type="tool_output")
        await engine.attach_evidence(claim.id, ev)
        await engine.verify(claim.id)
        result = await engine.check_completion_requirements(proc_id)
        assert result.complete is True

    async def test_check_completion_requirements_missing_evidence(self, monkeypatch, mock_add_data_points, mock_cognee):
        """check_completion_requirements reports missing when no verified claims."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        proc_id = uuid.uuid4()
        graph.query_cypher = AsyncMock(return_value=[])
        # Record claim but do NOT verify
        claim = make_claim_record(procedure_id=proc_id)
        await engine.record_claim(claim)
        result = await engine.check_completion_requirements(proc_id)
        assert result.complete is False

    async def test_check_completion_requirements_graph_failure_graceful(self, monkeypatch, mock_add_data_points, mock_cognee):
        """check_completion_requirements handles graph query failure gracefully."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        proc_id = uuid.uuid4()
        graph.query_cypher = AsyncMock(side_effect=Exception("Neo4j down"))
        claim = make_claim_record(procedure_id=proc_id)
        await engine.record_claim(claim)
        ev = make_evidence_ref(type="tool_output")
        await engine.attach_evidence(claim.id, ev)
        await engine.verify(claim.id)
        # Should not raise, should use fallback
        result = await engine.check_completion_requirements(proc_id)
        assert result.complete is True

    async def test_get_verification_state_empty(self, monkeypatch, mock_add_data_points, mock_cognee):
        """get_verification_state with no claims returns zeroes."""
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        summary = await engine.get_verification_state(uuid.uuid4())
        assert summary.total_claims == 0
        assert summary.verified == 0
        assert summary.pending == 0

    async def test_record_claim_sets_gateway_id(self, monkeypatch, mock_add_data_points, mock_cognee):
        """record_claim stamps gateway_id from engine onto claim."""
        engine = EvidenceAndVerificationEngine(
            AsyncMock(), TraceLedger(), dataset_name="test_ds", gateway_id="gw-42")
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        result = await engine.record_claim(claim)
        assert result.gateway_id == "gw-42"

    async def test_attach_evidence_emits_trace_event(self, monkeypatch, mock_add_data_points, mock_cognee):
        """Amendment 7.2: attach_evidence emits CLAIM_VERIFIED trace event."""
        engine, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)
        claim = make_claim_record()
        await engine.record_claim(claim)
        ev = make_evidence_ref(type="chunk_ref")
        await engine.attach_evidence(claim.id, ev)
        from elephantbroker.schemas.trace import TraceQuery
        events = await ledger.query_trace(TraceQuery())
        attach_events = [e for e in events if e.payload.get("action") == "attach_evidence"]
        assert len(attach_events) == 1
        assert attach_events[0].event_type == TraceEventType.CLAIM_VERIFIED
        assert attach_events[0].payload["evidence_type"] == "chunk_ref"

    async def test_claim_record_has_step_id_field(self):
        """Amendment 7.2: ClaimRecord schema has step_id field."""
        step = uuid.uuid4()
        claim = make_claim_record(step_id=step)
        assert claim.step_id == step

    async def test_claim_record_step_id_defaults_none(self):
        """ClaimRecord.step_id defaults to None."""
        claim = make_claim_record()
        assert claim.step_id is None

    async def test_get_claims_for_procedure_returns_filtered(self, monkeypatch, mock_add_data_points, mock_cognee):
        """get_claims_for_procedure returns only claims matching the given procedure_id."""
        engine, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)

        target_proc = uuid.uuid4()
        other_proc = uuid.uuid4()

        claim_a = make_claim_record(claim_text="Claim A", procedure_id=target_proc)
        claim_b = make_claim_record(claim_text="Claim B", procedure_id=other_proc)
        claim_c = make_claim_record(claim_text="Claim C", procedure_id=target_proc)

        await engine.record_claim(claim_a)
        await engine.record_claim(claim_b)
        await engine.record_claim(claim_c)

        result = await engine.get_claims_for_procedure(target_proc)
        assert len(result) == 2
        result_ids = {c.id for c in result}
        assert claim_a.id in result_ids
        assert claim_c.id in result_ids
        assert claim_b.id not in result_ids
