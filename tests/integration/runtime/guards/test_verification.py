"""Integration tests for evidence engine extensions (8 tests).

Tests the EvidenceAndVerificationEngine claim lifecycle: record, verify,
reject, completion checks, and batch verification patterns.  Cognee's
add_data_points and cognee.add are mocked to avoid real Neo4j/Qdrant.
"""
from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus, EvidenceRef
from elephantbroker.schemas.guards import CompletionCheckResult

GATEWAY_ID = "test-gw"


@pytest.fixture
def evidence_engine(mock_graph, trace_ledger):
    from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
    return EvidenceAndVerificationEngine(
        graph=mock_graph,
        trace_ledger=trace_ledger,
        gateway_id=GATEWAY_ID,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("elephantbroker.runtime.evidence.engine.add_data_points", new_callable=AsyncMock)
@patch("elephantbroker.runtime.evidence.engine.cognee")
async def test_record_claim_and_verify(mock_cognee, mock_add_dp, evidence_engine, mock_graph):
    """Record a claim, attach tool evidence, verify -> TOOL_SUPPORTED."""
    mock_cognee.add = AsyncMock()

    claim = ClaimRecord(
        claim_text="Deployed version 2.0 to staging",
        claim_type="deployment",
    )
    recorded = await evidence_engine.record_claim(claim)
    assert recorded.status == ClaimStatus.UNVERIFIED

    # Attach tool evidence
    evidence = EvidenceRef(
        type="tool_output",
        ref_value="deploy_tool returned success",
    )
    mock_graph.add_relation = AsyncMock()
    updated = await evidence_engine.attach_evidence(recorded.id, evidence)
    assert updated.status == ClaimStatus.SELF_SUPPORTED

    # Verify transitions based on evidence types
    verified = await evidence_engine.verify(recorded.id)
    assert verified.status == ClaimStatus.TOOL_SUPPORTED


@pytest.mark.asyncio
@patch("elephantbroker.runtime.evidence.engine.add_data_points", new_callable=AsyncMock)
@patch("elephantbroker.runtime.evidence.engine.cognee")
async def test_reject_claim(mock_cognee, mock_add_dp, evidence_engine):
    """reject(claim_id, reason) -> REJECTED status."""
    mock_cognee.add = AsyncMock()

    claim = ClaimRecord(claim_text="All tests pass", claim_type="testing")
    recorded = await evidence_engine.record_claim(claim)

    rejected = await evidence_engine.reject(recorded.id, "3 tests actually fail")
    assert rejected.status == ClaimStatus.REJECTED


@pytest.mark.asyncio
@patch("elephantbroker.runtime.evidence.engine.add_data_points", new_callable=AsyncMock)
@patch("elephantbroker.runtime.evidence.engine.cognee")
async def test_completion_check_complete(mock_cognee, mock_add_dp, evidence_engine, mock_graph):
    """Procedure with all proofs satisfied -> complete=True."""
    mock_cognee.add = AsyncMock()

    proc_id = uuid.uuid4()

    # Record a claim with tool evidence for this procedure
    claim = ClaimRecord(
        claim_text="Step 1 done",
        claim_type="step_completion",
        procedure_id=proc_id,
    )
    await evidence_engine.record_claim(claim)
    evidence = EvidenceRef(type="tool_output", ref_value="output log")
    mock_graph.add_relation = AsyncMock()
    await evidence_engine.attach_evidence(claim.id, evidence)
    await evidence_engine.verify(claim.id)

    # Mock graph to return procedure with steps that require tool_output
    mock_graph.query_cypher = AsyncMock(return_value=[{
        "p": {
            "eb_id": str(proc_id),
            "steps_json": json.dumps([{
                "instruction": "Complete deployment",
                "is_optional": False,
                "required_evidence": [{"proof_type": "tool_output", "description": "deploy log", "required": True}],
            }]),
            "approval_requirements": [],
        }
    }])

    result = await evidence_engine.check_completion_requirements(proc_id)
    assert result.complete is True
    assert len(result.missing_evidence) == 0


@pytest.mark.asyncio
@patch("elephantbroker.runtime.evidence.engine.add_data_points", new_callable=AsyncMock)
@patch("elephantbroker.runtime.evidence.engine.cognee")
async def test_completion_check_missing(mock_cognee, mock_add_dp, evidence_engine, mock_graph):
    """Procedure with partial proofs -> complete=False, missing_evidence populated."""
    mock_cognee.add = AsyncMock()

    proc_id = uuid.uuid4()

    # No claims recorded for this procedure
    mock_graph.query_cypher = AsyncMock(return_value=[{
        "p": {
            "eb_id": str(proc_id),
            "steps_json": json.dumps([{
                "instruction": "Run full test suite",
                "is_optional": False,
                "required_evidence": [{"proof_type": "tool_output", "description": "test results", "required": True}],
            }]),
            "approval_requirements": [],
        }
    }])

    result = await evidence_engine.check_completion_requirements(proc_id)
    assert result.complete is False
    assert len(result.missing_evidence) >= 1
    assert "test results" in result.missing_evidence[0]


@pytest.mark.asyncio
@patch("elephantbroker.runtime.evidence.engine.add_data_points", new_callable=AsyncMock)
@patch("elephantbroker.runtime.evidence.engine.cognee")
async def test_completion_check_missing_approval(mock_cognee, mock_add_dp, evidence_engine, mock_graph):
    """Procedure needs supervisor sign-off -> missing_approvals populated."""
    mock_cognee.add = AsyncMock()

    proc_id = uuid.uuid4()

    # Record a self-supported claim (not supervisor-verified)
    claim = ClaimRecord(
        claim_text="Deployment complete",
        procedure_id=proc_id,
    )
    await evidence_engine.record_claim(claim)
    evidence = EvidenceRef(type="chunk_ref", ref_value="some chunk")
    mock_graph.add_relation = AsyncMock()
    await evidence_engine.attach_evidence(claim.id, evidence)

    mock_graph.query_cypher = AsyncMock(return_value=[{
        "p": {
            "eb_id": str(proc_id),
            "steps_json": json.dumps([{
                "instruction": "Verify deployment",
                "is_optional": False,
                "required_evidence": [],
            }]),
            "approval_requirements": ["Supervisor must approve production deployment"],
        }
    }])

    result = await evidence_engine.check_completion_requirements(proc_id)
    assert result.complete is False
    assert len(result.missing_approvals) >= 1


@pytest.mark.asyncio
@patch("elephantbroker.runtime.evidence.engine.add_data_points", new_callable=AsyncMock)
@patch("elephantbroker.runtime.evidence.engine.cognee")
async def test_batch_verification_auto(mock_cognee, mock_add_dp, evidence_engine, mock_graph):
    """5 claims with tool evidence -> all auto-verified as TOOL_SUPPORTED."""
    mock_cognee.add = AsyncMock()
    mock_graph.add_relation = AsyncMock()

    proc_id = uuid.uuid4()
    claims = []
    for i in range(5):
        claim = ClaimRecord(
            claim_text=f"Step {i} completed",
            procedure_id=proc_id,
        )
        await evidence_engine.record_claim(claim)
        ev = EvidenceRef(type="tool_output", ref_value=f"output_{i}")
        await evidence_engine.attach_evidence(claim.id, ev)
        claims.append(claim)

    # Verify all
    for claim in claims:
        verified = await evidence_engine.verify(claim.id)
        assert verified.status == ClaimStatus.TOOL_SUPPORTED

    summary = await evidence_engine.get_verification_state(proc_id)
    assert summary.verified == 5
    assert summary.pending == 0


@pytest.mark.asyncio
@patch("elephantbroker.runtime.evidence.engine.add_data_points", new_callable=AsyncMock)
@patch("elephantbroker.runtime.evidence.engine.cognee")
async def test_batch_verification_flags(mock_cognee, mock_add_dp, evidence_engine, mock_graph):
    """3 claims with no evidence -> remain UNVERIFIED, flagged as pending."""
    mock_cognee.add = AsyncMock()

    proc_id = uuid.uuid4()
    for i in range(3):
        claim = ClaimRecord(
            claim_text=f"Unverified step {i}",
            procedure_id=proc_id,
        )
        await evidence_engine.record_claim(claim)

    summary = await evidence_engine.get_verification_state(proc_id)
    assert summary.pending == 3
    assert summary.verified == 0


@pytest.mark.asyncio
@patch("elephantbroker.runtime.evidence.engine.add_data_points", new_callable=AsyncMock)
@patch("elephantbroker.runtime.evidence.engine.cognee")
async def test_auto_goal_on_activation(mock_cognee, mock_add_dp, evidence_engine, mock_graph):
    """Recording a claim creates a trace event (simulating goal creation trigger)."""
    mock_cognee.add = AsyncMock()

    claim = ClaimRecord(
        claim_text="Complete onboarding checklist",
        claim_type="procedure_step",
    )
    recorded = await evidence_engine.record_claim(claim)

    # Verify trace ledger captured the claim event
    from elephantbroker.schemas.trace import TraceEventType
    trace_events = evidence_engine._trace._events
    claim_events = [e for e in trace_events if e.event_type == TraceEventType.CLAIM_MADE]
    assert len(claim_events) >= 1
    assert recorded.id in claim_events[0].claim_ids
