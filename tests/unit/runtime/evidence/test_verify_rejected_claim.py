"""TF-FN-019 G13 — evidence/engine.verify() refuses to re-verify a
REJECTED claim (audit-trail protection).

PROD #1186 RESOLVED in-PR. Before this commit, calling
``EvidenceAndVerificationEngine.verify(claim_id)`` on a claim in
``ClaimStatus.REJECTED`` would silently transition the claim to
``SELF_SUPPORTED`` (or similar) based on the evidence attached since
rejection — destroying the audit trail that recorded why the claim was
originally rejected.

Post-fix: the verify() method checks for the terminal REJECTED state at
the top of the function and raises ``ValueError`` with a descriptive
message. Re-evaluating a rejected claim now requires an explicit admin
reset path (not yet built — that work belongs to a follow-up).

Cross-flow: TF-FN-001 terminal-state pattern. This matches the
LifecycleStateMachine's DISPOSE-is-terminal invariant and the general
"once finalized, don't silently re-enter" shape.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus


async def test_verify_rejected_claim_raises_to_protect_audit_trail():
    """G13 (#1186 RESOLVED): verify() on a REJECTED claim now raises
    ValueError with a message pointing at the terminal-state protection.

    Pre-fix this would silently overwrite the REJECTED status; after
    this commit the raise is the contract. If the project ever builds
    an admin-reset path that legitimately un-rejects a claim, this test
    must be updated — NOT by making verify() permissive again, but by
    pointing the test at the new reset flow.
    """
    graph = AsyncMock()
    engine = EvidenceAndVerificationEngine(graph, TraceLedger(), dataset_name="t")

    rejected = ClaimRecord(
        id=uuid.uuid4(),
        claim_text="some claim",
        status=ClaimStatus.REJECTED,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    engine._claims[rejected.id] = rejected

    with pytest.raises(ValueError, match="Cannot re-verify a rejected claim"):
        with patch(
            "elephantbroker.runtime.evidence.engine.add_data_points",
            new_callable=AsyncMock,
        ):
            await engine.verify(rejected.id)

    # Critically: the stored claim's status remains REJECTED — the raise
    # happens before any transition logic runs.
    assert engine._claims[rejected.id].status == ClaimStatus.REJECTED
