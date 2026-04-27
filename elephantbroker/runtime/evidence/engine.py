"""Evidence and verification engine — claims, evidence, state transitions."""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

import cognee
from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import ClaimDataPoint, EvidenceDataPoint
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.interfaces.evidence_engine import IEvidenceAndVerificationEngine
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.schemas.evidence import (
    ClaimRecord,
    ClaimStatus,
    EvidenceRef,
    VerificationState,
    VerificationSummary,
)
from elephantbroker.runtime.observability import GatewayLoggerAdapter
from elephantbroker.schemas.trace import TraceEvent, TraceEventType


logger = logging.getLogger(__name__)


class EvidenceAndVerificationEngine(IEvidenceAndVerificationEngine):

    def __init__(self, graph: GraphAdapter, trace_ledger: ITraceLedger,
                 dataset_name: str = "elephantbroker", gateway_id: str = "") -> None:
        self._graph = graph
        self._trace = trace_ledger
        self._claims: dict[uuid.UUID, ClaimRecord] = {}
        self._claim_sessions: dict[uuid.UUID, uuid.UUID] = {}  # claim_id -> session_id
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})

    async def record_claim(self, claim: ClaimRecord, *,
                           session_id: uuid.UUID | None = None) -> ClaimRecord:
        claim.gateway_id = claim.gateway_id or self._gateway_id
        dp = ClaimDataPoint.from_schema(claim)
        await add_data_points([dp])  # CREATE
        await cognee.add(claim.claim_text, dataset_name=self._dataset_name)

        self._claims[claim.id] = claim
        if session_id is not None:
            self._claim_sessions[claim.id] = session_id
        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.CLAIM_MADE,
                claim_ids=[claim.id],
                payload={"action": "record_claim", "text": claim.claim_text[:100]},
            )
        )
        return claim

    async def attach_evidence(self, claim_id: uuid.UUID, evidence: EvidenceRef) -> ClaimRecord:
        claim = self._claims.get(claim_id)
        if claim is None:
            raise KeyError(f"Claim not found: {claim_id}")

        evidence.gateway_id = evidence.gateway_id or self._gateway_id
        ev_dp = EvidenceDataPoint.from_schema(evidence)
        await add_data_points([ev_dp])  # CREATE — evidence
        await cognee.add(evidence.ref_value, dataset_name=self._dataset_name)

        await self._graph.add_relation(str(evidence.id), str(claim_id), "SUPPORTS")

        claim.evidence_refs.append(evidence)
        claim.updated_at = datetime.now(UTC)

        # Auto-transition: unverified -> self_supported when evidence attached
        if claim.status == ClaimStatus.UNVERIFIED:
            claim.status = ClaimStatus.SELF_SUPPORTED

        # Update graph
        dp = ClaimDataPoint.from_schema(claim)
        await add_data_points([dp])  # UPDATE — claim status change, no cognee.add()

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.CLAIM_VERIFIED,
                claim_ids=[claim_id],
                gateway_id=self._gateway_id,
                payload={
                    "action": "attach_evidence",
                    "evidence_type": evidence.type,
                    "new_status": claim.status.value,
                },
            )
        )
        return claim

    async def verify(self, claim_id: uuid.UUID) -> ClaimRecord:
        claim = self._claims.get(claim_id)
        if claim is None:
            raise KeyError(f"Claim not found: {claim_id}")

        # #1186 RESOLVED (TF-FN-019 G13): REJECTED is a terminal state.
        # Re-verifying a rejected claim would silently overwrite the
        # audit trail — the forensic record of "this claim was rejected"
        # gets replaced by "self_supported" or similar, losing the
        # reason and reviewer context. Protect the audit trail by
        # refusing the transition; callers who need to re-evaluate a
        # previously rejected claim must explicitly reset to DRAFT via
        # a separate (not yet built) admin path.
        if claim.status == ClaimStatus.REJECTED:
            raise ValueError(
                f"Cannot re-verify a rejected claim: {claim_id} — "
                f"REJECTED is a terminal state; rejecting a previously verified "
                f"claim requires explicit reset, not re-evaluation."
            )

        # State transition based on evidence types
        evidence_types = {e.type for e in claim.evidence_refs}
        if "supervisor_sign_off" in evidence_types:
            claim.status = ClaimStatus.SUPERVISOR_VERIFIED
        elif "tool_output" in evidence_types:
            claim.status = ClaimStatus.TOOL_SUPPORTED
        elif claim.evidence_refs:
            claim.status = ClaimStatus.SELF_SUPPORTED

        claim.updated_at = datetime.now(UTC)
        dp = ClaimDataPoint.from_schema(claim)
        await add_data_points([dp])  # UPDATE — no cognee.add()

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.CLAIM_VERIFIED,
                claim_ids=[claim_id],
                payload={"action": "verify", "status": claim.status.value},
            )
        )
        return claim

    async def get_verification_state(self, session_id: uuid.UUID) -> VerificationSummary:
        # Filter claims by session_id when session association is available
        session_claims = [
            c for c in self._claims.values()
            if self._claim_sessions.get(c.id) == session_id
        ]
        # Fall back to all claims if no claims are session-tagged (backward compat)
        if not session_claims and self._claims:
            self._log.debug("No session-tagged claims for %s, returning all %d claims",
                           session_id, len(self._claims))
            session_claims = list(self._claims.values())

        total = len(session_claims)
        verified = sum(
            1 for c in session_claims
            if c.status in (ClaimStatus.TOOL_SUPPORTED, ClaimStatus.SUPERVISOR_VERIFIED)
        )
        pending = sum(
            1 for c in session_claims
            if c.status in (ClaimStatus.UNVERIFIED, ClaimStatus.SELF_SUPPORTED)
        )
        rejected = sum(1 for c in session_claims if c.status == ClaimStatus.REJECTED)
        return VerificationSummary(
            total_claims=total,
            verified=verified,
            pending=pending,
            disputed=0,
            retracted=rejected,
            coverage=verified / total if total > 0 else 0.0,
        )

    async def get_claim_verification(self, claim_id: uuid.UUID) -> VerificationState:
        claim = self._claims.get(claim_id)
        if claim is None:
            raise KeyError(f"Claim not found: {claim_id}")
        return VerificationState(
            claim_id=claim.id,
            status=claim.status,
            evidence_refs=list(claim.evidence_refs),
        )

    async def get_claims_for_procedure(self, procedure_id: uuid.UUID) -> list[ClaimRecord]:
        """Return all claims for a given procedure."""
        results = [c for c in self._claims.values() if c.procedure_id == procedure_id]
        self._log.debug("get_claims_for_procedure(%s): found %d claims", procedure_id, len(results))
        return results

    async def reject(self, claim_id: uuid.UUID, reason: str,
                     rejector_actor_id: uuid.UUID | None = None) -> ClaimRecord:
        """Explicitly reject a claim. Requires non-empty reason string."""
        if not reason or not reason.strip():
            raise ValueError("Rejection reason is required")

        claim = self._claims.get(claim_id)
        if claim is None:
            raise KeyError(f"Claim {claim_id} not found")

        claim.status = ClaimStatus.REJECTED
        claim.updated_at = datetime.now(UTC)

        dp = ClaimDataPoint.from_schema(claim)
        await add_data_points([dp])

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.CLAIM_VERIFIED,
                claim_ids=[claim_id],
                payload={
                    "action": "rejected",
                    "reason": reason,
                    "rejector_actor_id": str(rejector_actor_id) if rejector_actor_id else None,
                    "claim_text": claim.claim_text[:200],
                },
            )
        )
        return claim

    async def check_completion_requirements(self, procedure_id: uuid.UUID) -> "CompletionCheckResult":
        """Check if all ProofRequirements for a procedure are satisfied by verified claims."""
        from elephantbroker.schemas.guards import CompletionCheckResult

        proc_claims = [c for c in self._claims.values() if c.procedure_id == procedure_id]

        # Try to load procedure definition from graph
        proc_dict = None
        try:
            result = await self._graph.query_cypher(
                "MATCH (p:ProcedureDataPoint {eb_id: $pid, gateway_id: $gw}) RETURN p",
                {"pid": str(procedure_id), "gw": self._gateway_id},
            )
            if result:
                proc_dict = result[0].get("p") if isinstance(result[0], dict) else None
        except Exception as exc:
            self._log.warning("Failed to load procedure %s from graph: %s", procedure_id, exc)

        if proc_dict is None:
            # Fallback: if no graph data, check by claim existence
            complete = any(
                c.status in (ClaimStatus.TOOL_SUPPORTED, ClaimStatus.SUPERVISOR_VERIFIED)
                for c in proc_claims
            )
            return CompletionCheckResult(
                complete=complete,
                procedure_id=procedure_id,
            )

        # Parse steps from graph data
        steps_raw = proc_dict.get("steps_json") or proc_dict.get("steps", "[]")
        if isinstance(steps_raw, str):
            import json
            try:
                steps_data = json.loads(steps_raw)
            except (json.JSONDecodeError, TypeError):
                steps_data = []
        else:
            steps_data = steps_raw if isinstance(steps_raw, list) else []

        missing_evidence: list[str] = []
        unverified_claims: list[uuid.UUID] = []

        for step in steps_data:
            is_optional = step.get("is_optional", False)
            if is_optional:
                continue

            step_id_str = step.get("id") or step.get("step_id", "")
            required_evidence = step.get("required_evidence", [])

            # Fix A: If a non-optional step has no explicit proof requirements,
            # it still needs at least one verified claim referencing this step.
            if not required_evidence:
                has_step_claim = any(
                    c.step_id is not None
                    and str(c.step_id) == str(step_id_str)
                    and c.status not in (ClaimStatus.UNVERIFIED, ClaimStatus.REJECTED)
                    for c in proc_claims
                )
                if not has_step_claim:
                    instruction = step.get("instruction", "")[:80]
                    missing_evidence.append(
                        f"Step '{instruction}': no verified claim for this step"
                    )
                continue

            for proof_req in required_evidence:
                if not proof_req.get("required", True):
                    continue
                proof_type = proof_req.get("proof_type", "chunk_ref")
                description = proof_req.get("description", "")

                found = False
                for claim in proc_claims:
                    if claim.status == ClaimStatus.UNVERIFIED:
                        if claim.id not in unverified_claims:
                            unverified_claims.append(claim.id)
                        continue
                    if claim.status == ClaimStatus.REJECTED:
                        continue
                    # Fix B: Only match claims targeting this specific step.
                    # Claims with step_id=None are procedure-level and satisfy any step.
                    if claim.step_id is not None and step_id_str and str(claim.step_id) != str(step_id_str):
                        continue
                    for ev in claim.evidence_refs:
                        if ev.type == proof_type:
                            found = True
                            break
                    if found:
                        break

                if not found:
                    instruction = step.get("instruction", "")[:80]
                    missing_evidence.append(
                        f"Step '{instruction}': requires {proof_type} — {description}"
                    )

        # Check approval_requirements
        missing_approvals: list[str] = []
        approval_reqs = proc_dict.get("approval_requirements_json") or proc_dict.get("approval_requirements", [])
        if isinstance(approval_reqs, str):
            import json
            try:
                approval_reqs = json.loads(approval_reqs)
            except (json.JSONDecodeError, TypeError):
                approval_reqs = []

        for req in (approval_reqs or []):
            found = any(c.status == ClaimStatus.SUPERVISOR_VERIFIED for c in proc_claims)
            if not found:
                missing_approvals.append(req if isinstance(req, str) else str(req))

        complete = len(missing_evidence) == 0 and len(missing_approvals) == 0

        return CompletionCheckResult(
            complete=complete,
            procedure_id=procedure_id,
            missing_evidence=missing_evidence,
            missing_approvals=missing_approvals,
            unverified_claims=unverified_claims,
        )
