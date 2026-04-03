"""Stage 8: Identify Verification Gaps — scan claims for missing required evidence.

No LLM calls. Read-only analysis. Produces VerificationGap reports.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.evidence import VerificationGap

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
    from elephantbroker.runtime.interfaces.evidence_engine import IEvidenceAndVerificationEngine
    from elephantbroker.runtime.interfaces.procedure_engine import IProcedureEngine

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.verification_gaps")


class VerificationGapsStage:
    """Scan claims linked to procedures for missing required evidence.

    Algorithm:
    1. Query ClaimDataPoints where procedure_id IS NOT NULL and gateway_id matches
    2. For each claim, load the linked ProcedureDefinition
    3. For each ProcedureStep with required_evidence:
       Check if a matching EvidenceRef with the required ProofType exists
    4. Report gaps where required evidence is missing
    """

    def __init__(
        self,
        evidence_engine: IEvidenceAndVerificationEngine,
        procedure_engine: IProcedureEngine,
        graph: GraphAdapter,
    ) -> None:
        self._evidence = evidence_engine
        self._procedures = procedure_engine
        self._graph = graph

    @traced
    async def run(self, gateway_id: str) -> list[VerificationGap]:
        gaps: list[VerificationGap] = []

        # Query claims linked to procedures
        try:
            claim_results = await self._graph.query_cypher(
                "MATCH (c:ClaimDataPoint) "
                "WHERE c.procedure_id IS NOT NULL AND c.gateway_id = $gw "
                "RETURN properties(c) AS props",
                {"gw": gateway_id},
            )
        except Exception:
            logger.warning("Failed to query claims for verification gaps", exc_info=True)
            return []

        for row in claim_results:
            props = row.get("props", {})
            claim_id = props.get("eb_id", "")
            claim_text = props.get("claim_text", "")
            procedure_id = props.get("procedure_id", "")
            step_id = props.get("step_id")

            if not procedure_id:
                continue

            # Load procedure definition
            try:
                proc_result = await self._graph.query_cypher(
                    "MATCH (p:ProcedureDataPoint {eb_id: $pid, gateway_id: $gw}) "
                    "RETURN properties(p) AS props",
                    {"pid": procedure_id, "gw": gateway_id},
                )
            except Exception:
                continue

            if not proc_result:
                continue

            proc_props = proc_result[0].get("props", {})

            # Parse steps and check required evidence
            import json
            steps_raw = proc_props.get("steps_json") or proc_props.get("steps", "[]")
            try:
                steps = json.loads(steps_raw) if isinstance(steps_raw, str) else steps_raw
            except (json.JSONDecodeError, TypeError):
                steps = []

            for step in steps:
                required = step.get("required_evidence", [])
                if not required:
                    continue

                # Check if evidence exists for this claim + step
                try:
                    ev_results = await self._graph.query_cypher(
                        "MATCH (e:EvidenceDataPoint)-[:SUPPORTS]->(c:ClaimDataPoint {eb_id: $cid}) "
                        "RETURN properties(e) AS props",
                        {"cid": claim_id},
                    )
                except Exception:
                    ev_results = []

                existing_types = {
                    e.get("props", {}).get("evidence_type", "")
                    for e in ev_results
                }

                for req in required:
                    proof_type = req.get("proof_type", "")
                    if proof_type and proof_type not in existing_types:
                        import uuid as _uuid
                        gaps.append(VerificationGap(
                            id=_uuid.uuid4(),
                            claim_id=_uuid.UUID(claim_id) if claim_id else _uuid.uuid4(),
                            claim_text=claim_text,
                            procedure_id=_uuid.UUID(procedure_id) if procedure_id else None,
                            step_id=step.get("step_id") or step_id,
                            missing_proof_type=proof_type,
                            missing_proof_description=req.get("description", ""),
                            severity="medium",
                            gateway_id=gateway_id,
                        ))

        logger.info("Stage 8: %d verification gaps found (gateway=%s)", len(gaps), gateway_id)
        return gaps
