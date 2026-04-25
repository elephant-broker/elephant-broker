"""Cognee DataPoint subclasses mapping EB schemas to the graph engine."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from cognee.infrastructure.engine import DataPoint

from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.artifact import ToolArtifact
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus, EvidenceRef
from elephantbroker.schemas.fact import FactAssertion, FactCategory, MemoryClass
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.procedure import ProcedureDefinition


def _dt_to_epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _epoch_ms_to_dt(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)


# ---------------------------------------------------------------------------
# FactDataPoint
# ---------------------------------------------------------------------------

class FactDataPoint(DataPoint):
    text: str
    category: str
    scope: str = "session"
    confidence: float = 1.0
    memory_class: str = "episodic"
    session_key: str = ""
    session_id: str = ""
    source_actor_id: str = ""
    target_actor_ids: list[str] = []
    goal_ids: list[str] = []
    eb_created_at: int = 0
    eb_updated_at: int = 0
    use_count: int = 0
    successful_use_count: int = 0
    provenance_refs: list[str] = []
    embedding_ref: str | None = None
    token_size: int | None = None
    eb_id: str = ""
    gateway_id: str = ""
    decision_domain: str | None = None
    archived: bool = False
    autorecall_blacklisted: bool = False
    cognee_data_id: str | None = None
    metadata: dict[str, Any] = {"index_fields": ["text"]}

    @classmethod
    def from_schema(
        cls,
        fact: FactAssertion,
        *,
        cognee_data_id: str | None = None,
    ) -> FactDataPoint:
        # TODO-5-307: cognee_data_id is a storage-backend identifier that does
        # NOT live on FactAssertion. Callers that have captured an id (store
        # after cognee.add(), update after re-ingest, canonicalize after merge)
        # pass it explicitly; archive-style rewrites that need to preserve the
        # existing node property pass the value fetched from the graph entity.
        # Callers that omit it persist the node with cognee_data_id=None, which
        # the cascade treats as "nothing to clean on the Cognee side."
        return cls(
            id=fact.id,
            text=fact.text,
            category=str(fact.category),
            scope=fact.scope.value if hasattr(fact.scope, "value") else str(fact.scope),
            confidence=fact.confidence,
            memory_class=fact.memory_class.value if hasattr(fact.memory_class, "value") else str(fact.memory_class),
            session_key=fact.session_key or "",
            session_id=str(fact.session_id) if fact.session_id else "",
            source_actor_id=str(fact.source_actor_id) if fact.source_actor_id else "",
            target_actor_ids=[str(uid) for uid in fact.target_actor_ids],
            goal_ids=[str(uid) for uid in fact.goal_ids],
            eb_created_at=_dt_to_epoch_ms(fact.created_at),
            eb_updated_at=_dt_to_epoch_ms(fact.updated_at),
            use_count=fact.use_count,
            successful_use_count=fact.successful_use_count,
            provenance_refs=list(fact.provenance_refs),
            embedding_ref=fact.embedding_ref,
            token_size=fact.token_size,
            eb_id=str(fact.id),
            gateway_id=getattr(fact, "gateway_id", ""),
            decision_domain=getattr(fact, "decision_domain", None),
            archived=getattr(fact, "archived", False),
            autorecall_blacklisted=getattr(fact, "autorecall_blacklisted", False),
            cognee_data_id=cognee_data_id,
        )

    def to_schema(self) -> FactAssertion:
        return FactAssertion(
            id=uuid.UUID(self.eb_id) if self.eb_id else uuid.uuid4(),
            text=self.text,
            category=self.category,
            scope=self.scope,
            confidence=self.confidence,
            memory_class=self.memory_class,
            session_key=self.session_key,
            session_id=uuid.UUID(self.session_id) if self.session_id else None,
            source_actor_id=uuid.UUID(self.source_actor_id) if self.source_actor_id else None,
            target_actor_ids=[uuid.UUID(uid) for uid in self.target_actor_ids],
            goal_ids=[uuid.UUID(uid) for uid in self.goal_ids],
            created_at=_epoch_ms_to_dt(self.eb_created_at) if self.eb_created_at else datetime.now(UTC),
            updated_at=_epoch_ms_to_dt(self.eb_updated_at) if self.eb_updated_at else datetime.now(UTC),
            use_count=self.use_count,
            successful_use_count=self.successful_use_count,
            provenance_refs=list(self.provenance_refs),
            embedding_ref=self.embedding_ref,
            token_size=self.token_size,
            gateway_id=self.gateway_id,
            decision_domain=self.decision_domain,
            archived=self.archived,
            autorecall_blacklisted=self.autorecall_blacklisted,
        )


# ---------------------------------------------------------------------------
# ActorDataPoint
# ---------------------------------------------------------------------------

class ActorDataPoint(DataPoint):
    display_name: str
    actor_type: str
    authority_level: int = 0
    handles: list[str] = []
    org_id: str | None = None
    team_ids: list[str] = []
    trust_level: float = 0.5
    tags: list[str] = []
    eb_id: str = ""
    gateway_id: str = ""
    metadata: dict[str, Any] = {"index_fields": ["display_name"]}

    @classmethod
    def from_schema(cls, actor: ActorRef) -> ActorDataPoint:
        return cls(
            id=actor.id,
            display_name=actor.display_name,
            actor_type=actor.type.value,
            authority_level=actor.authority_level,
            handles=list(actor.handles),
            org_id=str(actor.org_id) if actor.org_id else None,
            team_ids=[str(t) for t in actor.team_ids],
            trust_level=actor.trust_level,
            tags=list(actor.tags),
            eb_id=str(actor.id),
            gateway_id=getattr(actor, "gateway_id", ""),
        )

    def to_schema(self) -> ActorRef:
        raw_team_ids = list(self.team_ids) if self.team_ids else []
        return ActorRef(
            id=uuid.UUID(self.eb_id) if self.eb_id else uuid.uuid4(),
            type=ActorType(self.actor_type),
            display_name=self.display_name,
            authority_level=self.authority_level,
            handles=list(self.handles),
            org_id=uuid.UUID(self.org_id) if self.org_id else None,
            team_ids=[uuid.UUID(t) for t in raw_team_ids],
            trust_level=self.trust_level,
            tags=list(self.tags),
            gateway_id=self.gateway_id,
        )


# ---------------------------------------------------------------------------
# GoalDataPoint
# ---------------------------------------------------------------------------

class GoalDataPoint(DataPoint):
    title: str
    description: str = ""
    status: str = "active"
    scope: str = "session"
    parent_goal_id: str | None = None
    eb_created_at: int = 0
    eb_updated_at: int = 0
    owner_actor_ids: list[str] = []
    success_criteria: list[str] = []
    blockers: list[str] = []
    confidence: float = 0.8
    evidence: list[str] = []
    eb_id: str = ""
    gateway_id: str = ""
    # Phase 8: org/team scoping for persistent goal visibility
    org_id: str | None = None
    team_id: str | None = None
    # Phase 7: auto-goal tracking metadata (source_type, source_system, etc.)
    # Named goal_meta to avoid collision with DataPoint.metadata (Cognee index_fields)
    # Type is dict (not str) because Neo4j round-trips JSON strings as native dicts
    # via clean_graph_props (`{`-prefix deserialization at runtime/graph_utils.py).
    #
    # **Storage str-coercion (TF-FN-020 G3 defensive note, R2-P3):**
    # ``GoalDataPoint.from_schema`` stores ``dict(goal.metadata)`` raw, but the
    # downstream ``GoalDataPoint.to_schema`` at line 226-227 explicitly coerces
    # every value to ``str`` (``goal_metadata = {str(k): str(v) for k, v in ...}``)
    # because the destination schema is ``GoalState.metadata: dict[str, str]``.
    # Round-trip consequence: ``int`` / ``float`` / ``bool`` / nested-dict values
    # passed via ``GoalState.metadata`` survive the storage hop, but reconstruct
    # as their ``str`` representation (e.g., ``42`` → ``"42"``, ``True`` → ``"True"``).
    # If a future caller needs preserved-type metadata round-trip, change
    # ``GoalState.metadata`` from ``dict[str, str]`` to ``dict[str, Any]``
    # AND drop the ``str(v)`` coercion at line 227. Or add a separate
    # ``goal_metadata_typed`` field that bypasses str-coercion.
    goal_meta: dict[str, Any] = {}
    metadata: dict[str, Any] = {"index_fields": ["title", "description"]}

    @classmethod
    def from_schema(cls, goal: GoalState) -> GoalDataPoint:
        return cls(
            id=goal.id,
            title=goal.title,
            description=goal.description,
            status=goal.status.value,
            scope=goal.scope.value,
            parent_goal_id=str(goal.parent_goal_id) if goal.parent_goal_id else None,
            eb_created_at=_dt_to_epoch_ms(goal.created_at),
            eb_updated_at=_dt_to_epoch_ms(goal.updated_at),
            owner_actor_ids=[str(uid) for uid in goal.owner_actor_ids],
            success_criteria=list(goal.success_criteria),
            blockers=list(goal.blockers),
            confidence=goal.confidence,
            evidence=list(goal.evidence),
            eb_id=str(goal.id),
            gateway_id=getattr(goal, "gateway_id", ""),
            org_id=str(goal.org_id) if goal.org_id else None,
            team_id=str(goal.team_id) if goal.team_id else None,
            goal_meta=dict(goal.metadata) if goal.metadata else {},
        )

    def to_schema(self) -> GoalState:
        goal_metadata: dict[str, str] = {}
        if isinstance(self.goal_meta, dict):
            goal_metadata = {str(k): str(v) for k, v in self.goal_meta.items()}
        return GoalState(
            id=uuid.UUID(self.eb_id) if self.eb_id else uuid.uuid4(),
            title=self.title,
            description=self.description,
            status=GoalStatus(self.status),
            scope=self.scope,
            parent_goal_id=uuid.UUID(self.parent_goal_id) if self.parent_goal_id else None,
            created_at=_epoch_ms_to_dt(self.eb_created_at) if self.eb_created_at else datetime.now(UTC),
            updated_at=_epoch_ms_to_dt(self.eb_updated_at) if self.eb_updated_at else datetime.now(UTC),
            owner_actor_ids=[uuid.UUID(uid) for uid in self.owner_actor_ids],
            success_criteria=list(self.success_criteria),
            blockers=list(self.blockers),
            confidence=self.confidence,
            evidence=list(self.evidence),
            gateway_id=self.gateway_id,
            org_id=uuid.UUID(self.org_id) if self.org_id else None,
            team_id=uuid.UUID(self.team_id) if self.team_id else None,
            metadata=goal_metadata,
        )


# ---------------------------------------------------------------------------
# ProcedureDataPoint
# ---------------------------------------------------------------------------

class ProcedureDataPoint(DataPoint):
    name: str
    description: str = ""
    scope: str = "session"
    eb_created_at: int = 0
    eb_updated_at: int = 0
    dp_version: int = 1
    source_actor_id: str | None = None
    eb_id: str = ""
    gateway_id: str = ""
    decision_domain: str | None = None
    # #1146 RESOLVED (R2-P2.1): mirrors ProcedureDefinition.is_manual_only
    # so the flag survives graph round-trip. Legacy procedures stored before
    # R2-P2.1 don't have this field and arrive as the default (False); the
    # to_schema / to_schema_from_dict auto-infer compensates to keep the
    # new model_validator happy (see those methods for the back-compat
    # logic).
    is_manual_only: bool = False
    # Stored as JSON strings for graph persistence
    steps_json: str = "[]"
    red_line_bindings_json: str = "[]"
    approval_requirements_json: str = "[]"
    metadata: dict[str, Any] = {"index_fields": ["name", "description"]}

    @classmethod
    def from_schema(cls, proc: ProcedureDefinition) -> ProcedureDataPoint:
        import json
        return cls(
            id=proc.id,
            name=proc.name,
            description=proc.description,
            scope=proc.scope.value,
            eb_created_at=_dt_to_epoch_ms(proc.created_at),
            eb_updated_at=_dt_to_epoch_ms(proc.updated_at),
            dp_version=proc.version,
            source_actor_id=str(proc.source_actor_id) if proc.source_actor_id else None,
            eb_id=str(proc.id),
            gateway_id=getattr(proc, "gateway_id", ""),
            decision_domain=getattr(proc, "decision_domain", None),
            # #1146 RESOLVED (R2-P2.1): persist is_manual_only so the flag
            # survives graph round-trip.
            is_manual_only=getattr(proc, "is_manual_only", False),
            steps_json=json.dumps([s.model_dump(mode="json") for s in proc.steps]) if proc.steps else "[]",
            red_line_bindings_json=json.dumps(proc.red_line_bindings) if proc.red_line_bindings else "[]",
            approval_requirements_json=json.dumps(proc.approval_requirements) if proc.approval_requirements else "[]",
        )

    def to_schema(self) -> ProcedureDefinition:
        import json
        from elephantbroker.schemas.procedure import ProcedureStep
        steps = []
        try:
            raw = json.loads(self.steps_json) if self.steps_json else []
            steps = [ProcedureStep(**s) for s in raw]
        except Exception:
            pass
        red_line_bindings = []
        try:
            red_line_bindings = json.loads(self.red_line_bindings_json) if self.red_line_bindings_json else []
        except Exception:
            pass
        approval_requirements = []
        try:
            approval_requirements = json.loads(self.approval_requirements_json) if self.approval_requirements_json else []
        except Exception:
            pass
        # #1146 RESOLVED (R2-P2.1): legacy-reconstruction auto-infer.
        # ProcedureDefinition's new model_validator requires
        # `activation_modes OR is_manual_only`. This DP path does NOT
        # currently persist activation_modes (orthogonal follow-up TD —
        # the storage schema lacks an `activation_modes_json` field), so
        # every reconstructed procedure arrives with activation_modes=[].
        # To satisfy the validator without breaking every graph read:
        #   - Honor stored is_manual_only when True.
        #   - Otherwise (stored False, or legacy record with no flag),
        #     auto-infer is_manual_only=True because the reconstructed
        #     activation_modes is always empty today.
        # A truly auto-triggered procedure cannot survive round-trip via
        # this DP path today — round-trip fidelity on activation_modes
        # is a separate IMPLEMENTED-PR-7-merge.md follow-up note.
        is_manual_only = True  # activation_modes not persisted → always manual-only on read
        return ProcedureDefinition(
            id=uuid.UUID(self.eb_id) if self.eb_id else uuid.uuid4(),
            name=self.name,
            description=self.description,
            scope=self.scope,
            created_at=_epoch_ms_to_dt(self.eb_created_at) if self.eb_created_at else datetime.now(UTC),
            updated_at=_epoch_ms_to_dt(self.eb_updated_at) if self.eb_updated_at else datetime.now(UTC),
            version=self.dp_version,
            source_actor_id=uuid.UUID(self.source_actor_id) if self.source_actor_id else None,
            gateway_id=self.gateway_id,
            decision_domain=self.decision_domain,
            steps=steps,
            red_line_bindings=red_line_bindings,
            approval_requirements=approval_requirements,
            is_manual_only=is_manual_only,
        )

    @classmethod
    def to_schema_from_dict(cls, d: dict) -> ProcedureDefinition:
        """Reconstruct ProcedureDefinition from a graph entity dict."""
        import json
        from elephantbroker.schemas.procedure import ProcedureStep
        steps = []
        steps_raw = d.get("steps_json") or d.get("steps", "[]")
        if isinstance(steps_raw, str):
            try:
                raw = json.loads(steps_raw)
                steps = [ProcedureStep(**s) for s in raw]
            except Exception:
                pass
        elif isinstance(steps_raw, list):
            steps = [ProcedureStep(**s) if isinstance(s, dict) else s for s in steps_raw]
        red_line_bindings = []
        rlb_raw = d.get("red_line_bindings_json") or d.get("red_line_bindings", "[]")
        if isinstance(rlb_raw, str):
            try:
                red_line_bindings = json.loads(rlb_raw)
            except Exception:
                pass
        elif isinstance(rlb_raw, list):
            red_line_bindings = rlb_raw
        approval_requirements = []
        ar_raw = d.get("approval_requirements_json") or d.get("approval_requirements", "[]")
        if isinstance(ar_raw, str):
            try:
                approval_requirements = json.loads(ar_raw)
            except Exception:
                pass
        elif isinstance(ar_raw, list):
            approval_requirements = ar_raw
        # #1146 RESOLVED (R2-P2.1): same auto-infer as to_schema() — the
        # reconstructed activation_modes list is always empty today
        # (activation_modes_json not yet in storage schema — orthogonal
        # follow-up TD), so unconditionally mark as manual-only to
        # satisfy ProcedureDefinition's new model_validator. Applies
        # uniformly to legacy records (no stored is_manual_only field)
        # and post-fix records.
        is_manual_only = True
        return ProcedureDefinition(
            id=uuid.UUID(d["eb_id"]) if d.get("eb_id") else uuid.uuid4(),
            name=d.get("name", ""),
            description=d.get("description", ""),
            scope=d.get("scope", "session"),
            decision_domain=d.get("decision_domain"),
            steps=steps,
            red_line_bindings=red_line_bindings,
            approval_requirements=approval_requirements,
            gateway_id=d.get("gateway_id", ""),
            is_manual_only=is_manual_only,
        )


# ---------------------------------------------------------------------------
# ClaimDataPoint
# ---------------------------------------------------------------------------

class ClaimDataPoint(DataPoint):
    claim_text: str
    claim_type: str = ""
    status: str = "unverified"
    procedure_id: str | None = None
    goal_id: str | None = None
    actor_id: str | None = None
    eb_created_at: int = 0
    eb_updated_at: int = 0
    eb_id: str = ""
    gateway_id: str = ""
    metadata: dict[str, Any] = {"index_fields": ["claim_text"]}

    @classmethod
    def from_schema(cls, claim: ClaimRecord) -> ClaimDataPoint:
        return cls(
            id=claim.id,
            claim_text=claim.claim_text,
            claim_type=claim.claim_type,
            status=claim.status.value,
            procedure_id=str(claim.procedure_id) if claim.procedure_id else None,
            goal_id=str(claim.goal_id) if claim.goal_id else None,
            actor_id=str(claim.actor_id) if claim.actor_id else None,
            eb_created_at=_dt_to_epoch_ms(claim.created_at),
            eb_updated_at=_dt_to_epoch_ms(claim.updated_at),
            eb_id=str(claim.id),
            gateway_id=getattr(claim, "gateway_id", ""),
        )

    def to_schema(self) -> ClaimRecord:
        return ClaimRecord(
            id=uuid.UUID(self.eb_id) if self.eb_id else uuid.uuid4(),
            claim_text=self.claim_text,
            claim_type=self.claim_type,
            status=ClaimStatus(self.status),
            procedure_id=uuid.UUID(self.procedure_id) if self.procedure_id else None,
            goal_id=uuid.UUID(self.goal_id) if self.goal_id else None,
            actor_id=uuid.UUID(self.actor_id) if self.actor_id else None,
            created_at=_epoch_ms_to_dt(self.eb_created_at) if self.eb_created_at else datetime.now(UTC),
            updated_at=_epoch_ms_to_dt(self.eb_updated_at) if self.eb_updated_at else datetime.now(UTC),
            gateway_id=self.gateway_id,
        )


# ---------------------------------------------------------------------------
# EvidenceDataPoint
# ---------------------------------------------------------------------------

class EvidenceDataPoint(DataPoint):
    evidence_type: str
    ref_value: str
    content_hash: str | None = None
    eb_created_at: int = 0
    created_by_actor_id: str | None = None
    eb_id: str = ""
    gateway_id: str = ""
    metadata: dict[str, Any] = {"index_fields": ["ref_value"]}

    @classmethod
    def from_schema(cls, ev: EvidenceRef) -> EvidenceDataPoint:
        return cls(
            id=ev.id,
            evidence_type=ev.type,
            ref_value=ev.ref_value,
            content_hash=ev.content_hash,
            eb_created_at=_dt_to_epoch_ms(ev.created_at),
            created_by_actor_id=str(ev.created_by_actor_id) if ev.created_by_actor_id else None,
            eb_id=str(ev.id),
            gateway_id=getattr(ev, "gateway_id", ""),
        )

    def to_schema(self) -> EvidenceRef:
        return EvidenceRef(
            id=uuid.UUID(self.eb_id) if self.eb_id else uuid.uuid4(),
            type=self.evidence_type,
            ref_value=self.ref_value,
            content_hash=self.content_hash,
            created_at=_epoch_ms_to_dt(self.eb_created_at) if self.eb_created_at else datetime.now(UTC),
            created_by_actor_id=uuid.UUID(self.created_by_actor_id) if self.created_by_actor_id else None,
            gateway_id=self.gateway_id,
        )


# ---------------------------------------------------------------------------
# ArtifactDataPoint
# ---------------------------------------------------------------------------

class ArtifactDataPoint(DataPoint):
    tool_name: str
    summary: str = ""
    content: str = ""
    session_id: str | None = None
    actor_id: str | None = None
    goal_id: str | None = None
    eb_created_at: int = 0
    token_estimate: int = 0
    tags: list[str] = []
    eb_id: str = ""
    gateway_id: str = ""
    metadata: dict[str, Any] = {"index_fields": ["summary"]}

    @classmethod
    def from_schema(cls, art: ToolArtifact) -> ArtifactDataPoint:
        return cls(
            id=art.artifact_id,
            tool_name=art.tool_name,
            summary=art.summary,
            content=art.content,
            session_id=str(art.session_id) if art.session_id else None,
            actor_id=str(art.actor_id) if art.actor_id else None,
            goal_id=str(art.goal_id) if art.goal_id else None,
            eb_created_at=_dt_to_epoch_ms(art.created_at),
            token_estimate=art.token_estimate,
            tags=list(art.tags),
            eb_id=str(art.artifact_id),
            gateway_id=getattr(art, "gateway_id", ""),
        )

    def to_schema(self) -> ToolArtifact:
        return ToolArtifact(
            artifact_id=uuid.UUID(self.eb_id) if self.eb_id else uuid.uuid4(),
            tool_name=self.tool_name,
            summary=self.summary,
            content=self.content,
            session_id=uuid.UUID(self.session_id) if self.session_id else None,
            actor_id=uuid.UUID(self.actor_id) if self.actor_id else None,
            goal_id=uuid.UUID(self.goal_id) if self.goal_id else None,
            created_at=_epoch_ms_to_dt(self.eb_created_at) if self.eb_created_at else datetime.now(UTC),
            token_estimate=self.token_estimate,
            tags=list(self.tags),
            gateway_id=self.gateway_id,
        )


# ---------------------------------------------------------------------------
# Phase 8: Organization & Team Graph Entities
# ---------------------------------------------------------------------------

class OrganizationDataPoint(DataPoint):
    """Neo4j node representing an organization.

    No gateway_id — orgs are business entities that span gateways.
    Two gateways (gw-prod, gw-staging) can serve the same org.
    """
    name: str
    display_label: str = ""
    eb_id: str = ""
    metadata: dict[str, Any] = {"index_fields": ["name"]}


class TeamDataPoint(DataPoint):
    """Neo4j node representing a team within an organization.

    No gateway_id — teams belong to orgs, not gateways.
    Multiple gateways can serve the same org+team.
    """
    name: str
    display_label: str = ""
    org_id: str
    eb_id: str = ""
    metadata: dict[str, Any] = {"index_fields": ["name"]}
