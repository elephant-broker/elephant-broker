"""Guard pipeline schemas — red-line enforcement, autonomy, approvals, completion gates."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class GuardOutcome(StrEnum):
    """Result of a guard layer check."""
    PASS = "pass"
    INFORM = "inform"
    WARN = "warn"
    REQUIRE_EVIDENCE = "require_evidence"
    REQUIRE_APPROVAL = "require_approval"
    BLOCK = "block"


class GuardActionType(StrEnum):
    """Type of action being checked by the guard pipeline."""
    MESSAGE_SEND = "message_send"
    TOOL_CALL = "tool_call"
    COMPLETION_CLAIM = "completion_claim"
    DELEGATION = "delegation"
    STATE_MUTATION = "state_mutation"


class StaticRulePatternType(StrEnum):
    """Pattern matching strategies for static rules."""
    KEYWORD = "keyword"
    PHRASE = "phrase"
    REGEX = "regex"
    TOOL_TARGET = "tool_target"


class AutonomyLevel(StrEnum):
    """How much freedom the agent has in a decision domain."""
    AUTONOMOUS = "autonomous"
    INFORM = "inform"
    APPROVE_FIRST = "approve_first"
    HARD_STOP = "hard_stop"


class DecisionDomain(StrEnum):
    """Built-in decision domains for autonomy classification."""
    FINANCIAL = "financial"
    DATA_ACCESS = "data_access"
    COMMUNICATION = "communication"
    CODE_CHANGE = "code_change"
    SCOPE_CHANGE = "scope_change"
    RESOURCE = "resource"
    INFO_SHARE = "info_share"
    DELEGATION = "delegation"
    RECORD_MUTATION = "record_mutation"
    UNCATEGORIZED = "uncategorized"


class ApprovalStatus(StrEnum):
    """Status of an approval request."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------
# Outcome ordering + max_outcome
# ---------------------------------------------------------------------------

_OUTCOME_ORDER: dict[str, int] = {
    "pass": 0,
    "inform": 1,
    "warn": 2,
    "require_evidence": 3,
    "require_approval": 4,
    "block": 5,
}


def max_outcome(a: GuardOutcome, b: GuardOutcome) -> GuardOutcome:
    """Return the more severe of two outcomes.

    #1140 RESOLVED (R2-P2): both arguments must be ``GuardOutcome`` enum
    instances. Previously ``hasattr(x, 'value')`` duck-typed any object
    with a ``.value`` attribute — including plain strings (whose
    ``.value`` check coincidentally held because of the ``str`` base
    class inheritance via ``StrEnum``). Now strictly rejects non-enum
    inputs with ``TypeError``.
    """
    if not isinstance(a, GuardOutcome):
        raise TypeError(
            f"max_outcome() expected GuardOutcome for 'a', got {type(a).__name__}: {a!r}"
        )
    if not isinstance(b, GuardOutcome):
        raise TypeError(
            f"max_outcome() expected GuardOutcome for 'b', got {type(b).__name__}: {b!r}"
        )
    oa = _OUTCOME_ORDER.get(a.value)
    ob = _OUTCOME_ORDER.get(b.value)
    if oa is None or ob is None:
        raise ValueError(f"Unknown GuardOutcome: {a!r} or {b!r}")
    return a if oa >= ob else b


AUTONOMY_TO_OUTCOME: dict[AutonomyLevel, GuardOutcome] = {
    AutonomyLevel.AUTONOMOUS: GuardOutcome.PASS,
    AutonomyLevel.INFORM: GuardOutcome.INFORM,
    AutonomyLevel.APPROVE_FIRST: GuardOutcome.REQUIRE_APPROVAL,
    AutonomyLevel.HARD_STOP: GuardOutcome.BLOCK,
}


# ---------------------------------------------------------------------------
# Pipeline types
# ---------------------------------------------------------------------------


class GuardCheckInput(BaseModel):
    """Input to the guard pipeline — describes what the agent is trying to do."""
    session_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    action_type: GuardActionType = GuardActionType.MESSAGE_SEND
    action_content: str = ""
    action_target: str | None = None
    action_metadata: dict[str, object] = Field(default_factory=dict)
    actor_id: uuid.UUID | None = None
    actor_type: str | None = None
    session_goal_ids: list[uuid.UUID] = Field(default_factory=list)
    active_procedure_ids: list[uuid.UUID] = Field(default_factory=list)


class GuardLayerResult(BaseModel):
    """Result of a single guard layer."""
    layer: int
    definitive: bool = False
    outcome: GuardOutcome = GuardOutcome.PASS
    matched_rules: list[str] = Field(default_factory=list)
    explanation: str = ""
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class GuardResult(BaseModel):
    """Final result of the full guard pipeline."""
    outcome: GuardOutcome = GuardOutcome.PASS
    triggered_layer: int | None = None
    matched_rules: list[str] = Field(default_factory=list)
    explanation: str = ""
    layer_results: list[GuardLayerResult] = Field(default_factory=list)
    constraints_reinjected: list[str] = Field(default_factory=list)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class GuardEvent(BaseModel):
    """Auditable guard event stored in Redis and trace ledger."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    session_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    input_summary: str = ""
    outcome: GuardOutcome = GuardOutcome.PASS
    triggered_layer: int | None = None
    matched_rules: list[str] = Field(default_factory=list)
    explanation: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    action_target: str | None = None
    decision_domain: str | None = None
    autonomy_level: str | None = None


# ---------------------------------------------------------------------------
# Rule types
# ---------------------------------------------------------------------------


class StaticRule(BaseModel):
    """A static guard rule matched by pattern."""
    id: str
    pattern_type: StaticRulePatternType = StaticRulePatternType.KEYWORD
    pattern: str = ""
    outcome: GuardOutcome = GuardOutcome.WARN
    description: str = ""
    enabled: bool = True
    source: str = "builtin"
    min_approval_authority: int | None = None
    org_id: str = ""


class StaticRuleMatch(BaseModel):
    """A match found by the static rule registry."""
    rule: StaticRule
    matched_text: str = ""
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class SemanticMatch(BaseModel):
    """A semantic similarity match from the guard index."""
    exemplar_text: str
    similarity: float = Field(ge=0.0, le=1.0)
    bm25_score: float = 0.0


class StructuralValidatorSpec(BaseModel):
    """Specification for a structural validator (Layer 3)."""
    id: str
    action_type: GuardActionType = GuardActionType.TOOL_CALL
    action_target_pattern: str | None = None
    required_fields: list[str] = Field(default_factory=list)
    outcome_on_fail: GuardOutcome = GuardOutcome.BLOCK
    description: str = ""
    enabled: bool = True


# ---------------------------------------------------------------------------
# Autonomy types
# ---------------------------------------------------------------------------


class CustomDomain(BaseModel):
    """User-defined decision domain."""
    name: str
    keywords: list[str] = Field(default_factory=list)
    tool_patterns: list[str] = Field(default_factory=list)


class AutonomyPolicy(BaseModel):
    """Per-profile autonomy configuration mapping domains to levels."""
    domain_levels: dict[str, AutonomyLevel] = Field(default_factory=dict)
    default_level: AutonomyLevel = AutonomyLevel.INFORM
    custom_domains: list[CustomDomain] = Field(default_factory=list)


class ApprovalRouting(BaseModel):
    """Configuration for how approvals are routed and handled."""
    timeout_seconds: int = Field(default=300, ge=30)
    timeout_action: AutonomyLevel = AutonomyLevel.HARD_STOP
    notify_channels: list[str] = Field(default_factory=list)


class ApprovalRequest(BaseModel):
    """A request for human approval.

    ``timeout_seconds`` (#1135 RESOLVED — R2-P2): callers pass the
    routing-resolved timeout (typically ``state.guard_policy.approval_routing.timeout_seconds``)
    so ``timeout_at`` reflects the policy-configured value instead of the
    previous hardcoded 300s. Default of 300 matches ``ApprovalRouting.timeout_seconds``
    default; ``ge=30`` floor matches the routing constraint.
    """
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    guard_event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    session_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    action_summary: str = ""
    explanation: str = ""
    decision_domain: str = "general"
    autonomy_level: AutonomyLevel = AutonomyLevel.APPROVE_FIRST
    matched_rules: list[str] = Field(default_factory=list)
    status: ApprovalStatus = ApprovalStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    timeout_seconds: int = Field(default=300, ge=30)
    timeout_at: datetime | None = None
    resolved_at: datetime | None = None
    resolved_by: str | None = None
    approval_message: str | None = None
    rejection_reason: str | None = None

    def model_post_init(self, __context: object) -> None:
        if self.timeout_at is None:
            self.timeout_at = self.created_at + timedelta(seconds=self.timeout_seconds)


# ---------------------------------------------------------------------------
# Completion types
# ---------------------------------------------------------------------------


class StepCheckResult(BaseModel):
    """Result of checking a single procedure step."""
    step_id: str
    complete: bool = False
    missing_evidence: list[str] = Field(default_factory=list)


class CompletionCheckResult(BaseModel):
    """Result of checking all completion requirements for a procedure."""
    complete: bool = False
    procedure_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    missing_evidence: list[str] = Field(default_factory=list)
    missing_approvals: list[str] = Field(default_factory=list)
    unverified_claims: list[uuid.UUID] = Field(default_factory=list)
