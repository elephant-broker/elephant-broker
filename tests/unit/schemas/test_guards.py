"""Tests for guard pipeline schemas (Phase 7 — §7.2)."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from itertools import product

import pytest

from elephantbroker.schemas.guards import (
    AUTONOMY_TO_OUTCOME,
    ApprovalRequest,
    ApprovalStatus,
    AutonomyLevel,
    CompletionCheckResult,
    GuardActionType,
    GuardCheckInput,
    GuardEvent,
    GuardLayerResult,
    GuardOutcome,
    GuardResult,
    StaticRule,
    StaticRulePatternType,
    StructuralValidatorSpec,
    max_outcome,
)


class TestGuardOutcome:
    def test_guard_outcome_values(self):
        values = {e.value for e in GuardOutcome}
        assert values == {"pass", "inform", "warn", "require_evidence", "require_approval", "block"}

    def test_max_outcome_pass_vs_block(self):
        assert max_outcome(GuardOutcome.PASS, GuardOutcome.BLOCK) == GuardOutcome.BLOCK

    def test_max_outcome_symmetric(self):
        for a, b in product(GuardOutcome, GuardOutcome):
            assert max_outcome(a, b) == max_outcome(b, a)

    def test_max_outcome_same(self):
        assert max_outcome(GuardOutcome.PASS, GuardOutcome.PASS) == GuardOutcome.PASS

    @pytest.mark.parametrize("a,b", list(product(GuardOutcome, GuardOutcome)))
    def test_max_outcome_all_pairs(self, a: GuardOutcome, b: GuardOutcome):
        result = max_outcome(a, b)
        assert result in GuardOutcome
        assert result == max_outcome(b, a)

    def test_max_outcome_unknown_raises(self):
        # #1140 RESOLVED (R2-P2): max_outcome now strict-checks
        # ``isinstance(x, GuardOutcome)`` before value lookup. A plain
        # string (even one that isn't a valid outcome value) fails the
        # type check FIRST, yielding TypeError rather than the prior
        # ValueError on the unknown-outcome branch.
        with pytest.raises(TypeError, match="max_outcome.*expected GuardOutcome"):
            max_outcome(GuardOutcome.PASS, "invalid")  # type: ignore[arg-type]


class TestAutonomyMapping:
    def test_autonomy_to_outcome_mapping(self):
        assert AUTONOMY_TO_OUTCOME[AutonomyLevel.AUTONOMOUS] == GuardOutcome.PASS
        assert AUTONOMY_TO_OUTCOME[AutonomyLevel.INFORM] == GuardOutcome.INFORM
        assert AUTONOMY_TO_OUTCOME[AutonomyLevel.APPROVE_FIRST] == GuardOutcome.REQUIRE_APPROVAL
        assert AUTONOMY_TO_OUTCOME[AutonomyLevel.HARD_STOP] == GuardOutcome.BLOCK


class TestGuardCheckInput:
    def test_guard_check_input_defaults(self):
        inp = GuardCheckInput()
        assert inp.action_type == GuardActionType.MESSAGE_SEND
        assert inp.action_content == ""
        assert inp.action_target is None
        assert inp.action_metadata == {}
        assert inp.session_id is not None


class TestGuardResult:
    def test_guard_result_serialization(self):
        result = GuardResult(
            outcome=GuardOutcome.BLOCK,
            triggered_layer=1,
            matched_rules=["rule1"],
            explanation="blocked",
            layer_results=[
                GuardLayerResult(layer=0, outcome=GuardOutcome.PASS),
                GuardLayerResult(layer=1, outcome=GuardOutcome.BLOCK, definitive=True),
            ],
        )
        json_str = result.model_dump_json()
        restored = GuardResult.model_validate_json(json_str)
        assert restored.outcome == GuardOutcome.BLOCK
        assert restored.triggered_layer == 1
        assert len(restored.layer_results) == 2


class TestGuardEvent:
    def test_guard_event_timestamp_auto(self):
        before = datetime.now(UTC)
        event = GuardEvent(session_id=uuid.uuid4(), input_summary="test", outcome=GuardOutcome.PASS)
        after = datetime.now(UTC)
        assert before <= event.timestamp <= after


class TestStaticRule:
    def test_static_rule_enabled_default(self):
        rule = StaticRule(id="r1", pattern="test")
        assert rule.enabled is True

    def test_structural_validator_spec(self):
        spec = StructuralValidatorSpec(
            id="v1",
            action_type=GuardActionType.TOOL_CALL,
            action_target_pattern="deploy.*",
            required_fields=["review_token"],
            outcome_on_fail=GuardOutcome.BLOCK,
            description="Require review token for deploys",
        )
        assert spec.id == "v1"
        assert spec.required_fields == ["review_token"]
        assert spec.outcome_on_fail == GuardOutcome.BLOCK
        assert spec.enabled is True


class TestCompletionCheckResult:
    def test_completion_check_result_complete(self):
        result = CompletionCheckResult(complete=True, procedure_id=uuid.uuid4())
        assert result.complete is True
        assert result.missing_evidence == []
        assert result.missing_approvals == []


class TestApprovalRequest:
    def test_approval_request_timeout_at(self):
        req = ApprovalRequest(
            guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(),
            action_summary="test",
        )
        assert req.timeout_at is not None
        assert req.timeout_at > req.created_at

    def test_approval_status_values(self):
        values = {e.value for e in ApprovalStatus}
        assert values == {"pending", "approved", "rejected", "timed_out", "cancelled"}


# --- Amendment 7.2 additional tests ---


class TestGuardActionTypeEnum:
    def test_guard_action_type_all_values(self):
        values = {e.value for e in GuardActionType}
        assert values == {"message_send", "tool_call", "completion_claim", "delegation", "state_mutation"}
        assert len(GuardActionType) == 5


class TestStaticRulePatternTypeEnum:
    def test_static_rule_pattern_type_all_values(self):
        values = {e.value for e in StaticRulePatternType}
        assert values == {"keyword", "phrase", "regex", "tool_target"}
        assert len(StaticRulePatternType) == 4


class TestAutonomyLevelEnum:
    def test_autonomy_level_all_values(self):
        values = {e.value for e in AutonomyLevel}
        assert values == {"autonomous", "inform", "approve_first", "hard_stop"}
        assert len(AutonomyLevel) == 4


class TestDecisionDomainEnum:
    def test_decision_domain_all_values(self):
        from elephantbroker.schemas.guards import DecisionDomain
        values = {e.value for e in DecisionDomain}
        assert values == {
            "financial", "data_access", "communication", "code_change",
            "scope_change", "resource", "info_share", "delegation",
            "record_mutation", "uncategorized",
        }
        assert len(DecisionDomain) == 10


class TestGuardLayerResultExtended:
    def test_guard_layer_result_defaults(self):
        result = GuardLayerResult(layer=0)
        assert result.layer == 0
        assert result.definitive is False
        assert result.outcome == GuardOutcome.PASS
        assert result.matched_rules == []
        assert result.explanation == ""
        assert result.confidence == 1.0


class TestGuardResultExtended:
    def test_guard_result_constraints_reinjected_field(self):
        result = GuardResult(
            outcome=GuardOutcome.WARN,
            constraints_reinjected=["no_sensitive_data", "require_review"],
        )
        assert result.constraints_reinjected == ["no_sensitive_data", "require_review"]

    def test_guard_result_confidence_field(self):
        """Amendment 7.2: GuardResult has confidence field."""
        result = GuardResult(outcome=GuardOutcome.BLOCK, confidence=0.95)
        assert result.confidence == 0.95
        # Default should be 1.0
        default_result = GuardResult()
        assert default_result.confidence == 1.0


class TestGuardEventExtended:
    def test_guard_event_all_optional_fields_populated(self):
        event = GuardEvent(
            session_id=uuid.uuid4(),
            input_summary="test input",
            outcome=GuardOutcome.REQUIRE_APPROVAL,
            triggered_layer=2,
            matched_rules=["rule1", "rule2"],
            explanation="Multiple rules triggered",
            action_target="shell_exec",
            decision_domain="code_change",
            autonomy_level="approve_first",
        )
        assert event.triggered_layer == 2
        assert len(event.matched_rules) == 2
        assert event.action_target == "shell_exec"
        assert event.decision_domain == "code_change"
        assert event.autonomy_level == "approve_first"


class TestStaticRuleExtended:
    def test_static_rule_all_fields_populated(self):
        rule = StaticRule(
            id="r1",
            pattern_type=StaticRulePatternType.REGEX,
            pattern=r"(?i)drop\s+table",
            outcome=GuardOutcome.BLOCK,
            description="Drop table detection",
            enabled=True,
            source="custom",
            min_approval_authority=3,
            org_id="org_123",
        )
        assert rule.id == "r1"
        assert rule.pattern_type == StaticRulePatternType.REGEX
        assert rule.outcome == GuardOutcome.BLOCK
        assert rule.source == "custom"
        assert rule.min_approval_authority == 3
        assert rule.org_id == "org_123"


class TestSemanticMatchConstruction:
    def test_semantic_match_construction(self):
        from elephantbroker.schemas.guards import SemanticMatch
        match = SemanticMatch(
            exemplar_text="delete production database",
            similarity=0.92,
            bm25_score=0.85,
        )
        assert match.exemplar_text == "delete production database"
        assert match.similarity == 0.92
        assert match.bm25_score == 0.85


class TestCustomDomainConstruction:
    def test_custom_domain_construction_with_keywords(self):
        from elephantbroker.schemas.guards import CustomDomain
        domain = CustomDomain(
            name="billing",
            keywords=["invoice", "payment", "charge"],
            tool_patterns=["stripe_*", "paypal_*"],
        )
        assert domain.name == "billing"
        assert len(domain.keywords) == 3
        assert len(domain.tool_patterns) == 2


class TestAutonomyPolicyExtended:
    def test_autonomy_policy_with_domain_levels(self):
        from elephantbroker.schemas.guards import AutonomyPolicy
        policy = AutonomyPolicy(
            domain_levels={
                "financial": AutonomyLevel.HARD_STOP,
                "code_change": AutonomyLevel.APPROVE_FIRST,
                "communication": AutonomyLevel.INFORM,
            },
            default_level=AutonomyLevel.AUTONOMOUS,
        )
        assert policy.domain_levels["financial"] == AutonomyLevel.HARD_STOP
        assert policy.default_level == AutonomyLevel.AUTONOMOUS
        assert len(policy.domain_levels) == 3


class TestApprovalRoutingExtended:
    def test_approval_routing_defaults(self):
        from elephantbroker.schemas.guards import ApprovalRouting
        routing = ApprovalRouting()
        assert routing.timeout_seconds == 300
        assert routing.timeout_action == AutonomyLevel.HARD_STOP
        assert routing.notify_channels == []


class TestApprovalRequestExtended:
    def test_approval_request_explanation_field(self):
        """Amendment 7.2: ApprovalRequest has explanation field."""
        req = ApprovalRequest(
            guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(),
            action_summary="deploy to prod",
            explanation="Matched 3 rules: drop_table, rm_rf, credential exposure",
        )
        assert req.explanation == "Matched 3 rules: drop_table, rm_rf, credential exposure"


class TestCompletionCheckResultExtended:
    def test_completion_check_result_populated_missing(self):
        evidence_id = uuid.uuid4()
        result = CompletionCheckResult(
            complete=False,
            procedure_id=uuid.uuid4(),
            missing_evidence=["test_report", "code_review"],
            missing_approvals=["manager_signoff"],
            unverified_claims=[evidence_id],
        )
        assert result.complete is False
        assert len(result.missing_evidence) == 2
        assert len(result.missing_approvals) == 1
        assert result.unverified_claims == [evidence_id]
