"""Tests for AutonomyClassifier + ToolDomainRegistry (Phase 7 — §7.16)."""
from __future__ import annotations

import pytest

from elephantbroker.runtime.guards.autonomy import (
    AutonomyClassifier,
    ToolDomainRegistry,
)
from elephantbroker.schemas.guards import (
    AutonomyLevel,
    AutonomyPolicy,
    CustomDomain,
    GuardActionType,
    GuardCheckInput,
)


class TestToolDomainRegistry:
    def test_default_mappings(self):
        reg = ToolDomainRegistry()
        assert reg.get_domain("transfer_funds") == "financial"
        assert reg.get_domain("send_email") == "communication"
        assert reg.get_domain("deploy_to_prod") == "code_change"

    def test_unknown_tool_returns_none(self):
        reg = ToolDomainRegistry()
        assert reg.get_domain("unknown_tool") is None

    def test_register_custom_tool(self):
        reg = ToolDomainRegistry()
        reg.register("my_tool", "custom_domain")
        assert reg.get_domain("my_tool") == "custom_domain"

    def test_extra_mappings(self):
        reg = ToolDomainRegistry(extra_mappings={"custom": "my_domain"})
        assert reg.get_domain("custom") == "my_domain"


class TestAutonomyClassifier:
    def test_tier1_tool_lookup(self):
        classifier = AutonomyClassifier()
        action = GuardCheckInput(
            action_type=GuardActionType.TOOL_CALL,
            action_target="transfer_funds",
        )
        domain = classifier.classify_domain(action)
        assert domain == "financial"
        assert classifier._last_tier == 1

    def test_tier3_procedure_domain(self):
        """Procedure domains are Tier 3 (after fact domains, per plan)."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(action_type=GuardActionType.MESSAGE_SEND, action_content="hello")
        domain = classifier.classify_domain(action, active_procedure_domains=["code_change"])
        assert domain == "code_change"
        assert classifier._last_tier == 3
        assert classifier._last_source == "procedure"

    def test_tier2_fact_domains(self):
        classifier = AutonomyClassifier()
        action = GuardCheckInput(action_type=GuardActionType.MESSAGE_SEND, action_content="hello")
        domain = classifier.classify_domain(action, recent_fact_domains=["financial", "financial", "code_change"])
        assert domain == "financial"
        assert classifier._last_tier == 2

    def test_tier4_keyword_heuristic(self):
        """Keyword heuristic is now Tier 4."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(action_type=GuardActionType.MESSAGE_SEND,
                                 action_content="process the payment please")
        domain = classifier.classify_domain(action)
        assert domain == "financial"
        assert classifier._last_tier == 4
        assert classifier._last_source == "keyword"

    def test_fallback_uncategorized(self):
        """Fallback returns 'uncategorized' (valid DecisionDomain), not 'general'."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(action_type=GuardActionType.MESSAGE_SEND,
                                 action_content="hello world")
        domain = classifier.classify_domain(action)
        assert domain == "uncategorized"
        assert classifier._last_tier == 0
        assert classifier._last_source == "uncategorized"

    def test_resolve_autonomy_from_policy(self):
        classifier = AutonomyClassifier()
        policy = AutonomyPolicy(
            domain_levels={"financial": AutonomyLevel.HARD_STOP},
            default_level=AutonomyLevel.INFORM,
        )
        assert classifier.resolve_autonomy("financial", policy) == AutonomyLevel.HARD_STOP
        assert classifier.resolve_autonomy("general", policy) == AutonomyLevel.INFORM

    def test_resolve_autonomy_none_policy(self):
        classifier = AutonomyClassifier()
        assert classifier.resolve_autonomy("anything", None) == AutonomyLevel.INFORM

    def test_resolve_autonomy_default_level(self):
        classifier = AutonomyClassifier()
        policy = AutonomyPolicy(default_level=AutonomyLevel.AUTONOMOUS)
        assert classifier.resolve_autonomy("unknown", policy) == AutonomyLevel.AUTONOMOUS

    def test_last_procedure_domain_wins(self):
        """LIFO: last activated procedure's domain is used."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(action_type=GuardActionType.MESSAGE_SEND, action_content="hello")
        domain = classifier.classify_domain(
            action, active_procedure_domains=["financial", "code_change"])
        assert domain == "code_change"

    def test_custom_domain_in_policy(self):
        classifier = AutonomyClassifier()
        policy = AutonomyPolicy(
            custom_domains=[CustomDomain(name="my_domain", keywords=["special"])],
            domain_levels={"my_domain": AutonomyLevel.HARD_STOP},
            default_level=AutonomyLevel.INFORM,
        )
        assert classifier.resolve_autonomy("my_domain", policy) == AutonomyLevel.HARD_STOP

    # --- Amendment 7.2 additional tests ---

    def test_tier2_fact_domains_take_priority_over_tier3_procedures(self):
        """Amendment 7.2 M5: fact domains (Tier 2) beat procedure domains (Tier 3)."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(
            action_type=GuardActionType.MESSAGE_SEND, action_content="hello")
        domain = classifier.classify_domain(
            action,
            recent_fact_domains=["financial"],
            active_procedure_domains=["code_change"],
        )
        assert domain == "financial"
        assert classifier._last_tier == 2
        assert classifier._last_source == "fact"

    def test_tier1_skipped_for_non_tool_action(self):
        """Tier 1 (static tool lookup) is skipped for MESSAGE_SEND actions."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(
            action_type=GuardActionType.MESSAGE_SEND,
            action_target="transfer_funds",
            action_content="unrelated text",
        )
        # Even though action_target is a known tool, Tier 1 only fires for TOOL_CALL
        domain = classifier.classify_domain(action)
        # Should NOT be "financial" from Tier 1; falls through to keyword or uncategorized
        assert classifier._last_tier != 1

    def test_tier2_empty_fact_domains_falls_through(self):
        """Empty recent_fact_domains list does not activate Tier 2."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(
            action_type=GuardActionType.MESSAGE_SEND, action_content="hello world")
        domain = classifier.classify_domain(action, recent_fact_domains=[])
        assert classifier._last_tier != 2

    def test_last_tier_tracking_across_calls(self):
        """_last_tier updates on each classify_domain call."""
        classifier = AutonomyClassifier()
        # First call: Tier 1
        tool_action = GuardCheckInput(
            action_type=GuardActionType.TOOL_CALL,
            action_target="transfer_funds",
        )
        classifier.classify_domain(tool_action)
        assert classifier._last_tier == 1
        # Second call: fallback to uncategorized
        generic = GuardCheckInput(
            action_type=GuardActionType.MESSAGE_SEND,
            action_content="hi there",
        )
        classifier.classify_domain(generic)
        assert classifier._last_tier == 0
        assert classifier._last_source == "uncategorized"

    def test_tool_call_empty_action_target(self):
        """TOOL_CALL with empty string action_target skips Tier 1."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(
            action_type=GuardActionType.TOOL_CALL,
            action_target="",
            action_content="process the payment",
        )
        domain = classifier.classify_domain(action)
        # Empty string is falsy, so Tier 1 is skipped; keyword heuristic picks up "payment"
        assert classifier._last_tier != 1
        assert domain == "financial"

    def test_tool_call_none_action_target(self):
        """TOOL_CALL with None action_target skips Tier 1."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(
            action_type=GuardActionType.TOOL_CALL,
            action_target=None,
            action_content="hello world",
        )
        domain = classifier.classify_domain(action)
        assert classifier._last_tier != 1

    def test_last_source_tracking(self):
        """Amendment 7.2: _last_source tracks the classification source."""
        classifier = AutonomyClassifier()
        # Static tool lookup
        action = GuardCheckInput(
            action_type=GuardActionType.TOOL_CALL,
            action_target="send_email",
        )
        classifier.classify_domain(action)
        assert classifier._last_source == "static"
        # Fact domain
        action2 = GuardCheckInput(
            action_type=GuardActionType.MESSAGE_SEND, action_content="hello")
        classifier.classify_domain(action2, recent_fact_domains=["data_access"])
        assert classifier._last_source == "fact"
        # Procedure domain
        classifier.classify_domain(action2, active_procedure_domains=["resource"])
        assert classifier._last_source == "procedure"

    def test_fallback_uncategorized_with_both_empty_contexts(self):
        """Fallback returns 'uncategorized' even when fact and procedure lists are empty."""
        classifier = AutonomyClassifier()
        action = GuardCheckInput(
            action_type=GuardActionType.MESSAGE_SEND,
            action_content="nothing special here",
        )
        domain = classifier.classify_domain(
            action,
            recent_fact_domains=[],
            active_procedure_domains=[],
        )
        assert domain == "uncategorized"
        assert classifier._last_tier == 0
        assert classifier._last_source == "uncategorized"

    def test_removed_keywords_do_not_trigger_domains(self):
        """ISSUE-20: 'chat', 'message', 'share' removed to avoid false positives."""
        classifier = AutonomyClassifier()
        for word in ("chat", "message", "share"):
            action = GuardCheckInput(
                action_type=GuardActionType.MESSAGE_SEND,
                action_content=f"let's {word} about the project",
            )
            domain = classifier.classify_domain(action)
            assert domain == "uncategorized", (
                f"'{word}' should not trigger any domain, got '{domain}'"
            )
