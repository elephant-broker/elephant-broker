"""Tests for guard configuration schemas — StrictnessPreset, GuardConfig, HitlConfig (Phase 7 — Amendment 7.2)."""
from __future__ import annotations

import pytest

from elephantbroker.schemas.config import GuardConfig, HitlConfig, StrictnessPreset
from elephantbroker.schemas.guards import StaticRule, StaticRulePatternType, StructuralValidatorSpec
from elephantbroker.schemas.profile import GuardPolicy


class TestStrictnessPreset:
    def test_strictness_preset_defaults(self):
        preset = StrictnessPreset()
        assert preset.bm25_threshold_multiplier == 1.0
        assert preset.semantic_threshold_override is None
        assert preset.warn_outcome_upgrade is None
        assert preset.structural_validators_enabled is True
        assert preset.reinjection_on == "elevated_risk"  # Amendment 7.2: changed from "any_non_pass"
        assert preset.llm_escalation_on == "ambiguous"  # Amendment 7.2: changed from "disabled"

    def test_strictness_preset_loose_values(self):
        preset = StrictnessPreset(
            bm25_threshold_multiplier=1.5,
            semantic_threshold_override=0.90,
            structural_validators_enabled=False,
            reinjection_on="block_only",
            llm_escalation_on="disabled",
        )
        assert preset.bm25_threshold_multiplier == 1.5
        assert preset.semantic_threshold_override == 0.90
        assert preset.structural_validators_enabled is False
        assert preset.reinjection_on == "block_only"

    def test_strictness_preset_medium_values(self):
        preset = StrictnessPreset(
            bm25_threshold_multiplier=1.0,
            reinjection_on="elevated_risk",
            llm_escalation_on="ambiguous",
        )
        assert preset.bm25_threshold_multiplier == 1.0
        assert preset.reinjection_on == "elevated_risk"
        assert preset.llm_escalation_on == "ambiguous"

    def test_strictness_preset_strict_values(self):
        preset = StrictnessPreset(
            bm25_threshold_multiplier=0.7,
            semantic_threshold_override=0.70,
            warn_outcome_upgrade="require_approval",
            reinjection_on="any_non_pass",
            llm_escalation_on="any_non_pass",
        )
        assert preset.bm25_threshold_multiplier == 0.7
        assert preset.semantic_threshold_override == 0.70
        assert preset.warn_outcome_upgrade == "require_approval"
        assert preset.llm_escalation_on == "any_non_pass"


class TestGuardConfig:
    def test_guard_config_defaults(self):
        config = GuardConfig()
        assert config.enabled is True
        assert config.history_ttl_seconds == 86400
        assert config.input_summary_max_chars == 500
        assert config.llm_escalation_max_tokens == 500  # Amendment 7.2: changed from 256
        assert config.llm_escalation_timeout_seconds == 10.0
        assert config.max_pattern_length == 500

    def test_guard_config_enabled_false(self):
        config = GuardConfig(enabled=False)
        assert config.enabled is False

    def test_guard_config_strictness_presets_three_present(self):
        config = GuardConfig()
        assert "loose" in config.strictness_presets
        assert "medium" in config.strictness_presets
        assert "strict" in config.strictness_presets
        assert len(config.strictness_presets) == 3
        # Verify loose preset values
        loose = config.strictness_presets["loose"]
        assert loose.bm25_threshold_multiplier == 1.5
        assert loose.structural_validators_enabled is False
        # Verify strict preset values
        strict = config.strictness_presets["strict"]
        assert strict.bm25_threshold_multiplier == 0.7
        assert strict.warn_outcome_upgrade == "require_approval"

    def test_guard_config_builtin_rules_enabled_default_true(self):
        """Amendment 7.2: builtin_rules_enabled defaults to True."""
        config = GuardConfig()
        assert config.builtin_rules_enabled is True

    def test_guard_config_max_history_events_default_50(self):
        """Amendment 7.2: max_history_events defaults to 50."""
        config = GuardConfig()
        assert config.max_history_events == 50


class TestHitlConfig:
    def test_hitl_config_defaults_disabled(self):
        config = HitlConfig()
        assert config.enabled is False
        assert config.default_url == "http://localhost:8421"
        assert config.timeout_seconds == 10.0
        assert config.approval_default_timeout_seconds == 300
        assert config.callback_hmac_secret == ""
        assert config.gateway_overrides == {}

    def test_hitl_config_enabled_with_url(self):
        config = HitlConfig(
            enabled=True,
            default_url="http://hitl-prod:8421",
            timeout_seconds=30.0,
            approval_default_timeout_seconds=600,
        )
        assert config.enabled is True
        assert config.default_url == "http://hitl-prod:8421"
        assert config.timeout_seconds == 30.0
        assert config.approval_default_timeout_seconds == 600

    def test_hitl_config_gateway_overrides(self):
        config = HitlConfig(
            default_url="http://default:8421",
            gateway_overrides={
                "gw1": "http://gw1-hitl:8421",
                "gw2": "http://gw2-hitl:8421",
            },
        )
        assert config.gateway_overrides["gw1"] == "http://gw1-hitl:8421"
        assert config.gateway_overrides["gw2"] == "http://gw2-hitl:8421"
        assert len(config.gateway_overrides) == 2


class TestGuardPolicy:
    def test_guard_policy_defaults(self):
        policy = GuardPolicy()
        assert policy.force_system_constraint_injection is True
        assert policy.preflight_check_strictness == "medium"
        assert policy.static_rules == []
        assert policy.redline_exemplars == []
        assert policy.structural_validators == []
        assert policy.bm25_block_threshold == 0.85
        assert policy.bm25_warn_threshold == 0.60
        assert policy.semantic_similarity_threshold == 0.80
        assert policy.llm_escalation_enabled is False

    def test_guard_policy_with_static_rules_coercion(self):
        """Field validator coerces dicts to StaticRule instances."""
        policy = GuardPolicy(
            static_rules=[
                {"id": "r1", "pattern": "api_key", "pattern_type": "keyword"},
                {"id": "r2", "pattern": "drop table", "pattern_type": "phrase"},
            ],
        )
        assert len(policy.static_rules) == 2
        assert isinstance(policy.static_rules[0], StaticRule)
        assert policy.static_rules[0].id == "r1"
        assert policy.static_rules[0].pattern_type == StaticRulePatternType.KEYWORD

    def test_guard_policy_with_structural_validators_coercion(self):
        """Field validator coerces dicts to StructuralValidatorSpec instances."""
        policy = GuardPolicy(
            structural_validators=[
                {
                    "id": "v1",
                    "action_type": "tool_call",
                    "action_target_pattern": "deploy.*",
                    "required_fields": ["review_token"],
                },
            ],
        )
        assert len(policy.structural_validators) == 1
        assert isinstance(policy.structural_validators[0], StructuralValidatorSpec)
        assert policy.structural_validators[0].id == "v1"
