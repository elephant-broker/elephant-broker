"""Tests for StaticRuleRegistry (Phase 7 — §7.4)."""
from __future__ import annotations

import pytest

from elephantbroker.runtime.guards.rules import StaticRuleRegistry
from elephantbroker.schemas.guards import (
    GuardActionType,
    GuardCheckInput,
    GuardOutcome,
    StaticRule,
    StaticRulePatternType,
)


def _action(content: str = "", target: str | None = None,
            action_type: GuardActionType = GuardActionType.MESSAGE_SEND) -> GuardCheckInput:
    return GuardCheckInput(action_content=content, action_target=target, action_type=action_type)


class TestStaticRuleRegistry:
    def test_load_builtin_rules(self):
        reg = StaticRuleRegistry()
        reg.load_rules()
        assert len(reg._rules) == 13  # 12 original + 1 credential keyword (Amendment 7.2)

    def test_keyword_match(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern_type=StaticRulePatternType.KEYWORD,
                       pattern="api_key", outcome=GuardOutcome.WARN),
        ], builtin_rules=[])  # No builtins to avoid interference
        matches = reg.match(_action("my api_key is xyz"))
        assert len(matches) >= 1
        assert matches[0].rule.id == "r1"

    def test_keyword_case_insensitive(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="API_KEY", pattern_type=StaticRulePatternType.KEYWORD),
        ])
        matches = reg.match(_action("my api_key is here"))
        assert len(matches) >= 1

    def test_phrase_match(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="dump all", pattern_type=StaticRulePatternType.PHRASE,
                       outcome=GuardOutcome.BLOCK),
        ])
        matches = reg.match(_action("please dump all data"))
        assert len(matches) >= 1

    def test_phrase_no_partial_match(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="dump all", pattern_type=StaticRulePatternType.PHRASE),
        ])
        matches = reg.match(_action("dumpall"))
        assert len(matches) == 0

    def test_regex_match(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern=r"(?i)drop\s+table", pattern_type=StaticRulePatternType.REGEX,
                       outcome=GuardOutcome.BLOCK),
        ])
        matches = reg.match(_action("DROP TABLE users"))
        assert len(matches) >= 1

    def test_invalid_regex_skipped(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="[invalid", pattern_type=StaticRulePatternType.REGEX),
        ])
        matches = reg.match(_action("test"))
        assert len(matches) == 0

    def test_tool_target_match(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="shell_exec", pattern_type=StaticRulePatternType.TOOL_TARGET,
                       outcome=GuardOutcome.REQUIRE_APPROVAL),
        ])
        matches = reg.match(_action(target="shell_exec", action_type=GuardActionType.TOOL_CALL))
        assert len(matches) >= 1

    def test_tool_target_no_partial(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="shell_exec", pattern_type=StaticRulePatternType.TOOL_TARGET),
        ])
        matches = reg.match(_action(target="shell_exec_v2", action_type=GuardActionType.TOOL_CALL))
        assert len(matches) == 0

    def test_disabled_rule_skipped(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="test", enabled=False),
        ])
        matches = reg.match(_action("this is a test"))
        assert len(matches) == 0

    def test_policy_overrides_builtin(self):
        reg = StaticRuleRegistry()
        reg.load_rules(
            policy_rules=[StaticRule(id="builtin_drop_table", pattern="custom_override",
                                    pattern_type=StaticRulePatternType.KEYWORD, outcome=GuardOutcome.WARN)],
        )
        # The builtin_drop_table rule should be overridden
        found = [r for r in reg._rules if r.id == "builtin_drop_table"]
        assert len(found) == 1
        assert found[0].pattern == "custom_override"

    def test_procedure_bindings_create_block_rules(self):
        reg = StaticRuleRegistry()
        reg.load_rules(procedure_bindings=["no_unreviewed_deploys"])
        found = [r for r in reg._rules if r.id == "proc_binding:no_unreviewed_deploys"]
        assert len(found) == 1
        assert found[0].outcome == GuardOutcome.BLOCK

    def test_sorted_by_severity(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="test", pattern_type=StaticRulePatternType.KEYWORD,
                       outcome=GuardOutcome.WARN),
            StaticRule(id="r2", pattern="test", pattern_type=StaticRulePatternType.KEYWORD,
                       outcome=GuardOutcome.BLOCK),
        ])
        matches = reg.match(_action("this is a test"))
        assert len(matches) == 2
        assert matches[0].rule.outcome == GuardOutcome.BLOCK  # Most severe first

    def test_no_matches_returns_empty(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="xyz", pattern_type=StaticRulePatternType.KEYWORD),
        ])
        matches = reg.match(_action("nothing here"))
        assert matches == []

    # --- Amendment 7.2 additional tests ---

    def test_empty_registry_no_matches(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[], builtin_rules=[])
        matches = reg.match(_action("anything at all"))
        assert matches == []

    def test_multiple_pattern_types_match_same_content(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="drop table", pattern_type=StaticRulePatternType.KEYWORD,
                       outcome=GuardOutcome.WARN),
            StaticRule(id="r2", pattern="drop table", pattern_type=StaticRulePatternType.PHRASE,
                       outcome=GuardOutcome.BLOCK),
            StaticRule(id="r3", pattern=r"drop\s+table", pattern_type=StaticRulePatternType.REGEX,
                       outcome=GuardOutcome.REQUIRE_APPROVAL),
        ], builtin_rules=[])
        matches = reg.match(_action("please drop table users"))
        assert len(matches) == 3

    def test_phrase_match_case_insensitive(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="Dump All", pattern_type=StaticRulePatternType.PHRASE,
                       outcome=GuardOutcome.BLOCK),
        ], builtin_rules=[])
        matches = reg.match(_action("please DUMP ALL data now"))
        assert len(matches) >= 1
        assert matches[0].rule.id == "r1"

    def test_regex_exceeding_max_pattern_length_skipped(self):
        reg = StaticRuleRegistry()
        long_pattern = "a" * 501
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern=long_pattern, pattern_type=StaticRulePatternType.REGEX,
                       outcome=GuardOutcome.BLOCK),
        ], builtin_rules=[])
        matches = reg.match(_action("a" * 600))
        assert len(matches) == 0

    def test_match_returns_confidence_1_0(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="secret", pattern_type=StaticRulePatternType.KEYWORD),
        ], builtin_rules=[])
        matches = reg.match(_action("this is a secret"))
        assert len(matches) == 1
        assert matches[0].confidence == 1.0

    def test_empty_content_no_matches(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="test", pattern_type=StaticRulePatternType.KEYWORD),
        ], builtin_rules=[])
        matches = reg.match(_action(""))
        assert matches == []

    def test_load_rules_twice_resets_state(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="first", pattern_type=StaticRulePatternType.KEYWORD),
        ], builtin_rules=[])
        assert len(reg._rules) == 1
        reg.load_rules(policy_rules=[
            StaticRule(id="r2", pattern="second", pattern_type=StaticRulePatternType.KEYWORD),
            StaticRule(id="r3", pattern="third", pattern_type=StaticRulePatternType.KEYWORD),
        ], builtin_rules=[])
        assert len(reg._rules) == 2
        # First rule should be gone
        ids = {r.id for r in reg._rules}
        assert "r1" not in ids
        assert "r2" in ids
        assert "r3" in ids

    def test_rule_source_field_preserved(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="test", source="custom_org"),
        ], builtin_rules=[])
        assert reg._rules[0].source == "custom_org"

    def test_matched_text_contains_actual_text(self):
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern=r"(?i)(drop\s+table\s+\w+)",
                       pattern_type=StaticRulePatternType.REGEX,
                       outcome=GuardOutcome.BLOCK),
        ], builtin_rules=[])
        matches = reg.match(_action("please DROP TABLE users"))
        assert len(matches) == 1
        assert "DROP TABLE users" in matches[0].matched_text

    def test_keyword_substring_match_behavior(self):
        """Keywords match as substrings — 'key' matches 'api_key'."""
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="key", pattern_type=StaticRulePatternType.KEYWORD),
        ], builtin_rules=[])
        matches = reg.match(_action("my api_key is here"))
        assert len(matches) >= 1

    def test_tool_target_none_target_no_match(self):
        """TOOL_TARGET rule does not match when action has no target."""
        reg = StaticRuleRegistry()
        reg.load_rules(policy_rules=[
            StaticRule(id="r1", pattern="shell_exec",
                       pattern_type=StaticRulePatternType.TOOL_TARGET),
        ], builtin_rules=[])
        matches = reg.match(_action(content="shell_exec", target=None))
        assert len(matches) == 0

    def test_procedure_binding_source_is_procedure(self):
        reg = StaticRuleRegistry()
        reg.load_rules(procedure_bindings=["review_required"], builtin_rules=[])
        found = [r for r in reg._rules if r.id == "proc_binding:review_required"]
        assert len(found) == 1
        assert found[0].source == "procedure"

    def test_builtin_credential_keyword_matches(self):
        """Amendment 7.2: builtin_credential_keyword matches 'api_key'."""
        reg = StaticRuleRegistry()
        reg.load_rules()  # Load all builtins
        matches = reg.match(_action("my api_key is abc123"))
        credential_matches = [m for m in matches if m.rule.id == "builtin_credential_keyword"]
        assert len(credential_matches) >= 1

    def test_builtin_drop_table_matches(self):
        reg = StaticRuleRegistry()
        reg.load_rules()
        matches = reg.match(_action("DROP TABLE users"))
        drop_matches = [m for m in matches if m.rule.id == "builtin_drop_table"]
        assert len(drop_matches) >= 1
        assert drop_matches[0].rule.outcome == GuardOutcome.BLOCK

    def test_builtin_rm_rf_matches(self):
        reg = StaticRuleRegistry()
        reg.load_rules()
        matches = reg.match(_action("rm -rf /var/data"))
        rm_matches = [m for m in matches if m.rule.id == "builtin_rm_rf"]
        assert len(rm_matches) >= 1
        assert rm_matches[0].rule.outcome == GuardOutcome.BLOCK

    def test_builtin_shell_exec_matches(self):
        reg = StaticRuleRegistry()
        reg.load_rules()
        matches = reg.match(_action(target="shell_exec", action_type=GuardActionType.TOOL_CALL))
        shell_matches = [m for m in matches if m.rule.id == "builtin_shell_exec"]
        assert len(shell_matches) >= 1
        assert shell_matches[0].rule.outcome == GuardOutcome.REQUIRE_APPROVAL

    def test_builtin_exfiltrate_matches(self):
        reg = StaticRuleRegistry()
        reg.load_rules()
        matches = reg.match(_action("attempt to exfiltrate user data"))
        exfil_matches = [m for m in matches if m.rule.id == "builtin_exfiltrate"]
        assert len(exfil_matches) >= 1
        assert exfil_matches[0].rule.outcome == GuardOutcome.BLOCK
