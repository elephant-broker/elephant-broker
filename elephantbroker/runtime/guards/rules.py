"""Static rule registry — pattern-based guard rule matching (Phase 7 — §7.4)."""
from __future__ import annotations

import logging
import re

from elephantbroker.schemas.guards import (
    GuardCheckInput,
    GuardOutcome,
    StaticRule,
    StaticRuleMatch,
    StaticRulePatternType,
)

logger = logging.getLogger(__name__)

MAX_PATTERN_LENGTH = 500


class StaticRuleRegistry:
    """Stores and matches static guard rules against agent actions."""

    def __init__(self) -> None:
        self._rules: list[StaticRule] = []
        self._compiled_regex: dict[str, re.Pattern] = {}

    def load_rules(
        self,
        policy_rules: list[StaticRule] | None = None,
        procedure_bindings: list[str] | None = None,
        builtin_rules: list[StaticRule] | None = None,
    ) -> None:
        """Merge 3 sources. Dedup by rule.id — policy overrides builtins."""
        merged: dict[str, StaticRule] = {}

        # Load builtins first (lowest priority)
        effective_builtins = builtin_rules if builtin_rules is not None else self.get_builtin_rules()
        for rule in effective_builtins:
            merged[rule.id] = rule

        # Policy rules override builtins
        for rule in (policy_rules or []):
            merged[rule.id] = rule

        # Procedure bindings → BLOCK rules
        for binding in (procedure_bindings or []):
            binding_id = f"proc_binding:{binding}"
            merged[binding_id] = StaticRule(
                id=binding_id,
                pattern_type=StaticRulePatternType.KEYWORD,
                pattern=binding,
                outcome=GuardOutcome.BLOCK,
                description=f"Procedure binding: {binding}",
                source="procedure",
            )

        self._rules = list(merged.values())
        self._compiled_regex.clear()

        # Pre-compile regex patterns
        for rule in self._rules:
            if rule.pattern_type == StaticRulePatternType.REGEX and rule.enabled:
                if len(rule.pattern) > MAX_PATTERN_LENGTH:
                    logger.warning("Rule %s pattern too long (%d chars), skipping", rule.id, len(rule.pattern))
                    continue
                try:
                    self._compiled_regex[rule.id] = re.compile(rule.pattern)
                except re.error as exc:
                    logger.warning("Rule %s has invalid regex: %s", rule.id, exc)

    def match(self, action: GuardCheckInput) -> list[StaticRuleMatch]:
        """All matches sorted by outcome severity desc."""
        matches: list[StaticRuleMatch] = []
        content = action.action_content or ""
        target = action.action_target or ""

        for rule in self._rules:
            if not rule.enabled:
                continue

            matched_text = ""

            if rule.pattern_type == StaticRulePatternType.KEYWORD:
                if rule.pattern.lower() in content.lower():
                    matched_text = rule.pattern

            elif rule.pattern_type == StaticRulePatternType.PHRASE:
                pattern = r'\b' + re.escape(rule.pattern) + r'\b'
                m = re.search(pattern, content, re.IGNORECASE)
                if m:
                    matched_text = m.group(0)

            elif rule.pattern_type == StaticRulePatternType.REGEX:
                compiled = self._compiled_regex.get(rule.id)
                if compiled:
                    m = compiled.search(content)
                    if m:
                        matched_text = m.group(0)

            elif rule.pattern_type == StaticRulePatternType.TOOL_TARGET:
                if rule.pattern == target:
                    matched_text = target

            if matched_text:
                matches.append(StaticRuleMatch(
                    rule=rule,
                    matched_text=matched_text,
                    confidence=1.0,
                ))

        # Sort by outcome severity descending
        from elephantbroker.schemas.guards import _OUTCOME_ORDER
        matches.sort(key=lambda m: _OUTCOME_ORDER.get(m.rule.outcome.value, 0), reverse=True)
        return matches

    @staticmethod
    def get_builtin_rules() -> list[StaticRule]:
        """Return the 16 built-in guard rules."""
        return [
            StaticRule(id="builtin_credential_keyword", pattern_type=StaticRulePatternType.KEYWORD,
                       pattern="api_key", outcome=GuardOutcome.WARN,
                       description="Potential credential exposure: api_key"),
            StaticRule(id="builtin_credential_secret", pattern_type=StaticRulePatternType.KEYWORD,
                       pattern="secret_key", outcome=GuardOutcome.WARN,
                       description="Potential credential exposure: secret_key"),
            StaticRule(id="builtin_password_pattern", pattern_type=StaticRulePatternType.KEYWORD,
                       pattern="password", outcome=GuardOutcome.WARN,
                       description="Potential credential exposure: password"),
            StaticRule(id="builtin_credential_generic", pattern_type=StaticRulePatternType.KEYWORD,
                       pattern="credential", outcome=GuardOutcome.WARN,
                       description="Potential credential exposure: credential"),
            StaticRule(id="builtin_drop_table", pattern_type=StaticRulePatternType.REGEX,
                       pattern=r"(?i)drop\s+table", outcome=GuardOutcome.BLOCK,
                       description="Destructive SQL: DROP TABLE"),
            StaticRule(id="builtin_truncate_table", pattern_type=StaticRulePatternType.REGEX,
                       pattern=r"(?i)truncate\s+table", outcome=GuardOutcome.BLOCK,
                       description="Destructive SQL: TRUNCATE TABLE"),
            StaticRule(id="builtin_rm_rf", pattern_type=StaticRulePatternType.REGEX,
                       pattern=r"(?i)rm\s+-rf\s+/", outcome=GuardOutcome.BLOCK,
                       description="Destructive command: rm -rf /"),
            StaticRule(id="builtin_curl_write", pattern_type=StaticRulePatternType.REGEX,
                       pattern=r"(?i)(curl|wget).*(-o|>)\s*/etc/", outcome=GuardOutcome.BLOCK,
                       description="Overwrite system files via curl/wget"),
            StaticRule(id="builtin_shell_exec", pattern_type=StaticRulePatternType.TOOL_TARGET,
                       pattern="shell_exec", outcome=GuardOutcome.REQUIRE_APPROVAL,
                       description="Shell execution requires approval"),
            StaticRule(id="builtin_run_command", pattern_type=StaticRulePatternType.TOOL_TARGET,
                       pattern="run_command", outcome=GuardOutcome.REQUIRE_APPROVAL,
                       description="Command execution requires approval"),
            StaticRule(id="builtin_exfiltrate", pattern_type=StaticRulePatternType.KEYWORD,
                       pattern="exfiltrate", outcome=GuardOutcome.BLOCK,
                       description="Data exfiltration attempt"),
            StaticRule(id="builtin_dump_all", pattern_type=StaticRulePatternType.PHRASE,
                       pattern="dump all", outcome=GuardOutcome.BLOCK,
                       description="Data dump attempt"),
            StaticRule(id="builtin_delete_production", pattern_type=StaticRulePatternType.PHRASE,
                       pattern="delete production", outcome=GuardOutcome.BLOCK,
                       description="Production deletion attempt"),
            StaticRule(id="builtin_credit_card_purchase", pattern_type=StaticRulePatternType.REGEX,
                       pattern=r"(?i)\b(buy|purchase|order|spend)\b.{0,50}\b(credit card|debit card|my card|saved card)\b",
                       outcome=GuardOutcome.REQUIRE_APPROVAL,
                       description="Purchase using saved payment method"),
            StaticRule(id="builtin_refactor_sensitive_subsystem", pattern_type=StaticRulePatternType.REGEX,
                       pattern=r"(?i)\b(refactor|rewrite)\b.{0,40}\b(auth|authentication|middleware|payment|billing|security)\b",
                       outcome=GuardOutcome.REQUIRE_APPROVAL,
                       description="Refactor sensitive production subsystem"),
            StaticRule(id="builtin_prod_deploy_natural", pattern_type=StaticRulePatternType.REGEX,
                       pattern=r"(?i)\b(deploy|ship|release)\b.{0,20}\b(to prod|to production|live)\b",
                       outcome=GuardOutcome.REQUIRE_APPROVAL,
                       description="Production deployment"),
        ]
