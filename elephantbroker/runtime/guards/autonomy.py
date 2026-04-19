"""Autonomy classifier — domain classification + autonomy level resolution (Phase 7 — §7.16)."""
from __future__ import annotations

import logging

from elephantbroker.schemas.guards import (
    AutonomyLevel,
    AutonomyPolicy,
    GuardActionType,
    GuardCheckInput,
)

logger = logging.getLogger(__name__)


# Default tool→domain mappings (~20 entries)
_DEFAULT_TOOL_DOMAINS: dict[str, str] = {
    # Financial
    "transfer_funds": "financial",
    "create_payment": "financial",
    "process_refund": "financial",
    "create_invoice": "financial",
    # Data access
    "query_database": "data_access",
    "read_file": "data_access",
    "search_logs": "data_access",
    "export_data": "data_access",
    # Communication
    "send_email": "communication",
    "send_message": "communication",
    "post_comment": "communication",
    "create_ticket": "communication",
    # Code change
    "deploy_to_prod": "code_change",
    "deploy_staging": "code_change",
    "git_push": "code_change",
    "merge_branch": "code_change",
    # Delegation
    "spawn_agent": "delegation",
    "delegate_task": "delegation",
    # Resource
    "create_resource": "resource",
    "delete_resource": "resource",
    # Record mutation
    "update_record": "record_mutation",
    "delete_record": "record_mutation",
}

# Keyword→domain heuristics for Tier 1 classification
_KEYWORD_DOMAINS: dict[str, list[str]] = {
    "financial": [
        "payment", "transfer", "refund", "invoice", "billing", "charge", "cost", "budget", "price",
        "credit card", "debit card", "card", "buy", "purchase", "order", "spend", "subscribe",
        "paypal", "venmo", "stripe", "checkout", "cart", "$",
    ],
    "data_access": ["database", "query", "export", "download", "backup", "dump"],
    "communication": ["email", "notify", "announce", "slack"],
    "code_change": [
        "deploy", "push", "merge", "release", "rollback", "commit",
        "refactor", "rewrite", "edit", "middleware", "endpoint",
        "migration", "schema change", "production code",
    ],
    "scope_change": ["scope", "requirements", "deadline", "priority", "redefine"],
    "resource": ["provision", "allocate", "scale", "instance", "cluster"],
    "info_share": ["publish", "broadcast", "distribute"],
    "delegation": ["delegate", "assign", "spawn", "handoff"],
    "record_mutation": ["delete", "update", "modify", "mutate", "drop", "truncate"],
    "planning": ["planning", "discussion", "technical planning", "design", "brainstorm", "architecture"],
}



class ToolDomainRegistry:
    """Maps tool names to decision domains."""

    def __init__(self, extra_mappings: dict[str, str] | None = None) -> None:
        self._mappings: dict[str, str] = dict(_DEFAULT_TOOL_DOMAINS)
        if extra_mappings:
            self._mappings.update(extra_mappings)

    def get_domain(self, tool_name: str) -> str | None:
        """Look up domain for a tool name. Returns None if unknown."""
        return self._mappings.get(tool_name)

    def register(self, tool_name: str, domain: str) -> None:
        self._mappings[tool_name] = domain


class AutonomyClassifier:
    """3-tier hybrid domain classification + autonomy resolution."""

    def __init__(self, tool_registry: ToolDomainRegistry | None = None,
                 redis=None, redis_keys=None) -> None:
        self._tools = tool_registry or ToolDomainRegistry()
        self._redis = redis
        self._keys = redis_keys
        self._last_tier: int = 0  # Track which tier resolved (for metrics)
        self._last_source: str = "uncategorized"  # Track source for metrics label

    def classify_domain(
        self,
        action: GuardCheckInput,
        active_procedure_domains: list[str] | None = None,
        recent_fact_domains: list[str] | None = None,
    ) -> str:
        """Classify the decision domain for an action using 3-tier hybrid approach.

        Tier 1: Static tool→domain lookup
        Tier 2: Fact domain context (recent facts tagged with domains)
        Tier 3: Keyword heuristic (cheapest fallback)
        """
        # Tier 1: Static tool lookup
        if action.action_type == GuardActionType.TOOL_CALL and action.action_target:
            domain = self._tools.get_domain(action.action_target)
            if domain:
                self._last_tier = 1
                self._last_source = "static"
                return domain

        # Tier 2: Recent fact domains from Redis (current conversation context)
        if recent_fact_domains:
            counts: dict[str, int] = {}
            for d in recent_fact_domains:
                counts[d] = counts.get(d, 0) + 1
            if counts:
                best = max(counts, key=counts.get)  # type: ignore[arg-type]
                self._last_tier = 2
                self._last_source = "fact"
                return best

        # Tier 3: Active procedure domains (ambient workflow context)
        if active_procedure_domains:
            self._last_tier = 3
            self._last_source = "procedure"
            return active_procedure_domains[-1]  # Last activated wins (LIFO)

        # Tier 4: Keyword heuristic on action content
        content_lower = (action.action_content or "").lower()
        for domain, keywords in _KEYWORD_DOMAINS.items():
            for kw in keywords:
                if kw in content_lower:
                    self._last_tier = 4
                    self._last_source = "keyword"
                    return domain

        self._last_tier = 0
        self._last_source = "uncategorized"
        return "uncategorized"

    def resolve_autonomy(self, domain: str, policy: AutonomyPolicy | None) -> AutonomyLevel:
        """Resolve autonomy level for a domain given the policy."""
        if policy is None:
            return AutonomyLevel.INFORM

        # Check custom domains first
        for custom in policy.custom_domains:
            if custom.name == domain:
                level = policy.domain_levels.get(domain, policy.default_level)
                return level

        # Check built-in domain levels
        level = policy.domain_levels.get(domain, policy.default_level)
        return level
