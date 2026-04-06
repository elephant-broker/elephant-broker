"""Authority rule store — PostgreSQL-backed (migrated from SQLite).

Table ``authority_rules`` is created by Alembic migration 0001_initial_schema.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from elephantbroker.runtime.db.pg_store import PostgresStore

logger = logging.getLogger(__name__)

# Default authority rules — used when no custom override exists in Postgres.
# matching_exempt_level: actors at or above this level skip require_matching_* checks.
AUTHORITY_DEFAULTS: dict[str, dict[str, Any]] = {
    "create_global_goal": {"min_authority_level": 90},
    "create_org_goal": {"min_authority_level": 70, "require_matching_org": True, "matching_exempt_level": 90},
    "create_team_goal": {"min_authority_level": 50, "require_matching_team": True, "matching_exempt_level": 70},
    "create_actor_goal": {"min_authority_level": 0, "require_self_ownership": True},
    "create_org": {"min_authority_level": 90},
    "create_team": {"min_authority_level": 70, "require_matching_org": True, "matching_exempt_level": 90},
    "add_team_member": {"min_authority_level": 50, "require_matching_team": True, "matching_exempt_level": 70},
    "remove_team_member": {"min_authority_level": 50, "require_matching_team": True, "matching_exempt_level": 70},
    "register_actor": {"min_authority_level": 70},
    "register_org_profile_override": {
        "min_authority_level": 70, "require_matching_org": True, "matching_exempt_level": 90,
    },
    "merge_actors": {"min_authority_level": 70},
}


class AuthorityRuleStore(PostgresStore):
    """PostgreSQL persistence for system-wide authority rules.

    Table schema (managed by Alembic)::

        authority_rules (
            action TEXT PRIMARY KEY,
            rule_json TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
    """

    async def get_rule(self, action: str) -> dict[str, Any]:
        """Get rule for an action. Custom override wins over default."""
        row = await self.fetchrow(
            "SELECT rule_json FROM authority_rules WHERE action = $1", action
        )
        if row is not None:
            return json.loads(row["rule_json"])
        default = AUTHORITY_DEFAULTS.get(action)
        if default is None:
            return {"min_authority_level": 90}  # unknown actions require system admin
        return dict(default)

    async def set_rule(self, action: str, rule: dict[str, Any]) -> None:
        """Upsert a custom authority rule."""
        now = datetime.now(UTC).isoformat()
        await self.execute(
            """INSERT INTO authority_rules (action, rule_json, updated_at)
               VALUES ($1, $2, $3)
               ON CONFLICT (action) DO UPDATE SET
                   rule_json = EXCLUDED.rule_json,
                   updated_at = EXCLUDED.updated_at""",
            action, json.dumps(rule), now,
        )
        logger.info("Authority rule updated: %s → %s", action, rule)

    async def get_rules(self) -> dict[str, dict[str, Any]]:
        """Get all rules (defaults merged with custom overrides)."""
        merged = {action: dict(rule) for action, rule in AUTHORITY_DEFAULTS.items()}
        rows = await self.fetch("SELECT action, rule_json FROM authority_rules")
        for row in rows:
            merged[row["action"]] = json.loads(row["rule_json"])
        return merged
