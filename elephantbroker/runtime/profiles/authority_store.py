"""SQLite-backed persistence for configurable authority rules.

Each rule defines the minimum authority level for an action (e.g. create_org=90),
optional org/team matching requirements, and an exempt level that bypasses matching.
Rules are merged: custom overrides from SQLite take precedence over defaults.

Follows the same SQLite pattern as ``ProcedureAuditStore`` and ``OrgOverrideStore``.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

# Default authority rules — used when no custom override exists in SQLite.
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


class AuthorityRuleStore:
    """SQLite persistence for system-wide authority rules.

    Table schema::

        authority_rules (
            action TEXT PRIMARY KEY,
            rule_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """

    def __init__(self, db_path: str = "data/authority_rules.db") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    async def init_db(self) -> None:
        """Create table if it doesn't exist."""
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS authority_rules (
                action TEXT PRIMARY KEY,
                rule_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )
        self._conn.commit()

    async def get_rule(self, action: str) -> dict[str, Any]:
        """Get rule for an action. Custom override wins over default."""
        if self._conn:
            cursor = self._conn.execute(
                "SELECT rule_json FROM authority_rules WHERE action = ?", (action,)
            )
            row = cursor.fetchone()
            if row is not None:
                return json.loads(row[0])
        # Fall back to default
        default = AUTHORITY_DEFAULTS.get(action)
        if default is None:
            return {"min_authority_level": 90}  # unknown actions require system admin
        return dict(default)

    async def set_rule(self, action: str, rule: dict[str, Any]) -> None:
        """Upsert a custom authority rule."""
        if not self._conn:
            raise RuntimeError("AuthorityRuleStore not initialized — call init_db() first")
        now = datetime.now(UTC).isoformat()
        self._conn.execute(
            """INSERT INTO authority_rules (action, rule_json, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT (action) DO UPDATE SET
                   rule_json = excluded.rule_json,
                   updated_at = excluded.updated_at""",
            (action, json.dumps(rule), now),
        )
        self._conn.commit()
        logger.info("Authority rule updated: %s → %s", action, rule)

    async def get_rules(self) -> dict[str, dict[str, Any]]:
        """Get all rules (defaults merged with custom overrides)."""
        merged = {action: dict(rule) for action, rule in AUTHORITY_DEFAULTS.items()}
        if self._conn:
            cursor = self._conn.execute("SELECT action, rule_json FROM authority_rules")
            for row in cursor.fetchall():
                merged[row[0]] = json.loads(row[1])
        return merged

    async def close(self) -> None:
        """Close the SQLite connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
