"""SQLite-backed persistence for organization-specific profile overrides.

Follows the same pattern as ``ProcedureAuditStore`` and ``SessionGoalAuditStore``
from Phase 5: synchronous SQLite connection, async method wrappers, ``init_db()``
for table creation.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ValidationError

from elephantbroker.schemas.profile import ProfilePolicy

logger = logging.getLogger(__name__)


class OrgOverrideStore:
    """SQLite persistence for org-specific profile overrides.

    Table schema::

        org_profile_overrides (
            org_id TEXT,
            profile_id TEXT,
            overrides_json TEXT,
            updated_at TEXT,
            updated_by_actor_id TEXT,
            PRIMARY KEY (org_id, profile_id)
        )
    """

    def __init__(self, db_path: str = "data/org_overrides.db") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    async def init_db(self) -> None:
        """Create table if it doesn't exist."""
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS org_profile_overrides (
                org_id TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                overrides_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                updated_by_actor_id TEXT,
                PRIMARY KEY (org_id, profile_id)
            )"""
        )
        self._conn.commit()

    async def get_override(self, org_id: str, profile_id: str) -> dict[str, Any] | None:
        """Load override for org+profile. Returns None if no override registered."""
        if not self._conn:
            return None
        cursor = self._conn.execute(
            "SELECT overrides_json FROM org_profile_overrides WHERE org_id = ? AND profile_id = ?",
            (org_id, profile_id),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    async def set_override(
        self,
        org_id: str,
        profile_id: str,
        overrides: dict[str, Any],
        actor_id: str | None = None,
    ) -> None:
        """Upsert override with strict validation.

        Rejects:
        - Unknown top-level keys (must be ``ProfilePolicy`` fields)
        - Unknown nested keys (e.g., ``scoring_weights.nonexistent_field``)
        - Invalid types (e.g., ``scoring_weights.turn_relevance="not_a_float"``)

        Raises ``ValueError`` on validation failure.
        """
        if not self._conn:
            raise RuntimeError("OrgOverrideStore not initialized — call init_db() first")

        # Validate top-level keys
        for key in overrides:
            if key not in ProfilePolicy.model_fields:
                raise ValueError(f"Unknown override key: {key!r} (not a ProfilePolicy field)")

        # Validate nested keys and types by attempting partial construction
        for key, value in overrides.items():
            if isinstance(value, dict):
                field_type = ProfilePolicy.model_fields[key].annotation
                # Resolve the actual type for nested Pydantic models
                if isinstance(field_type, type) and issubclass(field_type, BaseModel):
                    for nk in value:
                        if nk not in field_type.model_fields:
                            raise ValueError(f"Unknown nested override key: {key}.{nk!r}")
                    try:
                        field_type.model_validate(field_type().model_dump() | value)
                    except ValidationError as exc:
                        raise ValueError(f"Invalid override value for {key}: {exc}") from exc

        now = datetime.now(UTC).isoformat()
        self._conn.execute(
            """INSERT INTO org_profile_overrides (org_id, profile_id, overrides_json, updated_at, updated_by_actor_id)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT (org_id, profile_id) DO UPDATE SET
                   overrides_json = excluded.overrides_json,
                   updated_at = excluded.updated_at,
                   updated_by_actor_id = excluded.updated_by_actor_id""",
            (org_id, profile_id, json.dumps(overrides), now, actor_id),
        )
        self._conn.commit()
        logger.info("Set org override for org=%s profile=%s (by actor=%s)", org_id, profile_id, actor_id)

    async def delete_override(self, org_id: str, profile_id: str) -> None:
        """Remove override for org+profile."""
        if not self._conn:
            return
        self._conn.execute(
            "DELETE FROM org_profile_overrides WHERE org_id = ? AND profile_id = ?",
            (org_id, profile_id),
        )
        self._conn.commit()
        logger.info("Deleted org override for org=%s profile=%s", org_id, profile_id)

    async def list_overrides(self, org_id: str) -> list[dict[str, Any]]:
        """List all overrides for an org."""
        if not self._conn:
            return []
        cursor = self._conn.execute(
            "SELECT profile_id, overrides_json, updated_at, updated_by_actor_id "
            "FROM org_profile_overrides WHERE org_id = ? ORDER BY profile_id",
            (org_id,),
        )
        return [
            {
                "profile_id": row[0],
                "overrides": json.loads(row[1]),
                "updated_at": row[2],
                "updated_by_actor_id": row[3],
            }
            for row in cursor.fetchall()
        ]

    async def close(self) -> None:
        """Close the SQLite connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
