"""Org override store — PostgreSQL-backed (migrated from SQLite).

Table ``org_profile_overrides`` is created by Alembic migration 0001_initial_schema.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ValidationError

from elephantbroker.runtime.db.pg_store import PostgresStore
from elephantbroker.schemas.profile import ProfilePolicy

logger = logging.getLogger(__name__)


class OrgOverrideStore(PostgresStore):
    """PostgreSQL persistence for org-specific profile overrides.

    Table schema (managed by Alembic)::

        org_profile_overrides (
            org_id TEXT NOT NULL,
            profile_id TEXT NOT NULL,
            overrides_json TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            updated_by_actor_id TEXT,
            PRIMARY KEY (org_id, profile_id)
        )
    """

    async def get_override(self, org_id: str, profile_id: str) -> dict[str, Any] | None:
        """Load override for org+profile. Returns None if no override registered."""
        row = await self.fetchrow(
            "SELECT overrides_json FROM org_profile_overrides WHERE org_id = $1 AND profile_id = $2",
            org_id, profile_id,
        )
        if row is None:
            return None
        return json.loads(row["overrides_json"])

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
        # Validate top-level keys
        for key in overrides:
            if key not in ProfilePolicy.model_fields:
                raise ValueError(f"Unknown override key: {key!r} (not a ProfilePolicy field)")

        # Validate nested keys and types by attempting partial construction
        for key, value in overrides.items():
            if isinstance(value, dict):
                field_type = ProfilePolicy.model_fields[key].annotation
                if isinstance(field_type, type) and issubclass(field_type, BaseModel):
                    for nk in value:
                        if nk not in field_type.model_fields:
                            raise ValueError(f"Unknown nested override key: {key}.{nk!r}")
                    try:
                        field_type.model_validate(field_type().model_dump() | value)
                    except ValidationError as exc:
                        raise ValueError(f"Invalid override value for {key}: {exc}") from exc

        now = datetime.now(UTC).isoformat()
        await self.execute(
            """INSERT INTO org_profile_overrides
                   (org_id, profile_id, overrides_json, updated_at, updated_by_actor_id)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (org_id, profile_id) DO UPDATE SET
                   overrides_json = EXCLUDED.overrides_json,
                   updated_at = EXCLUDED.updated_at,
                   updated_by_actor_id = EXCLUDED.updated_by_actor_id""",
            org_id, profile_id, json.dumps(overrides), now, actor_id,
        )
        logger.info("Set org override for org=%s profile=%s (by actor=%s)", org_id, profile_id, actor_id)

    async def delete_override(self, org_id: str, profile_id: str) -> None:
        """Remove override for org+profile."""
        await self.execute(
            "DELETE FROM org_profile_overrides WHERE org_id = $1 AND profile_id = $2",
            org_id, profile_id,
        )
        logger.info("Deleted org override for org=%s profile=%s", org_id, profile_id)

    async def list_overrides(self, org_id: str) -> list[dict[str, Any]]:
        """List all overrides for an org."""
        rows = await self.fetch(
            "SELECT profile_id, overrides_json, updated_at, updated_by_actor_id "
            "FROM org_profile_overrides WHERE org_id = $1 ORDER BY profile_id",
            org_id,
        )
        return [
            {
                "profile_id": row["profile_id"],
                "overrides": json.loads(row["overrides_json"]),
                "updated_at": row["updated_at"],
                "updated_by_actor_id": row["updated_by_actor_id"],
            }
            for row in rows
        ]
