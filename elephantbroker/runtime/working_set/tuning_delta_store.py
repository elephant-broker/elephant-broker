"""TuningDeltaStore — PostgreSQL persistence for per-profile per-gateway weight tuning deltas.

Table ``tuning_deltas`` is created by Alembic migration 0001_initial_schema.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime

from elephantbroker.runtime.db.pg_store import PostgresStore

logger = logging.getLogger("elephantbroker.runtime.working_set.tuning_delta_store")


class TuningDeltaStore(PostgresStore):
    """PostgreSQL-backed store for scoring weight tuning deltas."""

    async def get_deltas(
        self, profile_id: str, org_id: str, gateway_id: str,
    ) -> dict[str, float]:
        """Get accumulated deltas for a (profile, org, gateway) triple."""
        if not self._ready:
            return {}
        rows = await self.fetch(
            "SELECT dimension, accumulated_delta FROM tuning_deltas "
            "WHERE profile_id = $1 AND org_id = $2 AND gateway_id = $3",
            profile_id, org_id, gateway_id,
        )
        return {row["dimension"]: row["accumulated_delta"] for row in rows}

    async def upsert_delta(
        self,
        profile_id: str,
        org_id: str,
        gateway_id: str,
        dimension: str,
        smoothed_delta: float,
        raw_delta: float,
    ) -> None:
        """Insert or update a tuning delta. Increments cycle_count."""
        if not self._ready:
            return
        now = datetime.now(UTC).isoformat()
        try:
            await self.execute(
                """INSERT INTO tuning_deltas
                   (profile_id, org_id, gateway_id, dimension,
                    accumulated_delta, last_raw_delta, cycle_count, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6, 1, $7)
                   ON CONFLICT (profile_id, org_id, gateway_id, dimension) DO UPDATE SET
                       accumulated_delta = $5,
                       last_raw_delta = $6,
                       cycle_count = tuning_deltas.cycle_count + 1,
                       updated_at = $7""",
                profile_id, org_id, gateway_id, dimension,
                smoothed_delta, raw_delta, now,
            )
        except Exception as exc:
            logger.warning("Failed to upsert tuning delta: %s", exc)

    async def clear_gateway(self, org_id: str, gateway_id: str) -> int:
        """Clear all tuning deltas for a gateway. Returns deleted count."""
        if not self._ready:
            return 0
        try:
            status = await self.execute(
                "DELETE FROM tuning_deltas WHERE org_id = $1 AND gateway_id = $2",
                org_id, gateway_id,
            )
            return int(status.split()[-1]) if status else 0
        except Exception as exc:
            logger.warning("Failed to clear gateway tuning: %s", exc)
            return 0
