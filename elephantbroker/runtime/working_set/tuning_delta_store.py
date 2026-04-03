"""TuningDeltaStore — SQLite persistence for per-profile per-gateway weight tuning deltas.

Keyed by (profile_id, org_id, gateway_id, dimension). Each consolidation cycle
upserts smoothed deltas. The ScoringTuner reads accumulated deltas to adjust
effective weights on top of base profile + org overrides.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import UTC, datetime

logger = logging.getLogger("elephantbroker.runtime.working_set.tuning_delta_store")


class TuningDeltaStore:
    """SQLite-backed store for scoring weight tuning deltas."""

    def __init__(self, db_path: str = "data/tuning_deltas.db") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    async def init_db(self) -> None:
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS tuning_deltas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id TEXT NOT NULL,
                org_id TEXT NOT NULL,
                gateway_id TEXT NOT NULL,
                dimension TEXT NOT NULL,
                accumulated_delta REAL NOT NULL DEFAULT 0.0,
                last_raw_delta REAL NOT NULL DEFAULT 0.0,
                cycle_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(profile_id, org_id, gateway_id, dimension)
            )
        """)
        self._conn.commit()

    async def get_deltas(
        self, profile_id: str, org_id: str, gateway_id: str,
    ) -> dict[str, float]:
        """Get accumulated deltas for a (profile, org, gateway) triple.

        Returns a dict mapping dimension name -> accumulated_delta.
        Empty dict if no tuning has been applied yet.
        """
        if not self._conn:
            return {}
        cursor = self._conn.execute(
            "SELECT dimension, accumulated_delta FROM tuning_deltas "
            "WHERE profile_id = ? AND org_id = ? AND gateway_id = ?",
            (profile_id, org_id, gateway_id),
        )
        return {row[0]: row[1] for row in cursor.fetchall()}

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
        if not self._conn:
            return
        now = datetime.now(UTC).isoformat()
        try:
            self._conn.execute(
                """INSERT INTO tuning_deltas
                   (profile_id, org_id, gateway_id, dimension,
                    accumulated_delta, last_raw_delta, cycle_count, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                   ON CONFLICT(profile_id, org_id, gateway_id, dimension)
                   DO UPDATE SET
                       accumulated_delta = ?,
                       last_raw_delta = ?,
                       cycle_count = cycle_count + 1,
                       updated_at = ?""",
                (
                    profile_id, org_id, gateway_id, dimension,
                    smoothed_delta, raw_delta, now,
                    smoothed_delta, raw_delta, now,
                ),
            )
            self._conn.commit()
        except Exception as exc:
            logger.warning("Failed to upsert tuning delta: %s", exc)

    async def clear_gateway(self, org_id: str, gateway_id: str) -> int:
        """Clear all tuning deltas for a gateway. Returns deleted count.

        Called via admin API: DELETE /admin/tuning/{gateway_id}
        Resets to pure base → profile → org_override weights.
        """
        if not self._conn:
            return 0
        try:
            cursor = self._conn.execute(
                "DELETE FROM tuning_deltas WHERE org_id = ? AND gateway_id = ?",
                (org_id, gateway_id),
            )
            self._conn.commit()
            return cursor.rowcount
        except Exception as exc:
            logger.warning("Failed to clear gateway tuning: %s", exc)
            return 0

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
