"""ScoringLedgerStore — PostgreSQL persistence for per-candidate scoring data.

Table ``scoring_ledger`` is created by Alembic migration 0001_initial_schema.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta

from elephantbroker.runtime.db.pg_store import PostgresStore

logger = logging.getLogger("elephantbroker.runtime.consolidation.scoring_ledger")


class ScoringLedgerStore(PostgresStore):
    """PostgreSQL-backed scoring ledger for Phase 9 weight tuning."""

    async def write_batch(self, entries: list[dict]) -> None:
        """Write a batch of scoring entries (one per scored candidate)."""
        if not self._ready or not entries:
            return
        try:
            await self.executemany(
                """INSERT INTO scoring_ledger
                   (fact_id, session_id, session_key, gateway_id, profile_id,
                    dim_scores_json, was_selected, successful_use_count_at_scoring, created_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
                [
                    (
                        e["fact_id"], e["session_id"], e["session_key"],
                        e["gateway_id"], e["profile_id"],
                        (e["dim_scores_json"] if isinstance(e["dim_scores_json"], str)
                         else json.dumps(e["dim_scores_json"])),
                        bool(e["was_selected"]),
                        e.get("successful_use_count_at_scoring", 0),
                        e.get("created_at", datetime.now(UTC).isoformat()),
                    )
                    for e in entries
                ],
            )
        except Exception as exc:
            logger.warning("Failed to write scoring ledger batch (%d entries): %s", len(entries), exc)

    async def query_for_correlation(
        self, gateway_id: str, cutoff_hours: int = 48,
    ) -> list[dict]:
        """Query ledger rows for Stage 9 correlation analysis."""
        if not self._ready:
            return []
        cutoff = (datetime.now(UTC) - timedelta(hours=cutoff_hours)).isoformat()
        rows = await self.fetch(
            "SELECT * FROM scoring_ledger WHERE gateway_id = $1 AND created_at > $2 ORDER BY created_at",
            gateway_id, cutoff,
        )
        for row in rows:
            if isinstance(row.get("dim_scores_json"), str):
                try:
                    row["dim_scores"] = json.loads(row["dim_scores_json"])
                except json.JSONDecodeError:
                    row["dim_scores"] = {}
        return rows

    async def cleanup_old(self, retention_seconds: int = 172800) -> int:
        """Delete entries older than retention_seconds. Returns deleted count."""
        if not self._ready:
            return 0
        cutoff = (datetime.now(UTC) - timedelta(seconds=retention_seconds)).isoformat()
        try:
            status = await self.execute(
                "DELETE FROM scoring_ledger WHERE created_at < $1", cutoff,
            )
            return int(status.split()[-1]) if status else 0
        except Exception as exc:
            logger.warning("Failed to cleanup scoring ledger: %s", exc)
            return 0
