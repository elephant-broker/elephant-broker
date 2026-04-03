"""ScoringLedgerStore — SQLite persistence for per-candidate scoring data.

WorkingSetManager writes one row per scored candidate after each build_working_set()
call. Stage 9 (Recompute Salience) reads these rows and joins with current
successful_use_count to correlate scoring dimensions with outcome.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import UTC, datetime, timedelta

logger = logging.getLogger("elephantbroker.runtime.consolidation.scoring_ledger")


class ScoringLedgerStore:
    """SQLite-backed scoring ledger for Phase 9 weight tuning."""

    def __init__(self, db_path: str = "data/scoring_ledger.db") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    async def init_db(self) -> None:
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS scoring_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fact_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                session_key TEXT NOT NULL,
                gateway_id TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                dim_scores_json TEXT NOT NULL,
                was_selected BOOLEAN NOT NULL,
                successful_use_count_at_scoring INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_scoring_ledger_gw ON scoring_ledger (gateway_id, created_at)"
        )
        self._conn.commit()

    async def write_batch(self, entries: list[dict]) -> None:
        """Write a batch of scoring entries (one per scored candidate)."""
        if not self._conn or not entries:
            return
        try:
            self._conn.executemany(
                """INSERT INTO scoring_ledger
                   (fact_id, session_id, session_key, gateway_id, profile_id,
                    dim_scores_json, was_selected, successful_use_count_at_scoring, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
            self._conn.commit()
        except Exception as exc:
            logger.warning("Failed to write scoring ledger batch (%d entries): %s", len(entries), exc)

    async def query_for_correlation(
        self, gateway_id: str, cutoff_hours: int = 48,
    ) -> list[dict]:
        """Query ledger rows for Stage 9 correlation analysis."""
        if not self._conn:
            return []
        cutoff = (datetime.now(UTC) - timedelta(hours=cutoff_hours)).isoformat()
        cursor = self._conn.execute(
            "SELECT * FROM scoring_ledger WHERE gateway_id = ? AND created_at > ? ORDER BY created_at",
            (gateway_id, cutoff),
        )
        cols = [d[0] for d in cursor.description]
        rows = []
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            # Parse JSON scores
            if isinstance(d.get("dim_scores_json"), str):
                try:
                    d["dim_scores"] = json.loads(d["dim_scores_json"])
                except json.JSONDecodeError:
                    d["dim_scores"] = {}
            rows.append(d)
        return rows

    async def cleanup_old(self, retention_seconds: int = 172800) -> int:
        """Delete entries older than retention_seconds. Returns deleted count."""
        if not self._conn:
            return 0
        cutoff = (datetime.now(UTC) - timedelta(seconds=retention_seconds)).isoformat()
        try:
            cursor = self._conn.execute(
                "DELETE FROM scoring_ledger WHERE created_at < ?", (cutoff,),
            )
            self._conn.commit()
            return cursor.rowcount
        except Exception as exc:
            logger.warning("Failed to cleanup scoring ledger: %s", exc)
            return 0

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
