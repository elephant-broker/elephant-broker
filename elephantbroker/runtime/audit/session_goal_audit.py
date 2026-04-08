"""SessionGoalAuditStore — SQLite audit trail for session goals."""
from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import UTC, datetime

logger = logging.getLogger("elephantbroker.runtime.audit.session_goal_audit")


class SessionGoalAuditStore:
    """Append-only SQLite audit for session goal lifecycle events."""

    def __init__(self, db_path: str = "data/session_goals_audit.db", enabled: bool = True) -> None:
        self._db_path = db_path
        self._enabled = enabled
        self._conn: sqlite3.Connection | None = None

    async def init_db(self) -> None:
        if not self._enabled:
            return
        import os
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute('''
            CREATE TABLE IF NOT EXISTS goal_events (
                event_id TEXT PRIMARY KEY,
                session_key TEXT NOT NULL,
                session_id TEXT NOT NULL,
                goal_id TEXT NOT NULL,
                goal_title TEXT NOT NULL,
                parent_goal_id TEXT,
                event_type TEXT NOT NULL,
                evidence TEXT,
                timestamp TEXT NOT NULL,
                gateway_id TEXT NOT NULL DEFAULT ''
            )
        ''')
        self._conn.commit()

    async def record_event(
        self, session_key: str, session_id: str,
        goal_id: str, goal_title: str,
        event_type: str, *,
        parent_goal_id: str | None = None,
        evidence: str | None = None,
        gateway_id: str = "",
    ) -> None:
        if not self._enabled or not self._conn:
            return
        try:
            self._conn.execute(
                '''INSERT INTO goal_events
                   (event_id, session_key, session_id, goal_id, goal_title,
                    parent_goal_id, event_type, evidence, timestamp, gateway_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    str(uuid.uuid4()), session_key, session_id,
                    goal_id, goal_title, parent_goal_id,
                    event_type, evidence,
                    datetime.now(UTC).isoformat(), gateway_id,
                ),
            )
            self._conn.commit()
        except Exception as exc:
            logger.warning("Failed to record goal audit event: %s", exc)

    async def get_session_events(self, session_key: str, session_id: str) -> list[dict]:
        if not self._enabled or not self._conn:
            return []
        cursor = self._conn.execute(
            'SELECT * FROM goal_events WHERE session_key=? AND session_id=? ORDER BY timestamp',
            (session_key, session_id),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    async def cleanup_old(self, retention_days: int = 90) -> int:
        """Delete events older than retention_days. Returns deleted count."""
        if not self._enabled or not self._conn:
            return 0
        from datetime import UTC as _UTC, timedelta
        from datetime import datetime as _dt
        cutoff = (_dt.now(_UTC) - timedelta(days=retention_days)).isoformat()
        try:
            cursor = self._conn.execute(
                "DELETE FROM goal_events WHERE timestamp < ?", (cutoff,),
            )
            self._conn.commit()
            return cursor.rowcount
        except Exception as exc:
            import logging
            logging.getLogger("elephantbroker.runtime.audit.session_goal_audit").warning(
                "Failed to cleanup old goal events: %s", exc,
            )
            return 0

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
