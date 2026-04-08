"""ProcedureAuditStore — SQLite audit trail for procedure compliance."""
from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import UTC, datetime

logger = logging.getLogger("elephantbroker.runtime.audit.procedure_audit")


class ProcedureAuditStore:
    """Append-only SQLite audit for procedure lifecycle events."""

    def __init__(self, db_path: str = "data/procedure_audit.db", enabled: bool = True) -> None:
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
            CREATE TABLE IF NOT EXISTS procedure_events (
                event_id TEXT PRIMARY KEY,
                session_key TEXT NOT NULL,
                session_id TEXT NOT NULL,
                procedure_id TEXT NOT NULL,
                procedure_name TEXT NOT NULL,
                execution_id TEXT,
                event_type TEXT NOT NULL,
                step_id TEXT,
                step_instruction TEXT,
                proof_type TEXT,
                proof_value TEXT,
                timestamp TEXT NOT NULL,
                gateway_id TEXT NOT NULL DEFAULT ''
            )
        ''')
        self._conn.commit()

    async def record_event(
        self, session_key: str, session_id: str,
        procedure_id: str, procedure_name: str,
        event_type: str, *,
        execution_id: str | None = None,
        step_id: str | None = None,
        step_instruction: str | None = None,
        proof_type: str | None = None,
        proof_value: str | None = None,
        gateway_id: str = "",
    ) -> None:
        if not self._enabled or not self._conn:
            return
        try:
            self._conn.execute(
                '''INSERT INTO procedure_events
                   (event_id, session_key, session_id, procedure_id, procedure_name,
                    execution_id, event_type, step_id, step_instruction, proof_type,
                    proof_value, timestamp, gateway_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    str(uuid.uuid4()), session_key, session_id,
                    procedure_id, procedure_name, execution_id,
                    event_type, step_id, step_instruction,
                    proof_type, proof_value,
                    datetime.now(UTC).isoformat(), gateway_id,
                ),
            )
            self._conn.commit()
        except Exception as exc:
            logger.warning("Failed to record procedure audit event: %s", exc)

    async def get_session_events(self, session_key: str, session_id: str) -> list[dict]:
        if not self._enabled or not self._conn:
            return []
        cursor = self._conn.execute(
            'SELECT * FROM procedure_events WHERE session_key=? AND session_id=? ORDER BY timestamp',
            (session_key, session_id),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    async def get_procedure_events(self, procedure_id: str) -> list[dict]:
        if not self._enabled or not self._conn:
            return []
        cursor = self._conn.execute(
            'SELECT * FROM procedure_events WHERE procedure_id=? ORDER BY timestamp',
            (procedure_id,),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    async def cleanup_old(self, retention_days: int = 90) -> int:
        """Delete events older than retention_days. Returns deleted count."""
        if not self._enabled or not self._conn:
            return 0
        from datetime import timedelta
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
        try:
            cursor = self._conn.execute(
                "DELETE FROM procedure_events WHERE timestamp < ?", (cutoff,),
            )
            self._conn.commit()
            return cursor.rowcount
        except Exception as exc:
            logger.warning("Failed to cleanup old procedure events: %s", exc)
            return 0

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
