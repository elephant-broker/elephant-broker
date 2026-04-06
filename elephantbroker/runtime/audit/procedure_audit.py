"""ProcedureAuditStore — PostgreSQL audit trail for procedure compliance.

Table ``procedure_events`` is created by Alembic migration 0001_initial_schema.
"""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta

from elephantbroker.runtime.db.pg_store import PostgresStore

logger = logging.getLogger("elephantbroker.runtime.audit.procedure_audit")


class ProcedureAuditStore(PostgresStore):
    """Append-only PostgreSQL audit for procedure lifecycle events."""

    def __init__(self, enabled: bool = True) -> None:
        super().__init__()
        self._enabled = enabled

    async def record_event(
        self, session_key: str, session_id: str,
        procedure_id: str, procedure_name: str,
        event_type: str, *,
        execution_id: str | None = None,
        step_id: str | None = None,
        step_instruction: str | None = None,
        proof_type: str | None = None,
        proof_value: str | None = None,
        gateway_id: str = "local",
    ) -> None:
        if not self._enabled or not self._ready:
            return
        try:
            await self.execute(
                """INSERT INTO procedure_events
                   (event_id, session_key, session_id, procedure_id, procedure_name,
                    execution_id, event_type, step_id, step_instruction, proof_type,
                    proof_value, timestamp, gateway_id)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)""",
                str(uuid.uuid4()), session_key, session_id,
                procedure_id, procedure_name, execution_id,
                event_type, step_id, step_instruction,
                proof_type, proof_value,
                datetime.now(UTC).isoformat(), gateway_id,
            )
        except Exception as exc:
            logger.warning("Failed to record procedure audit event: %s", exc)

    async def get_session_events(self, session_key: str, session_id: str) -> list[dict]:
        if not self._enabled or not self._ready:
            return []
        return await self.fetch(
            "SELECT * FROM procedure_events WHERE session_key=$1 AND session_id=$2 ORDER BY timestamp",
            session_key, session_id,
        )

    async def get_procedure_events(self, procedure_id: str) -> list[dict]:
        if not self._enabled or not self._ready:
            return []
        return await self.fetch(
            "SELECT * FROM procedure_events WHERE procedure_id=$1 ORDER BY timestamp",
            procedure_id,
        )

    async def cleanup_old(self, retention_days: int = 90) -> int:
        """Delete events older than retention_days. Returns deleted count."""
        if not self._enabled or not self._ready:
            return 0
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
        try:
            status = await self.execute(
                "DELETE FROM procedure_events WHERE timestamp < $1", cutoff,
            )
            # asyncpg returns "DELETE N" — extract count
            return int(status.split()[-1]) if status else 0
        except Exception as exc:
            logger.warning("Failed to cleanup old procedure events: %s", exc)
            return 0
