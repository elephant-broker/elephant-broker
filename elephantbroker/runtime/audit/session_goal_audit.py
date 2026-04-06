"""SessionGoalAuditStore — PostgreSQL audit trail for session goals.

Table ``goal_events`` is created by Alembic migration 0001_initial_schema.
"""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta

from elephantbroker.runtime.db.pg_store import PostgresStore

logger = logging.getLogger("elephantbroker.runtime.audit.session_goal_audit")


class SessionGoalAuditStore(PostgresStore):
    """Append-only PostgreSQL audit for session goal lifecycle events."""

    def __init__(self, enabled: bool = True) -> None:
        super().__init__()
        self._enabled = enabled

    async def record_event(
        self, session_key: str, session_id: str,
        goal_id: str, goal_title: str,
        event_type: str, *,
        parent_goal_id: str | None = None,
        evidence: str | None = None,
        gateway_id: str = "local",
    ) -> None:
        if not self._enabled or not self._ready:
            return
        try:
            await self.execute(
                """INSERT INTO goal_events
                   (event_id, session_key, session_id, goal_id, goal_title,
                    parent_goal_id, event_type, evidence, timestamp, gateway_id)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)""",
                str(uuid.uuid4()), session_key, session_id,
                goal_id, goal_title, parent_goal_id,
                event_type, evidence,
                datetime.now(UTC).isoformat(), gateway_id,
            )
        except Exception as exc:
            logger.warning("Failed to record goal audit event: %s", exc)

    async def get_session_events(self, session_key: str, session_id: str) -> list[dict]:
        if not self._enabled or not self._ready:
            return []
        return await self.fetch(
            "SELECT * FROM goal_events WHERE session_key=$1 AND session_id=$2 ORDER BY timestamp",
            session_key, session_id,
        )

    async def cleanup_old(self, retention_days: int = 90) -> int:
        """Delete events older than retention_days. Returns deleted count."""
        if not self._enabled or not self._ready:
            return 0
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
        try:
            status = await self.execute(
                "DELETE FROM goal_events WHERE timestamp < $1", cutoff,
            )
            return int(status.split()[-1]) if status else 0
        except Exception as exc:
            logger.warning("Failed to cleanup old goal events: %s", exc)
            return 0
