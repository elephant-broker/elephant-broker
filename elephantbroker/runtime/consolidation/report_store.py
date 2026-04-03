"""ConsolidationReportStore — SQLite persistence for consolidation reports and procedure suggestions."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import UTC, datetime, timedelta

from elephantbroker.schemas.consolidation import ConsolidationReport

logger = logging.getLogger("elephantbroker.runtime.consolidation.report_store")


class ConsolidationReportStore:
    """SQLite-backed store for consolidation reports and procedure suggestions."""

    def __init__(self, db_path: str = "data/consolidation_reports.db") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    async def init_db(self) -> None:
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS consolidation_reports (
                report_id TEXT PRIMARY KEY,
                org_id TEXT NOT NULL,
                gateway_id TEXT NOT NULL,
                profile_id TEXT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL DEFAULT 'running',
                summary_json TEXT,
                stages_json TEXT,
                error TEXT
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS procedure_suggestions (
                id TEXT PRIMARY KEY,
                report_id TEXT,
                gateway_id TEXT NOT NULL,
                pattern_description TEXT NOT NULL,
                tool_sequence_json TEXT NOT NULL,
                sessions_observed INTEGER NOT NULL DEFAULT 0,
                draft_procedure_json TEXT,
                confidence REAL NOT NULL DEFAULT 0.5,
                approval_status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL
            )
        """)
        self._conn.commit()

    async def save_report(self, report: ConsolidationReport) -> None:
        if not self._conn:
            return
        try:
            self._conn.execute(
                """INSERT OR REPLACE INTO consolidation_reports
                   (report_id, org_id, gateway_id, profile_id, started_at,
                    completed_at, status, summary_json, stages_json, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    report.id, report.org_id, report.gateway_id, report.profile_id,
                    report.started_at.isoformat(),
                    report.completed_at.isoformat() if report.completed_at else None,
                    report.status,
                    report.summary.model_dump_json() if report.summary else None,
                    json.dumps([sr.model_dump() for sr in report.stage_results]),
                    report.error,
                ),
            )
            self._conn.commit()
        except Exception as exc:
            logger.warning("Failed to save consolidation report: %s", exc)

    async def get_report(self, report_id: str) -> ConsolidationReport | None:
        if not self._conn:
            return None
        cursor = self._conn.execute(
            "SELECT * FROM consolidation_reports WHERE report_id = ?", (report_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cursor.description]
        data = dict(zip(cols, row))
        return _row_to_report(data)

    async def list_reports(self, gateway_id: str, limit: int = 10) -> list[ConsolidationReport]:
        if not self._conn:
            return []
        cursor = self._conn.execute(
            "SELECT * FROM consolidation_reports WHERE gateway_id = ? ORDER BY started_at DESC LIMIT ?",
            (gateway_id, limit),
        )
        cols = [d[0] for d in cursor.description]
        return [_row_to_report(dict(zip(cols, row))) for row in cursor.fetchall()]

    async def save_suggestion(self, suggestion_dict: dict) -> None:
        """Save a procedure suggestion from Stage 7."""
        if not self._conn:
            return
        try:
            self._conn.execute(
                """INSERT OR REPLACE INTO procedure_suggestions
                   (id, report_id, gateway_id, pattern_description, tool_sequence_json,
                    sessions_observed, draft_procedure_json, confidence, approval_status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(suggestion_dict.get("id", "")),
                    suggestion_dict.get("report_id", ""),
                    suggestion_dict.get("gateway_id", ""),
                    suggestion_dict.get("pattern_description", ""),
                    json.dumps(suggestion_dict.get("tool_sequence", [])),
                    suggestion_dict.get("sessions_observed", 0),
                    (json.dumps(suggestion_dict.get("draft_procedure"))
                     if suggestion_dict.get("draft_procedure") else None),
                    suggestion_dict.get("confidence", 0.5),
                    suggestion_dict.get("approval_status", "pending"),
                    suggestion_dict.get("created_at", datetime.now(UTC).isoformat()),
                ),
            )
            self._conn.commit()
        except Exception as exc:
            logger.warning("Failed to save procedure suggestion: %s", exc)

    async def list_suggestions(
        self, gateway_id: str, approval_status: str | None = None,
    ) -> list[dict]:
        if not self._conn:
            return []
        if approval_status:
            cursor = self._conn.execute(
                "SELECT * FROM procedure_suggestions "
                "WHERE gateway_id = ? AND approval_status = ? ORDER BY created_at DESC",
                (gateway_id, approval_status),
            )
        else:
            cursor = self._conn.execute(
                "SELECT * FROM procedure_suggestions WHERE gateway_id = ? ORDER BY created_at DESC",
                (gateway_id,),
            )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    async def update_suggestion_status(self, suggestion_id: str, status: str) -> bool:
        if not self._conn:
            return False
        try:
            cursor = self._conn.execute(
                "UPDATE procedure_suggestions SET approval_status = ? WHERE id = ?",
                (status, suggestion_id),
            )
            self._conn.commit()
            return cursor.rowcount > 0
        except Exception as exc:
            logger.warning("Failed to update suggestion status: %s", exc)
            return False

    async def cleanup_old(self, retention_days: int = 90) -> int:
        """Delete reports and suggestions older than retention_days. Returns deleted count."""
        if not self._conn:
            return 0
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
        try:
            c1 = self._conn.execute(
                "DELETE FROM consolidation_reports WHERE started_at < ?", (cutoff,),
            )
            c2 = self._conn.execute(
                "DELETE FROM procedure_suggestions WHERE created_at < ?", (cutoff,),
            )
            self._conn.commit()
            return c1.rowcount + c2.rowcount
        except Exception as exc:
            logger.warning("Failed to cleanup old reports: %s", exc)
            return 0

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


def _row_to_report(data: dict) -> ConsolidationReport:
    """Convert a database row dict to a ConsolidationReport."""
    from elephantbroker.schemas.consolidation import (
        ConsolidationSummary,
        StageResult,
    )

    summary = (
        ConsolidationSummary.model_validate_json(data["summary_json"])
        if data.get("summary_json")
        else ConsolidationSummary()
    )
    stages = []
    if data.get("stages_json"):
        for sr_data in json.loads(data["stages_json"]):
            stages.append(StageResult.model_validate(sr_data))

    return ConsolidationReport(
        id=data["report_id"],
        org_id=data["org_id"],
        gateway_id=data["gateway_id"],
        profile_id=data.get("profile_id"),
        started_at=datetime.fromisoformat(data["started_at"]),
        completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
        status=data["status"],
        summary=summary,
        stage_results=stages,
        error=data.get("error"),
    )
