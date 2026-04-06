"""ConsolidationReportStore — PostgreSQL persistence for consolidation reports.

Tables ``consolidation_reports`` and ``procedure_suggestions`` are created by
Alembic migration 0001_initial_schema.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta

from elephantbroker.runtime.db.pg_store import PostgresStore
from elephantbroker.schemas.consolidation import ConsolidationReport

logger = logging.getLogger("elephantbroker.runtime.consolidation.report_store")


class ConsolidationReportStore(PostgresStore):
    """PostgreSQL-backed store for consolidation reports and procedure suggestions."""

    async def save_report(self, report: ConsolidationReport) -> None:
        if not self._ready:
            return
        try:
            await self.execute(
                """INSERT INTO consolidation_reports
                   (report_id, org_id, gateway_id, profile_id, started_at,
                    completed_at, status, summary_json, stages_json, error)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                   ON CONFLICT (report_id) DO UPDATE SET
                       completed_at = EXCLUDED.completed_at,
                       status = EXCLUDED.status,
                       summary_json = EXCLUDED.summary_json,
                       stages_json = EXCLUDED.stages_json,
                       error = EXCLUDED.error""",
                report.id, report.org_id, report.gateway_id, report.profile_id,
                report.started_at.isoformat(),
                report.completed_at.isoformat() if report.completed_at else None,
                report.status,
                report.summary.model_dump_json() if report.summary else None,
                json.dumps([sr.model_dump() for sr in report.stage_results]),
                report.error,
            )
        except Exception as exc:
            logger.warning("Failed to save consolidation report: %s", exc)

    async def get_report(self, report_id: str) -> ConsolidationReport | None:
        if not self._ready:
            return None
        row = await self.fetchrow(
            "SELECT * FROM consolidation_reports WHERE report_id = $1", report_id,
        )
        if not row:
            return None
        return _row_to_report(dict(row))

    async def list_reports(self, gateway_id: str, limit: int = 10) -> list[ConsolidationReport]:
        if not self._ready:
            return []
        rows = await self.fetch(
            "SELECT * FROM consolidation_reports WHERE gateway_id = $1 ORDER BY started_at DESC LIMIT $2",
            gateway_id, limit,
        )
        return [_row_to_report(dict(row)) for row in rows]

    async def save_suggestion(self, suggestion_dict: dict) -> None:
        """Save a procedure suggestion from Stage 7."""
        if not self._ready:
            return
        try:
            await self.execute(
                """INSERT INTO procedure_suggestions
                   (id, report_id, gateway_id, pattern_description, tool_sequence_json,
                    sessions_observed, draft_procedure_json, confidence, approval_status, created_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                   ON CONFLICT (id) DO UPDATE SET
                       approval_status = EXCLUDED.approval_status""",
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
            )
        except Exception as exc:
            logger.warning("Failed to save procedure suggestion: %s", exc)

    async def list_suggestions(
        self, gateway_id: str, approval_status: str | None = None,
    ) -> list[dict]:
        if not self._ready:
            return []
        if approval_status:
            return await self.fetch(
                "SELECT * FROM procedure_suggestions "
                "WHERE gateway_id = $1 AND approval_status = $2 ORDER BY created_at DESC",
                gateway_id, approval_status,
            )
        return await self.fetch(
            "SELECT * FROM procedure_suggestions WHERE gateway_id = $1 ORDER BY created_at DESC",
            gateway_id,
        )

    async def update_suggestion_status(self, suggestion_id: str, status: str) -> bool:
        if not self._ready:
            return False
        try:
            result = await self.execute(
                "UPDATE procedure_suggestions SET approval_status = $1 WHERE id = $2",
                status, suggestion_id,
            )
            return result.endswith("1")
        except Exception as exc:
            logger.warning("Failed to update suggestion status: %s", exc)
            return False

    async def cleanup_old(self, retention_days: int = 90) -> int:
        """Delete reports and suggestions older than retention_days. Returns deleted count."""
        if not self._ready:
            return 0
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
        try:
            s1 = await self.execute(
                "DELETE FROM consolidation_reports WHERE started_at < $1", cutoff,
            )
            s2 = await self.execute(
                "DELETE FROM procedure_suggestions WHERE created_at < $1", cutoff,
            )
            c1 = int(s1.split()[-1]) if s1 else 0
            c2 = int(s2.split()[-1]) if s2 else 0
            return c1 + c2
        except Exception as exc:
            logger.warning("Failed to cleanup old reports: %s", exc)
            return 0


def _row_to_report(data: dict) -> ConsolidationReport:
    """Convert a database row dict to a ConsolidationReport."""
    from elephantbroker.schemas.consolidation import ConsolidationSummary, StageResult

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
