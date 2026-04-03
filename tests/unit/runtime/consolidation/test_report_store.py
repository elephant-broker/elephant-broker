"""Tests for ConsolidationReportStore."""
import pytest

from elephantbroker.runtime.consolidation.report_store import ConsolidationReportStore
from elephantbroker.schemas.consolidation import ConsolidationReport, ConsolidationSummary


@pytest.fixture
async def store(tmp_path):
    s = ConsolidationReportStore(db_path=str(tmp_path / "reports.db"))
    await s.init_db()
    yield s
    await s.close()


class TestReportStore:
    async def test_save_and_get_round_trip(self, store):
        report = ConsolidationReport(org_id="org", gateway_id="gw", status="completed")
        await store.save_report(report)
        loaded = await store.get_report(report.id)
        assert loaded is not None
        assert loaded.org_id == "org"
        assert loaded.status == "completed"

    async def test_list_reports(self, store):
        for i in range(3):
            r = ConsolidationReport(org_id="org", gateway_id="gw", status="completed")
            await store.save_report(r)
        reports = await store.list_reports("gw", limit=2)
        assert len(reports) == 2

    async def test_missing_report_returns_none(self, store):
        assert await store.get_report("nonexistent") is None

    async def test_save_and_list_suggestions(self, store):
        await store.save_suggestion({
            "id": "s1", "report_id": "r1", "gateway_id": "gw",
            "pattern_description": "test", "tool_sequence": ["a", "b"],
            "sessions_observed": 3, "confidence": 0.8,
        })
        suggestions = await store.list_suggestions("gw")
        assert len(suggestions) == 1

    async def test_update_suggestion_status(self, store):
        await store.save_suggestion({
            "id": "s1", "report_id": "r1", "gateway_id": "gw",
            "pattern_description": "test", "tool_sequence": [],
            "sessions_observed": 1, "confidence": 0.5,
        })
        ok = await store.update_suggestion_status("s1", "approved")
        assert ok is True
        suggestions = await store.list_suggestions("gw", approval_status="approved")
        assert len(suggestions) == 1
