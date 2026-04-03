"""Tests for ConsolidationEngine — Phase 9 full implementation."""
from elephantbroker.runtime.consolidation.engine import ConsolidationEngine
from elephantbroker.runtime.trace.ledger import TraceLedger


class TestConsolidationEngine:
    def _make(self):
        return ConsolidationEngine(TraceLedger())

    async def test_run_consolidation_returns_report(self):
        engine = self._make()
        result = await engine.run_consolidation("test_org", "test_gw")
        assert result.org_id == "test_org"
        assert result.gateway_id == "test_gw"
        assert result.status in ("completed", "failed", "partial")

    async def test_get_consolidation_report_none_without_store(self):
        engine = self._make()
        result = await engine.get_consolidation_report("nonexistent")
        assert result is None

    async def test_run_stage_returns_stage_result(self):
        engine = self._make()
        from elephantbroker.schemas.consolidation import ConsolidationContext
        ctx = ConsolidationContext(org_id="test", gateway_id="gw")
        result = await engine.run_stage(3, "test", "gw", ctx)
        assert result.stage == 3
