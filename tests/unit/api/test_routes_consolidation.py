"""Tests for consolidation API routes."""
from unittest.mock import AsyncMock, patch

from elephantbroker.schemas.consolidation import ConsolidationReport


class TestConsolidationRoutes:
    async def test_get_status_no_redis(self, client):
        resp = await client.get("/consolidation/status")
        assert resp.status_code == 200
        assert resp.json()["running"] is False

    async def test_list_reports_empty(self, client):
        resp = await client.get("/consolidation/reports")
        assert resp.status_code == 200

    async def test_list_suggestions_empty(self, client):
        resp = await client.get("/consolidation/suggestions")
        assert resp.status_code == 200

    async def test_run_consolidation_returns_200(self, client):
        report = ConsolidationReport(org_id="org", gateway_id="local", status="completed")
        with patch.object(
            client._transport.app.state.container.consolidation,
            "run_consolidation",
            new=AsyncMock(return_value=report),
        ):
            resp = await client.post("/consolidation/run", json={})
            assert resp.status_code == 200

    async def test_run_consolidation_409_when_locked(self, client):
        from elephantbroker.runtime.consolidation.engine import ConsolidationAlreadyRunningError

        with patch.object(
            client._transport.app.state.container.consolidation,
            "run_consolidation",
            new=AsyncMock(side_effect=ConsolidationAlreadyRunningError("gw")),
        ):
            resp = await client.post("/consolidation/run", json={})
            assert resp.status_code == 409
