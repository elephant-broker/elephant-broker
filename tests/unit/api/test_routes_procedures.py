"""Tests for procedure routes."""
import uuid
from unittest.mock import AsyncMock, MagicMock


class TestProcedureRoutes:
    async def test_create_procedure(self, client, monkeypatch, mock_add_data_points, mock_cognee):
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        # #1146: must include is_manual_only or activation_modes per R2-P2.1
        body = {"name": "Test procedure", "description": "A test", "is_manual_only": True}
        r = await client.post("/procedures/", json=body)
        assert r.status_code == 200
        assert r.json()["name"] == "Test procedure"

    async def test_get_procedure(self, client):
        r = await client.get(f"/procedures/{uuid.uuid4()}")
        assert r.status_code == 200

    async def test_activate_procedure(self, client, mock_graph, monkeypatch, mock_add_data_points, mock_cognee):
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc_id = uuid.uuid4()
        mock_graph.get_entity.return_value = {"eb_id": str(proc_id), "name": "test"}
        body = {"actor_id": str(uuid.uuid4())}
        r = await client.post(f"/procedures/{proc_id}/activate", json=body)
        assert r.status_code == 200

    async def test_create_procedure_missing_name_422(self, client):
        r = await client.post("/procedures/", json={})
        assert r.status_code == 422

    async def test_create_procedure_when_procedures_disabled(self, client, container, monkeypatch, mock_add_data_points, mock_cognee):
        container.procedure_engine = None
        # #1146: must include is_manual_only or activation_modes per R2-P2.1
        body = {"name": "Test proc", "is_manual_only": True}
        r = await client.post("/procedures/", json=body)
        assert r.status_code == 500


class TestProcedureRouteToolMetrics:
    """Gap #8: inc_procedure_tool(tool) must fire on each procedure route."""

    async def test_create_emits_tool_metric(self, client, container, monkeypatch, mock_add_data_points, mock_cognee):
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        container.metrics_ctx.inc_procedure_tool = MagicMock()
        body = {"name": "Test procedure", "description": "A test", "is_manual_only": True}
        r = await client.post("/procedures/", json=body)
        assert r.status_code == 200
        container.metrics_ctx.inc_procedure_tool.assert_called_once_with("create")

    async def test_activate_emits_tool_metric(self, client, container, mock_graph, monkeypatch, mock_add_data_points, mock_cognee):
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc_id = uuid.uuid4()
        mock_graph.get_entity.return_value = {"eb_id": str(proc_id), "name": "test"}
        container.metrics_ctx.inc_procedure_tool = MagicMock()
        body = {"actor_id": str(uuid.uuid4())}
        r = await client.post(f"/procedures/{proc_id}/activate", json=body)
        assert r.status_code == 200
        container.metrics_ctx.inc_procedure_tool.assert_called_once_with("activate")

    async def test_complete_step_emits_tool_metric(self, client, container):
        container.metrics_ctx.inc_procedure_tool = MagicMock()
        eid = uuid.uuid4()
        sid = uuid.uuid4()
        r = await client.post(f"/procedures/{eid}/step/{sid}/complete", json={})
        # Step may return 200 with completed=False (execution not found) — that's fine,
        # the metric fires at route entry regardless
        container.metrics_ctx.inc_procedure_tool.assert_called_once_with("complete_step")

    async def test_session_status_emits_tool_metric(self, client, container):
        container.metrics_ctx.inc_procedure_tool = MagicMock()
        r = await client.get("/procedures/session/status")
        assert r.status_code == 200
        container.metrics_ctx.inc_procedure_tool.assert_called_once_with("session_status")
