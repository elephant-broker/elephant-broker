"""Tests for procedure routes."""
import uuid
from unittest.mock import AsyncMock, MagicMock


class TestProcedureRoutes:
    async def test_create_procedure(self, client, monkeypatch, mock_add_data_points, mock_cognee):
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        body = {"name": "Test procedure", "description": "A test"}
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
        body = {"name": "Test proc"}
        r = await client.post("/procedures/", json=body)
        assert r.status_code == 500
