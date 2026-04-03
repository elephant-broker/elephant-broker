"""Unit tests for index_procedures task."""
from __future__ import annotations

from elephantbroker.runtime.adapters.cognee.tasks.index_procedures import index_procedures
from elephantbroker.schemas.procedure import ProcedureDefinition


class TestIndexProcedures:
    async def test_indexes_each_procedure(self, monkeypatch, mock_add_data_points):
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.index_procedures.add_data_points",
            mock_add_data_points,
        )
        procs = [
            ProcedureDefinition(name="Deploy"),
            ProcedureDefinition(name="Rollback"),
        ]
        ids = await index_procedures(procs)
        assert len(ids) == 2
        assert len(mock_add_data_points.calls) == 2

    async def test_empty_input_returns_empty(self, monkeypatch, mock_add_data_points):
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.index_procedures.add_data_points",
            mock_add_data_points,
        )
        ids = await index_procedures([])
        assert ids == []

    async def test_returns_entity_ids(self, monkeypatch, mock_add_data_points):
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.index_procedures.add_data_points",
            mock_add_data_points,
        )
        procs = [ProcedureDefinition(name="Test")]
        ids = await index_procedures(procs)
        assert len(ids) == 1
        assert ids[0] == str(procs[0].id)
