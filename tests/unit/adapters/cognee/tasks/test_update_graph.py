"""Unit tests for update_graph task."""
from __future__ import annotations

from unittest.mock import MagicMock

from elephantbroker.runtime.adapters.cognee.tasks.update_graph import update_graph


class TestUpdateGraph:
    async def test_processes_all_datapoints(self, monkeypatch, mock_add_data_points):
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.update_graph.add_data_points",
            mock_add_data_points,
        )
        dps = [MagicMock(), MagicMock(), MagicMock()]
        count = await update_graph(dps)
        assert count == 3
        assert len(mock_add_data_points.calls) == 3

    async def test_empty_input_returns_zero(self, monkeypatch, mock_add_data_points):
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.update_graph.add_data_points",
            mock_add_data_points,
        )
        count = await update_graph([])
        assert count == 0

    async def test_returns_correct_count(self, monkeypatch, mock_add_data_points):
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.update_graph.add_data_points",
            mock_add_data_points,
        )
        count = await update_graph([MagicMock()])
        assert count == 1
