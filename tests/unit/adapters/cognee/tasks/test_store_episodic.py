"""Unit tests for store_episodic task."""
from __future__ import annotations

from elephantbroker.runtime.adapters.cognee.tasks.store_episodic import store_episodic
from elephantbroker.schemas.fact import FactAssertion, FactCategory


class TestStoreEpisodic:
    async def test_stores_each_fact(self, monkeypatch, mock_add_data_points):
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.store_episodic.add_data_points",
            mock_add_data_points,
        )
        facts = [
            FactAssertion(text="fact 1", category=FactCategory.GENERAL),
            FactAssertion(text="fact 2", category=FactCategory.EVENT),
        ]
        ids = await store_episodic(facts)
        assert len(ids) == 2
        assert len(mock_add_data_points.calls) == 2

    async def test_empty_facts_returns_empty(self, monkeypatch, mock_add_data_points):
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.store_episodic.add_data_points",
            mock_add_data_points,
        )
        ids = await store_episodic([])
        assert ids == []

    async def test_returns_entity_ids(self, monkeypatch, mock_add_data_points):
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.store_episodic.add_data_points",
            mock_add_data_points,
        )
        facts = [FactAssertion(text="one", category=FactCategory.SYSTEM)]
        ids = await store_episodic(facts)
        assert len(ids) == 1
