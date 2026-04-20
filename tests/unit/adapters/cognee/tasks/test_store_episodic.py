"""Unit tests for store_episodic task."""
from __future__ import annotations

import logging

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

    # TODO-5-008 — Site 5 of cascade-pointer-wipe cluster
    async def test_emits_warning_for_stub_path(
        self, monkeypatch, mock_add_data_points, caplog,
    ):
        """The task is an orphaned stub with no graph adapter in scope.
        It cannot fetch an existing cognee_data_id, so it must explicitly
        pass None and log a WARNING so operators see the stub's
        pointer-wipe semantics if it is ever re-wired onto existing
        facts. See TODO-5-008 removal criteria (prefer facade.store)."""
        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.cognee.tasks.store_episodic.add_data_points",
            mock_add_data_points,
        )
        facts = [FactAssertion(text="stub path", category=FactCategory.GENERAL)]
        with caplog.at_level(
            logging.WARNING,
            logger="elephantbroker.runtime.adapters.cognee.tasks.store_episodic",
        ):
            await store_episodic(facts)

        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "stub path" in joined.lower() or "TODO-5-008" in joined
        assert "cognee_data_id" in joined
        # And the DP carried None explicitly (first-store default).
        stored_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert stored_dp.cognee_data_id is None
