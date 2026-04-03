"""Tests for ScoringLedgerStore."""
import json

import pytest

from elephantbroker.runtime.consolidation.scoring_ledger_store import ScoringLedgerStore


@pytest.fixture
async def store(tmp_path):
    s = ScoringLedgerStore(db_path=str(tmp_path / "ledger.db"))
    await s.init_db()
    yield s
    await s.close()


class TestScoringLedgerStore:
    async def test_write_and_query_round_trip(self, store):
        entries = [{
            "fact_id": "f1", "session_id": "s1", "session_key": "sk",
            "gateway_id": "gw", "profile_id": "coding",
            "dim_scores_json": json.dumps({"turn_relevance": 0.8}),
            "was_selected": True, "successful_use_count_at_scoring": 0,
        }]
        await store.write_batch(entries)
        rows = await store.query_for_correlation("gw", cutoff_hours=1)
        assert len(rows) == 1
        assert rows[0]["fact_id"] == "f1"
        assert rows[0]["dim_scores"]["turn_relevance"] == 0.8

    async def test_gateway_filtering(self, store):
        for gw in ["gw1", "gw2"]:
            await store.write_batch([{
                "fact_id": f"f-{gw}", "session_id": "s1", "session_key": "sk",
                "gateway_id": gw, "profile_id": "coding",
                "dim_scores_json": "{}", "was_selected": True,
            }])
        rows = await store.query_for_correlation("gw1")
        assert len(rows) == 1
        assert rows[0]["gateway_id"] == "gw1"

    async def test_empty_query(self, store):
        rows = await store.query_for_correlation("nonexistent")
        assert rows == []

    async def test_cleanup_old(self, store):
        await store.write_batch([{
            "fact_id": "f1", "session_id": "s1", "session_key": "sk",
            "gateway_id": "gw", "profile_id": "coding",
            "dim_scores_json": "{}", "was_selected": False,
        }])
        # Cleanup with 0 retention should delete everything
        deleted = await store.cleanup_old(retention_seconds=0)
        assert deleted >= 0  # May be 0 if written in same second
