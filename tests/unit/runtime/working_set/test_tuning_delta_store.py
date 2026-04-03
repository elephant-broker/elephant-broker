"""Tests for TuningDeltaStore."""
import pytest

from elephantbroker.runtime.working_set.tuning_delta_store import TuningDeltaStore


@pytest.fixture
async def store(tmp_path):
    s = TuningDeltaStore(db_path=str(tmp_path / "deltas.db"))
    await s.init_db()
    yield s
    await s.close()


class TestTuningDeltaStore:
    async def test_get_empty_deltas(self, store):
        deltas = await store.get_deltas("coding", "org1", "gw1")
        assert deltas == {}

    async def test_upsert_creates_row(self, store):
        await store.upsert_delta("coding", "org1", "gw1", "turn_relevance", 0.03, 0.05)
        deltas = await store.get_deltas("coding", "org1", "gw1")
        assert "turn_relevance" in deltas
        assert abs(deltas["turn_relevance"] - 0.03) < 0.001

    async def test_upsert_accumulates(self, store):
        await store.upsert_delta("coding", "org1", "gw1", "recency", 0.01, 0.02)
        await store.upsert_delta("coding", "org1", "gw1", "recency", 0.02, 0.03)
        deltas = await store.get_deltas("coding", "org1", "gw1")
        assert abs(deltas["recency"] - 0.02) < 0.001  # Latest smoothed value

    async def test_clear_gateway(self, store):
        await store.upsert_delta("coding", "org1", "gw1", "turn_relevance", 0.05, 0.05)
        await store.upsert_delta("coding", "org1", "gw1", "recency", 0.02, 0.02)
        deleted = await store.clear_gateway("org1", "gw1")
        assert deleted == 2
        assert await store.get_deltas("coding", "org1", "gw1") == {}

    async def test_gateway_isolation(self, store):
        await store.upsert_delta("coding", "org1", "gw1", "turn_relevance", 0.05, 0.05)
        await store.upsert_delta("coding", "org1", "gw2", "turn_relevance", 0.10, 0.10)
        d1 = await store.get_deltas("coding", "org1", "gw1")
        d2 = await store.get_deltas("coding", "org1", "gw2")
        assert abs(d1["turn_relevance"] - 0.05) < 0.001
        assert abs(d2["turn_relevance"] - 0.10) < 0.001
