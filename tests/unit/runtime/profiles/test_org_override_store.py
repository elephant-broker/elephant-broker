"""Tests for OrgOverrideStore (SQLite persistence)."""
import os
import tempfile

import pytest

from elephantbroker.runtime.profiles.org_override_store import OrgOverrideStore


@pytest.fixture
async def store():
    """Create a temporary OrgOverrideStore with in-memory-like temp file."""
    with tempfile.TemporaryDirectory() as tmp:
        s = OrgOverrideStore(db_path=os.path.join(tmp, "test_overrides.db"))
        await s.init_db()
        yield s
        await s.close()


class TestOrgOverrideStore:
    async def test_set_and_get_override(self, store):
        await store.set_override("acme", "coding", {"budgets": {"max_prompt_tokens": 10000}})
        result = await store.get_override("acme", "coding")
        assert result == {"budgets": {"max_prompt_tokens": 10000}}

    async def test_get_nonexistent_returns_none(self, store):
        result = await store.get_override("nope", "coding")
        assert result is None

    async def test_upsert_overwrites_existing(self, store):
        await store.set_override("acme", "coding", {"session_data_ttl_seconds": 7200})
        await store.set_override("acme", "coding", {"session_data_ttl_seconds": 14400})
        result = await store.get_override("acme", "coding")
        assert result == {"session_data_ttl_seconds": 14400}  # upserted value

    async def test_delete_override(self, store):
        await store.set_override("acme", "coding", {"session_data_ttl_seconds": 7200})
        await store.delete_override("acme", "coding")
        result = await store.get_override("acme", "coding")
        assert result is None

    async def test_list_overrides_for_org(self, store):
        await store.set_override("acme", "coding", {"session_data_ttl_seconds": 7200})
        await store.set_override("acme", "research", {"session_data_ttl_seconds": 14400})
        items = await store.list_overrides("acme")
        assert len(items) == 2
        profile_ids = [i["profile_id"] for i in items]
        assert "coding" in profile_ids
        assert "research" in profile_ids

    async def test_list_overrides_empty_org(self, store):
        items = await store.list_overrides("nope")
        assert items == []

    async def test_invalid_top_level_key_rejected(self, store):
        with pytest.raises(ValueError, match="Unknown override key"):
            await store.set_override("acme", "coding", {"nonexistent_field": 42})

    async def test_invalid_nested_key_rejected(self, store):
        with pytest.raises(ValueError, match="Unknown nested override key"):
            await store.set_override("acme", "coding", {"scoring_weights": {"nonexistent_dim": 0.5}})

    async def test_invalid_type_rejected(self, store):
        with pytest.raises(ValueError, match="Invalid override value"):
            await store.set_override("acme", "coding", {"scoring_weights": {"turn_relevance": "not_a_float"}})

    async def test_updated_at_and_actor_id_tracked(self, store):
        await store.set_override("acme", "coding", {"session_data_ttl_seconds": 7200}, actor_id="admin-uuid")
        items = await store.list_overrides("acme")
        assert len(items) == 1
        assert items[0]["updated_by_actor_id"] == "admin-uuid"
        assert items[0]["updated_at"] is not None
