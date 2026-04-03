"""Tests for AuthorityRuleStore (SQLite persistence)."""
import os
import tempfile

import pytest

from elephantbroker.runtime.profiles.authority_store import AUTHORITY_DEFAULTS, AuthorityRuleStore


@pytest.fixture
async def store():
    with tempfile.TemporaryDirectory() as tmp:
        s = AuthorityRuleStore(db_path=os.path.join(tmp, "test_rules.db"))
        await s.init_db()
        yield s
        await s.close()


class TestAuthorityRuleStore:
    async def test_get_defaults_returns_all_11_rules(self, store):
        rules = await store.get_rules()
        assert len(rules) >= 11
        for action in AUTHORITY_DEFAULTS:
            assert action in rules

    async def test_set_custom_rule(self, store):
        await store.set_rule("create_org", {"min_authority_level": 80})
        rule = await store.get_rule("create_org")
        assert rule["min_authority_level"] == 80

    async def test_custom_rule_overrides_default(self, store):
        default = await store.get_rule("create_org")
        assert default["min_authority_level"] == 90  # default
        await store.set_rule("create_org", {"min_authority_level": 80})
        custom = await store.get_rule("create_org")
        assert custom["min_authority_level"] == 80

    async def test_unset_action_returns_default(self, store):
        rule = await store.get_rule("create_global_goal")
        assert rule == {"min_authority_level": 90}

    async def test_unknown_action_returns_system_admin(self, store):
        rule = await store.get_rule("unknown_action")
        assert rule["min_authority_level"] == 90

    async def test_list_all_rules_merges_defaults_and_custom(self, store):
        await store.set_rule("create_org", {"min_authority_level": 80})
        rules = await store.get_rules()
        assert rules["create_org"]["min_authority_level"] == 80
        assert rules["create_global_goal"]["min_authority_level"] == 90  # default preserved

    async def test_matching_exempt_level_stored(self, store):
        await store.set_rule("create_team", {"min_authority_level": 50, "matching_exempt_level": 70})
        rule = await store.get_rule("create_team")
        assert rule["matching_exempt_level"] == 70

    async def test_org_lifecycle_rules_present(self, store):
        rules = await store.get_rules()
        assert "create_org" in rules
        assert "create_team" in rules
        assert "add_team_member" in rules
        assert "remove_team_member" in rules

    async def test_merge_actors_rule_present(self, store):
        rule = await store.get_rule("merge_actors")
        assert rule["min_authority_level"] == 70

    async def test_set_rule_persists_across_instances(self, store):
        await store.set_rule("create_org", {"min_authority_level": 75})
        # Create new instance pointing to same DB
        store2 = AuthorityRuleStore(db_path=store._db_path)
        await store2.init_db()
        rule = await store2.get_rule("create_org")
        assert rule["min_authority_level"] == 75
        await store2.close()
