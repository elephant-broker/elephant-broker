"""Tests for the authority check helper."""
import os
import tempfile
import uuid

import pytest
from unittest.mock import AsyncMock

from elephantbroker.api.routes._authority import check_authority
from elephantbroker.runtime.profiles.authority_store import AuthorityRuleStore
from elephantbroker.schemas.actor import ActorRef, ActorType


@pytest.fixture
async def auth_store():
    with tempfile.TemporaryDirectory() as tmp:
        s = AuthorityRuleStore(db_path=os.path.join(tmp, "test_auth.db"))
        await s.init_db()
        yield s
        await s.close()


def _mock_registry(actor: ActorRef | None = None):
    reg = AsyncMock()
    reg.resolve_actor = AsyncMock(return_value=actor)
    return reg


def _actor(authority: int = 0, org_id: str | None = None, team_ids: list | None = None) -> ActorRef:
    return ActorRef(
        type=ActorType.HUMAN_COORDINATOR,
        display_name="test",
        authority_level=authority,
        org_id=uuid.UUID(org_id) if org_id else None,
        team_ids=[uuid.UUID(t) for t in (team_ids or [])],
    )


class TestAuthorityChecks:
    async def test_system_admin_can_create_global_goal(self, auth_store):
        actor = _actor(authority=90)
        reg = _mock_registry(actor)
        result = await check_authority(reg, auth_store, actor.id, "create_global_goal")
        assert result.authority_level == 90

    async def test_org_admin_can_create_org_goal(self, auth_store):
        org = str(uuid.uuid4())
        actor = _actor(authority=70, org_id=org)
        reg = _mock_registry(actor)
        result = await check_authority(reg, auth_store, actor.id, "create_org_goal", target_org_id=org)
        assert result.authority_level == 70

    async def test_org_admin_cannot_create_goal_in_other_org(self, auth_store):
        actor = _actor(authority=70, org_id=str(uuid.uuid4()))
        reg = _mock_registry(actor)
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await check_authority(reg, auth_store, actor.id, "create_org_goal", target_org_id=str(uuid.uuid4()))
        assert exc_info.value.status_code == 403

    async def test_system_admin_exempt_from_org_matching(self, auth_store):
        actor = _actor(authority=90, org_id=str(uuid.uuid4()))
        reg = _mock_registry(actor)
        # Different org but authority >= matching_exempt_level (90)
        result = await check_authority(reg, auth_store, actor.id, "create_org_goal", target_org_id=str(uuid.uuid4()))
        assert result.authority_level == 90

    async def test_team_lead_can_create_team_goal(self, auth_store):
        team = str(uuid.uuid4())
        actor = _actor(authority=50, team_ids=[team])
        reg = _mock_registry(actor)
        result = await check_authority(reg, auth_store, actor.id, "create_team_goal", target_team_id=team)
        assert result.authority_level == 50

    async def test_team_lead_cannot_create_org_goal(self, auth_store):
        actor = _actor(authority=50)
        reg = _mock_registry(actor)
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await check_authority(reg, auth_store, actor.id, "create_org_goal")
        assert exc_info.value.status_code == 403

    async def test_regular_actor_can_create_actor_goal(self, auth_store):
        actor = _actor(authority=0)
        reg = _mock_registry(actor)
        result = await check_authority(reg, auth_store, actor.id, "create_actor_goal")
        assert result.authority_level == 0

    async def test_regular_actor_cannot_create_team_goal(self, auth_store):
        actor = _actor(authority=10)
        reg = _mock_registry(actor)
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await check_authority(reg, auth_store, actor.id, "create_team_goal")
        assert exc_info.value.status_code == 403

    async def test_unknown_actor_returns_404(self, auth_store):
        reg = _mock_registry(None)  # actor not found
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await check_authority(reg, auth_store, uuid.uuid4(), "create_org")
        assert exc_info.value.status_code == 404

    async def test_agent_default_authority_is_zero(self, auth_store):
        actor = _actor(authority=0)
        reg = _mock_registry(actor)
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await check_authority(reg, auth_store, actor.id, "register_actor")
        assert exc_info.value.status_code == 403

    async def test_bootstrap_mode_allows_create_org(self, auth_store):
        reg = _mock_registry(None)  # doesn't matter
        result = await check_authority(reg, auth_store, uuid.uuid4(), "create_org", bootstrap_mode=True)
        assert result.authority_level == 90
        assert result.display_name == "bootstrap-admin"

    async def test_bootstrap_mode_not_for_non_bootstrap_actions(self, auth_store):
        reg = _mock_registry(None)
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await check_authority(reg, auth_store, uuid.uuid4(), "merge_actors", bootstrap_mode=True)
        assert exc_info.value.status_code == 404  # actor not found (not a bootstrap action)

    async def test_org_admin_exempt_from_team_matching(self, auth_store):
        team = str(uuid.uuid4())
        other_team = str(uuid.uuid4())
        # Authority 70 >= matching_exempt_level for add_team_member (70)
        actor = _actor(authority=70, team_ids=[team])
        reg = _mock_registry(actor)
        result = await check_authority(reg, auth_store, actor.id, "add_team_member", target_team_id=other_team)
        assert result.authority_level == 70

    async def test_custom_authority_rule_applied(self, auth_store):
        await auth_store.set_rule("create_org", {"min_authority_level": 50})
        actor = _actor(authority=50)
        reg = _mock_registry(actor)
        result = await check_authority(reg, auth_store, actor.id, "create_org")
        assert result.authority_level == 50
