"""Tests for the _authority.py check_authority helper."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from elephantbroker.api.routes._authority import BOOTSTRAP_ACTIONS, check_authority
from elephantbroker.schemas.actor import ActorRef, ActorType


def _make_actor(
    authority_level: int = 90,
    org_id: uuid.UUID | None = None,
    team_ids: list[uuid.UUID] | None = None,
) -> ActorRef:
    return ActorRef(
        type=ActorType.HUMAN_COORDINATOR,
        display_name="test-admin",
        authority_level=authority_level,
        org_id=org_id,
        team_ids=team_ids or [],
    )


def _mock_registry(actor: ActorRef | None = None) -> AsyncMock:
    reg = AsyncMock()
    reg.resolve_actor = AsyncMock(return_value=actor)
    return reg


def _mock_authority_store(rule: dict | None = None) -> AsyncMock:
    store = AsyncMock()
    store.get_rule = AsyncMock(return_value=rule or {"min_authority_level": 90})
    return store


# ---------------------------------------------------------------------------
# Bootstrap mode
# ---------------------------------------------------------------------------

class TestBootstrapMode:
    @pytest.mark.asyncio
    async def test_bootstrap_allows_bootstrap_actions(self):
        """In bootstrap mode, BOOTSTRAP_ACTIONS are allowed without actor resolution."""
        reg = _mock_registry()  # resolve_actor never called
        store = _mock_authority_store()

        for action in BOOTSTRAP_ACTIONS:
            result = await check_authority(
                reg, store, uuid.uuid4(), action, bootstrap_mode=True,
            )
            assert result.display_name == "bootstrap-admin"
            assert result.authority_level == 90
        # resolve_actor should NOT have been called
        reg.resolve_actor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bootstrap_does_not_bypass_non_bootstrap_action(self):
        """Non-bootstrap actions still require actor resolution even in bootstrap mode."""
        reg = _mock_registry(actor=None)
        store = _mock_authority_store()

        with pytest.raises(HTTPException) as exc_info:
            await check_authority(
                reg, store, uuid.uuid4(), "merge_actors", bootstrap_mode=True,
            )
        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# Normal authority checks
# ---------------------------------------------------------------------------

class TestNormalAuthority:
    @pytest.mark.asyncio
    async def test_sufficient_authority_passes(self):
        """Actor with authority >= min_level passes."""
        actor = _make_actor(authority_level=90)
        reg = _mock_registry(actor)
        store = _mock_authority_store({"min_authority_level": 70})

        result = await check_authority(reg, store, actor.id, "register_actor")
        assert result.id == actor.id

    @pytest.mark.asyncio
    async def test_insufficient_authority_raises_403(self):
        """Actor with authority < min_level gets 403."""
        actor = _make_actor(authority_level=30)
        reg = _mock_registry(actor)
        store = _mock_authority_store({"min_authority_level": 70})

        with pytest.raises(HTTPException) as exc_info:
            await check_authority(reg, store, actor.id, "register_actor")
        assert exc_info.value.status_code == 403
        assert "authority_level >= 70" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_actor_not_found_raises_404(self):
        """Unknown actor ID gets 404."""
        reg = _mock_registry(actor=None)
        store = _mock_authority_store()

        with pytest.raises(HTTPException) as exc_info:
            await check_authority(reg, store, uuid.uuid4(), "create_org")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_exempt_level_bypasses_org_matching(self):
        """Actor at or above matching_exempt_level skips org match check."""
        actor = _make_actor(authority_level=90, org_id=uuid.uuid4())
        reg = _mock_registry(actor)
        store = _mock_authority_store({
            "min_authority_level": 70,
            "require_matching_org": True,
            "matching_exempt_level": 90,
        })
        # target_org_id does NOT match actor.org_id — but exempt level saves it
        result = await check_authority(
            reg, store, actor.id, "create_team",
            target_org_id=str(uuid.uuid4()),
        )
        assert result.id == actor.id

    @pytest.mark.asyncio
    async def test_org_mismatch_raises_403(self):
        """Actor below exempt level with wrong org gets 403."""
        actor_org = uuid.uuid4()
        actor = _make_actor(authority_level=70, org_id=actor_org)
        reg = _mock_registry(actor)
        store = _mock_authority_store({
            "min_authority_level": 50,
            "require_matching_org": True,
            "matching_exempt_level": 90,
        })
        with pytest.raises(HTTPException) as exc_info:
            await check_authority(
                reg, store, actor.id, "create_team",
                target_org_id=str(uuid.uuid4()),  # different org
            )
        assert exc_info.value.status_code == 403
        assert "not in target org" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_team_mismatch_raises_403(self):
        """Actor not on target team gets 403 when require_matching_team is set."""
        actor = _make_actor(authority_level=60, team_ids=[uuid.uuid4()])
        reg = _mock_registry(actor)
        store = _mock_authority_store({
            "min_authority_level": 50,
            "require_matching_team": True,
            "matching_exempt_level": 90,
        })
        with pytest.raises(HTTPException) as exc_info:
            await check_authority(
                reg, store, actor.id, "add_team_member",
                target_team_id=str(uuid.uuid4()),  # different team
            )
        assert exc_info.value.status_code == 403
        assert "not on target team" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_team_match_succeeds(self):
        """Actor on target team passes the check."""
        team_id = uuid.uuid4()
        actor = _make_actor(authority_level=60, team_ids=[team_id])
        reg = _mock_registry(actor)
        store = _mock_authority_store({
            "min_authority_level": 50,
            "require_matching_team": True,
            "matching_exempt_level": 90,
        })
        result = await check_authority(
            reg, store, actor.id, "add_team_member",
            target_team_id=str(team_id),
        )
        assert result.id == actor.id
