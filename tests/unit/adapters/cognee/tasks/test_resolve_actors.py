"""Unit tests for resolve_actors task."""
from __future__ import annotations

import pytest

from elephantbroker.runtime.adapters.cognee.tasks.resolve_actors import resolve_actors
from elephantbroker.schemas.actor import ActorRef, ActorType


class TestResolveActors:
    async def test_exact_handle_match(self):
        """@username should match actor with that handle."""
        actor = ActorRef(
            type=ActorType.WORKER_AGENT, display_name="Bot One", handles=["bot1"],
        )
        messages = [{"role": "user", "content": "Hey @bot1, run the tests"}]
        result = await resolve_actors(messages, [actor])
        assert len(result) == 1
        assert result[0].id == actor.id

    async def test_case_insensitive_name(self):
        """Display name match should be case-insensitive."""
        actor = ActorRef(
            type=ActorType.MANAGER_AGENT, display_name="Alice Smith",
        )
        messages = [{"role": "user", "content": "I talked to alice smith yesterday"}]
        result = await resolve_actors(messages, [actor])
        assert len(result) == 1
        assert result[0].id == actor.id

    async def test_empty_messages(self):
        """Empty messages should return empty results."""
        actor = ActorRef(type=ActorType.WORKER_AGENT, display_name="bot")
        result = await resolve_actors([], [actor])
        assert result == []

    async def test_no_known_actors(self):
        """No known actors should return empty."""
        messages = [{"role": "user", "content": "Hey @someone"}]
        result = await resolve_actors(messages, [])
        assert result == []

    async def test_no_matches(self):
        """Messages without matching references return empty."""
        actor = ActorRef(
            type=ActorType.WORKER_AGENT, display_name="Specific Bot",
            handles=["specificbot"],
        )
        messages = [{"role": "user", "content": "No actors mentioned here"}]
        result = await resolve_actors(messages, [actor])
        assert result == []

    async def test_deduplication(self):
        """Same actor mentioned twice should appear once."""
        actor = ActorRef(
            type=ActorType.WORKER_AGENT, display_name="Bot One", handles=["bot1"],
        )
        messages = [
            {"role": "user", "content": "@bot1 please help"},
            {"role": "user", "content": "Bot One is great"},
        ]
        result = await resolve_actors(messages, [actor])
        assert len(result) == 1

    async def test_multiple_actors_resolved(self):
        """Multiple different actors can be resolved."""
        actor_a = ActorRef(
            type=ActorType.WORKER_AGENT, display_name="Alpha", handles=["alpha"],
        )
        actor_b = ActorRef(
            type=ActorType.MANAGER_AGENT, display_name="Beta", handles=["beta"],
        )
        messages = [{"role": "user", "content": "@alpha and @beta should coordinate"}]
        result = await resolve_actors(messages, [actor_a, actor_b])
        assert len(result) == 2

    async def test_handle_with_at_prefix(self):
        """Handle stored with @ prefix should still match."""
        actor = ActorRef(
            type=ActorType.WORKER_AGENT, display_name="Bot", handles=["@mybot"],
        )
        messages = [{"role": "user", "content": "Ask @mybot to do it"}]
        result = await resolve_actors(messages, [actor])
        assert len(result) == 1
