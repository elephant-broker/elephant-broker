"""Unit tests for hitl_middleware.plugins.base — 4 tests."""
from __future__ import annotations

import uuid

import pytest

from hitl_middleware.models import ApprovalIntent, NotificationIntent
from hitl_middleware.plugins.base import HitlPlugin


class TestHitlPluginABC:
    def test_abc_cannot_instantiate(self):
        """HitlPlugin is abstract and cannot be instantiated directly."""
        with pytest.raises(TypeError):
            HitlPlugin()

    def test_subclass_must_implement_methods(self):
        """Subclass without implementations cannot be instantiated."""

        class IncompletePlugin(HitlPlugin):
            pass

        with pytest.raises(TypeError):
            IncompletePlugin()

    async def test_close_default_is_noop(self):
        """Default close() implementation is a no-op coroutine."""

        class MinimalPlugin(HitlPlugin):
            async def send_notification(self, intent: NotificationIntent) -> bool:
                return True

            async def send_approval_request(self, intent: ApprovalIntent) -> bool:
                return True

        plugin = MinimalPlugin()
        # Should not raise
        await plugin.close()

    async def test_concrete_subclass_works(self):
        """A fully implemented subclass can be instantiated and called."""

        class ConcretePlugin(HitlPlugin):
            async def send_notification(self, intent: NotificationIntent) -> bool:
                return True

            async def send_approval_request(self, intent: ApprovalIntent) -> bool:
                return False

        plugin = ConcretePlugin()
        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4()
        )
        result = await plugin.send_notification(intent)
        assert result is True
