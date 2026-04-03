"""Unit tests for hitl_middleware.plugins.registry — 8 tests."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from hitl_middleware.models import ApprovalIntent, NotificationIntent
from hitl_middleware.plugins.base import HitlPlugin
from hitl_middleware.plugins.registry import PluginRegistry


def _make_mock_plugin(notify_return=True, approval_return=True) -> HitlPlugin:
    """Create a mock plugin that returns specified values."""
    plugin = AsyncMock(spec=HitlPlugin)
    plugin.send_notification = AsyncMock(return_value=notify_return)
    plugin.send_approval_request = AsyncMock(return_value=approval_return)
    plugin.close = AsyncMock()
    return plugin


def _notification_intent() -> NotificationIntent:
    return NotificationIntent(guard_event_id=uuid.uuid4(), session_id=uuid.uuid4())


def _approval_intent() -> ApprovalIntent:
    return ApprovalIntent(
        request_id=uuid.uuid4(),
        guard_event_id=uuid.uuid4(),
        session_id=uuid.uuid4(),
    )


class TestPluginRegistry:
    def test_register_adds_plugin(self):
        """register() increments plugin_count."""
        registry = PluginRegistry()
        assert registry.plugin_count == 0
        registry.register(_make_mock_plugin())
        assert registry.plugin_count == 1

    async def test_dispatch_notification_fans_out(self):
        """dispatch_notification calls all registered plugins."""
        registry = PluginRegistry()
        p1 = _make_mock_plugin()
        p2 = _make_mock_plugin()
        registry.register(p1)
        registry.register(p2)

        result = await registry.dispatch_notification(_notification_intent())
        assert result is True
        p1.send_notification.assert_awaited_once()
        p2.send_notification.assert_awaited_once()

    async def test_partial_failure(self):
        """If one plugin fails, others still get called; returns True if any succeed."""
        registry = PluginRegistry()
        failing = _make_mock_plugin()
        failing.send_notification = AsyncMock(side_effect=RuntimeError("fail"))
        succeeding = _make_mock_plugin(notify_return=True)
        registry.register(failing)
        registry.register(succeeding)

        result = await registry.dispatch_notification(_notification_intent())
        assert result is True
        succeeding.send_notification.assert_awaited_once()

    async def test_dispatch_approval_fans_out(self):
        """dispatch_approval calls all registered plugins."""
        registry = PluginRegistry()
        p1 = _make_mock_plugin()
        p2 = _make_mock_plugin()
        registry.register(p1)
        registry.register(p2)

        result = await registry.dispatch_approval(_approval_intent())
        assert result is True
        p1.send_approval_request.assert_awaited_once()
        p2.send_approval_request.assert_awaited_once()

    async def test_all_fail_returns_false(self):
        """If all plugins return False, dispatch returns False."""
        registry = PluginRegistry()
        registry.register(_make_mock_plugin(notify_return=False))
        registry.register(_make_mock_plugin(notify_return=False))

        result = await registry.dispatch_notification(_notification_intent())
        assert result is False

    async def test_any_success_returns_true(self):
        """If at least one plugin succeeds, returns True."""
        registry = PluginRegistry()
        registry.register(_make_mock_plugin(notify_return=False))
        registry.register(_make_mock_plugin(notify_return=True))

        result = await registry.dispatch_notification(_notification_intent())
        assert result is True

    async def test_close_closes_all(self):
        """close() calls close() on every plugin."""
        registry = PluginRegistry()
        p1 = _make_mock_plugin()
        p2 = _make_mock_plugin()
        registry.register(p1)
        registry.register(p2)

        await registry.close()
        p1.close.assert_awaited_once()
        p2.close.assert_awaited_once()

    async def test_empty_registry_returns_false(self):
        """dispatch on empty registry returns False."""
        registry = PluginRegistry()
        result = await registry.dispatch_notification(_notification_intent())
        assert result is False
        result2 = await registry.dispatch_approval(_approval_intent())
        assert result2 is False
