"""Integration tests for multi-plugin dispatch scenarios."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from hitl_middleware.models import ApprovalIntent, NotificationIntent
from hitl_middleware.plugins.base import HitlPlugin
from hitl_middleware.plugins.registry import PluginRegistry


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _make_fake_plugin(*, notify_return: bool = True, approval_return: bool = True) -> HitlPlugin:
    """Create a fake plugin with mocked send methods (bypasses ABC enforcement)."""
    plugin = AsyncMock(spec=HitlPlugin)
    plugin.send_notification = AsyncMock(return_value=notify_return)
    plugin.send_approval_request = AsyncMock(return_value=approval_return)
    return plugin


def _make_notification() -> NotificationIntent:
    return NotificationIntent(
        guard_event_id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        action_summary="multi-plugin test",
    )


# ---------------------------------------------------------------------------
# 1. test_two_webhook_plugins_both_receive
# ---------------------------------------------------------------------------


async def test_two_webhook_plugins_both_receive():
    """Two registered plugins both receive dispatch_notification."""
    registry = PluginRegistry()
    p1 = _make_fake_plugin(notify_return=True)
    p2 = _make_fake_plugin(notify_return=True)
    registry.register(p1)
    registry.register(p2)

    intent = _make_notification()
    result = await registry.dispatch_notification(intent)

    assert result is True
    p1.send_notification.assert_awaited_once_with(intent)
    p2.send_notification.assert_awaited_once_with(intent)


# ---------------------------------------------------------------------------
# 2. test_mixed_results_any_true
# ---------------------------------------------------------------------------


async def test_mixed_results_any_true():
    """One plugin succeeds, one fails -- dispatch returns True (any-of semantics)."""
    registry = PluginRegistry()
    p_ok = _make_fake_plugin(notify_return=True)
    p_fail = _make_fake_plugin(notify_return=False)
    registry.register(p_ok)
    registry.register(p_fail)

    intent = _make_notification()
    result = await registry.dispatch_notification(intent)

    assert result is True
    p_ok.send_notification.assert_awaited_once()
    p_fail.send_notification.assert_awaited_once()
