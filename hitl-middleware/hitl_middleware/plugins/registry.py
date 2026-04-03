"""Plugin registry for HITL Middleware — dispatches intents to all registered plugins."""
from __future__ import annotations

import logging

from hitl_middleware.models import ApprovalIntent, NotificationIntent
from hitl_middleware.plugins.base import HitlPlugin

logger = logging.getLogger("hitl_middleware.plugins.registry")


class PluginRegistry:
    """DI registry that fans out intents to all registered plugins."""

    def __init__(self) -> None:
        self._plugins: list[HitlPlugin] = []

    def register(self, plugin: HitlPlugin) -> None:
        """Register a plugin for intent dispatch."""
        self._plugins.append(plugin)
        logger.info("Registered HITL plugin: %s", type(plugin).__name__)

    @property
    def plugin_count(self) -> int:
        return len(self._plugins)

    async def dispatch_notification(self, intent: NotificationIntent) -> bool:
        """Fan out notification to all plugins. Returns True if any succeeded."""
        if not self._plugins:
            logger.warning("No plugins registered — notification not dispatched")
            return False
        results: list[bool] = []
        for plugin in self._plugins:
            try:
                ok = await plugin.send_notification(intent)
                results.append(ok)
            except Exception as exc:
                logger.error("Plugin %s notification failed: %s", type(plugin).__name__, exc)
                results.append(False)
        return any(results)

    async def dispatch_approval(self, intent: ApprovalIntent) -> bool:
        """Fan out approval request to all plugins. Returns True if any succeeded."""
        if not self._plugins:
            logger.warning("No plugins registered — approval not dispatched")
            return False
        results: list[bool] = []
        for plugin in self._plugins:
            try:
                ok = await plugin.send_approval_request(intent)
                results.append(ok)
            except Exception as exc:
                logger.error("Plugin %s approval failed: %s", type(plugin).__name__, exc)
                results.append(False)
        return any(results)

    async def close(self) -> None:
        """Close all registered plugins."""
        for plugin in self._plugins:
            try:
                await plugin.close()
            except Exception as exc:
                logger.warning("Plugin %s close failed: %s", type(plugin).__name__, exc)
