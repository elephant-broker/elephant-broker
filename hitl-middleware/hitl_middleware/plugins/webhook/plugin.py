"""Webhook plugin — dispatches intents as HTTP POST requests to configured endpoints."""
from __future__ import annotations

import logging

import httpx

from hitl_middleware.config import WebhookConfig
from hitl_middleware.models import ApprovalIntent, NotificationIntent
from hitl_middleware.plugins.base import HitlPlugin

logger = logging.getLogger("hitl_middleware.plugins.webhook")


class WebhookPlugin(HitlPlugin):
    """Sends HITL intents to configured webhook URLs via HTTP POST."""

    def __init__(self, config: WebhookConfig) -> None:
        self._config = config
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        """Lazy initialization of httpx client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def send_notification(self, intent: NotificationIntent) -> bool:
        """POST notification intent to all enabled notification endpoints."""
        endpoints = [e for e in self._config.notification_endpoints if e.enabled]
        if not endpoints:
            logger.debug("No enabled notification endpoints configured")
            return False

        payload = intent.model_dump(mode="json")
        client = self._get_client()
        any_success = False

        for endpoint in endpoints:
            try:
                resp = await client.post(
                    endpoint.url,
                    json=payload,
                    headers=endpoint.headers,
                    timeout=endpoint.timeout_seconds,
                )
                if resp.is_success:
                    logger.info("Notification sent to %s (status=%d)", endpoint.url, resp.status_code)
                    any_success = True
                else:
                    logger.warning("Notification to %s failed: status=%d", endpoint.url, resp.status_code)
            except httpx.HTTPError as exc:
                logger.error("Notification to %s failed: %s", endpoint.url, exc)
            except Exception as exc:
                logger.error("Notification to %s unexpected error: %s", endpoint.url, exc)

        return any_success

    async def send_approval_request(self, intent: ApprovalIntent) -> bool:
        """POST approval intent to all enabled approval endpoints."""
        endpoints = [e for e in self._config.approval_endpoints if e.enabled]
        if not endpoints:
            logger.debug("No enabled approval endpoints configured")
            return False

        payload = intent.model_dump(mode="json")
        client = self._get_client()
        any_success = False

        for endpoint in endpoints:
            try:
                resp = await client.post(
                    endpoint.url,
                    json=payload,
                    headers=endpoint.headers,
                    timeout=endpoint.timeout_seconds,
                )
                if resp.is_success:
                    logger.info("Approval request sent to %s (status=%d)", endpoint.url, resp.status_code)
                    any_success = True
                else:
                    logger.warning("Approval to %s failed: status=%d", endpoint.url, resp.status_code)
            except httpx.HTTPError as exc:
                logger.error("Approval to %s failed: %s", endpoint.url, exc)
            except Exception as exc:
                logger.error("Approval to %s unexpected error: %s", endpoint.url, exc)

        return any_success

    async def close(self) -> None:
        """Shutdown httpx client."""
        if self._client:
            await self._client.aclose()
            self._client = None
