"""HITL middleware client — HTTP client for calling HITL from the runtime (Phase 7 — §7.18)."""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, Field

from elephantbroker.schemas.config import HitlConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Intent models (shared between runtime and HITL middleware)
# ---------------------------------------------------------------------------


class NotificationIntent(BaseModel):
    """Fire-and-forget notification for INFORM/WARN outcomes."""
    guard_event_id: uuid.UUID
    session_id: uuid.UUID
    session_key: str = ""
    gateway_id: str = ""
    agent_key: str = ""
    action_summary: str = ""
    decision_domain: str = ""
    outcome: str = "inform"
    matched_rules: list[str] = Field(default_factory=list)
    explanation: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ApprovalIntent(BaseModel):
    """Approval request dispatched to HITL middleware."""
    request_id: uuid.UUID
    guard_event_id: uuid.UUID
    session_id: uuid.UUID
    session_key: str = ""
    gateway_id: str = ""
    agent_key: str = ""
    action_summary: str = ""
    decision_domain: str = ""
    matched_rules: list[str] = Field(default_factory=list)
    explanation: str = ""
    approve_callback_url: str = ""
    reject_callback_url: str = ""
    timeout_seconds: int = 300
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


# ---------------------------------------------------------------------------
# HITL Client
# ---------------------------------------------------------------------------


class HitlClient:
    """HTTP client for calling HITL middleware from the runtime."""

    def __init__(self, config: HitlConfig, gateway_id: str = "", metrics=None) -> None:
        self._config = config
        self._gateway_id = gateway_id
        self._client = None
        self._metrics = metrics
        self._log = logging.getLogger(__name__)

    async def _get_client(self):
        if self._client is None:
            try:
                import httpx
                self._client = httpx.AsyncClient(timeout=self._config.timeout_seconds)
            except ImportError:
                self._log.warning("httpx not available — HITL client disabled")
                return None
        return self._client

    async def notify(self, intent: NotificationIntent) -> bool:
        """Fire-and-forget notification for INFORM outcomes."""
        return await self._post_with_retry(
            "/intents/notify", intent.model_dump(mode="json"), "notify",
        )

    async def request_approval(self, intent: ApprovalIntent) -> bool:
        """Send approval request for APPROVE_FIRST."""
        return await self._post_with_retry(
            "/intents/approval", intent.model_dump(mode="json"), "approval",
        )

    async def _post_with_retry(self, path: str, payload: dict, label: str) -> bool:
        """POST with exponential backoff retry. Fail-open on final failure."""
        if not self._config.enabled:
            return False
        client = await self._get_client()
        if client is None:
            return False
        url = f"{self._resolve_url()}{path}"
        max_attempts = 1 + self._config.retry_count
        last_exc: Exception | None = None
        for attempt in range(max_attempts):
            try:
                resp = await client.post(
                    url,
                    json=payload,
                    headers=self._trace_headers(),
                )
                if resp.status_code == 200:
                    return True
                self._log.warning("HITL %s got status %d (attempt %d/%d)",
                                  label, resp.status_code, attempt + 1, max_attempts)
            except Exception as exc:
                last_exc = exc
                self._log.warning("HITL %s failed (attempt %d/%d): %s",
                                  label, attempt + 1, max_attempts, exc)
            if attempt < max_attempts - 1:
                delay = self._config.retry_delay_seconds * (2 ** attempt)
                await asyncio.sleep(delay)
        self._log.error("HITL %s exhausted %d attempts — failing open", label, max_attempts)
        if self._metrics:
            self._metrics.inc_hitl_retry_exhausted()
        return False

    def _resolve_url(self) -> str:
        return self._config.gateway_overrides.get(self._gateway_id, self._config.default_url)

    def _trace_headers(self) -> dict:
        """W3C traceparent propagation."""
        headers: dict[str, str] = {}
        try:
            from opentelemetry.propagate import inject
            inject(headers)
        except ImportError:
            pass
        return headers

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
