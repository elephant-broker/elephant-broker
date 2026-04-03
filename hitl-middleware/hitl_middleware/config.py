"""HITL Middleware configuration."""
from __future__ import annotations

import os

from pydantic import BaseModel, Field


class WebhookEndpoint(BaseModel):
    """A single webhook endpoint configuration."""

    url: str
    headers: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float = Field(default=10.0, ge=1.0)
    enabled: bool = True


class WebhookConfig(BaseModel):
    """Webhook plugin configuration."""

    notification_endpoints: list[WebhookEndpoint] = Field(default_factory=list)
    approval_endpoints: list[WebhookEndpoint] = Field(default_factory=list)
    retry_count: int = Field(default=3, ge=0)
    retry_delay_seconds: float = Field(default=1.0, ge=0.1)


class HitlMiddlewareConfig(BaseModel):
    """Top-level HITL Middleware configuration."""

    host: str = "0.0.0.0"
    port: int = Field(default=8421, ge=1, le=65535)
    log_level: str = "INFO"
    callback_secret: str = ""
    runtime_url: str = "http://localhost:8420"
    webhook: WebhookConfig = Field(default_factory=WebhookConfig)

    @classmethod
    def from_env(cls) -> HitlMiddlewareConfig:
        """Load configuration from environment variables."""
        return cls(
            host=os.environ.get("HITL_HOST", "0.0.0.0"),
            port=int(os.environ.get("HITL_PORT", "8421")),
            log_level=os.environ.get("HITL_LOG_LEVEL", "INFO"),
            callback_secret=os.environ.get("EB_HITL_CALLBACK_SECRET", ""),
            runtime_url=os.environ.get("EB_RUNTIME_URL", "http://localhost:8420"),
        )
