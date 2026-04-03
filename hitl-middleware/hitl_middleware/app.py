"""HITL Middleware FastAPI application factory."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from hitl_middleware.config import HitlMiddlewareConfig
from hitl_middleware.plugins.registry import PluginRegistry
from hitl_middleware.plugins.webhook.plugin import WebhookPlugin
from hitl_middleware.router import router

logger = logging.getLogger("hitl_middleware.app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan — cleanup plugins on shutdown."""
    yield
    registry: PluginRegistry = app.state.registry
    await registry.close()
    logger.info("HITL Middleware shutdown — plugins closed")


def create_app(config: HitlMiddlewareConfig | None = None) -> FastAPI:
    """Create and configure the HITL Middleware FastAPI application.

    Args:
        config: Middleware configuration. If None, loads from environment.

    Returns:
        Configured FastAPI application with plugins wired.
    """
    config = config or HitlMiddlewareConfig.from_env()

    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))

    app = FastAPI(
        title="HITL Middleware",
        description="Human-in-the-Loop middleware for ElephantBroker guard approval workflows",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Wire plugin registry
    registry = PluginRegistry()

    # Register webhook plugin if configured
    if config.webhook.notification_endpoints or config.webhook.approval_endpoints:
        webhook_plugin = WebhookPlugin(config=config.webhook)
        registry.register(webhook_plugin)
        logger.info(
            "Webhook plugin registered: %d notification + %d approval endpoints",
            len(config.webhook.notification_endpoints),
            len(config.webhook.approval_endpoints),
        )

    # Attach to app state
    app.state.config = config
    app.state.registry = registry

    # Include routes
    app.include_router(router)

    logger.info("HITL Middleware created (port=%d, plugins=%d)", config.port, registry.plugin_count)
    return app
