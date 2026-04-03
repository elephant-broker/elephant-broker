"""Shared fixtures for HITL Middleware integration tests."""
from __future__ import annotations

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from hitl_middleware.app import create_app
from hitl_middleware.config import HitlMiddlewareConfig, WebhookConfig, WebhookEndpoint


@pytest.fixture
def webhook_config():
    return WebhookConfig(
        notification_endpoints=[WebhookEndpoint(url="http://mock-webhook:9999/notify")],
        approval_endpoints=[WebhookEndpoint(url="http://mock-webhook:9999/approval")],
    )


@pytest.fixture
def config(webhook_config):
    return HitlMiddlewareConfig(
        callback_secret="integration-secret",
        runtime_url="http://mock-runtime:8420",
        webhook=webhook_config,
    )


@pytest.fixture
def app(config):
    return create_app(config)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def make_notify_payload(**overrides) -> dict:
    """Build a valid NotificationIntent JSON payload with sensible defaults."""
    base = {
        "guard_event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "session_key": "agent:main:main",
        "gateway_id": "gw-test",
        "agent_key": "gw-test:agent1",
        "action_summary": "Test action summary",
        "decision_domain": "tool_call",
        "outcome": "inform",
        "matched_rules": ["rule-1"],
        "explanation": "Test explanation",
    }
    base.update(overrides)
    return base


def make_approval_payload(**overrides) -> dict:
    """Build a valid ApprovalIntent JSON payload with sensible defaults."""
    request_id = overrides.pop("request_id", str(uuid.uuid4()))
    base = {
        "request_id": request_id,
        "guard_event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "session_key": "agent:main:main",
        "gateway_id": "gw-test",
        "agent_key": "gw-test:agent1",
        "action_summary": "Action requiring approval",
        "decision_domain": "tool_call",
        "matched_rules": ["rule-approval-1"],
        "explanation": "Needs human sign-off",
        "approve_callback_url": "http://test/callbacks/approve",
        "reject_callback_url": "http://test/callbacks/reject",
        "timeout_seconds": 300,
    }
    base.update(overrides)
    return base
