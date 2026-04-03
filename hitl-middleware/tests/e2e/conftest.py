"""Shared fixtures for HITL Middleware end-to-end tests."""
from __future__ import annotations

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from hitl_middleware.app import create_app
from hitl_middleware.config import HitlMiddlewareConfig, WebhookConfig, WebhookEndpoint


@pytest.fixture
def e2e_config():
    return HitlMiddlewareConfig(
        callback_secret="e2e-test-secret",
        runtime_url="http://mock-runtime:8420",
        webhook=WebhookConfig(
            notification_endpoints=[WebhookEndpoint(url="http://mock-webhook:9999/notify")],
            approval_endpoints=[WebhookEndpoint(url="http://mock-webhook:9999/approval")],
        ),
    )


@pytest.fixture
def e2e_app(e2e_config):
    return create_app(e2e_config)


@pytest.fixture
async def e2e_client(e2e_app):
    transport = ASGITransport(app=e2e_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def make_notify_payload(**overrides) -> dict:
    base = {
        "guard_event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "session_key": "agent:main:main",
        "gateway_id": "gw-e2e",
        "agent_key": "gw-e2e:agent1",
        "action_summary": "E2E notification",
        "decision_domain": "tool_call",
        "outcome": "inform",
        "matched_rules": ["rule-e2e"],
        "explanation": "E2E explanation",
    }
    base.update(overrides)
    return base


def make_approval_payload(**overrides) -> dict:
    request_id = overrides.pop("request_id", str(uuid.uuid4()))
    base = {
        "request_id": request_id,
        "guard_event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "session_key": "agent:main:main",
        "gateway_id": "gw-e2e",
        "agent_key": "gw-e2e:agent1",
        "action_summary": "E2E action requiring approval",
        "decision_domain": "tool_call",
        "matched_rules": ["rule-e2e-approval"],
        "explanation": "Needs E2E sign-off",
        "approve_callback_url": "http://test/callbacks/approve",
        "reject_callback_url": "http://test/callbacks/reject",
        "timeout_seconds": 300,
    }
    base.update(overrides)
    return base
