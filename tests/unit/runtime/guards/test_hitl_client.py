"""Tests for HitlClient (Phase 7 — §7.18)."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.guards.hitl_client import (
    ApprovalIntent,
    HitlClient,
    NotificationIntent,
)
from elephantbroker.schemas.config import HitlConfig


class TestHitlClient:
    async def test_disabled_skips_notify(self):
        client = HitlClient(HitlConfig(enabled=False))
        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4(),
            action_summary="test", outcome="inform",
        )
        result = await client.notify(intent)
        assert result is False

    async def test_disabled_skips_approval(self):
        client = HitlClient(HitlConfig(enabled=False))
        intent = ApprovalIntent(
            request_id=uuid.uuid4(), guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(), action_summary="test",
        )
        result = await client.request_approval(intent)
        assert result is False

    async def test_url_resolution_default(self):
        client = HitlClient(HitlConfig(default_url="http://hitl:8421"))
        assert client._resolve_url() == "http://hitl:8421"

    async def test_url_resolution_gateway_override(self):
        client = HitlClient(
            HitlConfig(default_url="http://default:8421",
                       gateway_overrides={"gw1": "http://gw1-hitl:8421"}),
            gateway_id="gw1",
        )
        assert client._resolve_url() == "http://gw1-hitl:8421"

    async def test_notification_intent_serialization(self):
        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4(),
            action_summary="test", outcome="inform",
        )
        data = intent.model_dump(mode="json")
        restored = NotificationIntent.model_validate(data)
        assert restored.outcome == "inform"

    async def test_approval_intent_serialization(self):
        intent = ApprovalIntent(
            request_id=uuid.uuid4(), guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(), action_summary="deploy prod",
            decision_domain="code_change", timeout_seconds=300,
        )
        data = intent.model_dump(mode="json")
        restored = ApprovalIntent.model_validate(data)
        assert restored.decision_domain == "code_change"
        assert restored.timeout_seconds == 300

    # --- Amendment 7.2 additional tests ---

    @pytest.mark.asyncio
    async def test_enabled_notify_success(self):
        """Enabled HITL client sends notification via httpx."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        client = HitlClient(HitlConfig(enabled=True, default_url="http://hitl:8421"))
        client._client = mock_client

        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4(),
            action_summary="test action", outcome="inform",
        )
        result = await client.notify(intent)
        assert result is True
        mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_enabled_notify_failure(self):
        """Enabled HITL client handles httpx errors gracefully."""
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("Connection refused"))

        client = HitlClient(HitlConfig(enabled=True, default_url="http://hitl:8421"))
        client._client = mock_client

        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4(),
            action_summary="test action", outcome="inform",
        )
        result = await client.notify(intent)
        assert result is False

    @pytest.mark.asyncio
    async def test_enabled_approval_request_success(self):
        """Enabled HITL client sends approval request successfully."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        client = HitlClient(HitlConfig(enabled=True, default_url="http://hitl:8421"))
        client._client = mock_client

        intent = ApprovalIntent(
            request_id=uuid.uuid4(), guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(), action_summary="deploy prod",
        )
        result = await client.request_approval(intent)
        assert result is True
        mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_enabled_approval_request_failure(self):
        """Enabled HITL client handles approval request errors."""
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("Timeout"))

        client = HitlClient(HitlConfig(enabled=True, default_url="http://hitl:8421"))
        client._client = mock_client

        intent = ApprovalIntent(
            request_id=uuid.uuid4(), guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(), action_summary="deploy prod",
        )
        result = await client.request_approval(intent)
        assert result is False

    @pytest.mark.asyncio
    async def test_lazy_client_initialization(self):
        """Client is only created on first use."""
        client = HitlClient(HitlConfig(enabled=True, default_url="http://hitl:8421"))
        assert client._client is None
        # We mock httpx import to avoid actual http client creation
        mock_http_client = AsyncMock()
        with patch("elephantbroker.runtime.guards.hitl_client.httpx", create=True) as mock_httpx:
            mock_httpx.AsyncClient = MagicMock(return_value=mock_http_client)
            import elephantbroker.runtime.guards.hitl_client as hitl_mod
            # Temporarily inject httpx
            with patch.dict("sys.modules", {"httpx": mock_httpx}):
                result = await client._get_client()
                # Client should be created
                assert client._client is not None

    @pytest.mark.asyncio
    async def test_client_reused_on_second_call(self):
        """Client is reused on subsequent calls (lazy singleton)."""
        client = HitlClient(HitlConfig(enabled=True))
        mock_client = AsyncMock()
        client._client = mock_client
        result = await client._get_client()
        assert result is mock_client

    @pytest.mark.asyncio
    async def test_close_disposes_client(self):
        """close() calls aclose on the httpx client."""
        client = HitlClient(HitlConfig(enabled=True))
        mock_http = AsyncMock()
        client._client = mock_http
        await client.close()
        mock_http.aclose.assert_called_once()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_close_no_client_noop(self):
        """close() when no client is a no-op."""
        client = HitlClient(HitlConfig(enabled=True))
        assert client._client is None
        await client.close()  # Should not raise
        assert client._client is None

    async def test_url_resolution_unknown_gateway_falls_back(self):
        """Unknown gateway_id falls back to default_url."""
        client = HitlClient(
            HitlConfig(default_url="http://default:8421",
                       gateway_overrides={"gw1": "http://gw1:8421"}),
            gateway_id="gw_unknown",
        )
        assert client._resolve_url() == "http://default:8421"

    async def test_notify_full_intent_fields(self):
        """NotificationIntent carries all optional fields."""
        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4(),
            session_key="agent:main:main", gateway_id="gw1",
            agent_key="gw1:agent1", action_summary="deploy to prod",
            decision_domain="code_change", outcome="warn",
            matched_rules=["builtin_drop_table"], explanation="Matched SQL pattern",
        )
        data = intent.model_dump(mode="json")
        assert data["session_key"] == "agent:main:main"
        assert data["gateway_id"] == "gw1"
        assert data["agent_key"] == "gw1:agent1"
        assert data["decision_domain"] == "code_change"
        assert data["matched_rules"] == ["builtin_drop_table"]

    async def test_approval_intent_timeout_propagation(self):
        """ApprovalIntent timeout_seconds propagates correctly."""
        intent = ApprovalIntent(
            request_id=uuid.uuid4(), guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(), action_summary="risky op",
            timeout_seconds=600,
        )
        assert intent.timeout_seconds == 600
        data = intent.model_dump(mode="json")
        restored = ApprovalIntent.model_validate(data)
        assert restored.timeout_seconds == 600


class TestHitlRetry:
    """GUARD-GAP-6: Retry logic with exponential backoff."""

    @pytest.mark.asyncio
    async def test_notify_succeeds_on_second_attempt(self):
        """Retry succeeds after transient failure."""
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=[Exception("conn reset"), ok_resp])

        config = HitlConfig(enabled=True, retry_count=2, retry_delay_seconds=0.0)
        client = HitlClient(config)
        client._client = mock_client

        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4(),
            action_summary="test", outcome="inform",
        )
        result = await client.notify(intent)
        assert result is True
        assert mock_client.post.call_count == 2

    @pytest.mark.asyncio
    async def test_approval_succeeds_on_third_attempt(self):
        """Approval succeeds on final retry."""
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=[
            Exception("timeout"), Exception("conn refused"), ok_resp,
        ])

        config = HitlConfig(enabled=True, retry_count=2, retry_delay_seconds=0.0)
        client = HitlClient(config)
        client._client = mock_client

        intent = ApprovalIntent(
            request_id=uuid.uuid4(), guard_event_id=uuid.uuid4(),
            session_id=uuid.uuid4(), action_summary="deploy",
        )
        result = await client.request_approval(intent)
        assert result is True
        assert mock_client.post.call_count == 3

    @pytest.mark.asyncio
    async def test_all_retries_exhausted_returns_false(self):
        """All retries fail → returns False (fail-open)."""
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("always fails"))

        config = HitlConfig(enabled=True, retry_count=2, retry_delay_seconds=0.0)
        client = HitlClient(config)
        client._client = mock_client

        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4(),
            action_summary="test", outcome="inform",
        )
        result = await client.notify(intent)
        assert result is False
        assert mock_client.post.call_count == 3  # 1 initial + 2 retries

    @pytest.mark.asyncio
    async def test_no_retries_when_retry_count_zero(self):
        """retry_count=0 means single attempt, no retries."""
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("fail"))

        config = HitlConfig(enabled=True, retry_count=0, retry_delay_seconds=0.0)
        client = HitlClient(config)
        client._client = mock_client

        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4(),
            action_summary="test", outcome="inform",
        )
        result = await client.notify(intent)
        assert result is False
        assert mock_client.post.call_count == 1

    @pytest.mark.asyncio
    async def test_retry_on_non_200_status(self):
        """Non-200 status triggers retry."""
        bad_resp = MagicMock()
        bad_resp.status_code = 503
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=[bad_resp, ok_resp])

        config = HitlConfig(enabled=True, retry_count=1, retry_delay_seconds=0.0)
        client = HitlClient(config)
        client._client = mock_client

        intent = NotificationIntent(
            guard_event_id=uuid.uuid4(), session_id=uuid.uuid4(),
            action_summary="test", outcome="inform",
        )
        result = await client.notify(intent)
        assert result is True
        assert mock_client.post.call_count == 2
