"""Unit tests for router POST /callbacks/approve and /callbacks/reject — 19 tests.

Amendment 7.2 Batch 1: Added HMAC validation tests and updated existing tests
to use valid HMAC tokens now that validate_hmac_token is wired into handlers.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from hitl_middleware.app import create_app
from hitl_middleware.config import HitlMiddlewareConfig
from hitl_middleware.security import compute_hmac_token

SECRET = "test-secret"


def _valid_callback(
    rid: str | None = None,
    *,
    message: str = "",
    approved_by: str = "",
    reason: str = "",
    rejected_by: str = "",
) -> tuple[str, dict, str]:
    """Build a callback payload with a valid HMAC signature."""
    rid = rid or str(uuid.uuid4())
    now = datetime.now(UTC)
    sig = compute_hmac_token(rid, now, SECRET)
    payload: dict = {"request_id": rid, "created_at": now.isoformat()}
    if message:
        payload["message"] = message
    if approved_by:
        payload["approved_by"] = approved_by
    if reason:
        payload["reason"] = reason
    if rejected_by:
        payload["rejected_by"] = rejected_by
    return rid, payload, sig


@pytest.fixture
def config():
    return HitlMiddlewareConfig(callback_secret=SECRET, runtime_url="http://runtime:8420")


@pytest.fixture
def config_no_secret():
    return HitlMiddlewareConfig(callback_secret="", runtime_url="http://runtime:8420")


@pytest.fixture
def app(config):
    return create_app(config)


@pytest.fixture
def app_no_secret(config_no_secret):
    return create_app(config_no_secret)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
async def client_no_secret(app_no_secret):
    transport = ASGITransport(app=app_no_secret)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _mock_runtime_response(status_code: int = 200, json_data: dict | None = None):
    """Create a mock httpx.Response."""
    return httpx.Response(
        status_code=status_code,
        json=json_data or {"status": "ok"},
        request=httpx.Request("PATCH", "http://runtime:8420/guards/approvals/test"),
    )


def _patch_httpx(status_code: int = 200):
    """Context manager that patches httpx.AsyncClient for runtime calls."""
    mock_instance = AsyncMock()
    mock_instance.patch = AsyncMock(return_value=_mock_runtime_response(status_code))
    mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
    mock_instance.__aexit__ = AsyncMock(return_value=False)
    return patch("hitl_middleware.router.httpx.AsyncClient", return_value=mock_instance), mock_instance


class TestApproveCallback:
    async def test_approve_valid_hmac_succeeds(self, client):
        """Approve callback with valid HMAC token succeeds."""
        rid, payload, sig = _valid_callback(message="Approved")
        patcher, mock = _patch_httpx()
        with patcher:
            resp = await client.post(
                "/callbacks/approve", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            assert resp.status_code == 200

    async def test_approve_invalid_hmac_403(self, client):
        """Approve with wrong HMAC signature returns 403."""
        rid, payload, _ = _valid_callback()
        resp = await client.post(
            "/callbacks/approve", json=payload,
            headers={"X-HITL-Signature": "invalid-hmac-token"},
        )
        assert resp.status_code == 403
        assert "Invalid HMAC" in resp.json()["detail"]

    async def test_approve_missing_hmac_403(self, client):
        """Approve without HMAC header returns 403."""
        _, payload, _ = _valid_callback()
        resp = await client.post("/callbacks/approve", json=payload)
        assert resp.status_code == 403

    async def test_approve_empty_hmac_403(self, client):
        """Approve with empty HMAC header returns 403."""
        _, payload, _ = _valid_callback()
        resp = await client.post(
            "/callbacks/approve", json=payload,
            headers={"X-HITL-Signature": ""},
        )
        assert resp.status_code == 403

    async def test_approve_expired_token_410(self, client):
        """Approve with expired created_at returns 410."""
        rid = str(uuid.uuid4())
        old_time = datetime(2020, 1, 1, tzinfo=UTC)
        sig = compute_hmac_token(rid, old_time, SECRET)
        resp = await client.post(
            "/callbacks/approve",
            json={"request_id": rid, "created_at": old_time.isoformat()},
            headers={"X-HITL-Signature": sig},
        )
        assert resp.status_code == 410
        assert "expired" in resp.json()["detail"].lower()

    async def test_approve_calls_runtime_patch(self, client):
        """Approve forwards PATCH to runtime URL."""
        rid, payload, sig = _valid_callback(message="ok")
        patcher, mock = _patch_httpx()
        with patcher:
            await client.post(
                "/callbacks/approve", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            mock.patch.assert_awaited_once()
            call_url = mock.patch.call_args[0][0]
            assert f"/guards/approvals/{rid}" in call_url

    async def test_approve_with_optional_message(self, client):
        """Approve sends the optional message to runtime."""
        rid, payload, sig = _valid_callback(message="Ship it!", approved_by="alice")
        patcher, mock = _patch_httpx()
        with patcher:
            resp = await client.post(
                "/callbacks/approve", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            assert resp.status_code == 200
            call_json = mock.patch.call_args[1]["json"]
            assert call_json["message"] == "Ship it!"

    async def test_approve_runtime_unreachable_502(self, client):
        """If runtime is unreachable, returns 502."""
        _, payload, sig = _valid_callback()
        mock_instance = AsyncMock()
        mock_instance.patch = AsyncMock(side_effect=httpx.ConnectError("refused"))
        mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_instance.__aexit__ = AsyncMock(return_value=False)
        with patch("hitl_middleware.router.httpx.AsyncClient", return_value=mock_instance):
            resp = await client.post(
                "/callbacks/approve", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            assert resp.status_code == 502

    async def test_approve_runtime_404(self, client):
        """If runtime returns 404, we return 404."""
        _, payload, sig = _valid_callback()
        patcher, _ = _patch_httpx(404)
        with patcher:
            resp = await client.post(
                "/callbacks/approve", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            assert resp.status_code == 404

    async def test_approve_response_format(self, client):
        """Approve response has status and request_id."""
        rid, payload, sig = _valid_callback()
        patcher, _ = _patch_httpx()
        with patcher:
            resp = await client.post(
                "/callbacks/approve", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            body = resp.json()
            assert body["status"] == "approved"
            assert body["request_id"] == rid


class TestRejectCallback:
    async def test_reject_valid_hmac_succeeds(self, client):
        """Reject callback with valid HMAC and reason succeeds."""
        rid, payload, sig = _valid_callback(reason="Too risky")
        patcher, _ = _patch_httpx()
        with patcher:
            resp = await client.post(
                "/callbacks/reject", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            assert resp.status_code == 200

    async def test_reject_invalid_hmac_403(self, client):
        """Reject with wrong HMAC signature returns 403."""
        _, payload, _ = _valid_callback(reason="no")
        resp = await client.post(
            "/callbacks/reject", json=payload,
            headers={"X-HITL-Signature": "wrong-token"},
        )
        assert resp.status_code == 403
        assert "Invalid HMAC" in resp.json()["detail"]

    async def test_reject_missing_hmac_403(self, client):
        """Reject without HMAC header returns 403."""
        _, payload, _ = _valid_callback(reason="no")
        resp = await client.post("/callbacks/reject", json=payload)
        assert resp.status_code == 403

    async def test_reject_expired_token_410(self, client):
        """Reject with expired created_at returns 410."""
        rid = str(uuid.uuid4())
        old_time = datetime(2020, 1, 1, tzinfo=UTC)
        sig = compute_hmac_token(rid, old_time, SECRET)
        resp = await client.post(
            "/callbacks/reject",
            json={"request_id": rid, "created_at": old_time.isoformat(), "reason": "old"},
            headers={"X-HITL-Signature": sig},
        )
        assert resp.status_code == 410

    async def test_reject_missing_reason_422(self, client):
        """Reject without reason returns 422."""
        _, payload, sig = _valid_callback()
        # payload has no "reason" key
        resp = await client.post(
            "/callbacks/reject", json=payload,
            headers={"X-HITL-Signature": sig},
        )
        assert resp.status_code == 422

    async def test_reject_calls_runtime_patch(self, client):
        """Reject forwards PATCH to runtime URL with rejection data."""
        rid, payload, sig = _valid_callback(reason="Policy violation")
        patcher, mock = _patch_httpx()
        with patcher:
            await client.post(
                "/callbacks/reject", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            call_json = mock.patch.call_args[1]["json"]
            assert call_json["status"] == "rejected"
            assert call_json["reason"] == "Policy violation"

    async def test_reject_with_rejected_by(self, client):
        """Reject sends rejected_by to runtime."""
        _, payload, sig = _valid_callback(reason="Nope", rejected_by="bob")
        patcher, mock = _patch_httpx()
        with patcher:
            resp = await client.post(
                "/callbacks/reject", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            assert resp.status_code == 200
            call_json = mock.patch.call_args[1]["json"]
            assert call_json["resolved_by"] == "bob"

    async def test_callback_empty_secret_configured_403(self, client_no_secret):
        """If callback_secret is empty, both callbacks return 403."""
        _, payload, _ = _valid_callback()
        resp = await client_no_secret.post(
            "/callbacks/approve", json=payload,
            headers={"X-HITL-Signature": "sig"},
        )
        assert resp.status_code == 403

    async def test_reject_response_format(self, client):
        """Reject response has status and request_id."""
        rid, payload, sig = _valid_callback(reason="Denied")
        patcher, _ = _patch_httpx()
        with patcher:
            resp = await client.post(
                "/callbacks/reject", json=payload,
                headers={"X-HITL-Signature": sig},
            )
            body = resp.json()
            assert body["status"] == "rejected"
            assert body["request_id"] == rid
