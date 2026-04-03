"""Integration tests for HMAC token generation, validation, and expiry."""
from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from hitl_middleware.security import compute_hmac_token, is_token_expired, validate_hmac_token


# ---------------------------------------------------------------------------
# 1. test_generate_then_validate_success
# ---------------------------------------------------------------------------


async def test_generate_then_validate_success():
    """compute_hmac_token then validate_hmac_token with same params returns True."""
    request_id = str(uuid.uuid4())
    created_at = datetime.now(UTC)
    secret = "roundtrip-secret"

    token = compute_hmac_token(request_id, created_at, secret)
    assert validate_hmac_token(token, request_id, created_at, secret) is True


# ---------------------------------------------------------------------------
# 2. test_generate_then_tamper_fails
# ---------------------------------------------------------------------------


async def test_generate_then_tamper_fails():
    """Tampering with the token causes validation to fail."""
    request_id = str(uuid.uuid4())
    created_at = datetime.now(UTC)
    secret = "tamper-secret"

    token = compute_hmac_token(request_id, created_at, secret)
    # Flip the first hex character
    tampered = ("0" if token[0] != "0" else "1") + token[1:]
    assert validate_hmac_token(tampered, request_id, created_at, secret) is False


# ---------------------------------------------------------------------------
# 3. test_generate_wrong_secret_fails
# ---------------------------------------------------------------------------


async def test_generate_wrong_secret_fails():
    """Token computed with secret A fails validation with secret B."""
    request_id = str(uuid.uuid4())
    created_at = datetime.now(UTC)

    token = compute_hmac_token(request_id, created_at, "secret-A")
    assert validate_hmac_token(token, request_id, created_at, "secret-B") is False


# ---------------------------------------------------------------------------
# 4. test_token_expiry_check
# ---------------------------------------------------------------------------


async def test_token_expiry_check():
    """is_token_expired returns True for old timestamps, False for fresh ones."""
    # Fresh timestamp -- not expired
    fresh = datetime.now(UTC)
    assert is_token_expired(fresh, timeout_seconds=300) is False

    # Old timestamp -- expired (10 minutes ago, 5-minute window)
    old = datetime.now(UTC) - timedelta(minutes=10)
    assert is_token_expired(old, timeout_seconds=300) is True
