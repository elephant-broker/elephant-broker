"""Unit tests for hitl_middleware.security — 12 tests."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from hitl_middleware.security import compute_hmac_token, is_token_expired, validate_hmac_token


class TestComputeHmac:
    def test_deterministic(self):
        """Same inputs produce the same token."""
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        t1 = compute_hmac_token("req-1", ts, "secret")
        t2 = compute_hmac_token("req-1", ts, "secret")
        assert t1 == t2

    def test_different_secrets(self):
        """Different secrets produce different tokens."""
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        t1 = compute_hmac_token("req-1", ts, "secret-a")
        t2 = compute_hmac_token("req-1", ts, "secret-b")
        assert t1 != t2

    def test_different_request_ids(self):
        """Different request IDs produce different tokens."""
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        t1 = compute_hmac_token("req-1", ts, "secret")
        t2 = compute_hmac_token("req-2", ts, "secret")
        assert t1 != t2

    def test_different_timestamps(self):
        """Different timestamps produce different tokens."""
        ts1 = datetime(2025, 1, 1, tzinfo=UTC)
        ts2 = datetime(2025, 6, 1, tzinfo=UTC)
        t1 = compute_hmac_token("req-1", ts1, "secret")
        t2 = compute_hmac_token("req-1", ts2, "secret")
        assert t1 != t2

    def test_token_is_hex_string(self):
        """Token is a hex-encoded SHA-256 digest (64 chars)."""
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        token = compute_hmac_token("req-1", ts, "secret")
        assert len(token) == 64
        assert all(c in "0123456789abcdef" for c in token)


class TestValidateHmac:
    def test_correct_token(self):
        """Correct token validates successfully."""
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        token = compute_hmac_token("req-1", ts, "secret")
        assert validate_hmac_token(token, "req-1", ts, "secret") is True

    def test_wrong_token(self):
        """Wrong token is rejected."""
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        assert validate_hmac_token("badtoken", "req-1", ts, "secret") is False

    def test_wrong_secret(self):
        """Token computed with different secret is rejected."""
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        token = compute_hmac_token("req-1", ts, "secret-a")
        assert validate_hmac_token(token, "req-1", ts, "secret-b") is False

    def test_tampered_request_id(self):
        """Token for different request_id is rejected."""
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        token = compute_hmac_token("req-1", ts, "secret")
        assert validate_hmac_token(token, "req-2", ts, "secret") is False

    def test_empty_secret_returns_false(self):
        """Empty secret causes immediate rejection."""
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        assert validate_hmac_token("anything", "req-1", ts, "") is False


class TestIsTokenExpired:
    def test_fresh_token(self):
        """Token created now is not expired."""
        ts = datetime.now(UTC)
        assert is_token_expired(ts, timeout_seconds=300) is False

    def test_old_token(self):
        """Token created long ago is expired."""
        ts = datetime.now(UTC) - timedelta(hours=2)
        assert is_token_expired(ts, timeout_seconds=300) is True
