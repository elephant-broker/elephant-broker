"""HMAC callback security for HITL Middleware."""
from __future__ import annotations

import hashlib
import hmac
import time
from datetime import datetime


def compute_hmac_token(request_id: str, created_at: datetime, secret: str) -> str:
    """Compute HMAC-SHA256 token for a callback URL.

    Token = HMAC-SHA256(secret, "{request_id}:{created_at_unix}")
    """
    unix_ts = str(int(created_at.timestamp()))
    message = f"{request_id}:{unix_ts}"
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def validate_hmac_token(
    token: str,
    request_id: str,
    created_at: datetime,
    secret: str,
) -> bool:
    """Validate an HMAC token against request parameters.

    Uses timing-safe comparison to prevent timing attacks.
    """
    if not secret:
        return False
    expected = compute_hmac_token(request_id, created_at, secret)
    return hmac.compare_digest(token, expected)


def is_token_expired(created_at: datetime, timeout_seconds: int) -> bool:
    """Check if a callback token has expired."""
    elapsed = time.time() - created_at.timestamp()
    return elapsed > timeout_seconds
