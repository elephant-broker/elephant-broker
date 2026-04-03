"""HITL Middleware API routes."""
from __future__ import annotations

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.propagate import extract as otel_extract

from hitl_middleware.models import (
    ApprovalIntent,
    ApproveCallback,
    NotificationIntent,
    RejectCallback,
)
from hitl_middleware.security import is_token_expired, validate_hmac_token

logger = logging.getLogger("hitl_middleware.router")
tracer = trace.get_tracer("hitl_middleware.router")

router = APIRouter()


def _extract_trace_context(request: Request) -> otel_context.Context:
    """Extract W3C trace context from incoming request headers."""
    carrier = dict(request.headers)
    return otel_extract(carrier=carrier)


# --- Intent endpoints (called by runtime) ---


@router.post("/intents/notify")
async def receive_notification(intent: NotificationIntent, request: Request) -> dict[str, Any]:
    """Receive a notification intent from the runtime and dispatch to plugins."""
    ctx = _extract_trace_context(request)
    with tracer.start_as_current_span(
        "hitl.notify", context=ctx,
        attributes={"gateway_id": intent.gateway_id, "outcome": intent.outcome},
    ):
        registry = request.app.state.registry
        try:
            ok = await registry.dispatch_notification(intent)
        except Exception as exc:
            logger.error("Notification dispatch error (gateway=%s): %s", intent.gateway_id, exc)
            raise HTTPException(status_code=500, detail="Notification dispatch failed") from exc
        logger.info("Notification dispatched (gateway=%s, outcome=%s, ok=%s)", intent.gateway_id, intent.outcome, ok)
        return {"dispatched": ok, "plugin_count": registry.plugin_count}


@router.post("/intents/approval")
async def receive_approval(intent: ApprovalIntent, request: Request) -> dict[str, Any]:
    """Receive an approval intent from the runtime and dispatch to plugins."""
    ctx = _extract_trace_context(request)
    with tracer.start_as_current_span(
        "hitl.approval", context=ctx,
        attributes={"gateway_id": intent.gateway_id, "request_id": str(intent.request_id)},
    ):
        registry = request.app.state.registry
        try:
            ok = await registry.dispatch_approval(intent)
        except Exception as exc:
            logger.error("Approval dispatch error (gateway=%s, request=%s): %s", intent.gateway_id, intent.request_id, exc)
            raise HTTPException(status_code=500, detail="Approval dispatch failed") from exc
        logger.info("Approval dispatched (gateway=%s, request=%s, ok=%s)", intent.gateway_id, intent.request_id, ok)
        return {"dispatched": ok, "request_id": str(intent.request_id)}


# --- Callback endpoints (called by external systems) ---


@router.post("/callbacks/approve")
async def approve_callback(
    callback: ApproveCallback,
    request: Request,
    x_hitl_signature: str = Header(default=""),
) -> dict[str, Any]:
    """Callback from external system to approve a pending action.

    Validates HMAC signature, then calls the runtime's approval PATCH endpoint.
    """
    with tracer.start_as_current_span(
        "hitl.callback.approve",
        attributes={"request_id": str(callback.request_id)},
    ):
        config = request.app.state.config

        if not config.callback_secret:
            logger.warning("Approve callback rejected: callback_secret not configured")
            raise HTTPException(status_code=403, detail="Callback secret not configured")

        if not x_hitl_signature:
            logger.warning("Approve callback rejected: missing HMAC signature (request_id=%s)", callback.request_id)
            raise HTTPException(status_code=403, detail="Missing X-HITL-Signature header")

        if not validate_hmac_token(x_hitl_signature, str(callback.request_id), callback.created_at, config.callback_secret):
            logger.warning("Approve callback rejected: invalid HMAC signature (request_id=%s)", callback.request_id)
            raise HTTPException(status_code=403, detail="Invalid HMAC signature")

        if is_token_expired(callback.created_at, 3600):
            logger.warning("Approve callback rejected: token expired (request_id=%s)", callback.request_id)
            raise HTTPException(status_code=410, detail="Callback token expired")

        # Forward approval to runtime
        runtime_url = config.runtime_url
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.patch(
                    f"{runtime_url}/guards/approvals/{callback.request_id}",
                    json={
                        "status": "approved",
                        "message": callback.message,
                        "resolved_by": callback.approved_by or "external",
                        "agent_id": "",
                    },
                )
                if resp.status_code == 404:
                    raise HTTPException(status_code=404, detail="Approval request not found")
                if not resp.is_success:
                    raise HTTPException(status_code=502, detail=f"Runtime returned {resp.status_code}")
        except httpx.HTTPError as exc:
            logger.error("Runtime PATCH failed for approve (request_id=%s): %s", callback.request_id, exc)
            raise HTTPException(status_code=502, detail="Runtime unreachable") from exc

        logger.info("Approval callback processed (request_id=%s, approved_by=%s)", callback.request_id, callback.approved_by or "external")
        return {"status": "approved", "request_id": str(callback.request_id)}


@router.post("/callbacks/reject")
async def reject_callback(
    callback: RejectCallback,
    request: Request,
    x_hitl_signature: str = Header(default=""),
) -> dict[str, Any]:
    """Callback from external system to reject a pending action.

    Validates HMAC signature, then calls the runtime's approval PATCH endpoint.
    """
    with tracer.start_as_current_span(
        "hitl.callback.reject",
        attributes={"request_id": str(callback.request_id)},
    ):
        config = request.app.state.config

        if not config.callback_secret:
            logger.warning("Reject callback rejected: callback_secret not configured")
            raise HTTPException(status_code=403, detail="Callback secret not configured")

        if not x_hitl_signature:
            logger.warning("Reject callback rejected: missing HMAC signature (request_id=%s)", callback.request_id)
            raise HTTPException(status_code=403, detail="Missing X-HITL-Signature header")

        if not validate_hmac_token(x_hitl_signature, str(callback.request_id), callback.created_at, config.callback_secret):
            logger.warning("Reject callback rejected: invalid HMAC signature (request_id=%s)", callback.request_id)
            raise HTTPException(status_code=403, detail="Invalid HMAC signature")

        if is_token_expired(callback.created_at, 3600):
            logger.warning("Reject callback rejected: token expired (request_id=%s)", callback.request_id)
            raise HTTPException(status_code=410, detail="Callback token expired")

        if not callback.reason:
            raise HTTPException(status_code=422, detail="Rejection reason is required")

        # Forward rejection to runtime
        runtime_url = config.runtime_url
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.patch(
                    f"{runtime_url}/guards/approvals/{callback.request_id}",
                    json={
                        "status": "rejected",
                        "reason": callback.reason,
                        "resolved_by": callback.rejected_by or "external",
                        "agent_id": "",
                    },
                )
                if resp.status_code == 404:
                    raise HTTPException(status_code=404, detail="Approval request not found")
                if not resp.is_success:
                    raise HTTPException(status_code=502, detail=f"Runtime returned {resp.status_code}")
        except httpx.HTTPError as exc:
            logger.error("Runtime PATCH failed for reject (request_id=%s): %s", callback.request_id, exc)
            raise HTTPException(status_code=502, detail="Runtime unreachable") from exc

        logger.info("Rejection callback processed (request_id=%s, reason=%s)", callback.request_id, callback.reason[:80])
        return {"status": "rejected", "request_id": str(callback.request_id)}


# --- Health endpoint ---


@router.get("/health")
async def health(request: Request) -> dict[str, Any]:
    """Health check endpoint."""
    registry = request.app.state.registry
    return {
        "status": "ok",
        "plugins_registered": registry.plugin_count,
    }
