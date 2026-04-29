"""OpenClaw gateway WebSocket client for live-mode scenario testing."""
from __future__ import annotations
import asyncio
import base64
import binascii
import hashlib
import json
import logging
import time
import uuid
from typing import AsyncIterator

import websockets

logger = logging.getLogger(__name__)


class OpenClawClient:
    """Connect to an OpenClaw gateway via WebSocket and drive conversations.

    Usage:
        client = OpenClawClient("ws://localhost:18789", token="my-token")
        await client.connect()
        session_key = await client.create_session(agent_id="main")
        response = await client.send_and_wait(session_key, "Hello, remember X")
        history = await client.get_history(session_key)
        await client.close()
    """

    def __init__(self, gateway_url: str, token: str,
                 agent_id: str = "main", timeout: float = 60.0,
                 device_private_key: str | None = None,
                 device_token: str | None = None):
        self.gateway_url = gateway_url
        self.token = token
        self.agent_id = agent_id
        self.timeout = timeout
        self._device_private_key = device_private_key
        self._device_token = device_token
        self._ws = None
        self._pending: dict[str, asyncio.Future] = {}
        self._deferred_chat_events: dict[str, dict] = {}
        self._listener_task: asyncio.Task | None = None

    @staticmethod
    def _b64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    async def connect(self) -> None:
        """Connect to gateway, complete 3-step handshake with Ed25519 device auth."""
        self._ws = await websockets.connect(self.gateway_url)

        # Step 1: Receive challenge
        challenge = json.loads(
            await asyncio.wait_for(self._ws.recv(), self.timeout)
        )
        assert challenge["event"] == "connect.challenge"
        nonce = challenge.get("payload", {}).get("nonce", "")

        # Step 2: Build connect params
        client_id = "cli"
        client_mode = "cli"
        role = "operator"
        platform = "python"

        params = {
            "minProtocol": 3,
            "maxProtocol": 3,
            "client": {
                "id": client_id,
                "version": "0.1.0",
                "platform": platform,
                "mode": client_mode,
            },
            "role": role,
            "scopes": [
                "operator.admin", "operator.read", "operator.write",
                "operator.approvals", "operator.pairing",
            ],
            "auth": {"token": self.token},
        }

        # Add device identity if private key provided
        if self._device_private_key:
            if not nonce:
                raise RuntimeError(
                    "Gateway challenge missing nonce — cannot sign device auth"
                )
            # Lazy import: PyNaCl only required for device auth, not token-only connections
            try:
                import nacl.signing
            except ImportError:
                raise RuntimeError(
                    "Device auth requires PyNaCl: pip install 'elephant-broker[scenario]'"
                )
            try:
                key_bytes = base64.b64decode(self._device_private_key)
            except binascii.Error:
                raise RuntimeError(
                    "Invalid device_private_key: not valid base64"
                )
            try:
                signing_key = nacl.signing.SigningKey(key_bytes)
            except (ValueError, TypeError) as exc:
                raise RuntimeError(
                    "Invalid device_private_key: must be 32-byte Ed25519 seed"
                ) from exc
            verify_key_bytes = bytes(signing_key.verify_key)
            device_id = hashlib.sha256(verify_key_bytes).hexdigest()
            pubkey_b64url = self._b64url_encode(verify_key_bytes)

            signed_at_ms = int(time.time() * 1000)
            scopes_str = ",".join(params["scopes"])
            payload_str = (
                f"v3|{device_id}|{client_id}|{client_mode}|{role}|{scopes_str}"
                f"|{signed_at_ms}|{self.token}|{nonce}|{platform}|"
            )
            signed = signing_key.sign(payload_str.encode("utf-8"))
            signature_b64url = self._b64url_encode(signed.signature)

            params["device"] = {
                "id": device_id,
                "publicKey": pubkey_b64url,
                "signature": signature_b64url,
                "signedAt": signed_at_ms,
                "nonce": nonce,
            }
        elif self._device_token:
            # Use previously obtained device token (no signing needed)
            params["auth"]["deviceToken"] = self._device_token

        connect_id = str(uuid.uuid4())
        await self._ws.send(json.dumps({
            "type": "req",
            "id": connect_id,
            "method": "connect",
            "params": params,
        }))

        # Step 3: Receive hello-ok
        hello = json.loads(
            await asyncio.wait_for(self._ws.recv(), self.timeout)
        )
        assert hello.get("ok") is True, f"Connect failed: {hello}"

        # Save deviceToken if provided in response
        auth_resp = hello.get("payload", {}).get("auth", {})
        if auth_resp.get("deviceToken"):
            self._device_token = auth_resp["deviceToken"]

        self._listener_task = asyncio.create_task(self._listen())

    async def _listen(self) -> None:
        """Background listener dispatching responses and events."""
        try:
            async for raw in self._ws:
                msg = json.loads(raw)
                msg_type = msg.get("type")
                if msg_type == "res" and msg.get("id") in self._pending:
                    self._pending[msg["id"]].set_result(msg)
                elif msg_type == "event" and msg.get("event") == "chat":
                    payload = msg.get("payload", {})
                    run_id = payload.get("runId")
                    state = payload.get("state")
                    if state in ("final", "error"):
                        if run_id and run_id in self._pending:
                            self._pending[run_id].set_result(payload)
                        elif run_id:
                            self._deferred_chat_events[run_id] = payload
        except websockets.ConnectionClosed:
            pass

    async def _rpc(self, method: str, params: dict) -> dict:
        """Send a request and wait for the response."""
        req_id = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()
        self._pending[req_id] = future
        await self._ws.send(json.dumps({
            "type": "req", "id": req_id,
            "method": method, "params": params,
        }))
        try:
            result = await asyncio.wait_for(future, self.timeout)
        finally:
            self._pending.pop(req_id, None)
        if not result.get("ok", True):
            raise RuntimeError(f"RPC {method} failed: {result.get('error')}")
        return result.get("payload", result)

    # --- Session Management ---

    async def create_session(self, agent_id: str | None = None,
                              label: str = "eb-live-test") -> str:
        """Create a new OpenClaw session. Returns session key."""
        result = await self._rpc("sessions.create", {
            "agentId": agent_id or self.agent_id,
            "label": label,
        })
        return result["key"]

    async def list_sessions(self) -> list[dict]:
        result = await self._rpc("sessions.list", {})
        return result.get("sessions", result)

    async def reset_session(self, session_key: str, reason: str = "new") -> dict:
        return await self._rpc("sessions.reset", {
            "key": session_key, "reason": reason})

    async def delete_session(self, session_key: str) -> dict:
        return await self._rpc("sessions.delete", {
            "key": session_key, "emitLifecycleHooks": True})

    # --- Messaging ---

    async def send_and_wait(self, session_key: str, message: str,
                             thinking: str | None = None) -> dict:
        """Send a user message and wait for the complete agent response."""
        params = {
            "key": session_key,
            "message": message,
        }
        if thinking is not None:
            params["thinking"] = thinking
        send_result = await self._rpc("sessions.send", params)
        run_id = send_result.get("runId")

        if not run_id:
            # Retry trade-off (TD documented): a missed runId means we re-send
            # the full message, which is acceptable for our test infra (idempotent
            # at the gateway layer) but not safe for production agents.
            logger.debug("send_and_wait: no runId in initial response, retrying...")
            for attempt in range(3):
                await asyncio.sleep(2)
                send_result = await self._rpc("sessions.send", params)
                run_id = send_result.get("runId")
                logger.debug("send_and_wait: retry %d/3, runId=%s", attempt + 1, run_id)
                if run_id:
                    break
            if not run_id:
                logger.debug("send_and_wait: giving up after 3 retries, no runId")
                return send_result

        future = asyncio.get_running_loop().create_future()
        self._pending[run_id] = future
        if run_id in self._deferred_chat_events:
            future.set_result(self._deferred_chat_events.pop(run_id))
        try:
            result = await asyncio.wait_for(future, self.timeout)
        finally:
            self._pending.pop(run_id, None)
        return result

    async def get_history(self, session_key: str,
                           limit: int = 200) -> list[dict]:
        result = await self._rpc("sessions.get", {
            "key": session_key, "limit": limit})
        return result.get("messages", [])

    # --- Lifecycle ---

    async def close(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
        if self._ws:
            await self._ws.close()
