"""Tests for the OpenClaw WebSocket client and scenario runner."""
from __future__ import annotations

import base64
import hashlib
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock, PropertyMock
import asyncio

from tests.scenarios.openclaw_client import OpenClawClient
from tests.scenarios.base import ScenarioResult


# ---------------------------------------------------------------------------
# OpenClawClient tests
# ---------------------------------------------------------------------------


class TestOpenClawClientInit:
    def test_defaults(self):
        client = OpenClawClient("ws://localhost:18789", token="tok")
        assert client.gateway_url == "ws://localhost:18789"
        assert client.token == "tok"
        assert client.agent_id == "main"
        assert client.timeout == 60.0
        assert client._device_private_key is None
        assert client._device_token is None
        assert client._ws is None
        assert client._pending == {}
        assert client._listener_task is None

    def test_custom_params(self):
        client = OpenClawClient("ws://host:1234", token="t",
                                agent_id="worker", timeout=30.0,
                                device_private_key="abc", device_token="dtk")
        assert client.agent_id == "worker"
        assert client.timeout == 30.0
        assert client._device_private_key == "abc"
        assert client._device_token == "dtk"


class TestOpenClawClientConnect:
    @pytest.mark.asyncio
    async def test_connect_handshake(self):
        """Test the 3-step WebSocket handshake (no device key — token-only)."""
        mock_ws = AsyncMock()
        # recv returns challenge (with nonce) then hello-ok
        mock_ws.recv = AsyncMock(side_effect=[
            json.dumps({"event": "connect.challenge", "payload": {"nonce": "test-nonce"}}),
            json.dumps({"ok": True, "payload": {}}),
        ])
        mock_ws.send = AsyncMock()
        # Make the listener loop end immediately
        mock_ws.__aiter__ = MagicMock(return_value=iter([]))

        with patch("tests.scenarios.openclaw_client.websockets.connect",
                   new_callable=lambda: lambda *a, **kw: _async_return(mock_ws)):
            client = OpenClawClient("ws://localhost:18789", token="test-token")
            await client.connect()

            # Verify send was called with the connect message
            assert mock_ws.send.called
            sent = json.loads(mock_ws.send.call_args[0][0])
            assert sent["type"] == "req"
            assert sent["method"] == "connect"
            assert sent["params"]["auth"]["token"] == "test-token"
            assert sent["params"]["minProtocol"] == 3
            assert sent["params"]["maxProtocol"] == 3
            assert sent["params"]["client"]["id"] == "cli"
            assert sent["params"]["client"]["mode"] == "cli"
            assert sent["params"]["role"] == "operator"
            # No device key provided — no device block in params
            assert "device" not in sent["params"]

            # Listener task was created
            assert client._listener_task is not None

            await client.close()

    @pytest.mark.asyncio
    async def test_connect_fails_on_bad_hello(self):
        """If the hello response is not ok, assertion error is raised."""
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(side_effect=[
            json.dumps({"event": "connect.challenge", "payload": {"nonce": "n"}}),
            json.dumps({"ok": False, "error": "bad auth"}),
        ])
        mock_ws.send = AsyncMock()

        with patch("tests.scenarios.openclaw_client.websockets.connect",
                   new_callable=lambda: lambda *a, **kw: _async_return(mock_ws)):
            client = OpenClawClient("ws://localhost:18789", token="bad-token")
            with pytest.raises(AssertionError, match="Connect failed"):
                await client.connect()

    @pytest.mark.asyncio
    async def test_connect_ed25519_device_auth(self):
        """Test Ed25519 device signing path: params.device block is built correctly."""
        import nacl.signing

        # Generate a real Ed25519 key pair for deterministic verification
        real_key = nacl.signing.SigningKey.generate()
        private_key_b64 = base64.b64encode(bytes(real_key)).decode()
        verify_key_bytes = bytes(real_key.verify_key)
        expected_device_id = hashlib.sha256(verify_key_bytes).hexdigest()

        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(side_effect=[
            json.dumps({"event": "connect.challenge", "payload": {"nonce": "challenge-nonce-42"}}),
            json.dumps({"ok": True, "payload": {}}),
        ])
        mock_ws.send = AsyncMock()
        mock_ws.__aiter__ = MagicMock(return_value=iter([]))

        with patch("tests.scenarios.openclaw_client.websockets.connect",
                   new_callable=lambda: lambda *a, **kw: _async_return(mock_ws)):
            client = OpenClawClient(
                "ws://localhost:18789", token="test-token",
                device_private_key=private_key_b64,
            )
            await client.connect()

            sent = json.loads(mock_ws.send.call_args[0][0])
            device = sent["params"]["device"]

            # Verify device block has all required fields
            assert "id" in device
            assert "publicKey" in device
            assert "signature" in device
            assert "signedAt" in device
            assert "nonce" in device

            # Verify device ID is SHA-256 of the public key
            assert device["id"] == expected_device_id

            # Verify nonce is passed through from challenge
            assert device["nonce"] == "challenge-nonce-42"

            # Verify signedAt is a recent millisecond timestamp
            assert isinstance(device["signedAt"], int)
            assert device["signedAt"] > 0

            # Verify the v3 payload format: pipe-delimited with shared values
            # Reconstruct what the payload should be
            scopes_str = ",".join(sent["params"]["scopes"])
            expected_payload = (
                f"v3|{device['id']}|cli|cli|operator|{scopes_str}"
                f"|{device['signedAt']}|test-token|challenge-nonce-42|python|"
            )

            # Verify the signature is valid by checking with the verify key
            sig_bytes = base64.urlsafe_b64decode(
                device["signature"] + "=="  # re-pad
            )
            real_key.verify_key.verify(
                expected_payload.encode("utf-8"), sig_bytes
            )

            await client.close()

    @pytest.mark.asyncio
    async def test_connect_device_token_reuse(self):
        """When device_token is provided (no private key), it's sent in auth block."""
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(side_effect=[
            json.dumps({"event": "connect.challenge", "payload": {"nonce": "n"}}),
            json.dumps({"ok": True, "payload": {}}),
        ])
        mock_ws.send = AsyncMock()
        mock_ws.__aiter__ = MagicMock(return_value=iter([]))

        with patch("tests.scenarios.openclaw_client.websockets.connect",
                   new_callable=lambda: lambda *a, **kw: _async_return(mock_ws)):
            client = OpenClawClient(
                "ws://localhost:18789", token="test-token",
                device_token="dtk",
            )
            await client.connect()

            sent = json.loads(mock_ws.send.call_args[0][0])
            # device_token should be in auth block
            assert sent["params"]["auth"]["deviceToken"] == "dtk"
            # No device signing block should be present
            assert "device" not in sent["params"]

            await client.close()

    @pytest.mark.asyncio
    async def test_connect_saves_device_token_from_hello(self):
        """deviceToken from hello-ok response is saved on the client."""
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(side_effect=[
            json.dumps({"event": "connect.challenge", "payload": {"nonce": "n"}}),
            json.dumps({"ok": True, "payload": {"auth": {"deviceToken": "saved-token"}}}),
        ])
        mock_ws.send = AsyncMock()
        mock_ws.__aiter__ = MagicMock(return_value=iter([]))

        with patch("tests.scenarios.openclaw_client.websockets.connect",
                   new_callable=lambda: lambda *a, **kw: _async_return(mock_ws)):
            client = OpenClawClient("ws://localhost:18789", token="test-token")
            assert client._device_token is None
            await client.connect()
            assert client._device_token == "saved-token"

            await client.close()


class TestOpenClawClientSessions:
    @pytest.mark.asyncio
    async def test_create_session(self):
        """create_session sends sessions.create RPC."""
        client = OpenClawClient("ws://test", token="t")
        client._rpc = AsyncMock(return_value={"key": "agent:main:main"})

        key = await client.create_session(agent_id="main")
        assert key == "agent:main:main"
        client._rpc.assert_called_once_with("sessions.create", {
            "agentId": "main", "label": "eb-live-test",
        })

    @pytest.mark.asyncio
    async def test_create_session_default_agent(self):
        """create_session uses self.agent_id when not specified."""
        client = OpenClawClient("ws://test", token="t", agent_id="worker")
        client._rpc = AsyncMock(return_value={"key": "agent:worker:worker"})

        key = await client.create_session()
        assert key == "agent:worker:worker"
        client._rpc.assert_called_once_with("sessions.create", {
            "agentId": "worker", "label": "eb-live-test",
        })

    @pytest.mark.asyncio
    async def test_create_session_custom_label(self):
        client = OpenClawClient("ws://test", token="t")
        client._rpc = AsyncMock(return_value={"key": "k"})

        await client.create_session(label="custom-label")
        client._rpc.assert_called_once_with("sessions.create", {
            "agentId": "main", "label": "custom-label",
        })

    @pytest.mark.asyncio
    async def test_list_sessions(self):
        client = OpenClawClient("ws://test", token="t")
        client._rpc = AsyncMock(return_value={
            "sessions": [{"key": "a"}, {"key": "b"}]
        })
        sessions = await client.list_sessions()
        assert len(sessions) == 2

    @pytest.mark.asyncio
    async def test_reset_session(self):
        client = OpenClawClient("ws://test", token="t")
        client._rpc = AsyncMock(return_value={"ok": True})
        result = await client.reset_session("key:1", reason="test")
        client._rpc.assert_called_once_with("sessions.reset", {
            "key": "key:1", "reason": "test",
        })

    @pytest.mark.asyncio
    async def test_reset_session_default_reason(self):
        client = OpenClawClient("ws://test", token="t")
        client._rpc = AsyncMock(return_value={"ok": True})
        await client.reset_session("key:1")
        client._rpc.assert_called_once_with("sessions.reset", {
            "key": "key:1", "reason": "new",
        })

    @pytest.mark.asyncio
    async def test_delete_session(self):
        client = OpenClawClient("ws://test", token="t")
        client._rpc = AsyncMock(return_value={"ok": True})
        result = await client.delete_session("key:1")
        client._rpc.assert_called_once_with("sessions.delete", {
            "key": "key:1", "emitLifecycleHooks": True,
        })


class TestOpenClawClientMessaging:
    @pytest.mark.asyncio
    async def test_get_history(self):
        client = OpenClawClient("ws://test", token="t")
        client._rpc = AsyncMock(return_value={
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
            ]
        })
        history = await client.get_history("key:1")
        assert len(history) == 2
        assert history[0]["role"] == "user"
        assert history[1]["role"] == "assistant"

    @pytest.mark.asyncio
    async def test_get_history_custom_limit(self):
        client = OpenClawClient("ws://test", token="t")
        client._rpc = AsyncMock(return_value={"messages": []})
        await client.get_history("key:1", limit=50)
        client._rpc.assert_called_once_with("sessions.get", {
            "key": "key:1", "limit": 50,
        })

    @pytest.mark.asyncio
    async def test_send_and_wait_no_run_id(self):
        """If sessions.send returns no runId, return immediately."""
        client = OpenClawClient("ws://test", token="t")
        client._rpc = AsyncMock(return_value={"text": "instant response"})
        result = await client.send_and_wait("key:1", "hello")
        assert result["text"] == "instant response"


class TestOpenClawClientLifecycle:
    @pytest.mark.asyncio
    async def test_close_cancels_listener_and_ws(self):
        client = OpenClawClient("ws://test", token="t")
        client._ws = AsyncMock()
        mock_task = MagicMock()
        client._listener_task = mock_task

        await client.close()
        mock_task.cancel.assert_called_once()
        client._ws.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_no_ws(self):
        """close() handles None _ws gracefully."""
        client = OpenClawClient("ws://test", token="t")
        # Should not raise
        await client.close()

    @pytest.mark.asyncio
    async def test_close_no_listener_task(self):
        """close() handles None _listener_task gracefully."""
        client = OpenClawClient("ws://test", token="t")
        client._ws = AsyncMock()
        await client.close()
        client._ws.close.assert_called_once()


class TestOpenClawClientRPC:
    @pytest.mark.asyncio
    async def test_rpc_sends_correct_format(self):
        """_rpc sends a properly formatted request and resolves the future."""
        client = OpenClawClient("ws://test", token="t")
        mock_ws = AsyncMock()
        client._ws = mock_ws

        # We need to simulate the response being set on the future
        original_send = mock_ws.send

        async def fake_send(data):
            msg = json.loads(data)
            req_id = msg["id"]
            # Simulate the listener resolving this future
            if req_id in client._pending:
                client._pending[req_id].set_result({
                    "ok": True, "payload": {"result": "success"},
                })

        mock_ws.send = AsyncMock(side_effect=fake_send)

        result = await client._rpc("test.method", {"param": "value"})
        assert result == {"result": "success"}

    @pytest.mark.asyncio
    async def test_rpc_raises_on_failure(self):
        """_rpc raises RuntimeError when response ok=False."""
        client = OpenClawClient("ws://test", token="t")
        mock_ws = AsyncMock()
        client._ws = mock_ws

        async def fake_send(data):
            msg = json.loads(data)
            req_id = msg["id"]
            if req_id in client._pending:
                client._pending[req_id].set_result({
                    "ok": False, "error": "not found",
                })

        mock_ws.send = AsyncMock(side_effect=fake_send)

        with pytest.raises(RuntimeError, match="RPC test.method failed"):
            await client._rpc("test.method", {})


# ---------------------------------------------------------------------------
# Aggregate reward tests (from runner)
# ---------------------------------------------------------------------------


class TestAggregateReward:
    def test_empty_results(self):
        from tests.scenarios.runner import compute_aggregate_reward
        assert compute_aggregate_reward([]) == 0.0

    def test_all_passing(self):
        from tests.scenarios.runner import compute_aggregate_reward
        results = [
            ScenarioResult(
                name="a", passed=True, steps=[], trace_summary={},
                trace_assertions=[], duration_ms=100, errors=[],
                reward_score=1.0,
            ),
            ScenarioResult(
                name="b", passed=True, steps=[], trace_summary={},
                trace_assertions=[], duration_ms=200, errors=[],
                reward_score=1.0,
            ),
        ]
        assert compute_aggregate_reward(results) == 1.0

    def test_mixed_results(self):
        from tests.scenarios.runner import compute_aggregate_reward
        results = [
            ScenarioResult(
                name="a", passed=True, steps=[], trace_summary={},
                trace_assertions=[], duration_ms=100, errors=[],
                reward_score=1.0,
            ),
            ScenarioResult(
                name="b", passed=False, steps=[], trace_summary={},
                trace_assertions=[], duration_ms=200, errors=[],
                reward_score=0.0,
            ),
        ]
        assert compute_aggregate_reward(results) == 0.5

    def test_single_result(self):
        from tests.scenarios.runner import compute_aggregate_reward
        results = [
            ScenarioResult(
                name="x", passed=True, steps=[], trace_summary={},
                trace_assertions=[], duration_ms=50, errors=[],
                reward_score=0.75,
            ),
        ]
        assert compute_aggregate_reward(results) == 0.75


# ---------------------------------------------------------------------------
# Runner registry tests
# ---------------------------------------------------------------------------


class TestRunner:
    def test_register_decorator(self):
        from tests.scenarios.runner import SCENARIOS
        # BasicMemoryScenario should be registered
        assert "basic_memory" in SCENARIOS

    def test_all_known_scenarios_registered(self):
        from tests.scenarios.runner import SCENARIOS
        expected = {
            "basic_memory", "context_lifecycle", "multi_turn_memory",
            "goal_driven", "procedure_execution", "guard_check",
            "subagent_lifecycle",
        }
        for name in expected:
            assert name in SCENARIOS, f"Scenario '{name}' not registered"

    def test_required_phase_attribute(self):
        """Verify all registered scenarios have required_phase."""
        from tests.scenarios.runner import SCENARIOS
        for name, cls in SCENARIOS.items():
            assert hasattr(cls, "required_phase"), f"{name} missing required_phase"
            assert isinstance(cls.required_phase, int), f"{name}.required_phase is not int"

    def test_name_attribute_matches_key(self):
        """Each registered scenario's name matches its registry key."""
        from tests.scenarios.runner import SCENARIOS
        for key, cls in SCENARIOS.items():
            assert cls.name == key, f"Registry key '{key}' != cls.name '{cls.name}'"

    def test_register_adds_to_scenarios(self):
        """The register decorator adds to the SCENARIOS dict."""
        from tests.scenarios.runner import register, SCENARIOS

        class _TestOnlyScenario:
            name = "_test_only_temp"

        register(_TestOnlyScenario)
        assert "_test_only_temp" in SCENARIOS
        # Clean up
        del SCENARIOS["_test_only_temp"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _async_return(value):
    """Helper to make a coroutine that returns a value (for patching awaitable)."""
    return value
