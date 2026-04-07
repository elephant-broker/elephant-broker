"""Tests for LLM client adapter."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from elephantbroker.runtime.adapters.llm.client import LLMClient
from elephantbroker.schemas.config import LLMConfig


def _make_response(content: str, status_code: int = 200, usage: dict | None = None) -> httpx.Response:
    body = {
        "choices": [{"message": {"content": content}}],
        "usage": usage or {"prompt_tokens": 10, "completion_tokens": 5},
    }
    return httpx.Response(status_code, json=body, request=httpx.Request("POST", "http://test"))


@pytest.fixture
def config():
    return LLMConfig(api_key="test-key")


@pytest.fixture
def client(config):
    return LLMClient(config)


class TestLLMClientInit:
    def test_init_creates_client(self, client):
        assert client._client is not None
        # Default LLMConfig.model is "openai/gemini/gemini-2.5-pro" — Cognee requires
        # the prefix; LLMClient strips it before sending to LiteLLM (see _model).
        assert client._config.model == "openai/gemini/gemini-2.5-pro"

    def test_init_sets_auth_header(self, client):
        assert client._client.headers["authorization"] == "Bearer test-key"


class TestComplete:
    async def test_sends_correct_request(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response("Hello!")
            await client.complete("You are helpful", "Say hi")
            call_args = mock_post.call_args
            assert "/chat/completions" in call_args.args[0]
            payload = call_args.kwargs["json"]
            assert payload["model"] == "gemini/gemini-2.5-pro"
            assert len(payload["messages"]) == 2
            assert payload["messages"][0]["role"] == "system"
            assert payload["messages"][1]["role"] == "user"

    async def test_returns_content(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response("Hello!")
            result = await client.complete("sys", "usr")
            assert result == "Hello!"

    async def test_custom_max_tokens(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response("ok")
            await client.complete("sys", "usr", max_tokens=100)
            payload = mock_post.call_args.kwargs["json"]
            assert payload["max_tokens"] == 100

    async def test_custom_temperature(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response("ok")
            await client.complete("sys", "usr", temperature=0.5)
            payload = mock_post.call_args.kwargs["json"]
            assert payload["temperature"] == 0.5

    async def test_http_error_raises(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = httpx.Response(
                500, json={"error": "fail"}, request=httpx.Request("POST", "http://test")
            )
            with pytest.raises(httpx.HTTPStatusError):
                await client.complete("sys", "usr")


class TestCompleteJson:
    async def test_with_schema(self, client):
        schema = {"type": "object", "properties": {"facts": {"type": "array"}}}
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response('{"facts": []}')
            await client.complete_json("sys", "usr", json_schema=schema)
            payload = mock_post.call_args.kwargs["json"]
            assert payload["response_format"]["type"] == "json_schema"
            assert payload["response_format"]["json_schema"]["schema"] == schema

    async def test_without_schema(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response('{"key": "value"}')
            await client.complete_json("sys", "usr")
            payload = mock_post.call_args.kwargs["json"]
            assert "response_format" not in payload

    async def test_parses_response(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response('{"count": 42}')
            result = await client.complete_json("sys", "usr")
            assert result == {"count": 42}

    async def test_invalid_json_raises(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response("not json at all")
            with pytest.raises(json.JSONDecodeError):
                await client.complete_json("sys", "usr")

    async def test_temperature_is_zero(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response('{}')
            await client.complete_json("sys", "usr")
            payload = mock_post.call_args.kwargs["json"]
            assert payload["temperature"] == 0.0


class TestClose:
    async def test_close_closes_client(self, client):
        with patch.object(client._client, "aclose", new_callable=AsyncMock) as mock_close:
            await client.close()
            mock_close.assert_called_once()
