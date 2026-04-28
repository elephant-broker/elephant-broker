"""Tests for LLM client adapter."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

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


class TestLLMClientRetryBehavior:
    """TF-04-015 #1457: pin the actual retry policy in
    ``LLMClient._post_with_retry`` (client.py:38-67).

    The flow doc historically claimed a generic "retries on transient
    failures" policy. The implementation only retries on HTTP 429
    (Too Many Requests) with backoffs ``[1.0, 2.0, 4.0]s``; every other
    non-2xx status raises immediately via ``response.raise_for_status()``
    at line 44. These two tests pin both branches so a future widening
    or narrowing of the retry policy is caught.
    """

    async def test_llm_retry_behavior_429_then_200_succeeds(self, client, monkeypatch):
        """429 retries: a 429 followed by a 200 succeeds, with two POSTs."""
        # Patch out the backoff sleep so the test does not actually wait
        # 1.0s between attempts. The real sleep call is inside
        # ``_post_with_retry`` via ``asyncio.sleep``.
        async def _no_sleep(_delay):
            return None

        monkeypatch.setattr(
            "elephantbroker.runtime.adapters.llm.client.asyncio.sleep", _no_sleep,
        )
        responses = [
            httpx.Response(
                429, json={"error": "rate limited"},
                request=httpx.Request("POST", "http://test"),
            ),
            _make_response("Hello after retry!"),
        ]
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.side_effect = responses
            result = await client.complete("sys", "usr")
        assert result == "Hello after retry!"
        assert mock_post.call_count == 2

    async def test_llm_retry_behavior_502_raises_immediately(self, client):
        """502 does NOT retry: a single POST raises HTTPStatusError."""
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = httpx.Response(
                502, json={"error": "bad gateway"},
                request=httpx.Request("POST", "http://test"),
            )
            with pytest.raises(httpx.HTTPStatusError):
                await client.complete("sys", "usr")
        assert mock_post.call_count == 1


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

    async def test_parses_markdown_fenced_response(self, client):
        """LiteLLM Gemini backends wrap JSON in ```json...``` fences; the
        client must strip the fences before json.loads (Task #36, follow-up
        to 1e0cb47). Prior to this fix, extract_facts on staging was silently
        losing every fact because complete_json raised JSONDecodeError on
        fenced content."""
        fenced = '```json\n{"facts": [{"text": "hello"}]}\n```'
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response(fenced)
            result = await client.complete_json("sys", "usr")
            assert result == {"facts": [{"text": "hello"}]}

    async def test_parses_fence_without_language_tag(self, client):
        fenced = '```\n{"count": 3}\n```'
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response(fenced)
            result = await client.complete_json("sys", "usr")
            assert result == {"count": 3}

    async def test_unfenced_json_still_parses(self, client):
        """Backward compat: stripper is a no-op on fence-free content."""
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response('{"count": 42}')
            result = await client.complete_json("sys", "usr")
            assert result == {"count": 42}

    async def test_invalid_json_log_includes_content_snippet(self, client, caplog):
        """On parse failure, the warning log must include a truncated
        content snippet so operators grepping journalctl can see what the
        proxy actually returned. Historically the log only showed the
        JSONDecodeError message which was uniformly 'Expecting value: line
        1 column 1 (char 0)' for every fenced-content failure, giving no
        hint that fences were the problem."""
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response("garbage-not-json")
            with caplog.at_level("WARNING", logger="elephantbroker.adapters.llm"):
                with pytest.raises(json.JSONDecodeError):
                    await client.complete_json("sys", "usr")
            assert "garbage-not-json" in caplog.text

    async def test_temperature_is_zero(self, client):
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response('{}')
            await client.complete_json("sys", "usr")
            payload = mock_post.call_args.kwargs["json"]
            assert payload["temperature"] == 0.0


class TestLLMClientMetrics:
    """Gap #4: inc_llm_call must fire on complete/complete_json success + error paths."""

    async def test_complete_success_emits_metric(self, config):
        """inc_llm_call('complete', 'success', model) fires on successful complete()."""
        metrics = MagicMock()
        client = LLMClient(config, metrics=metrics)
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response("Hello!")
            await client.complete("sys", "usr")
        metrics.inc_llm_call.assert_called_once_with("complete", "success", client._model)

    async def test_complete_error_emits_metric(self, config):
        """inc_llm_call('complete', 'error', model) fires on failed complete()."""
        metrics = MagicMock()
        client = LLMClient(config, metrics=metrics)
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = httpx.Response(
                500, json={"error": "fail"}, request=httpx.Request("POST", "http://test"),
            )
            with pytest.raises(httpx.HTTPStatusError):
                await client.complete("sys", "usr")
        metrics.inc_llm_call.assert_called_once_with("complete", "error", client._model)

    async def test_complete_json_success_emits_metric(self, config):
        """inc_llm_call('complete_json', 'success', model) fires on successful complete_json()."""
        metrics = MagicMock()
        client = LLMClient(config, metrics=metrics)
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response('{"count": 42}')
            await client.complete_json("sys", "usr")
        metrics.inc_llm_call.assert_called_once_with("complete_json", "success", client._model)

    async def test_complete_json_error_emits_metric(self, config):
        """inc_llm_call('complete_json', 'error', model) fires on failed complete_json()."""
        metrics = MagicMock()
        client = LLMClient(config, metrics=metrics)
        with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = _make_response("not-json")
            with pytest.raises(json.JSONDecodeError):
                await client.complete_json("sys", "usr")
        metrics.inc_llm_call.assert_called_once_with("complete_json", "error", client._model)


class TestClose:
    async def test_close_closes_client(self, client):
        with patch.object(client._client, "aclose", new_callable=AsyncMock) as mock_close:
            await client.close()
            mock_close.assert_called_once()
