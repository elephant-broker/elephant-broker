"""LLM client adapter for chat completions via OpenAI-compatible endpoint."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx

from elephantbroker.runtime.adapters.llm.util import strip_markdown_fences
from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.config import LLMConfig

logger = logging.getLogger("elephantbroker.adapters.llm")


class LLMClient:
    """HTTP client for LLM chat completions."""

    def __init__(self, config: LLMConfig) -> None:
        self._config = config
        # LiteLLM expects e.g. "gemini/gemini-2.5-pro", but Cognee requires
        # the "openai/" prefix in the config.  Strip it before sending.
        model = config.model
        if model.startswith("openai/"):
            model = model[len("openai/"):]
        self._model = model
        self._endpoint = config.endpoint.rstrip("/")
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if config.api_key:
            headers["Authorization"] = f"Bearer {config.api_key}"
        self._client = httpx.AsyncClient(headers=headers, timeout=120.0)
        self._max_retries = 3
        self._retry_backoffs = [1.0, 2.0, 4.0]

    async def _post_with_retry(self, url: str, payload: dict) -> httpx.Response:
        """POST with exponential backoff on 429 Too Many Requests."""
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            response = await self._client.post(url, json=payload)
            if response.status_code != 429:
                response.raise_for_status()
                return response
            # 429 — retry with backoff
            if attempt < self._max_retries:
                delay = self._retry_backoffs[attempt]
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        pass
                logger.warning(
                    "LLM 429 Too Many Requests (attempt %d/%d), retrying in %.1fs",
                    attempt + 1, self._max_retries, delay,
                )
                await asyncio.sleep(delay)
            else:
                last_exc = httpx.HTTPStatusError(
                    f"429 after {self._max_retries} retries",
                    request=response.request, response=response,
                )
        if last_exc is None:
            raise RuntimeError("LLM request failed with no exception captured")
        raise last_exc

    @traced
    async def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        """Send a chat completion request and return the response text."""
        effective_max_tokens = max_tokens or self._config.max_tokens
        effective_temperature = temperature if temperature is not None else self._config.temperature

        payload: dict[str, Any] = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": effective_max_tokens,
            "temperature": effective_temperature,
        }

        logger.debug(
            "LLM complete: model=%s, max_tokens=%d, temperature=%.2f, system_len=%d, user_len=%d",
            self._model, effective_max_tokens, effective_temperature,
            len(system_prompt), len(user_prompt),
        )

        response = await self._post_with_retry(f"{self._endpoint}/chat/completions", payload)

        data = response.json()
        content = data["choices"][0]["message"]["content"]

        usage = data.get("usage", {})
        logger.info(
            "LLM completion received: input_tokens=%s, output_tokens=%s",
            usage.get("prompt_tokens", "?"), usage.get("completion_tokens", "?"),
        )

        return content

    @traced
    async def complete_json(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_tokens: int | None = None,
        json_schema: dict | None = None,
    ) -> dict:
        """Send a chat completion with JSON response format."""
        effective_max_tokens = max_tokens or self._config.max_tokens

        payload: dict[str, Any] = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": effective_max_tokens,
            "temperature": 0.0,
        }

        if json_schema is not None:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "response", "schema": json_schema},
            }

        logger.debug(
            "LLM complete_json: model=%s, max_tokens=%d, has_schema=%s",
            self._model, effective_max_tokens, json_schema is not None,
        )

        response = await self._post_with_retry(f"{self._endpoint}/chat/completions", payload)

        data = response.json()
        content = data["choices"][0]["message"]["content"]

        # Retry once if LLM returns empty/whitespace response
        if not content or not content.strip():
            logger.warning("LLM returned empty response, retrying once")
            response = await self._post_with_retry(f"{self._endpoint}/chat/completions", payload)
            data = response.json()
            content = data["choices"][0]["message"]["content"]

        # Staging LiteLLM proxy wraps Gemini responses in ```json...``` fences
        # even when response_format is set — observer found 26 "LLM returned
        # invalid JSON" warnings in a 2-hour window, including 10/10 empty
        # facts arrays on the hot extract_facts path. Shared helper lives in
        # adapters/llm/util.py so goal_refinement's cheap-model path and this
        # high-level LLMClient path use identical fence handling. No-op on
        # fence-free content (backward compat).
        stripped = strip_markdown_fences(content)
        try:
            return json.loads(stripped)
        except json.JSONDecodeError as exc:
            logger.warning(
                "LLM returned invalid JSON: %s | content[:200]=%r", exc, content[:200]
            )
            raise

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()
