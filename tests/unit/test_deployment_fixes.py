"""Tests for deployment fixes §19-§36.

Covers: LLMClient openai/ prefix stripping, _ensure_session_id fallback,
AgentMessage extra="allow" round-trip, from_yaml env var overrides,
assemble response shape, RerankerConfig default, OTEL ImportError warning,
Cognee telemetry env var.
"""
from __future__ import annotations

import os
import uuid
from unittest.mock import patch

import pytest

from elephantbroker.schemas.config import (
    ElephantBrokerConfig,
    LLMConfig,
    RerankerConfig,
)
from elephantbroker.schemas.context import AgentMessage, AssembleResult


# ---------------------------------------------------------------------------
# Fix #19: LLMClient strips openai/ prefix
# ---------------------------------------------------------------------------

class TestLLMClientPrefixStrip:
    """Deployment Fix #19: LLMClient must strip openai/ prefix before sending to LiteLLM."""

    def test_strips_openai_prefix(self):
        from elephantbroker.runtime.adapters.llm.client import LLMClient
        config = LLMConfig(model="openai/gemini/gemini-2.5-pro", api_key="k")
        client = LLMClient(config)
        assert client._model == "gemini/gemini-2.5-pro"

    def test_no_prefix_unchanged(self):
        from elephantbroker.runtime.adapters.llm.client import LLMClient
        config = LLMConfig(model="gemini/gemini-2.5-pro", api_key="k")
        client = LLMClient(config)
        assert client._model == "gemini/gemini-2.5-pro"

    def test_double_openai_prefix_strips_once(self):
        from elephantbroker.runtime.adapters.llm.client import LLMClient
        config = LLMConfig(model="openai/openai/model", api_key="k")
        client = LLMClient(config)
        assert client._model == "openai/model"


# ---------------------------------------------------------------------------
# Fix #20: _ensure_session_id empty→fallback
# ---------------------------------------------------------------------------

class TestEnsureSessionId:
    """Deployment Fix #20: Empty session_id generates a fallback UUID."""

    def _make_lifecycle(self):
        from elephantbroker.runtime.context.lifecycle import ContextLifecycle
        return ContextLifecycle(gateway_id="test-gw")

    def test_non_empty_passthrough(self):
        lc = self._make_lifecycle()
        sid = "real-session-id"
        assert lc._ensure_session_id(sid, "sk") == sid

    def test_empty_generates_fallback(self):
        lc = self._make_lifecycle()
        result = lc._ensure_session_id("", "sk")
        assert result != ""
        # Should be a valid UUID
        uuid.UUID(result)

    def test_fallback_cached_per_session_key(self):
        lc = self._make_lifecycle()
        first = lc._ensure_session_id("", "sk1")
        second = lc._ensure_session_id("", "sk1")
        assert first == second

    def test_different_session_keys_different_fallbacks(self):
        lc = self._make_lifecycle()
        a = lc._ensure_session_id("", "sk-a")
        b = lc._ensure_session_id("", "sk-b")
        assert a != b

    def test_dict_pop_clears_cache(self):
        """Fallback cache entry is removed when dict.pop() is called (as bootstrap does on real ID)."""
        lc = self._make_lifecycle()
        lc._ensure_session_id("", "sk")
        assert "sk" in lc._fallback_session_ids
        lc._fallback_session_ids.pop("sk", None)
        assert "sk" not in lc._fallback_session_ids

    def test_cache_bounded_at_128(self):
        """Fallback cache should not grow beyond 128 entries."""
        lc = self._make_lifecycle()
        for i in range(200):
            lc._ensure_session_id("", f"sk-{i}")
        assert len(lc._fallback_session_ids) <= 128


# ---------------------------------------------------------------------------
# Fix #22: AgentMessage extra="allow" round-trip
# ---------------------------------------------------------------------------

class TestAgentMessageExtraAllow:
    """Deployment Fix #22: AgentMessage preserves provider-specific fields."""

    def test_extra_fields_preserved(self):
        msg = AgentMessage.model_validate({
            "role": "user",
            "content": "hello",
            "tool_use_id": "toolu_123",
        })
        assert msg.role == "user"
        assert msg.tool_use_id == "toolu_123"  # type: ignore[attr-defined]

    def test_round_trip_with_extras(self):
        data = {
            "role": "tool",
            "content": "result",
            "tool_use_id": "toolu_456",
            "custom_field": "preserved",
        }
        msg = AgentMessage.model_validate(data)
        dumped = msg.model_dump(mode="json", exclude_none=True)
        assert dumped["tool_use_id"] == "toolu_456"
        assert dumped["custom_field"] == "preserved"

    def test_multipart_content_passthrough(self):
        """Fix #16: content: Any passes through arrays intact."""
        data = {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "toolu_1", "name": "search"}],
        }
        msg = AgentMessage.model_validate(data)
        assert isinstance(msg.content, list)
        assert msg.content[0]["type"] == "tool_use"


# ---------------------------------------------------------------------------
# Fix #28 + TODO-3-010: from_yaml env var overrides
# ---------------------------------------------------------------------------

class TestFromYamlEnvOverrides:
    """Deployment Fix #28: from_yaml() respects env vars for OTEL, reranker, HITL, LLM."""

    def _write_yaml(self, tmp_path, content: str) -> str:
        p = tmp_path / "config.yaml"
        p.write_text(content)
        return str(p)

    def test_otel_endpoint_override(self, tmp_path):
        path = self._write_yaml(tmp_path, "infra:\n  otel_endpoint: null\n")
        with patch.dict(os.environ, {"EB_OTEL_ENDPOINT": "http://otel:4317"}, clear=False):
            cfg = ElephantBrokerConfig.from_yaml(path)
        assert cfg.infra.otel_endpoint == "http://otel:4317"

    def test_reranker_endpoint_override(self, tmp_path):
        path = self._write_yaml(tmp_path, "reranker:\n  endpoint: http://old:1235\n")
        with patch.dict(os.environ, {"EB_RERANKER_ENDPOINT": "http://new:1235"}, clear=False):
            cfg = ElephantBrokerConfig.from_yaml(path)
        assert cfg.reranker.endpoint == "http://new:1235"

    def test_reranker_api_key_override(self, tmp_path):
        path = self._write_yaml(tmp_path, "reranker:\n  api_key: old\n")
        with patch.dict(os.environ, {"EB_RERANKER_API_KEY": "new-key"}, clear=False):
            cfg = ElephantBrokerConfig.from_yaml(path)
        assert cfg.reranker.api_key == "new-key"

    def test_hitl_callback_secret_override(self, tmp_path):
        path = self._write_yaml(tmp_path, "hitl:\n  callback_hmac_secret: old\n")
        with patch.dict(os.environ, {"EB_HITL_CALLBACK_SECRET": "new-secret"}, clear=False):
            cfg = ElephantBrokerConfig.from_yaml(path)
        assert cfg.hitl.callback_hmac_secret == "new-secret"

    def test_llm_model_override(self, tmp_path):
        path = self._write_yaml(tmp_path, "llm:\n  model: old-model\n")
        with patch.dict(os.environ, {"EB_LLM_MODEL": "openai/gpt-4o"}, clear=False):
            cfg = ElephantBrokerConfig.from_yaml(path)
        assert cfg.llm.model == "openai/gpt-4o"

    def test_llm_endpoint_override(self, tmp_path):
        path = self._write_yaml(tmp_path, "llm:\n  endpoint: http://old:8811/v1\n")
        with patch.dict(os.environ, {"EB_LLM_ENDPOINT": "http://new:8811/v1"}, clear=False):
            cfg = ElephantBrokerConfig.from_yaml(path)
        assert cfg.llm.endpoint == "http://new:8811/v1"

    def test_no_env_uses_yaml_values(self, tmp_path):
        path = self._write_yaml(tmp_path, "llm:\n  model: yaml-model\n  endpoint: http://yaml:8811/v1\n")
        env = {k: v for k, v in os.environ.items() if not k.startswith("EB_")}
        with patch.dict(os.environ, env, clear=True):
            cfg = ElephantBrokerConfig.from_yaml(path)
        assert cfg.llm.model == "yaml-model"
        assert cfg.llm.endpoint == "http://yaml:8811/v1"


# ---------------------------------------------------------------------------
# Fix #21 + TODO-3-001: Assemble response shape
# ---------------------------------------------------------------------------

class TestAssembleResponseShape:
    """Fixes #21/#TODO-3-001: AssembleResult serialization preserves required fields."""

    def test_exclude_unset_keeps_messages_and_tokens(self):
        """exclude_unset=True must keep messages=[] and estimated_tokens=0."""
        result = AssembleResult(messages=[], estimated_tokens=0)
        data = result.model_dump(mode="json", exclude_none=True, exclude_unset=True)
        assert "messages" in data
        assert "estimated_tokens" in data
        assert data["messages"] == []
        assert data["estimated_tokens"] == 0

    def test_exclude_unset_strips_unset_optional(self):
        result = AssembleResult(messages=[], estimated_tokens=0)
        data = result.model_dump(mode="json", exclude_none=True, exclude_unset=True)
        # system_prompt_addition defaults to None, excluded by exclude_none
        assert "system_prompt_addition" not in data

    def test_exclude_defaults_would_strip_required(self):
        """Demonstrate the bug that exclude_defaults=True would cause."""
        result = AssembleResult(messages=[], estimated_tokens=0)
        data = result.model_dump(mode="json", exclude_none=True, exclude_defaults=True)
        # This is the bug: exclude_defaults strips fields equal to their default
        assert "messages" not in data or "estimated_tokens" not in data


# ---------------------------------------------------------------------------
# TODO-3-008: RerankerConfig default api_key
# ---------------------------------------------------------------------------

class TestRerankerConfigDefault:
    """TODO-3-008: RerankerConfig.api_key must default to empty string, not a hardcoded key."""

    def test_default_api_key_is_empty(self):
        cfg = RerankerConfig()
        assert cfg.api_key == ""
        assert "sk-" not in cfg.api_key


# ---------------------------------------------------------------------------
# TODO-3-005: OTEL ImportError warning
# ---------------------------------------------------------------------------

class TestOtelImportWarning:
    """TODO-3-005: Log warning when OTEL endpoint configured but exporter not installed."""

    def test_warns_on_missing_otel_exporter(self):
        import sys
        import logging
        from elephantbroker.schemas.config import InfraConfig
        from elephantbroker.runtime.observability import setup_tracing

        config = InfraConfig(otel_endpoint="http://otel:4317")
        # Force ImportError by temporarily removing the exporter module
        mod_key = "opentelemetry.exporter.otlp.proto.grpc.trace_exporter"
        saved = sys.modules.get(mod_key)
        sys.modules[mod_key] = None  # type: ignore[assignment]  # makes import raise ImportError
        try:
            with patch.object(
                logging.getLogger("elephantbroker.observability"), "warning"
            ) as mock_warn:
                setup_tracing(config, "test-gw")
                mock_warn.assert_called_once()
                assert "opentelemetry-exporter-otlp-proto-grpc" in mock_warn.call_args[0][0]
        finally:
            if saved is not None:
                sys.modules[mod_key] = saved
            else:
                sys.modules.pop(mod_key, None)


# ---------------------------------------------------------------------------
# Fix #30: Cognee telemetry env var set at package init time
# ---------------------------------------------------------------------------

class TestCogneeTelemetryEnvVar:
    """Deployment Fix #30: COGNEE_DISABLE_TELEMETRY must be set at import time."""

    def test_telemetry_env_var_set_after_import(self):
        import elephantbroker  # noqa: F401
        assert os.environ.get("COGNEE_DISABLE_TELEMETRY") == "true"
