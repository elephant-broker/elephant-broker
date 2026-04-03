"""Tests for config schemas."""
import os

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.config import CogneeConfig, ElephantBrokerConfig, InfraConfig, LLMConfig


class TestCogneeConfig:
    def test_defaults(self):
        c = CogneeConfig()
        assert c.neo4j_uri == "bolt://localhost:7687"
        assert c.default_dataset == "elephantbroker"

    def test_embedding_defaults(self):
        c = CogneeConfig()
        assert c.embedding_provider == "openai"
        assert c.embedding_model == "openai/text-embedding-3-large"
        assert c.embedding_endpoint == "http://localhost:8811/v1"
        assert c.embedding_api_key == ""
        assert c.embedding_dimensions == 1024

    def test_embedding_dimensions_must_be_positive(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            CogneeConfig(embedding_dimensions=0)


class TestLLMConfig:
    def test_defaults(self):
        c = LLMConfig()
        assert c.model == "gemini/gemini-2.5-pro"
        assert c.endpoint == "http://localhost:8811/v1"
        assert c.api_key == ""
        assert c.max_tokens == 8192
        assert c.temperature == 0.1
        assert c.extraction_max_input_tokens == 4000
        assert c.extraction_max_output_tokens == 16384
        assert c.extraction_max_facts_per_batch == 10
        assert c.summarization_max_output_tokens == 200
        assert c.summarization_min_artifact_chars == 500
        assert c.ingest_batch_size == 6
        assert c.ingest_batch_timeout_seconds == 60.0
        assert c.ingest_buffer_ttl_seconds == 300
        assert c.extraction_context_facts == 20
        assert c.extraction_context_ttl_seconds == 3600

    def test_custom_values(self):
        c = LLMConfig(model="gpt-4o", endpoint="https://api.openai.com/v1", api_key="sk-test")
        assert c.model == "gpt-4o"
        assert c.endpoint == "https://api.openai.com/v1"
        assert c.api_key == "sk-test"


class TestInfraConfig:
    def test_defaults(self):
        c = InfraConfig()
        assert c.redis_url == "redis://localhost:6379"
        assert c.otel_endpoint is None


class TestElephantBrokerConfig:
    def test_defaults(self):
        c = ElephantBrokerConfig()
        assert c.default_profile == "coding"
        assert c.enable_guards is True
        assert c.max_concurrent_sessions == 100

    def test_llm_always_created(self):
        c = ElephantBrokerConfig()
        assert c.llm is not None
        assert isinstance(c.llm, LLMConfig)
        assert c.llm.model == "gemini/gemini-2.5-pro"

    def test_max_sessions_minimum(self):
        with pytest.raises(ValidationError):
            ElephantBrokerConfig(max_concurrent_sessions=0)

    def test_json_round_trip(self):
        c = ElephantBrokerConfig(default_profile="research", enable_trace_ledger=False)
        data = c.model_dump(mode="json")
        restored = ElephantBrokerConfig.model_validate(data)
        assert restored.default_profile == "research"
        assert restored.enable_trace_ledger is False

    def test_json_round_trip_with_llm(self):
        c = ElephantBrokerConfig(llm=LLMConfig(model="gpt-4o", api_key="sk-test"))
        data = c.model_dump(mode="json")
        restored = ElephantBrokerConfig.model_validate(data)
        assert restored.llm is not None
        assert restored.llm.model == "gpt-4o"
        assert restored.llm.api_key == "sk-test"

    def test_from_env_defaults(self):
        # Clear any EB_ vars that might exist
        env_keys = [k for k in os.environ if k.startswith("EB_")]
        saved = {k: os.environ.pop(k) for k in env_keys}
        try:
            c = ElephantBrokerConfig.from_env()
            assert c.default_profile == "coding"
            assert c.cognee.neo4j_uri == "bolt://localhost:7687"
            assert c.infra.redis_url == "redis://localhost:6379"
            assert c.enable_guards is True
        finally:
            os.environ.update(saved)

    def test_from_env_always_creates_llm(self):
        env_keys = [k for k in os.environ if k.startswith("EB_")]
        saved = {k: os.environ.pop(k) for k in env_keys}
        try:
            c = ElephantBrokerConfig.from_env()
            assert c.llm is not None
            assert c.llm.model == "gemini/gemini-2.5-pro"
        finally:
            os.environ.update(saved)

    def test_from_env_with_llm_vars(self):
        env_keys = [k for k in os.environ if k.startswith("EB_")]
        saved = {k: os.environ.pop(k) for k in env_keys}
        try:
            os.environ["EB_LLM_MODEL"] = "gpt-4o"
            os.environ["EB_LLM_ENDPOINT"] = "https://api.openai.com/v1"
            os.environ["EB_LLM_API_KEY"] = "sk-test"
            c = ElephantBrokerConfig.from_env()
            assert c.llm is not None
            assert c.llm.model == "gpt-4o"
            assert c.llm.endpoint == "https://api.openai.com/v1"
            assert c.llm.api_key == "sk-test"
        finally:
            for k in ["EB_LLM_MODEL", "EB_LLM_ENDPOINT", "EB_LLM_API_KEY"]:
                os.environ.pop(k, None)
            os.environ.update(saved)

    def test_from_env_overrides(self):
        env_keys = [k for k in os.environ if k.startswith("EB_")]
        saved = {k: os.environ.pop(k) for k in env_keys}
        try:
            os.environ["EB_DEFAULT_PROFILE"] = "research"
            os.environ["EB_NEO4J_URI"] = "bolt://prod:7687"
            os.environ["EB_ENABLE_GUARDS"] = "false"
            os.environ["EB_MAX_CONCURRENT_SESSIONS"] = "50"
            c = ElephantBrokerConfig.from_env()
            assert c.default_profile == "research"
            assert c.cognee.neo4j_uri == "bolt://prod:7687"
            assert c.enable_guards is False
            assert c.max_concurrent_sessions == 50
        finally:
            for k in ["EB_DEFAULT_PROFILE", "EB_NEO4J_URI", "EB_ENABLE_GUARDS", "EB_MAX_CONCURRENT_SESSIONS"]:
                os.environ.pop(k, None)
            os.environ.update(saved)

    def test_from_env_embedding_overrides(self):
        env_keys = [k for k in os.environ if k.startswith("EB_")]
        saved = {k: os.environ.pop(k) for k in env_keys}
        try:
            os.environ["EB_EMBEDDING_PROVIDER"] = "custom"
            os.environ["EB_EMBEDDING_MODEL"] = "my-model"
            os.environ["EB_EMBEDDING_ENDPOINT"] = "http://embed:9999/v1"
            os.environ["EB_EMBEDDING_API_KEY"] = "sk-test"
            os.environ["EB_EMBEDDING_DIMENSIONS"] = "512"
            c = ElephantBrokerConfig.from_env()
            assert c.cognee.embedding_provider == "custom"
            assert c.cognee.embedding_model == "my-model"
            assert c.cognee.embedding_endpoint == "http://embed:9999/v1"
            assert c.cognee.embedding_api_key == "sk-test"
            assert c.cognee.embedding_dimensions == 512
        finally:
            for k in ["EB_EMBEDDING_PROVIDER", "EB_EMBEDDING_MODEL", "EB_EMBEDDING_ENDPOINT",
                       "EB_EMBEDDING_API_KEY", "EB_EMBEDDING_DIMENSIONS"]:
                os.environ.pop(k, None)
            os.environ.update(saved)


class TestRerankerConfig:
    def test_defaults(self):
        from elephantbroker.schemas.config import RerankerConfig
        r = RerankerConfig()
        assert r.endpoint == "http://localhost:1235"
        assert r.model == "Qwen/Qwen3-Reranker-4B"

class TestLLMConfigValidation:
    def test_max_tokens_minimum(self):
        with pytest.raises(ValidationError):
            LLMConfig(max_tokens=0)

    def test_temperature_range(self):
        c = LLMConfig(temperature=0.0)
        assert c.temperature == 0.0
        c2 = LLMConfig(temperature=2.0)
        assert c2.temperature == 2.0
        with pytest.raises(ValidationError):
            LLMConfig(temperature=2.1)

    def test_from_env_api_key_fallback(self):
        """EB_LLM_API_KEY falls back to EB_EMBEDDING_API_KEY."""
        import os
        env_keys = [k for k in os.environ if k.startswith("EB_")]
        saved = {k: os.environ.pop(k) for k in env_keys}
        try:
            os.environ["EB_EMBEDDING_API_KEY"] = "embed-key"
            c = ElephantBrokerConfig.from_env()
            assert c.llm.api_key == "embed-key"
        finally:
            os.environ.pop("EB_EMBEDDING_API_KEY", None)
            os.environ.update(saved)

    def test_from_env_llm_key_takes_precedence(self):
        """EB_LLM_API_KEY takes precedence over EB_EMBEDDING_API_KEY."""
        import os
        env_keys = [k for k in os.environ if k.startswith("EB_")]
        saved = {k: os.environ.pop(k) for k in env_keys}
        try:
            os.environ["EB_EMBEDDING_API_KEY"] = "embed-key"
            os.environ["EB_LLM_API_KEY"] = "llm-key"
            c = ElephantBrokerConfig.from_env()
            assert c.llm.api_key == "llm-key"
        finally:
            os.environ.pop("EB_EMBEDDING_API_KEY", None)
            os.environ.pop("EB_LLM_API_KEY", None)
            os.environ.update(saved)

class TestInfraConfigMetrics:
    def test_metrics_ttl_default(self):
        from elephantbroker.schemas.config import InfraConfig
        c = InfraConfig()
        assert c.metrics_ttl_seconds == 3600
