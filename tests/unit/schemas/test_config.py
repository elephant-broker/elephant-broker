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
        assert c.embedding_model == "gemini/text-embedding-004"
        assert c.embedding_endpoint == "http://localhost:8811/v1"
        assert c.embedding_api_key == ""
        assert c.embedding_dimensions == 768

    def test_embedding_dimensions_must_be_positive(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            CogneeConfig(embedding_dimensions=0)


class TestLLMConfig:
    def test_defaults(self):
        c = LLMConfig()
        assert c.model == "openai/gemini/gemini-2.5-pro"
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
        assert c.llm.model == "openai/gemini/gemini-2.5-pro"

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
            assert c.llm.model == "openai/gemini/gemini-2.5-pro"
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


# =============================================================================
# from_yaml() env override coverage — locks the contract
# =============================================================================
#
# Before the fix, from_yaml() only env-overrode 14 hardcoded vars; the other ~40
# env vars from_env() reads were silently ignored. The bulk test below iterates
# through ENV_OVERRIDE_BINDINGS and verifies EVERY binding actually reaches its
# target field. Adding a new env var to from_env() now requires adding the
# matching binding here, or this test will fail.

class TestFromYamlEnvOverrides:
    """Verify from_yaml() applies env overrides for every binding in ENV_OVERRIDE_BINDINGS."""

    @pytest.fixture
    def yaml_path(self, tmp_path):
        """Minimal YAML — has explicit values for the fields the tests probe so we
        can distinguish 'YAML value' from 'env override applied'."""
        yaml_content = """
gateway:
  gateway_id: "yaml-gw"
  gateway_short_name: "yaml-short"
  org_id: "yaml-org"
  team_id: "yaml-team"
  agent_authority_level: 0
cognee:
  neo4j_uri: "bolt://yaml-neo4j:7687"
  neo4j_user: "yaml-user"
  neo4j_password: "yaml-password"
  qdrant_url: "http://yaml-qdrant:6333"
  embedding_model: "yaml-embed-model"
  embedding_dimensions: 512
  embedding_api_key: ""
llm:
  model: "yaml-llm-model"
  endpoint: "http://yaml-llm:8811/v1"
  api_key: ""
  max_tokens: 1234
  temperature: 0.5
infra:
  redis_url: "redis://yaml-redis:6379"
  log_level: "WARNING"
  trace:
    memory_max_events: 1000
    otel_logs_enabled: false
  clickhouse:
    enabled: false
    host: "yaml-ch"
    port: 9999
default_profile: "research"
max_concurrent_sessions: 25
enable_trace_ledger: true
"""
        path = tmp_path / "test.yaml"
        path.write_text(yaml_content.lstrip())
        return str(path)

    @pytest.fixture(autouse=True)
    def clean_env(self):
        """Save & restore EB_* env vars around each test for full isolation."""
        env_keys = [k for k in os.environ if k.startswith("EB_")]
        saved = {k: os.environ.pop(k) for k in env_keys}
        yield
        for k in [k for k in os.environ if k.startswith("EB_")]:
            os.environ.pop(k, None)
        os.environ.update(saved)

    # ----- Baseline -----

    def test_no_env_returns_yaml_values(self, yaml_path):
        """With no env vars set, every YAML value must reach the config object intact."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.gateway.gateway_id == "yaml-gw"
        assert cfg.gateway.org_id == "yaml-org"
        assert cfg.cognee.neo4j_password == "yaml-password"
        assert cfg.cognee.embedding_dimensions == 512
        assert cfg.llm.max_tokens == 1234
        assert cfg.llm.temperature == 0.5
        assert cfg.infra.log_level == "WARNING"
        assert cfg.infra.trace.memory_max_events == 1000
        assert cfg.default_profile == "research"

    # ----- Regression: previously-broken vars -----

    def test_neo4j_password_now_overrides(self, yaml_path):
        """Canonical regression: EB_NEO4J_PASSWORD was silently ignored before the fix."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_NEO4J_PASSWORD"] = "production-secret"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.cognee.neo4j_password == "production-secret"

    def test_log_level_now_overrides(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_LOG_LEVEL"] = "DEBUG"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.infra.log_level == "DEBUG"

    def test_embedding_model_now_overrides(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_EMBEDDING_MODEL"] = "openai/text-embedding-3-large"
        os.environ["EB_EMBEDDING_DIMENSIONS"] = "1024"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.cognee.embedding_model == "openai/text-embedding-3-large"
        assert cfg.cognee.embedding_dimensions == 1024

    def test_compaction_llm_model_now_overrides(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_COMPACTION_LLM_MODEL"] = "gemini/gemini-2.5-flash-lite"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.compaction_llm.model == "gemini/gemini-2.5-flash-lite"

    # ----- Type coercers -----

    def test_int_coercer(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_LLM_MAX_TOKENS"] = "4096"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.llm.max_tokens == 4096
        assert isinstance(cfg.llm.max_tokens, int)

    def test_float_coercer(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_LLM_TEMPERATURE"] = "0.7"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.llm.temperature == 0.7
        assert isinstance(cfg.llm.temperature, float)

    def test_bool_coercer_true(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_TRACE_OTEL_LOGS_ENABLED"] = "true"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.infra.trace.otel_logs_enabled is True

    def test_bool_coercer_false(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_ENABLE_TRACE_LEDGER"] = "false"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.enable_trace_ledger is False

    def test_bool_coercer_alternative_truthy_forms(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        for value in ("1", "yes", "on", "TRUE", "True", "YES"):
            os.environ["EB_CLICKHOUSE_ENABLED"] = value
            cfg = ElephantBrokerConfig.from_yaml(yaml_path)
            assert cfg.infra.clickhouse.enabled is True, f"failed for {value!r}"
        del os.environ["EB_CLICKHOUSE_ENABLED"]

    def test_bool_coercer_falsy_forms(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        for value in ("false", "0", "no", "off", "FALSE", "anything-else", ""):
            os.environ["EB_CLICKHOUSE_ENABLED"] = value
            cfg = ElephantBrokerConfig.from_yaml(yaml_path)
            assert cfg.infra.clickhouse.enabled is False, f"failed for {value!r}"
        del os.environ["EB_CLICKHOUSE_ENABLED"]

    def test_str_or_none_empty_becomes_none(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_OTEL_ENDPOINT"] = ""
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.infra.otel_endpoint is None

    def test_str_or_none_set_value(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_OTEL_ENDPOINT"] = "http://otel:4317"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.infra.otel_endpoint == "http://otel:4317"

    def test_str_or_none_org_id_empty_clears_to_none(self, yaml_path):
        """Setting EB_ORG_ID to empty string must produce None, not empty string,
        so consolidation/profile code paths see 'unset' rather than 'set to ""'."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_ORG_ID"] = ""
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.gateway.org_id is None

    # ----- Nested-path overrides (infra.trace.* and infra.clickhouse.*) -----

    def test_nested_path_infra_trace(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_TRACE_MEMORY_MAX_EVENTS"] = "5000"
        os.environ["EB_TRACE_OTEL_LOGS_ENABLED"] = "true"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.infra.trace.memory_max_events == 5000
        assert cfg.infra.trace.otel_logs_enabled is True

    def test_nested_path_infra_clickhouse(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_CLICKHOUSE_ENABLED"] = "true"
        os.environ["EB_CLICKHOUSE_HOST"] = "ch-prod"
        os.environ["EB_CLICKHOUSE_PORT"] = "8124"
        os.environ["EB_CLICKHOUSE_DATABASE"] = "eb_traces"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.infra.clickhouse.enabled is True
        assert cfg.infra.clickhouse.host == "ch-prod"
        assert cfg.infra.clickhouse.port == 8124
        assert cfg.infra.clickhouse.database == "eb_traces"

    # ----- API key fallback chains -----

    def test_api_key_fallback_llm_to_embedding(self, yaml_path):
        """When llm.api_key is empty in YAML and EB_EMBEDDING_API_KEY is set,
        llm.api_key should pick up the embedding key (mirrors from_env behavior)."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_EMBEDDING_API_KEY"] = "sk-shared-key"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.cognee.embedding_api_key == "sk-shared-key"
        assert cfg.llm.api_key == "sk-shared-key"

    def test_api_key_fallback_compaction_to_llm(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_LLM_API_KEY"] = "sk-llm"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.compaction_llm.api_key == "sk-llm"

    def test_api_key_fallback_successful_use_to_llm(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_LLM_API_KEY"] = "sk-llm"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.successful_use.api_key == "sk-llm"

    def test_api_key_fallback_blocker_extraction_to_llm(self, yaml_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_LLM_API_KEY"] = "sk-llm"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.blocker_extraction.api_key == "sk-llm"

    def test_api_key_full_chain_via_embedding_only(self, yaml_path):
        """Setting only EB_EMBEDDING_API_KEY must propagate through ALL 5 sections
        (cognee → llm → compaction_llm + successful_use + blocker_extraction)."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_EMBEDDING_API_KEY"] = "sk-master"
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.cognee.embedding_api_key == "sk-master"
        assert cfg.llm.api_key == "sk-master"
        assert cfg.compaction_llm.api_key == "sk-master"
        assert cfg.successful_use.api_key == "sk-master"
        assert cfg.blocker_extraction.api_key == "sk-master"

    def test_api_key_explicit_compaction_not_overridden_by_fallback(self, tmp_path):
        """An explicit compaction_llm.api_key in YAML must NOT be overwritten
        by the llm→compaction fallback chain."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        yaml_with_compaction = """
llm:
  api_key: "sk-llm-explicit"
compaction_llm:
  api_key: "sk-compaction-explicit"
"""
        path = tmp_path / "explicit.yaml"
        path.write_text(yaml_with_compaction.lstrip())
        cfg = ElephantBrokerConfig.from_yaml(str(path))
        assert cfg.llm.api_key == "sk-llm-explicit"
        assert cfg.compaction_llm.api_key == "sk-compaction-explicit"

    def test_api_key_no_fallback_when_nothing_set(self, yaml_path):
        """Without any env or explicit YAML keys, all api_keys remain empty (no spurious fallback)."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.cognee.embedding_api_key == ""
        assert cfg.llm.api_key == ""
        assert cfg.compaction_llm.api_key == ""
        assert cfg.successful_use.api_key == ""
        assert cfg.blocker_extraction.api_key == ""

    # ----- Validation propagation -----

    def test_invalid_int_raises(self, yaml_path):
        """Non-numeric env value for an int field must raise at load time."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_LLM_MAX_TOKENS"] = "not-a-number"
        with pytest.raises(ValueError):
            ElephantBrokerConfig.from_yaml(yaml_path)

    def test_validation_error_on_embedding_dimensions_zero(self, yaml_path):
        """Constraint violation (embedding_dimensions ge=1) must surface as ValidationError."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_EMBEDDING_DIMENSIONS"] = "0"
        with pytest.raises(ValidationError):
            ElephantBrokerConfig.from_yaml(yaml_path)

    def test_validation_error_on_temperature_out_of_range(self, yaml_path):
        """Constraint violation (temperature le=2.0) must surface as ValidationError."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_LLM_TEMPERATURE"] = "5.0"
        with pytest.raises(ValidationError):
            ElephantBrokerConfig.from_yaml(yaml_path)

    # ----- Back-compat: original 14 vars still work -----

    def test_back_compat_original_14_vars(self, yaml_path):
        """Regression check: the originally-supported 14 env vars must keep working
        the same way they did before the fix."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        os.environ["EB_GATEWAY_ID"] = "env-gw"
        os.environ["EB_ORG_ID"] = "env-org"
        os.environ["EB_TEAM_ID"] = "env-team"
        os.environ["EB_NEO4J_URI"] = "bolt://env-neo4j:7687"
        os.environ["EB_QDRANT_URL"] = "http://env-qdrant:6333"
        os.environ["EB_REDIS_URL"] = "redis://env-redis:6379"
        os.environ["EB_OTEL_ENDPOINT"] = "http://env-otel:4317"
        os.environ["EB_EMBEDDING_API_KEY"] = "env-embed-key"
        os.environ["EB_LLM_API_KEY"] = "env-llm-key"
        os.environ["EB_LLM_MODEL"] = "env-llm-model"
        os.environ["EB_LLM_ENDPOINT"] = "http://env-llm:8811/v1"
        os.environ["EB_RERANKER_ENDPOINT"] = "http://env-reranker:1235"
        os.environ["EB_RERANKER_API_KEY"] = "env-reranker-key"
        os.environ["EB_HITL_CALLBACK_SECRET"] = "env-hitl-secret"

        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        assert cfg.gateway.gateway_id == "env-gw"
        assert cfg.gateway.org_id == "env-org"
        assert cfg.gateway.team_id == "env-team"
        assert cfg.cognee.neo4j_uri == "bolt://env-neo4j:7687"
        assert cfg.cognee.qdrant_url == "http://env-qdrant:6333"
        assert cfg.infra.redis_url == "redis://env-redis:6379"
        assert cfg.infra.otel_endpoint == "http://env-otel:4317"
        assert cfg.cognee.embedding_api_key == "env-embed-key"
        assert cfg.llm.api_key == "env-llm-key"
        assert cfg.llm.model == "env-llm-model"
        assert cfg.llm.endpoint == "http://env-llm:8811/v1"
        assert cfg.reranker.endpoint == "http://env-reranker:1235"
        assert cfg.reranker.api_key == "env-reranker-key"
        assert cfg.hitl.callback_hmac_secret == "env-hitl-secret"

    # ----- Bulk: every binding actually applies -----

    def test_every_binding_applies(self, yaml_path):
        """Iterate through ENV_OVERRIDE_BINDINGS, set each var to a probe value,
        verify the value reaches the corresponding config field.

        This is the contract test: if anyone removes a binding without removing
        the corresponding from_env() read, this test will fail because that env
        var stops overriding YAML.
        """
        from elephantbroker.schemas.config import ENV_OVERRIDE_BINDINGS, ElephantBrokerConfig

        # Probe values that satisfy ALL field constraints in the bindings list:
        #   - int=12345  (smallest constraint is ge=0, largest is ge=3600 — 12345 OK)
        #   - float=1.5  (temperature constraint 0.0-2.0 AND ingest_batch_timeout >=1.0 — 1.5 OK)
        #   - bool=true
        #   - str="probe-{var.lower()}"  (unique per var so we can verify the right value lands in the right field)
        expected: list[tuple[str, object]] = []
        for env_var, dotted_path, coercer in ENV_OVERRIDE_BINDINGS:
            if coercer == "int":
                raw, exp = "12345", 12345
            elif coercer == "float":
                raw, exp = "1.5", 1.5
            elif coercer == "bool":
                raw, exp = "true", True
            else:  # str or str_or_none
                raw = f"probe-{env_var.lower()}"
                exp = raw
            os.environ[env_var] = raw
            expected.append((dotted_path, exp))

        cfg = ElephantBrokerConfig.from_yaml(yaml_path)
        cfg_dict = cfg.model_dump()

        for dotted_path, exp in expected:
            cur = cfg_dict
            for part in dotted_path.split("."):
                assert part in cur, f"path {dotted_path}: missing intermediate key {part!r}"
                cur = cur[part]
            assert cur == exp, f"binding {dotted_path}: expected {exp!r}, got {cur!r}"

    def test_every_binding_has_unique_env_var_name(self):
        """Sanity check: no duplicate env var names in the registry."""
        from elephantbroker.schemas.config import ENV_OVERRIDE_BINDINGS
        names = [b[0] for b in ENV_OVERRIDE_BINDINGS]
        duplicates = {n for n in names if names.count(n) > 1}
        assert not duplicates, f"duplicate env var names in ENV_OVERRIDE_BINDINGS: {duplicates}"

    def test_every_binding_has_known_coercer(self):
        """Sanity check: every binding uses one of the known type coercers."""
        from elephantbroker.schemas.config import ENV_OVERRIDE_BINDINGS
        valid = {"str", "int", "float", "bool", "str_or_none"}
        for env_var, _, coercer in ENV_OVERRIDE_BINDINGS:
            assert coercer in valid, f"{env_var}: unknown coercer {coercer!r}"
