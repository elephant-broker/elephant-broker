"""Runtime configuration schemas."""
from __future__ import annotations

import os

from pydantic import BaseModel, ConfigDict, Field, model_validator

# F9 (TODO-3-613): well-known embedding model → expected vector dimensions.
# When the operator picks a model from this map, the schema validator refuses
# to start with a mismatched `embedding_dimensions` value. The map is
# intentionally conservative — only models we have personally verified the
# output dimensions for are listed. Unknown models pass through without a
# constraint (the validator only protects known cases). Adding a model here
# should always be paired with verifying the dim against the upstream provider
# docs OR a live API probe — guessing is the exact failure mode this prevents.
#
# The cost of this check is that orphaned Qdrant collections (one of the
# nastiest debugging experiences in Cognee deployments) become impossible
# for the well-known model paths.
KNOWN_EMBEDDING_DIMS: dict[str, int] = {
    # Google / Gemini
    "gemini/text-embedding-004": 768,
    # OpenAI
    "text-embedding-3-large": 3072,
    "text-embedding-3-small": 1536,
    "text-embedding-ada-002": 1536,
    # Voyage AI
    "voyage-3": 1024,
    "voyage-3-large": 1024,
    "voyage-code-3": 1024,
    # Cohere
    "cohere/embed-english-v3.0": 1024,
    "cohere/embed-multilingual-v3.0": 1024,
    # BGE / OSS
    "BAAI/bge-large-en-v1.5": 1024,
    "BAAI/bge-base-en-v1.5": 768,
    "BAAI/bge-small-en-v1.5": 384,
}

# Imported at module top for the new `consolidation` regular field (F4 fix).
# The previous code used a `@property` with a lazy import, claiming a circular
# dependency — that claim was wrong: schemas/consolidation.py imports nothing
# from schemas/config.py, so the top-level import here is safe and lets
# Pydantic discover the field for env-binding application.
from elephantbroker.schemas.consolidation import ConsolidationConfig


class _StrictBase(BaseModel):
    """Base class for every config submodel.

    Sets ``extra="forbid"`` so unknown YAML/dict keys raise ``ValidationError``
    at load time instead of being silently swallowed. This catches operator
    typos like ``guards: enabld: true`` (which would otherwise leave
    ``guards.enabled`` at its default and silently change runtime behavior).

    All config schemas in this file inherit from ``_StrictBase`` rather than
    ``BaseModel`` directly. If you add a new submodel, inherit from this base
    so the strictness contract holds across the whole config tree.
    """
    model_config = ConfigDict(extra="forbid")


class CogneeConfig(_StrictBase):
    """Configuration for the Cognee knowledge plane."""
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    # Empty by default — runtime refuses to boot with an empty password unless
    # EB_DEV_MODE=true is set (see RuntimeContainer.from_config). The legacy
    # "elephant_dev" sentinel was a foot-gun: prod hosts that forgot to set
    # EB_NEO4J_PASSWORD would silently authenticate with the dev password.
    neo4j_password: str = ""
    qdrant_url: str = "http://localhost:6333"
    default_dataset: str = "elephantbroker"  # DANGER: changing this orphans all existing Cognee data
    embedding_provider: str = "openai"  # API client style — openai SDK shape works for any LiteLLM-routed backend
    embedding_model: str = "gemini/text-embedding-004"
    embedding_endpoint: str = "http://localhost:8811/v1"
    embedding_api_key: str = ""
    embedding_dimensions: int = Field(default=768, ge=1)  # must match embedding_model output dim

    @model_validator(mode="after")
    def _check_embedding_dimensions_match_known_model(self) -> "CogneeConfig":
        """F9: refuse to start if embedding_dimensions disagrees with a known model.

        Mismatched dims silently orphan Qdrant collections — every retrieval
        breaks until an operator notices that search returns nothing. The
        check fires only for models present in `KNOWN_EMBEDDING_DIMS`;
        unknown models pass through (the operator is on their own).
        """
        expected = KNOWN_EMBEDDING_DIMS.get(self.embedding_model)
        if expected is not None and expected != self.embedding_dimensions:
            raise ValueError(
                f"embedding_dimensions={self.embedding_dimensions} does not match "
                f"the known output dimension of embedding_model={self.embedding_model!r} "
                f"(expected {expected}). Mismatched dimensions orphan Qdrant collections "
                f"and silently break all retrieval. Either set embedding_dimensions={expected} "
                f"or pick a different embedding_model. To bypass this check, choose a model "
                f"name not in KNOWN_EMBEDDING_DIMS (the validator only protects known models)."
            )
        return self


class LLMConfig(_StrictBase):
    """LLM configuration for extraction, classification, and summarization."""
    # Cognee requires the "openai/" prefix to route through its OpenAI-compatible
    # client. Cognee strips the prefix internally before sending to LiteLLM, so
    # LiteLLM sees "gemini/gemini-2.5-pro". Without the prefix, Cognee hangs on
    # the LLM connection test at startup.
    model: str = "openai/gemini/gemini-2.5-pro"
    endpoint: str = "http://localhost:8811/v1"
    api_key: str = ""
    max_tokens: int = Field(default=8192, ge=1)
    temperature: float = Field(default=0.1, ge=0.0, le=2.0)
    extraction_max_input_tokens: int = Field(default=4000, ge=100)
    extraction_max_output_tokens: int = Field(default=16384, ge=100)
    extraction_max_facts_per_batch: int = Field(default=10, ge=1)
    summarization_max_output_tokens: int = Field(default=200, ge=10)
    summarization_min_artifact_chars: int = Field(default=500, ge=1)
    ingest_batch_size: int = Field(default=6, ge=1)
    ingest_batch_timeout_seconds: float = Field(default=60.0, ge=1.0)
    ingest_buffer_ttl_seconds: int = Field(default=300, ge=60)
    extraction_context_facts: int = Field(default=20, ge=0)
    extraction_context_ttl_seconds: int = Field(default=3600, ge=60)


class RerankerConfig(_StrictBase):
    """Reranker configuration (Phase 5+)."""
    endpoint: str = "http://localhost:1235"
    api_key: str = ""
    model: str = "Qwen/Qwen3-Reranker-4B"
    enabled: bool = True
    timeout_seconds: float = Field(default=10.0, ge=1.0)
    batch_size: int = Field(default=32, ge=1)
    max_documents: int = Field(default=100, ge=1)
    fallback_on_error: bool = True
    top_n: int | None = None


class TraceConfig(_StrictBase):
    """TraceLedger in-memory retention and OTEL log export."""
    memory_max_events: int = Field(default=10_000, ge=100)
    memory_ttl_seconds: int = Field(default=3600, ge=60)
    otel_logs_enabled: bool = False


class ClickHouseConfig(_StrictBase):
    """ClickHouse connection for cross-session analytics (Stage 7)."""
    enabled: bool = False
    host: str = "localhost"
    port: int = 8123
    database: str = "otel"
    # F5: ClickHouse auth — historically hardcoded to the bare host/port/database
    # tuple, which silently broke whenever an operator pointed at a managed
    # ClickHouse cluster (Altinity, Aiven, ClickHouse Cloud) that requires auth.
    # Defaults match clickhouse-connect's own defaults so unauthenticated local
    # dev still works without operator intervention.
    user: str = "default"
    password: str = ""
    logs_table: str = "otel_logs"


class InfraConfig(_StrictBase):
    """Infrastructure configuration."""
    redis_url: str = "redis://localhost:6379"
    otel_endpoint: str | None = None
    log_level: str = "INFO"
    metrics_ttl_seconds: int = Field(default=3600, ge=60)
    trace: TraceConfig = Field(default_factory=TraceConfig)
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)


# --- Phase 5 config models ---


class EmbeddingCacheConfig(_StrictBase):
    """Redis-backed embedding cache configuration."""
    enabled: bool = True
    ttl_seconds: int = Field(default=3600, ge=60)
    key_prefix: str = "eb:emb_cache"


class ScoringConfig(_StrictBase):
    """Working set scoring pipeline configuration."""
    neutral_use_prior: float = Field(default=0.5, ge=0.0, le=1.0)
    cheap_prune_max_candidates: int = Field(default=80, ge=1)
    semantic_blend_weight: float = Field(default=0.6, ge=0.0, le=1.0)
    merge_similarity_threshold: float = Field(default=0.95, ge=0.0, le=1.0)
    snapshot_ttl_seconds: int = Field(default=300, ge=30)
    session_goals_ttl_seconds: int = Field(default=86400, ge=60)
    working_set_build_global_goals_filter_by_actors: bool = True


class VerificationMultipliers(_StrictBase):
    """Multipliers for claim verification status on confidence scoring."""
    supervisor_verified: float = Field(default=1.0, ge=0.0, le=2.0)
    tool_supported: float = Field(default=0.9, ge=0.0, le=2.0)
    self_supported: float = Field(default=0.7, ge=0.0, le=2.0)
    unverified: float = Field(default=0.5, ge=0.0, le=2.0)
    no_claim: float = Field(default=0.8, ge=0.0, le=2.0)


class ConflictDetectionConfig(_StrictBase):
    """Global penalty values for contradiction detection layers."""
    supersession_penalty: float = Field(default=1.0, ge=0.0)
    contradiction_edge_penalty: float = Field(default=0.9, ge=0.0)
    layer2_penalty: float = Field(default=0.7, ge=0.0)
    # Layer 2 detection thresholds (global defaults, can be overridden per-profile in ScoringWeights)
    similarity_threshold: float = Field(default=0.9, ge=0.0, le=1.0)
    confidence_gap_threshold: float = Field(default=0.3, ge=0.0, le=1.0)
    redundancy_similarity_threshold: float = Field(default=0.85, ge=0.0, le=1.0)


class SuccessfulUseConfig(_StrictBase):
    """Configuration for successful-use feedback.

    When enabled, fires an LLM-based batch evaluation to determine which
    injected facts actually contributed to agent actions.  Off by default
    because it is expensive.
    """
    enabled: bool = False
    # F8 (TODO-3-612): historically defaulted to host.docker.internal:8811 from
    # the Docker-only era. The Docker setup is unsupported (Dockerfile has known
    # dep issues) and every native venv install resolved that DNS name to
    # nothing, breaking successful-use feedback whenever an operator forgot to
    # override the endpoint. Defaulting to localhost matches every other LLM
    # config in this file.
    endpoint: str = "http://localhost:8811/v1"
    api_key: str = ""  # Falls back to EB_LLM_API_KEY if empty
    model: str = "gemini/gemini-2.5-flash"
    batch_size: int = Field(default=5, ge=1)
    batch_timeout_seconds: float = Field(default=120.0, ge=10.0)
    feed_last_facts: int = Field(default=20, ge=1)
    min_confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    run_async: bool = True


class GoalInjectionConfig(_StrictBase):
    """Controls goal injection into extraction prompts."""
    enabled: bool = True
    max_session_goals: int = Field(default=5, ge=0)
    max_persistent_goals: int = Field(default=3, ge=0)
    include_persistent_goals: bool = True


class GoalRefinementConfig(_StrictBase):
    """Goal refinement pipeline configuration."""
    hints_enabled: bool = True
    refinement_task_enabled: bool = True
    model: str = "gemini/gemini-2.5-flash"
    max_subgoals_per_session: int = Field(default=10, ge=1)
    feed_recent_messages: int = Field(default=6, ge=1)
    run_refinement_async: bool = True
    progress_confidence_delta: float = Field(default=0.1, ge=0.0, le=1.0)
    subgoal_dedup_threshold: float = Field(default=0.6, ge=0.0, le=1.0)


class ProcedureCandidateConfig(_StrictBase):
    """Controls how procedures are surfaced in the working set."""
    enabled: bool = True
    filter_by_relevance: bool = True
    relevance_threshold: float = Field(default=0.3, ge=0.0, le=1.0)
    top_k: int = Field(default=3, ge=1)
    always_include_proof_required: bool = True


class AuditConfig(_StrictBase):
    """SQLite audit trail configuration."""
    procedure_audit_enabled: bool = True
    procedure_audit_db_path: str = "data/procedure_audit.db"
    session_goal_audit_enabled: bool = True
    session_goal_audit_db_path: str = "data/session_goals_audit.db"
    org_overrides_db_path: str = "data/org_overrides.db"
    authority_rules_db_path: str = "data/authority_rules.db"
    # Phase 9 consolidation stores
    consolidation_reports_db_path: str = "data/consolidation_reports.db"
    tuning_deltas_db_path: str = "data/tuning_deltas.db"
    scoring_ledger_db_path: str = "data/scoring_ledger.db"
    retention_days: int = Field(default=90, ge=7)


class ProfileCacheConfig(_StrictBase):
    """Profile resolution cache configuration."""
    ttl_seconds: int = Field(default=300, ge=10)


class GatewayConfig(_StrictBase):
    """Gateway identity configuration.

    In production the gateway_id comes from the TS plugin via HTTP headers.
    The Python runtime config is a fallback for standalone/dev mode.
    org_id and team_id are set per-deployment to bind the gateway to an org/team.

    The default is intentionally empty so the runtime container can refuse
    to boot a host that never set its own gateway_id (see
    ``RuntimeContainer.from_config``). The startup guard accepts the legacy
    "local" sentinel only when ``EB_ALLOW_DEFAULT_GATEWAY_ID=true`` is set,
    so dev/test environments can opt in explicitly.
    """
    gateway_id: str = ""
    gateway_short_name: str = ""
    register_agent_identity: bool = True
    register_agent_actor: bool = True
    org_id: str | None = None
    team_id: str | None = None
    agent_authority_level: int = Field(default=0, ge=0)

    @property
    def effective_short_name(self) -> str:
        return self.gateway_short_name or self.gateway_id[:8]


# --- Phase 6 config models ---


class ContextAssemblyConfig(_StrictBase):
    """Configuration for the 4-block context assembly pipeline."""
    max_context_window_fraction: float = Field(default=0.15, ge=0.01, le=0.5)
    fallback_context_window: int = Field(default=128000, ge=1000)
    enable_dynamic_budget: bool = True
    system_overlay_budget_fraction: float = Field(default=0.25, ge=0.05, le=0.5)
    goal_block_budget_fraction: float = Field(default=0.10, ge=0.0, le=0.3)
    evidence_budget_max_tokens: int = Field(default=500, ge=0)
    compaction_trigger_multiplier: float = Field(default=2.0, ge=1.5, le=5.0)
    compaction_summary_max_tokens: int = Field(default=1000, ge=100)


class ArtifactCaptureConfig(_StrictBase):
    """Configuration for automatic tool artifact capture."""
    enabled: bool = True
    min_content_chars: int = Field(default=200, ge=0)
    max_content_chars: int = Field(default=50000, ge=1000)
    skip_tools: list[str] = Field(default_factory=list)


class ArtifactAssemblyConfig(_StrictBase):
    """Configuration for artifact placeholder rendering in context assembly."""
    placeholder_enabled: bool = True
    placeholder_min_tokens: int = Field(default=100, ge=0)
    placeholder_template: str = '[Tool output: {tool_name} — {summary}\n → Call artifact_search("{artifact_id}") for full output]'


class AsyncAnalysisConfig(_StrictBase):
    """Configuration for async injection analysis (AD-24)."""
    enabled: bool = False
    topic_continuation_threshold: float = Field(default=0.6, ge=0.3, le=0.9)
    batch_size: int = Field(default=20, ge=1)


class StrictnessPreset(_StrictBase):
    """Strictness preset controlling guard layer behavior."""
    bm25_threshold_multiplier: float = Field(default=1.0, ge=0.1, le=3.0)
    semantic_threshold_override: float | None = None
    warn_outcome_upgrade: str | None = None
    structural_validators_enabled: bool = True
    reinjection_on: str = "elevated_risk"
    llm_escalation_on: str = "ambiguous"


class GuardConfig(_StrictBase):
    """Guard engine configuration."""
    enabled: bool = True
    builtin_rules_enabled: bool = True
    history_ttl_seconds: int = Field(default=86400, ge=60)
    max_history_events: int = Field(default=50, ge=1)
    input_summary_max_chars: int = Field(default=500, ge=50)
    llm_escalation_max_tokens: int = Field(default=500, ge=50)
    llm_escalation_timeout_seconds: float = Field(default=10.0, ge=1.0)
    max_pattern_length: int = Field(default=500, ge=10)
    strictness_presets: dict[str, StrictnessPreset] = Field(default_factory=lambda: {
        "loose": StrictnessPreset(
            bm25_threshold_multiplier=1.5,
            semantic_threshold_override=0.90,
            structural_validators_enabled=False,
            reinjection_on="block_only",
            llm_escalation_on="disabled",
        ),
        "medium": StrictnessPreset(
            bm25_threshold_multiplier=1.0,
            reinjection_on="elevated_risk",
            llm_escalation_on="ambiguous",
        ),
        "strict": StrictnessPreset(
            bm25_threshold_multiplier=0.7,
            semantic_threshold_override=0.70,
            warn_outcome_upgrade="require_approval",
            reinjection_on="any_non_pass",
            llm_escalation_on="any_non_pass",
        ),
    })


class HitlConfig(_StrictBase):
    """Human-in-the-loop middleware configuration."""
    enabled: bool = False
    default_url: str = "http://localhost:8421"
    timeout_seconds: float = Field(default=10.0, ge=1.0)
    approval_default_timeout_seconds: int = Field(default=300, ge=30)
    callback_hmac_secret: str = ""
    gateway_overrides: dict[str, str] = Field(default_factory=dict)
    retry_count: int = Field(default=2, ge=0, description="Max retries on transient failures")
    retry_delay_seconds: float = Field(default=0.5, ge=0.0, description="Base delay for exponential backoff")


class CompactionLLMConfig(_StrictBase):
    """LLM configuration for compaction summarization."""
    model: str = "gemini/gemini-2.5-flash"
    # F7 (TODO-3-609): empty string is the inheritance sentinel — when left
    # empty (default or explicit ""), `_apply_inheritance_fallbacks()` copies
    # `llm.endpoint` into this field at load time. This matches the existing
    # `api_key` inheritance behavior and removes the operator footgun where
    # setting EB_LLM_ENDPOINT silently left compaction pinned to localhost.
    endpoint: str = ""
    api_key: str = ""
    max_tokens: int = Field(default=2000, ge=100)
    temperature: float = Field(default=0.2, ge=0.0, le=2.0)


class BlockerExtractionConfig(_StrictBase):
    """Configuration for automatic LLM-based blocker extraction (Phase 9 RT-2)."""
    enabled: bool = False
    # F8 (TODO-3-612): see SuccessfulUseConfig.endpoint comment — same fix.
    endpoint: str = "http://localhost:8811/v1"
    api_key: str = ""  # Falls back to EB_LLM_API_KEY if empty
    model: str = "gemini/gemini-2.5-flash"
    run_every_n_turns: int = Field(default=3, ge=1)
    recent_messages_window: int = Field(default=10, ge=1)


# =============================================================================
# Environment variable override registry
# =============================================================================
#
# This list defines EVERY env var that overrides a YAML field when loading via
# `ElephantBrokerConfig.from_yaml()` (or its `load()` wrapper). The contract is:
# if any source code references `os.environ["EB_*"]` or `os.getenv("EB_*")`, that
# same env var must appear here so the registry — and the packaged default.yaml
# that the runtime now boots from — covers it. The inverse contract test in
# `tests/unit/schemas/test_config.py` walks the source tree and asserts this.
#
# Each entry is `(env_var_name, dotted_config_path, type_coercer)`:
#   - env_var_name: e.g. "EB_LLM_MAX_TOKENS"
#   - dotted_config_path: e.g. "llm.max_tokens" — supports nesting (e.g. "infra.trace.otel_logs_enabled")
#   - type_coercer: one of "str", "int", "float", "bool", "str_or_none"
#
# DO NOT remove entries without bumping a major version — operators may rely
# on env vars overriding YAML, and removing a binding silently breaks them.
#
# Special fallback chains (api_key + endpoint inheritance) are applied separately in
# `_apply_inheritance_fallbacks()` after this registry is processed.
# -----------------------------------------------------------------------------

ENV_OVERRIDE_BINDINGS: list[tuple[str, str, str]] = [
    # --- Identity (gateway, org, team, default profile) ---
    ("EB_GATEWAY_ID", "gateway.gateway_id", "str"),
    ("EB_GATEWAY_SHORT_NAME", "gateway.gateway_short_name", "str"),
    ("EB_ORG_ID", "gateway.org_id", "str_or_none"),
    ("EB_TEAM_ID", "gateway.team_id", "str_or_none"),
    ("EB_AGENT_AUTHORITY_LEVEL", "gateway.agent_authority_level", "int"),
    ("EB_DEFAULT_PROFILE", "default_profile", "str"),

    # --- Cognee (Neo4j + Qdrant + Embedding) ---
    ("EB_NEO4J_URI", "cognee.neo4j_uri", "str"),
    ("EB_NEO4J_USER", "cognee.neo4j_user", "str"),
    ("EB_NEO4J_PASSWORD", "cognee.neo4j_password", "str"),
    ("EB_QDRANT_URL", "cognee.qdrant_url", "str"),
    ("EB_DEFAULT_DATASET", "cognee.default_dataset", "str"),
    ("EB_EMBEDDING_PROVIDER", "cognee.embedding_provider", "str"),
    ("EB_EMBEDDING_MODEL", "cognee.embedding_model", "str"),
    ("EB_EMBEDDING_ENDPOINT", "cognee.embedding_endpoint", "str"),
    ("EB_EMBEDDING_API_KEY", "cognee.embedding_api_key", "str"),
    ("EB_EMBEDDING_DIMENSIONS", "cognee.embedding_dimensions", "int"),

    # --- LLM (primary extraction/classification/summarization) ---
    ("EB_LLM_MODEL", "llm.model", "str"),
    ("EB_LLM_ENDPOINT", "llm.endpoint", "str"),
    ("EB_LLM_API_KEY", "llm.api_key", "str"),
    ("EB_LLM_MAX_TOKENS", "llm.max_tokens", "int"),
    ("EB_LLM_TEMPERATURE", "llm.temperature", "float"),
    ("EB_LLM_EXTRACTION_MAX_INPUT_TOKENS", "llm.extraction_max_input_tokens", "int"),
    ("EB_LLM_EXTRACTION_MAX_OUTPUT_TOKENS", "llm.extraction_max_output_tokens", "int"),
    ("EB_LLM_EXTRACTION_MAX_FACTS", "llm.extraction_max_facts_per_batch", "int"),
    ("EB_LLM_SUMMARIZATION_MAX_OUTPUT_TOKENS", "llm.summarization_max_output_tokens", "int"),
    ("EB_LLM_SUMMARIZATION_MIN_CHARS", "llm.summarization_min_artifact_chars", "int"),
    ("EB_INGEST_BATCH_SIZE", "llm.ingest_batch_size", "int"),
    ("EB_INGEST_BATCH_TIMEOUT", "llm.ingest_batch_timeout_seconds", "float"),
    ("EB_INGEST_BUFFER_TTL", "llm.ingest_buffer_ttl_seconds", "int"),
    ("EB_EXTRACTION_CONTEXT_FACTS", "llm.extraction_context_facts", "int"),
    ("EB_EXTRACTION_CONTEXT_TTL", "llm.extraction_context_ttl_seconds", "int"),

    # --- Compaction LLM (separate cheaper model for compaction summaries) ---
    ("EB_COMPACTION_LLM_MODEL", "compaction_llm.model", "str"),
    ("EB_COMPACTION_LLM_ENDPOINT", "compaction_llm.endpoint", "str"),
    ("EB_COMPACTION_LLM_API_KEY", "compaction_llm.api_key", "str"),

    # --- Reranker ---
    ("EB_RERANKER_ENDPOINT", "reranker.endpoint", "str"),
    ("EB_RERANKER_API_KEY", "reranker.api_key", "str"),
    ("EB_RERANKER_MODEL", "reranker.model", "str"),
    # F10 (TODO-3-608): the reranker.enabled toggle was unbound, leaving
    # operators without a runtime kill-switch when their Qwen3-Reranker
    # service is unavailable. Adding the binding lets `EB_RERANKER_ENABLED=false`
    # downgrade retrieval to scoring-only ranking without restarting with a
    # tuned YAML.
    ("EB_RERANKER_ENABLED", "reranker.enabled", "bool"),

    # --- Infra (Redis + OTEL + log level + metrics) ---
    ("EB_REDIS_URL", "infra.redis_url", "str"),
    ("EB_OTEL_ENDPOINT", "infra.otel_endpoint", "str_or_none"),
    ("EB_LOG_LEVEL", "infra.log_level", "str"),
    ("EB_METRICS_TTL_SECONDS", "infra.metrics_ttl_seconds", "int"),

    # --- Trace ledger (nested under infra.trace) ---
    ("EB_TRACE_OTEL_LOGS_ENABLED", "infra.trace.otel_logs_enabled", "bool"),
    ("EB_TRACE_MEMORY_MAX_EVENTS", "infra.trace.memory_max_events", "int"),

    # --- ClickHouse (nested under infra.clickhouse) ---
    ("EB_CLICKHOUSE_ENABLED", "infra.clickhouse.enabled", "bool"),
    ("EB_CLICKHOUSE_HOST", "infra.clickhouse.host", "str"),
    ("EB_CLICKHOUSE_PORT", "infra.clickhouse.port", "int"),
    ("EB_CLICKHOUSE_DATABASE", "infra.clickhouse.database", "str"),
    # F5: managed ClickHouse clusters require auth — these were missing.
    ("EB_CLICKHOUSE_USER", "infra.clickhouse.user", "str"),
    ("EB_CLICKHOUSE_PASSWORD", "infra.clickhouse.password", "str"),

    # --- Embedding cache ---
    ("EB_EMBEDDING_CACHE_ENABLED", "embedding_cache.enabled", "bool"),
    ("EB_EMBEDDING_CACHE_TTL", "embedding_cache.ttl_seconds", "int"),

    # --- Working set scoring ---
    ("EB_SCORING_SNAPSHOT_TTL", "scoring.snapshot_ttl_seconds", "int"),
    ("EB_SESSION_GOALS_TTL", "scoring.session_goals_ttl_seconds", "int"),

    # --- HITL ---
    # F10 (TODO-3-608): same fix as EB_RERANKER_ENABLED — operators need a
    # runtime toggle for the HITL middleware integration so they can disable
    # human-in-the-loop without redeploying with a different YAML.
    ("EB_HITL_ENABLED", "hitl.enabled", "bool"),
    ("EB_HITL_CALLBACK_SECRET", "hitl.callback_hmac_secret", "str"),

    # --- Successful-use feedback (Phase 9, off by default) ---
    ("EB_SUCCESSFUL_USE_ENABLED", "successful_use.enabled", "bool"),
    ("EB_SUCCESSFUL_USE_ENDPOINT", "successful_use.endpoint", "str"),
    ("EB_SUCCESSFUL_USE_API_KEY", "successful_use.api_key", "str"),
    ("EB_SUCCESSFUL_USE_MODEL", "successful_use.model", "str"),
    ("EB_SUCCESSFUL_USE_BATCH_SIZE", "successful_use.batch_size", "int"),

    # --- Blocker extraction (Phase 9, off by default) ---
    ("EB_BLOCKER_EXTRACTION_ENABLED", "blocker_extraction.enabled", "bool"),
    ("EB_BLOCKER_EXTRACTION_ENDPOINT", "blocker_extraction.endpoint", "str"),
    ("EB_BLOCKER_EXTRACTION_API_KEY", "blocker_extraction.api_key", "str"),
    ("EB_BLOCKER_EXTRACTION_MODEL", "blocker_extraction.model", "str"),
    ("EB_BLOCKER_EXTRACTION_EVERY_N_TURNS", "blocker_extraction.run_every_n_turns", "int"),

    # --- Consolidation pipeline (Phase 9) ---
    # F4 (TODO-3-009): the two vars below were previously read directly from
    # os.environ inside an `ElephantBrokerConfig.consolidation` @property —
    # invisible to this registry, untestable by the contract test, and
    # racy if the caller mutated the env between bootstrap and first access.
    # Routing them through the registry kills both bugs.
    ("EB_DEV_CONSOLIDATION_AUTO_TRIGGER", "consolidation.dev_auto_trigger_interval", "str"),
    ("EB_CONSOLIDATION_BATCH_SIZE", "consolidation.batch_size", "int"),

    # --- Top-level toggles & global limits ---
    ("EB_ENABLE_TRACE_LEDGER", "enable_trace_ledger", "bool"),
    ("EB_GUARDS_ENABLED", "guards.enabled", "bool"),
    ("EB_MAX_CONCURRENT_SESSIONS", "max_concurrent_sessions", "int"),
    ("EB_CONSOLIDATION_MIN_RETENTION_SECONDS", "consolidation_min_retention_seconds", "int"),
]


def _coerce_env_value(raw: str, coercer: str) -> object:
    """Convert a raw env var string to the target type. Raises ValueError on bad input."""
    if coercer == "str":
        return raw
    if coercer == "str_or_none":
        return raw if raw else None
    if coercer == "int":
        return int(raw)
    if coercer == "float":
        return float(raw)
    if coercer == "bool":
        return raw.strip().lower() in ("true", "1", "yes", "on")
    raise ValueError(f"Unknown coercer: {coercer!r}")


def _set_nested(target: dict, dotted_path: str, value: object) -> None:
    """Set a value at a dotted path in a nested dict, creating intermediate dicts as needed."""
    parts = dotted_path.split(".")
    cur = target
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


def _apply_env_overrides(yaml_data: dict) -> None:
    """Mutate ``yaml_data`` to apply every env var present in ``ENV_OVERRIDE_BINDINGS``.

    For each binding, if the env var is set in ``os.environ`` (any value, including
    empty string for ``str_or_none``), the YAML field at the dotted path is replaced
    with the coerced env value. Type coercion failures (e.g. ``int("foo")``) raise
    ``ValueError`` and propagate to the caller.
    """
    for env_var, dotted_path, coercer in ENV_OVERRIDE_BINDINGS:
        if env_var not in os.environ:
            continue
        raw = os.environ[env_var]
        value = _coerce_env_value(raw, coercer)
        _set_nested(yaml_data, dotted_path, value)


def _apply_inheritance_fallbacks(yaml_data: dict) -> None:
    """Apply secret + endpoint inheritance chains so operators don't have to duplicate values.

    Renamed from ``_apply_api_key_fallbacks`` (F7, TODO-3-609) when endpoint
    inheritance was added — the original name is no longer accurate. Operators
    repeatedly hit the footgun of setting ``EB_LLM_ENDPOINT`` to a remote
    LiteLLM proxy and finding compaction pinned to ``localhost`` because the
    derived endpoints didn't share the inheritance pattern with api_key.

    Inheritance tiers:
      1. ``llm.api_key`` ← ``cognee.embedding_api_key`` (if llm.api_key empty)
      2. ``compaction_llm.api_key`` / ``successful_use.api_key`` /
         ``blocker_extraction.api_key`` ← ``llm.api_key`` (each only if its own value is empty)
      3. ``compaction_llm.endpoint`` ← ``llm.endpoint`` (if compaction_llm.endpoint empty)

    The fallbacks fire only when the target field is empty after env override
    application — explicit YAML or env values are always respected.
    """
    cognee = yaml_data.setdefault("cognee", {})
    llm = yaml_data.setdefault("llm", {})

    # Tier 1: llm.api_key ← cognee.embedding_api_key
    if not llm.get("api_key") and cognee.get("embedding_api_key"):
        llm["api_key"] = cognee["embedding_api_key"]

    # Tier 2: derived LLMs (compaction / successful_use / blocker_extraction) ← llm.api_key
    llm_key = llm.get("api_key", "")
    if llm_key:
        for section in ("compaction_llm", "successful_use", "blocker_extraction"):
            sec = yaml_data.setdefault(section, {})
            if not sec.get("api_key"):
                sec["api_key"] = llm_key

    # Tier 3 (F7): compaction_llm.endpoint ← llm.endpoint
    # Only compaction_llm participates in endpoint inheritance for now —
    # successful_use and blocker_extraction still default to host.docker.internal
    # and have their own per-host overrides via env. F8 fixes those defaults
    # to localhost; widening endpoint inheritance to them is a follow-up.
    llm_endpoint = llm.get("endpoint", "")
    if llm_endpoint:
        compaction = yaml_data.setdefault("compaction_llm", {})
        if not compaction.get("endpoint"):
            compaction["endpoint"] = llm_endpoint


class ElephantBrokerConfig(_StrictBase):
    """Top-level runtime configuration."""
    cognee: CogneeConfig = Field(default_factory=CogneeConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    reranker: RerankerConfig = Field(default_factory=RerankerConfig)
    infra: InfraConfig = Field(default_factory=InfraConfig)
    default_profile: str = "coding"
    enable_trace_ledger: bool = True
    max_concurrent_sessions: int = Field(default=100, ge=1)
    # Phase 5 config sections
    embedding_cache: EmbeddingCacheConfig = Field(default_factory=EmbeddingCacheConfig)
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    verification_multipliers: VerificationMultipliers = Field(default_factory=VerificationMultipliers)
    conflict_detection: ConflictDetectionConfig = Field(default_factory=ConflictDetectionConfig)
    successful_use: SuccessfulUseConfig = Field(default_factory=SuccessfulUseConfig)
    goal_injection: GoalInjectionConfig = Field(default_factory=GoalInjectionConfig)
    goal_refinement: GoalRefinementConfig = Field(default_factory=GoalRefinementConfig)
    procedure_candidates: ProcedureCandidateConfig = Field(default_factory=ProcedureCandidateConfig)
    audit: AuditConfig = Field(default_factory=AuditConfig)
    # Phase 7 config sections
    guards: GuardConfig = Field(default_factory=GuardConfig)
    hitl: HitlConfig = Field(default_factory=HitlConfig)
    # Phase 6 config sections
    context_assembly: ContextAssemblyConfig = Field(default_factory=ContextAssemblyConfig)
    artifact_capture: ArtifactCaptureConfig = Field(default_factory=ArtifactCaptureConfig)
    artifact_assembly: ArtifactAssemblyConfig = Field(default_factory=ArtifactAssemblyConfig)
    async_analysis: AsyncAnalysisConfig = Field(default_factory=AsyncAnalysisConfig)
    compaction_llm: CompactionLLMConfig = Field(default_factory=CompactionLLMConfig)
    consolidation_min_retention_seconds: int = Field(default=172800, ge=3600)
    # Phase 8 config sections
    profile_cache: ProfileCacheConfig = Field(default_factory=ProfileCacheConfig)
    # Phase 9 config sections
    blocker_extraction: BlockerExtractionConfig = Field(default_factory=BlockerExtractionConfig)
    # F4 (TODO-3-009): consolidation was previously a `@property` that read
    # EB_DEV_CONSOLIDATION_AUTO_TRIGGER + EB_CONSOLIDATION_BATCH_SIZE directly
    # from os.environ on first access and cached the result. That created two
    # bugs: (1) a TOCTOU race — env vars set after the first access were
    # silently ignored — and (2) the env vars were invisible to
    # ENV_OVERRIDE_BINDINGS, the registry, and the inverse contract test.
    # Making it a regular field routes both vars through the standard
    # registry path so they behave like every other binding.
    consolidation: ConsolidationConfig = Field(default_factory=ConsolidationConfig)

    @classmethod
    def from_yaml(cls, path: str) -> ElephantBrokerConfig:
        """Load config from a YAML file, then apply environment variable overrides.

        Resolution order: env var (if set) > YAML value > schema default.

        Every env var that overrides a YAML field MUST be declared in
        ``ENV_OVERRIDE_BINDINGS`` — the registry is the single source of
        truth. Removing or renaming a binding silently breaks operators
        relying on the env var path, so the registry doubles as the
        contract test target (see ``test_every_binding_applies``).

        After env overrides are applied, ``_apply_inheritance_fallbacks()`` runs
        to populate empty derived secrets (compaction_llm, successful_use,
        blocker_extraction) from ``llm.api_key``, populate ``llm.api_key`` from
        ``cognee.embedding_api_key`` if both are empty, and copy ``llm.endpoint``
        into ``compaction_llm.endpoint`` when the latter is empty (F7).

        The merged dict is re-validated through ``cls.model_validate()`` so any
        type or constraint violation (e.g. ``EB_EMBEDDING_DIMENSIONS=0`` would
        violate ``ge=1``) raises a ``ValidationError`` at load time.
        """
        import yaml  # requires pyyaml
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        # Two-pass validation. First pass validates the raw YAML payload so
        # any malformed YAML or operator typo fails before we touch env vars
        # (clearer error reporting). Thanks to ``extra="forbid"`` on every
        # config submodel (Bucket A2 — see ``_StrictBase.model_config``),
        # this first pass catches typos like ``guards: enabld: true`` at YAML
        # parse time instead of silently leaving the field at its default —
        # G3 (TODO-3-016) confirmed this naturally resolves the BLR concern
        # about the two-pass pattern's value depending on strict schemas.
        yaml_config = cls(**data)
        yaml_data = yaml_config.model_dump()
        _apply_env_overrides(yaml_data)
        _apply_inheritance_fallbacks(yaml_data)
        # Second pass re-validates after env overrides + inheritance fallbacks,
        # so any constraint violation introduced by an env override
        # (e.g. ``EB_EMBEDDING_DIMENSIONS=0`` would violate ``ge=1``) raises
        # a ``ValidationError`` at load time rather than at first use.
        return cls.model_validate(yaml_data)

    @classmethod
    def load(cls, path: str | None = None) -> ElephantBrokerConfig:
        """Load runtime config from a YAML file (or the packaged default).

        Single entry point for the runtime, CLI, and tests. ``path`` may be:

        - A filesystem path (e.g. ``/etc/elephantbroker/default.yaml``) — used
          in production where operators ship a tuned YAML beside the venv.
        - ``None`` — falls back to the packaged
          ``elephantbroker/config/default.yaml`` resource. This is the path
          tests and standalone-dev use, and it lets the runtime boot with
          zero on-disk config (provided env vars supply the secrets and the
          startup safety guard is satisfied).

        Env vars in ``ENV_OVERRIDE_BINDINGS`` then override any YAML values,
        and api-key inheritance fallbacks fire to populate empty secrets.
        This replaces the legacy ``from_env()`` classmethod, which had to
        duplicate every default and drift from the registry whenever a new
        field was added (F2/F3 — D5 OPERATOR LOCKED). The packaged default
        YAML is now the single source of truth for both YAML-only and
        env-only callers.
        """
        if path is None:
            # `as_file()` materializes the resource on disk if needed (e.g.
            # when loaded from a zipimport wheel). For our wheel layout the
            # file is already a real file under site-packages, so this is a
            # no-op context manager — but the abstraction lets us survive
            # any future packaging change without touching this code.
            from importlib.resources import as_file, files
            ref = files("elephantbroker.config") / "default.yaml"
            with as_file(ref) as default_path:
                return cls.from_yaml(str(default_path))
        return cls.from_yaml(path)
