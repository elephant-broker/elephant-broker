"""Runtime configuration schemas."""
from __future__ import annotations

import os

from pydantic import BaseModel, Field


class CogneeConfig(BaseModel):
    """Configuration for the Cognee knowledge plane."""
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "elephant_dev"  # dev/test default — override via EB_NEO4J_PASSWORD in production
    qdrant_url: str = "http://localhost:6333"
    default_dataset: str = "elephantbroker"  # DANGER: changing this orphans all existing Cognee data
    embedding_provider: str = "openai"
    embedding_model: str = "openai/text-embedding-3-large"
    embedding_endpoint: str = "http://localhost:8811/v1"
    embedding_api_key: str = ""
    embedding_dimensions: int = Field(default=1024, ge=1)


class LLMConfig(BaseModel):
    """LLM configuration for extraction, classification, and summarization."""
    model: str = "gemini/gemini-2.5-pro"
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


class RerankerConfig(BaseModel):
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


class TraceConfig(BaseModel):
    """TraceLedger in-memory retention and OTEL log export."""
    memory_max_events: int = Field(default=10_000, ge=100)
    memory_ttl_seconds: int = Field(default=3600, ge=60)
    otel_logs_enabled: bool = False


class ClickHouseConfig(BaseModel):
    """ClickHouse connection for cross-session analytics (Stage 7)."""
    enabled: bool = False
    host: str = "localhost"
    port: int = 8123
    database: str = "otel"
    logs_table: str = "otel_logs"


class InfraConfig(BaseModel):
    """Infrastructure configuration."""
    redis_url: str = "redis://localhost:6379"
    otel_endpoint: str | None = None
    log_level: str = "INFO"
    metrics_ttl_seconds: int = Field(default=3600, ge=60)
    trace: TraceConfig = Field(default_factory=TraceConfig)
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)


# --- Phase 5 config models ---


class EmbeddingCacheConfig(BaseModel):
    """Redis-backed embedding cache configuration."""
    enabled: bool = True
    ttl_seconds: int = Field(default=3600, ge=60)
    key_prefix: str = "eb:emb_cache"


class ScoringConfig(BaseModel):
    """Working set scoring pipeline configuration."""
    neutral_use_prior: float = Field(default=0.5, ge=0.0, le=1.0)
    cheap_prune_max_candidates: int = Field(default=80, ge=1)
    semantic_blend_weight: float = Field(default=0.6, ge=0.0, le=1.0)
    merge_similarity_threshold: float = Field(default=0.95, ge=0.0, le=1.0)
    snapshot_ttl_seconds: int = Field(default=300, ge=30)
    session_goals_ttl_seconds: int = Field(default=86400, ge=60)
    working_set_build_global_goals_filter_by_actors: bool = True


class VerificationMultipliers(BaseModel):
    """Multipliers for claim verification status on confidence scoring."""
    supervisor_verified: float = Field(default=1.0, ge=0.0, le=2.0)
    tool_supported: float = Field(default=0.9, ge=0.0, le=2.0)
    self_supported: float = Field(default=0.7, ge=0.0, le=2.0)
    unverified: float = Field(default=0.5, ge=0.0, le=2.0)
    no_claim: float = Field(default=0.8, ge=0.0, le=2.0)


class ConflictDetectionConfig(BaseModel):
    """Global penalty values for contradiction detection layers."""
    supersession_penalty: float = Field(default=1.0, ge=0.0)
    contradiction_edge_penalty: float = Field(default=0.9, ge=0.0)
    layer2_penalty: float = Field(default=0.7, ge=0.0)
    # Layer 2 detection thresholds (global defaults, can be overridden per-profile in ScoringWeights)
    similarity_threshold: float = Field(default=0.9, ge=0.0, le=1.0)
    confidence_gap_threshold: float = Field(default=0.3, ge=0.0, le=1.0)
    redundancy_similarity_threshold: float = Field(default=0.85, ge=0.0, le=1.0)


class SuccessfulUseConfig(BaseModel):
    """Configuration for successful-use feedback.

    When enabled, fires an LLM-based batch evaluation to determine which
    injected facts actually contributed to agent actions.  Off by default
    because it is expensive.
    """
    enabled: bool = False
    endpoint: str = "http://host.docker.internal:8811/v1"
    api_key: str = ""  # Falls back to EB_LLM_API_KEY if empty
    model: str = "gemini/gemini-2.5-flash"
    batch_size: int = Field(default=5, ge=1)
    batch_timeout_seconds: float = Field(default=120.0, ge=10.0)
    feed_last_facts: int = Field(default=20, ge=1)
    min_confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    run_async: bool = True


class GoalInjectionConfig(BaseModel):
    """Controls goal injection into extraction prompts."""
    enabled: bool = True
    max_session_goals: int = Field(default=5, ge=0)
    max_persistent_goals: int = Field(default=3, ge=0)
    include_persistent_goals: bool = True


class GoalRefinementConfig(BaseModel):
    """Goal refinement pipeline configuration."""
    hints_enabled: bool = True
    refinement_task_enabled: bool = True
    model: str = "gemini/gemini-2.5-flash"
    max_subgoals_per_session: int = Field(default=10, ge=1)
    feed_recent_messages: int = Field(default=6, ge=1)
    run_refinement_async: bool = True
    progress_confidence_delta: float = Field(default=0.1, ge=0.0, le=1.0)
    subgoal_dedup_threshold: float = Field(default=0.6, ge=0.0, le=1.0)


class ProcedureCandidateConfig(BaseModel):
    """Controls how procedures are surfaced in the working set."""
    enabled: bool = True
    filter_by_relevance: bool = True
    relevance_threshold: float = Field(default=0.3, ge=0.0, le=1.0)
    top_k: int = Field(default=3, ge=1)
    always_include_proof_required: bool = True


class AuditConfig(BaseModel):
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


class ProfileCacheConfig(BaseModel):
    """Profile resolution cache configuration."""
    ttl_seconds: int = Field(default=300, ge=10)


class GatewayConfig(BaseModel):
    """Gateway identity configuration.

    In production the gateway_id comes from the TS plugin via HTTP headers.
    The Python runtime config is a fallback for standalone/dev mode.
    org_id and team_id are set per-deployment to bind the gateway to an org/team.
    """
    gateway_id: str = "local"
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


class ContextAssemblyConfig(BaseModel):
    """Configuration for the 4-block context assembly pipeline."""
    max_context_window_fraction: float = Field(default=0.15, ge=0.01, le=0.5)
    fallback_context_window: int = Field(default=128000, ge=1000)
    enable_dynamic_budget: bool = True
    system_overlay_budget_fraction: float = Field(default=0.25, ge=0.05, le=0.5)
    goal_block_budget_fraction: float = Field(default=0.10, ge=0.0, le=0.3)
    evidence_budget_max_tokens: int = Field(default=500, ge=0)
    compaction_trigger_multiplier: float = Field(default=2.0, ge=1.5, le=5.0)
    compaction_summary_max_tokens: int = Field(default=1000, ge=100)


class ArtifactCaptureConfig(BaseModel):
    """Configuration for automatic tool artifact capture."""
    enabled: bool = True
    min_content_chars: int = Field(default=200, ge=0)
    max_content_chars: int = Field(default=50000, ge=1000)
    skip_tools: list[str] = Field(default_factory=list)


class ArtifactAssemblyConfig(BaseModel):
    """Configuration for artifact placeholder rendering in context assembly."""
    placeholder_enabled: bool = True
    placeholder_min_tokens: int = Field(default=100, ge=0)
    placeholder_template: str = '[Tool output: {tool_name} — {summary}\n → Call artifact_search("{artifact_id}") for full output]'


class AsyncAnalysisConfig(BaseModel):
    """Configuration for async injection analysis (AD-24)."""
    enabled: bool = False
    topic_continuation_threshold: float = Field(default=0.6, ge=0.3, le=0.9)
    batch_size: int = Field(default=20, ge=1)


class StrictnessPreset(BaseModel):
    """Strictness preset controlling guard layer behavior."""
    bm25_threshold_multiplier: float = Field(default=1.0, ge=0.1, le=3.0)
    semantic_threshold_override: float | None = None
    warn_outcome_upgrade: str | None = None
    structural_validators_enabled: bool = True
    reinjection_on: str = "elevated_risk"
    llm_escalation_on: str = "ambiguous"


class GuardConfig(BaseModel):
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


class HitlConfig(BaseModel):
    """Human-in-the-loop middleware configuration."""
    enabled: bool = False
    default_url: str = "http://localhost:8421"
    timeout_seconds: float = Field(default=10.0, ge=1.0)
    approval_default_timeout_seconds: int = Field(default=300, ge=30)
    callback_hmac_secret: str = ""
    gateway_overrides: dict[str, str] = Field(default_factory=dict)
    retry_count: int = Field(default=2, ge=0, description="Max retries on transient failures")
    retry_delay_seconds: float = Field(default=0.5, ge=0.0, description="Base delay for exponential backoff")


class CompactionLLMConfig(BaseModel):
    """LLM configuration for compaction summarization."""
    model: str = "gemini/gemini-2.5-flash"
    endpoint: str = "http://localhost:8811/v1"
    api_key: str = ""
    max_tokens: int = Field(default=2000, ge=100)
    temperature: float = Field(default=0.2, ge=0.0, le=2.0)


class BlockerExtractionConfig(BaseModel):
    """Configuration for automatic LLM-based blocker extraction (Phase 9 RT-2)."""
    enabled: bool = False
    endpoint: str = "http://host.docker.internal:8811/v1"
    api_key: str = ""  # Falls back to EB_LLM_API_KEY if empty
    model: str = "gemini/gemini-2.5-flash"
    run_every_n_turns: int = Field(default=3, ge=1)
    recent_messages_window: int = Field(default=10, ge=1)


class ElephantBrokerConfig(BaseModel):
    """Top-level runtime configuration."""
    cognee: CogneeConfig = Field(default_factory=CogneeConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    reranker: RerankerConfig = Field(default_factory=RerankerConfig)
    infra: InfraConfig = Field(default_factory=InfraConfig)
    default_profile: str = "coding"
    enable_trace_ledger: bool = True
    enable_guards: bool = True
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

    @property
    def consolidation(self):
        """Lazy import to avoid circular dependency with schemas/consolidation.py."""
        from elephantbroker.schemas.consolidation import ConsolidationConfig
        if not hasattr(self, "_consolidation_cache"):
            object.__setattr__(self, "_consolidation_cache", ConsolidationConfig(
                dev_auto_trigger_interval=os.environ.get("EB_DEV_CONSOLIDATION_AUTO_TRIGGER", "0"),
                batch_size=int(os.environ.get("EB_CONSOLIDATION_BATCH_SIZE", "500")),
            ))
        return self._consolidation_cache

    @classmethod
    def from_yaml(cls, path: str) -> ElephantBrokerConfig:
        """Load config from YAML file, then apply environment variable overrides.

        Resolution order: env var (if set) > yaml value > model default.
        The YAML file contains literal defaults — no string interpolation.
        """
        import yaml  # requires pyyaml
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        # Build base config from YAML
        yaml_config = cls(**data)
        # Build env config (will have defaults for unset vars)
        env_config = cls.from_env()
        # Merge: for top-level fields, if an EB_* env var is explicitly SET,
        # use the env value. Otherwise keep YAML value.
        # We check a curated set of env vars rather than all fields.
        env_overrides: dict = {}
        if os.environ.get("EB_GATEWAY_ID"):
            env_overrides.setdefault("gateway", {})["gateway_id"] = os.environ["EB_GATEWAY_ID"]
        if os.environ.get("EB_ORG_ID"):
            env_overrides.setdefault("gateway", {})["org_id"] = os.environ["EB_ORG_ID"]
        if os.environ.get("EB_TEAM_ID"):
            env_overrides.setdefault("gateway", {})["team_id"] = os.environ["EB_TEAM_ID"]
        if os.environ.get("EB_NEO4J_URI"):
            env_overrides.setdefault("cognee", {})["neo4j_uri"] = os.environ["EB_NEO4J_URI"]
        if os.environ.get("EB_QDRANT_URL"):
            env_overrides.setdefault("cognee", {})["qdrant_url"] = os.environ["EB_QDRANT_URL"]
        if os.environ.get("EB_REDIS_URL"):
            env_overrides.setdefault("infra", {})["redis_url"] = os.environ["EB_REDIS_URL"]
        if os.environ.get("EB_OTEL_ENDPOINT"):
            env_overrides.setdefault("infra", {})["otel_endpoint"] = os.environ["EB_OTEL_ENDPOINT"]
        if os.environ.get("EB_EMBEDDING_API_KEY"):
            env_overrides.setdefault("cognee", {})["embedding_api_key"] = os.environ["EB_EMBEDDING_API_KEY"]
        if os.environ.get("EB_LLM_API_KEY"):
            env_overrides.setdefault("llm", {})["api_key"] = os.environ["EB_LLM_API_KEY"]
        if os.environ.get("EB_LLM_MODEL"):
            env_overrides.setdefault("llm", {})["model"] = os.environ["EB_LLM_MODEL"]
        if os.environ.get("EB_LLM_ENDPOINT"):
            env_overrides.setdefault("llm", {})["endpoint"] = os.environ["EB_LLM_ENDPOINT"]
        if os.environ.get("EB_RERANKER_ENDPOINT"):
            env_overrides.setdefault("reranker", {})["endpoint"] = os.environ["EB_RERANKER_ENDPOINT"]
        if os.environ.get("EB_RERANKER_API_KEY"):
            env_overrides.setdefault("reranker", {})["api_key"] = os.environ["EB_RERANKER_API_KEY"]
        if os.environ.get("EB_HITL_CALLBACK_SECRET"):
            env_overrides.setdefault("hitl", {})["callback_hmac_secret"] = os.environ["EB_HITL_CALLBACK_SECRET"]
        # Apply env overrides on top of YAML config
        if env_overrides:
            yaml_data = yaml_config.model_dump()
            for section, overrides in env_overrides.items():
                if section in yaml_data and isinstance(yaml_data[section], dict):
                    yaml_data[section].update(overrides)
                else:
                    yaml_data[section] = overrides
            return cls.model_validate(yaml_data)
        return yaml_config

    @classmethod
    def from_env(cls) -> ElephantBrokerConfig:
        """Create config from environment variables with EB_ prefix."""
        cognee = CogneeConfig(
            neo4j_uri=os.environ.get("EB_NEO4J_URI", "bolt://localhost:7687"),
            neo4j_user=os.environ.get("EB_NEO4J_USER", "neo4j"),
            neo4j_password=os.environ.get("EB_NEO4J_PASSWORD", "elephant_dev"),
            qdrant_url=os.environ.get("EB_QDRANT_URL", "http://localhost:6333"),
            default_dataset=os.environ.get("EB_DEFAULT_DATASET", "elephantbroker"),
            embedding_provider=os.environ.get("EB_EMBEDDING_PROVIDER", "openai"),
            embedding_model=os.environ.get("EB_EMBEDDING_MODEL", "openai/text-embedding-3-large"),
            embedding_endpoint=os.environ.get("EB_EMBEDDING_ENDPOINT", "http://localhost:8811/v1"),
            embedding_api_key=os.environ.get("EB_EMBEDDING_API_KEY", ""),
            embedding_dimensions=int(os.environ.get("EB_EMBEDDING_DIMENSIONS", "1024")),
        )
        embedding_api_key = os.environ.get("EB_EMBEDDING_API_KEY", "")
        llm_api_key = os.environ.get("EB_LLM_API_KEY", "") or embedding_api_key
        llm = LLMConfig(
            model=os.environ.get("EB_LLM_MODEL", "gemini/gemini-2.5-pro"),
            endpoint=os.environ.get("EB_LLM_ENDPOINT", "http://localhost:8811/v1"),
            api_key=llm_api_key,
            max_tokens=int(os.environ.get("EB_LLM_MAX_TOKENS", "8192")),
            temperature=float(os.environ.get("EB_LLM_TEMPERATURE", "0.1")),
            extraction_max_input_tokens=int(os.environ.get("EB_LLM_EXTRACTION_MAX_INPUT_TOKENS", "4000")),
            extraction_max_output_tokens=int(os.environ.get("EB_LLM_EXTRACTION_MAX_OUTPUT_TOKENS", "16384")),
            extraction_max_facts_per_batch=int(os.environ.get("EB_LLM_EXTRACTION_MAX_FACTS", "10")),
            summarization_max_output_tokens=int(os.environ.get("EB_LLM_SUMMARIZATION_MAX_OUTPUT_TOKENS", "200")),
            summarization_min_artifact_chars=int(os.environ.get("EB_LLM_SUMMARIZATION_MIN_CHARS", "500")),
            ingest_batch_size=int(os.environ.get("EB_INGEST_BATCH_SIZE", "6")),
            ingest_batch_timeout_seconds=float(os.environ.get("EB_INGEST_BATCH_TIMEOUT", "60.0")),
            ingest_buffer_ttl_seconds=int(os.environ.get("EB_INGEST_BUFFER_TTL", "300")),
            extraction_context_facts=int(os.environ.get("EB_EXTRACTION_CONTEXT_FACTS", "20")),
            extraction_context_ttl_seconds=int(os.environ.get("EB_EXTRACTION_CONTEXT_TTL", "3600")),
        )
        reranker = RerankerConfig(
            endpoint=os.environ.get("EB_RERANKER_ENDPOINT", "http://localhost:1235"),
            api_key=os.environ.get("EB_RERANKER_API_KEY", ""),
            model=os.environ.get("EB_RERANKER_MODEL", "Qwen/Qwen3-Reranker-4B"),
        )
        trace_config = TraceConfig(
            otel_logs_enabled=os.environ.get("EB_TRACE_OTEL_LOGS_ENABLED", "false").lower() == "true",
            memory_max_events=int(os.environ.get("EB_TRACE_MEMORY_MAX_EVENTS", "10000")),
        )
        clickhouse_config = ClickHouseConfig(
            enabled=os.environ.get("EB_CLICKHOUSE_ENABLED", "false").lower() == "true",
            host=os.environ.get("EB_CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("EB_CLICKHOUSE_PORT", "8123")),
            database=os.environ.get("EB_CLICKHOUSE_DATABASE", "otel"),
        )
        infra = InfraConfig(
            redis_url=os.environ.get("EB_REDIS_URL", "redis://localhost:6379"),
            otel_endpoint=os.environ.get("EB_OTEL_ENDPOINT"),
            log_level=os.environ.get("EB_LOG_LEVEL", "INFO"),
            metrics_ttl_seconds=int(os.environ.get("EB_METRICS_TTL_SECONDS", "3600")),
            trace=trace_config,
            clickhouse=clickhouse_config,
        )
        embedding_cache = EmbeddingCacheConfig(
            enabled=os.environ.get("EB_EMBEDDING_CACHE_ENABLED", "true").lower() == "true",
            ttl_seconds=int(os.environ.get("EB_EMBEDDING_CACHE_TTL", "3600")),
        )
        scoring = ScoringConfig(
            snapshot_ttl_seconds=int(os.environ.get("EB_SCORING_SNAPSHOT_TTL", "300")),
            session_goals_ttl_seconds=int(os.environ.get("EB_SESSION_GOALS_TTL", "86400")),
        )
        gateway = GatewayConfig(
            gateway_id=os.environ.get("EB_GATEWAY_ID", "local"),
            gateway_short_name=os.environ.get("EB_GATEWAY_SHORT_NAME", ""),
            org_id=os.environ.get("EB_ORG_ID") or None,
            team_id=os.environ.get("EB_TEAM_ID") or None,
            agent_authority_level=int(os.environ.get("EB_AGENT_AUTHORITY_LEVEL", "0")),
        )
        compaction_llm_api_key = os.environ.get("EB_COMPACTION_LLM_API_KEY", "") or llm_api_key
        compaction_llm = CompactionLLMConfig(
            model=os.environ.get("EB_COMPACTION_LLM_MODEL", "gemini/gemini-2.5-flash"),
            endpoint=os.environ.get("EB_COMPACTION_LLM_ENDPOINT", llm.endpoint),
            api_key=compaction_llm_api_key,
        )
        return cls(
            cognee=cognee,
            llm=llm,
            reranker=reranker,
            infra=infra,
            default_profile=os.environ.get("EB_DEFAULT_PROFILE", "coding"),
            enable_trace_ledger=os.environ.get("EB_ENABLE_TRACE_LEDGER", "true").lower() == "true",
            enable_guards=os.environ.get("EB_ENABLE_GUARDS", "true").lower() == "true",
            max_concurrent_sessions=int(os.environ.get("EB_MAX_CONCURRENT_SESSIONS", "100")),
            embedding_cache=embedding_cache,
            scoring=scoring,
            gateway=gateway,
            compaction_llm=compaction_llm,
            consolidation_min_retention_seconds=int(os.environ.get("EB_CONSOLIDATION_MIN_RETENTION_SECONDS", "172800")),
            # Phase 9 env overrides
            successful_use=SuccessfulUseConfig(
                enabled=os.environ.get("EB_SUCCESSFUL_USE_ENABLED", "false").lower() == "true",
                endpoint=os.environ.get("EB_SUCCESSFUL_USE_ENDPOINT", "http://host.docker.internal:8811/v1"),
                api_key=os.environ.get("EB_SUCCESSFUL_USE_API_KEY", "") or llm_api_key,
                model=os.environ.get("EB_SUCCESSFUL_USE_MODEL", "gemini/gemini-2.5-flash"),
                batch_size=int(os.environ.get("EB_SUCCESSFUL_USE_BATCH_SIZE", "5")),
            ),
            blocker_extraction=BlockerExtractionConfig(
                enabled=os.environ.get("EB_BLOCKER_EXTRACTION_ENABLED", "false").lower() == "true",
                endpoint=os.environ.get("EB_BLOCKER_EXTRACTION_ENDPOINT", "http://host.docker.internal:8811/v1"),
                api_key=os.environ.get("EB_BLOCKER_EXTRACTION_API_KEY", "") or llm_api_key,
                model=os.environ.get("EB_BLOCKER_EXTRACTION_MODEL", "gemini/gemini-2.5-flash"),
                run_every_n_turns=int(os.environ.get("EB_BLOCKER_EXTRACTION_EVERY_N_TURNS", "3")),
            ),
        )
