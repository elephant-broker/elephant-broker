"""Profile configuration schemas — controls scoring, budgets, compaction, and guards per profile."""
from __future__ import annotations

import logging
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from elephantbroker.schemas.guards import ApprovalRouting, AutonomyPolicy, StaticRule, StructuralValidatorSpec
from elephantbroker.schemas.working_set import ScoringWeights


class GraphMode(StrEnum):
    """Graph traversal mode for a profile."""
    LOCAL = "local"
    HYBRID = "hybrid"
    GLOBAL = "global"


class IsolationLevel(StrEnum):
    """Controls how strictly memory is partitioned."""
    NONE = "none"
    LOOSE = "loose"
    STRICT = "strict"


class IsolationScope(StrEnum):
    """What dimension isolation is applied on."""
    GLOBAL = "global"
    SESSION_KEY = "session_key"
    ACTOR = "actor"
    SUBAGENT_INHERIT = "subagent_inherit"


class CompactionPolicy(BaseModel):
    """How compaction behaves for this profile."""
    cadence: str = "balanced"
    target_tokens: int = Field(default=4000, ge=100)
    preserve_goal_state: bool = True
    preserve_open_questions: bool = True
    preserve_evidence_refs: bool = True


class RetrievalPolicy(BaseModel):
    """Controls the 5-source retrieval pipeline per profile."""
    isolation_level: IsolationLevel = IsolationLevel.LOOSE
    isolation_scope: IsolationScope = IsolationScope.SESSION_KEY
    structural_enabled: bool = True
    structural_fetch_k: int = Field(default=20, ge=0)
    structural_weight: float = Field(default=0.4, ge=0.0)
    keyword_enabled: bool = True
    keyword_fetch_k: int = Field(default=15, ge=0)
    keyword_weight: float = Field(default=0.3, ge=0.0)
    vector_enabled: bool = True
    vector_fetch_k: int = Field(default=20, ge=0)
    vector_weight: float = Field(default=0.5, ge=0.0)
    graph_expansion_enabled: bool = True
    graph_mode: GraphMode = GraphMode.HYBRID
    graph_max_depth: int = Field(default=2, ge=1, le=5)
    graph_expansion_weight: float = Field(default=0.2, ge=0.0)
    artifact_enabled: bool = True
    artifact_fetch_k: int = Field(default=10, ge=0)
    root_top_k: int = Field(default=40, ge=1)


class AutorecallPolicy(BaseModel):
    """Controls automatic recall injection via before_agent_start hook."""
    enabled: bool = True
    require_successful_use_prior: bool = False
    require_not_in_compact_state: bool = True
    retrieval: RetrievalPolicy = Field(default_factory=RetrievalPolicy)
    auto_recall_injection_top_k: int = Field(default=10, ge=1)
    min_similarity: float = Field(default=0.3, ge=0.0, le=1.0)
    extraction_max_facts_per_batch_before_dedup: int = Field(default=5, ge=0)
    dedup_similarity: float = Field(default=0.95, ge=0.0, le=1.0)
    extraction_focus: list[str] = Field(default_factory=list)
    custom_categories: list[str] = Field(default_factory=list)
    superseded_confidence_factor: float = Field(default=0.3, ge=0.0, le=1.0)


class VerificationPolicy(BaseModel):
    """How strictly claims must be verified."""
    proof_required_for_completion: bool = False
    supervisor_sampling_rate: float = Field(default=0.0, ge=0.0, le=1.0)


_profile_logger = logging.getLogger("elephantbroker.schemas.profile")


class GuardPolicy(BaseModel):
    """Red-line guard configuration."""
    force_system_constraint_injection: bool = True
    preflight_check_strictness: str = "medium"
    # Phase 7: extended guard policy fields
    static_rules: list[object] = Field(default_factory=list)
    redline_exemplars: list[str] = Field(default_factory=list)
    structural_validators: list[object] = Field(default_factory=list)
    bm25_block_threshold: float = Field(default=0.85, ge=0.0, le=1.0)
    bm25_warn_threshold: float = Field(default=0.60, ge=0.0, le=1.0)
    semantic_similarity_threshold: float = Field(default=0.80, ge=0.0, le=1.0)
    llm_escalation_enabled: bool = False
    autonomy: AutonomyPolicy = Field(default_factory=AutonomyPolicy)
    approval_routing: ApprovalRouting = Field(default_factory=ApprovalRouting)
    near_miss_escalation_threshold: int = Field(default=3, ge=1)
    near_miss_window_turns: int = Field(default=5, ge=1)
    load_procedure_redline_bindings: bool = True

    @field_validator("static_rules", mode="before")
    @classmethod
    def _coerce_static_rules(cls, v: list) -> list:
        """Coerce dicts to StaticRule instances with warning on failure."""
        if not isinstance(v, list):
            return v
        result: list = []
        for item in v:
            if isinstance(item, dict):
                try:
                    result.append(StaticRule(**item))
                except Exception as exc:
                    _profile_logger.warning("Failed to coerce static_rule dict to StaticRule: %s — %s", item, exc)
                    result.append(item)  # preserve raw dict for fail-soft
            else:
                result.append(item)
        return result

    @field_validator("structural_validators", mode="before")
    @classmethod
    def _coerce_structural_validators(cls, v: list) -> list:
        """Coerce dicts to StructuralValidatorSpec instances with warning on failure."""
        if not isinstance(v, list):
            return v
        result: list = []
        for item in v:
            if isinstance(item, dict):
                try:
                    result.append(StructuralValidatorSpec(**item))
                except Exception as exc:
                    _profile_logger.warning(
                        "Failed to coerce structural_validator dict to StructuralValidatorSpec: %s — %s",
                        item, exc,
                    )
                    result.append(item)  # preserve raw dict for fail-soft
            else:
                result.append(item)
        return result


class Budgets(BaseModel):
    """Token and item budgets for this profile."""
    mem0_fetch_k: int = Field(default=20, ge=1)
    graph_fetch_k: int = Field(default=15, ge=1)
    artifact_fetch_k: int = Field(default=10, ge=1)
    final_prompt_k: int = Field(default=30, ge=1)
    root_top_k: int = Field(default=40, ge=1)
    max_prompt_tokens: int = Field(default=8000, ge=100)
    max_system_overlay_tokens: int = Field(default=1500, ge=0)
    subagent_packet_tokens: int = Field(default=3000, ge=100)


class AssemblyPlacementPolicy(BaseModel):
    """Per-profile controls for how context is placed in the 4-block assembly."""
    system_prompt_constraints: bool = True
    system_prompt_procedures: bool = True
    system_prompt_guards: bool = True
    system_context_goals: bool = True
    system_context_blockers: bool = True
    context_working_set: bool = True
    evidence_refs: bool = True
    replace_tool_outputs: bool = True
    replace_tool_output_min_tokens: int = Field(default=100, ge=0)
    keep_last_n_tool_outputs: int = Field(default=1, ge=0)
    conversation_dedup_enabled: bool = True
    conversation_dedup_threshold: float = Field(default=0.7, ge=0.3, le=1.0)
    goal_injection_cadence: Literal["always", "smart"] = "smart"
    goal_reminder_interval: int = Field(default=5, ge=1)


class SuccessfulUseThresholds(BaseModel):
    """Thresholds for the successful-use scanner in ``ContextLifecycle._track_successful_use``.

    Set once per profile via ``ProfilePolicy.successful_use_thresholds``; when
    left unset the module defaults below apply. Defaults match the J-1
    calibration baseline (0.15/0.3/0.15/0.15/3) that shipped in PR #6.
    T-2 (2026-04-21) made these per-profile-configurable so profiles with
    different signal quality (research = looser, managerial = stricter)
    can tune independently without recompiling.
    """
    s1_direct_quote_ratio: float = Field(default=0.15, ge=0.0, le=1.0)
    s2_tool_correlation_overlap: float = Field(default=0.3, ge=0.0, le=1.0)
    s3_jaccard_score: float = Field(default=0.15, ge=0.0, le=1.0)
    use_confidence_gate: float = Field(default=0.15, ge=0.0, le=1.0)
    s6_ignored_turns_floor: int = Field(default=3, ge=1)


class ProfilePolicy(BaseModel):
    """Complete policy configuration for a profile."""
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    extends: str | None = None
    graph_mode: GraphMode = GraphMode.HYBRID
    budgets: Budgets = Field(default_factory=Budgets)
    scoring_weights: ScoringWeights = Field(default_factory=ScoringWeights)
    compaction: CompactionPolicy = Field(default_factory=CompactionPolicy)
    autorecall: AutorecallPolicy = Field(default_factory=AutorecallPolicy)
    retrieval: RetrievalPolicy = Field(default_factory=RetrievalPolicy)
    verification: VerificationPolicy = Field(default_factory=VerificationPolicy)
    guards: GuardPolicy = Field(default_factory=GuardPolicy)
    # Phase 6 additions
    session_data_ttl_seconds: int = Field(default=86400, ge=3600)
    assembly_placement: AssemblyPlacementPolicy = Field(default_factory=AssemblyPlacementPolicy)
    ingest_batch_size: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Per-profile override for the ingest buffer flush threshold "
            "(messages). When None, falls back to LLMConfig.ingest_batch_size "
            "(global EB_INGEST_BATCH_SIZE). Independent from "
            "AutorecallPolicy.extraction_max_facts_per_batch_before_dedup "
            "(which caps LLM extraction output, not the buffer flush)."
        ),
    )
    successful_use_thresholds: SuccessfulUseThresholds | None = Field(
        default=None,
        description=(
            "Per-profile override for the successful-use scanner thresholds "
            "(S1/S2/S3/use-confidence gate + S6 ignored-turns floor). "
            "When None, the scanner uses module defaults (see "
            "SuccessfulUseThresholds). T-2 (2026-04-21)."
        ),
    )
