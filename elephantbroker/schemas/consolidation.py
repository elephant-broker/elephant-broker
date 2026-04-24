"""Consolidation pipeline schemas.

Models for the 9-stage consolidation ("sleep") pipeline, stage results,
configuration, and reporting.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class ConsolidationConfig(BaseModel):
    """Configuration for the consolidation pipeline."""

    # Fact loading
    batch_size: int = Field(default=500, ge=50, le=5000)
    active_session_protection_hours: float = Field(default=1.0, ge=0.0)

    # Stage 1: Cluster Near-Duplicates
    cluster_similarity_threshold: float = Field(default=0.92, ge=0.5, le=1.0)

    # Stage 2: Canonicalize
    canonicalize_divergence_threshold: float = Field(default=0.7, ge=0.0, le=1.0)
    # Text similarity below this triggers LLM merge review

    # Stage 3: Strengthen
    strengthen_success_ratio_threshold: float = Field(default=0.5, ge=0.0, le=1.0)
    strengthen_min_use_count: int = Field(default=3, ge=1)
    strengthen_boost_factor: float = Field(default=0.1, ge=0.01, le=0.5)
    # new_confidence = min(1.0, old_confidence + boost_factor * success_ratio)

    # Stage 4: Decay
    decay_recalled_unused_factor: float = Field(default=0.85, ge=0.1, le=1.0)
    # For facts recalled but never used: confidence *= this factor per cycle
    decay_never_recalled_factor: float = Field(default=0.95, ge=0.1, le=1.0)
    # For facts never recalled: gentler time-based decay
    decay_archival_threshold: float = Field(default=0.05, ge=0.0, le=0.5)
    # Below this confidence → mark for archival
    decay_scope_multipliers: dict[str, float] = Field(
        default_factory=lambda: {
            "session": 1.5,       # Session-scoped facts decay 50% faster
            "actor": 1.0,         # Actor-scoped: base rate
            "team": 0.8,          # Team-scoped: 20% slower
            "organization": 0.7,  # Org-scoped: 30% slower
            "global": 0.5,        # Global-scoped: 50% slower
            # #1141 RESOLVED (R2-P2): previously the 3 scopes below
            # (task, subagent, artifact) were missing from the default dict.
            # decay.py:60 uses `.get(scope_key, 1.0)` so missing scopes
            # silently got base rate, but now the policy is EXPLICIT + auditable.
            # Keys MUST match Scope enum values in schemas/base.py:16-25.
            "task": 1.0,          # Task-scoped: base rate (conservative default)
            "subagent": 1.0,      # Subagent-scoped: base rate
            "artifact": 1.0,      # Artifact-scoped: base rate
        },
        description="Decay multiplier per Scope. All 8 Scope enum values present post-#1141.",
    )

    # Stage 5: Prune Bad Autorecall
    autorecall_blacklist_min_recalls: int = Field(default=5, ge=2)
    autorecall_blacklist_max_success_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    # Blacklist if use_count >= min_recalls AND successful/use <= max_success_ratio

    # Stage 6: Promote
    promote_session_threshold: int = Field(default=3, ge=2)
    # Number of distinct sessions a fact must appear in for promotion
    promote_artifact_injected_threshold: int = Field(default=3, ge=1)

    # Stage 7: Refine Procedures
    pattern_recurrence_threshold: int = Field(default=3, ge=2)
    pattern_min_steps: int = Field(default=3, ge=2)
    max_patterns_per_run: int = Field(default=10, ge=1)

    # Stage 9: Recompute Salience
    ema_alpha: float = Field(default=0.3, ge=0.01, le=1.0)
    # EMA smoothing factor: higher = more responsive to recent data
    max_weight_adjustment_pct: float = Field(default=0.05, ge=0.01, le=0.20)
    # Max accumulated delta as fraction of BASE weight (5% default)
    # Caps referenced to base (profile + org override), NOT current tuned weight
    # Prevents convergence trap where drifted weights get smaller caps
    min_correlation_samples: int = Field(default=20, ge=5)
    # Minimum scored facts needed before adjusting weights

    # LLM usage
    llm_calls_per_run_cap: int = Field(default=50, ge=0)
    # Maximum LLM calls across all stages in one consolidation run

    # Dev auto-trigger (parsed from EB_DEV_CONSOLIDATION_AUTO_TRIGGER)
    dev_auto_trigger_interval: str = "0"
    # "0" = disabled. Supports "1m", "5m", "1h", "1d" suffixes.


class DuplicateCluster(BaseModel):
    """A cluster of near-duplicate facts from Stage 1."""
    cluster_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    fact_ids: list[str]
    canonical_candidate_id: str  # Highest-confidence fact in cluster
    avg_similarity: float
    session_keys: list[str]  # Distinct sessions spanned


class CanonicalResult(BaseModel):
    """Result of canonicalizing a duplicate cluster in Stage 2."""
    cluster_id: uuid.UUID
    new_canonical_fact_id: str       # Newly created fact (LLM-synthesized or deterministic)
    canonical_text: str              # The synthesized text
    archived_fact_ids: list[str]     # ALL original facts in the cluster (now archived)
    merged_provenance: list[str]     # Union of all provenance_refs
    merged_use_count: int            # Sum of all use_counts
    merged_successful_use_count: int  # Sum of all successful_use_counts
    merged_goal_ids: list[str]       # Union of all goal_ids
    llm_used: bool = False           # True if LLM synthesized the text


class StrengthenResult(BaseModel):
    """Result of strengthening a useful fact in Stage 3."""
    fact_id: str
    old_confidence: float
    new_confidence: float
    success_ratio: float
    boosted: bool


class DecayResult(BaseModel):
    """Result of decaying an unused fact in Stage 4."""
    fact_id: str
    old_confidence: float
    new_confidence: float
    decay_reason: str  # "recalled_unused", "never_recalled", "time_based"
    archived: bool  # True if decayed below archival threshold


class PromotionResult(BaseModel):
    """Result of promoting a fact in Stage 6."""
    fact_id: str
    old_memory_class: str
    new_memory_class: str
    old_scope: str
    new_scope: str
    reason: str  # "recurring_with_goal", "recurring_with_use", "persistent_goal_link"
    sessions_seen: int


class ConsolidationContext(BaseModel):
    """Shared context passed between consolidation stages.

    Note: resolved_profile, scoring_ledger_store, and facts are typed as Any
    to avoid circular imports. At runtime they are ProfilePolicy,
    ScoringLedgerStore, and list[FactAssertion] respectively.
    """
    model_config = {"arbitrary_types_allowed": True}

    org_id: str
    gateway_id: str
    profile_id: str | None = None
    resolved_profile: Any = None         # ProfilePolicy at runtime
    scoring_ledger_store: Any = None     # ScoringLedgerStore at runtime (for Stage 9)
    facts: list[Any] = Field(default_factory=list)  # list[FactAssertion] at runtime
    total_facts_loaded: int = 0
    clusters: list[DuplicateCluster] = Field(default_factory=list)
    llm_calls_used: int = 0
    llm_calls_cap: int = 50
    stage_results: list[StageResult] = Field(default_factory=list)


class StageResult(BaseModel):
    """Result from a single consolidation stage."""
    stage: int
    name: str
    items_processed: int = 0
    items_affected: int = 0
    llm_calls_made: int = 0
    duration_ms: int = 0
    details: dict = Field(default_factory=dict)


class ConsolidationSummary(BaseModel):
    """Summary of a full consolidation run."""
    duplicates_merged: int = 0
    facts_strengthened: int = 0
    facts_decayed: int = 0
    facts_archived: int = 0
    autorecall_blacklisted: int = 0
    episodic_promoted: int = 0
    procedures_suggested: int = 0
    verification_gaps_found: int = 0
    weight_adjustments: dict[str, float] = Field(default_factory=dict)
    global_promotion_candidates: int = 0  # Flagged for human review


class ConsolidationReport(BaseModel):
    """Full report from a consolidation run."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    org_id: str
    gateway_id: str
    profile_id: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    status: str = "running"  # running, completed, failed, partial
    stage_results: list[StageResult] = Field(default_factory=list)
    summary: ConsolidationSummary = Field(default_factory=ConsolidationSummary)
    error: str | None = None  # If failed, the error message


class DomainSuggestion(BaseModel):
    """A new decision domain suggested by Tier 3 auto-discovery."""
    action_target: str              # Tool name or message pattern
    suggested_domain: str           # Closest existing domain or new name
    occurrences: int                # Times seen as UNCATEGORIZED
    similarity_to_existing: float   # Cosine similarity to nearest existing domain
    gateway_id: str = ""
