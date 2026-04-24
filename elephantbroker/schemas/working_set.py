"""Working-set competition schemas — the heart of context assembly."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field

from elephantbroker.schemas.config import (
    ConflictDetectionConfig,
    ScoringConfig,
    VerificationMultipliers,
)


class ScoringWeights(BaseModel):
    """Weight vector for the 11-dimension scoring system."""
    turn_relevance: float = 1.0
    session_goal_relevance: float = 1.0
    global_goal_relevance: float = 0.5
    recency: float = 0.8
    successful_use_prior: float = 0.6
    confidence: float = 0.4
    evidence_strength: float = 0.3
    novelty: float = 0.5
    # #1147 RESOLVED (R2-P2): penalty weights MUST be <= 0. A positive value
    # would invert the intent of "penalty" — redundant / contradictory facts
    # would BOOST their score instead of being deprioritised. Prior schema had
    # no sign constraint, so a misconfigured profile could silently ship with
    # broken scoring.
    redundancy_penalty: float = Field(default=-0.7, le=0.0)
    contradiction_penalty: float = Field(default=-1.0, le=0.0)
    cost_penalty: float = Field(default=-0.3, le=0.0)
    # Phase 5 additions (GAP-7)
    recency_half_life_hours: float = Field(default=69.0, ge=1.0)
    evidence_refs_for_max_score: int = Field(default=3, ge=1)
    # Per-profile detection thresholds (GAP-8)
    redundancy_similarity_threshold: float = Field(default=0.85, ge=0.0, le=1.0)
    contradiction_similarity_threshold: float = Field(default=0.9, ge=0.0, le=1.0)
    contradiction_confidence_gap: float = Field(default=0.3, ge=0.0, le=1.0)

    def weighted_sum(self, scores: WorkingSetScores) -> float:
        """Compute the weighted sum of scores using these weights."""
        return (
            self.turn_relevance * scores.turn_relevance
            + self.session_goal_relevance * scores.session_goal_relevance
            + self.global_goal_relevance * scores.global_goal_relevance
            + self.recency * scores.recency
            + self.successful_use_prior * scores.successful_use_prior
            + self.confidence * scores.confidence
            + self.evidence_strength * scores.evidence_strength
            + self.novelty * scores.novelty
            + self.redundancy_penalty * scores.redundancy_penalty
            + self.contradiction_penalty * scores.contradiction_penalty
            + self.cost_penalty * scores.cost_penalty
        )


class WorkingSetScores(BaseModel):
    """Raw scores on each dimension for a candidate fact."""
    turn_relevance: float = 0.0
    session_goal_relevance: float = 0.0
    global_goal_relevance: float = 0.0
    recency: float = 0.0
    successful_use_prior: float = 0.0
    confidence: float = 0.0
    evidence_strength: float = 0.0
    novelty: float = 0.0
    redundancy_penalty: float = 0.0
    contradiction_penalty: float = 0.0
    cost_penalty: float = 0.0
    final: float = 0.0


class WorkingSetItem(BaseModel):
    """A fact that has been scored and may be included in the working set.

    T-3: `source_type` carries the DataPoint-type semantic — "what KIND of thing
    is this". `retrieval_source` (new, nullable) carries retrieval-path
    provenance — "which of the 5 fact-retrieval sources produced this fact".
    Previously both semantics were overloaded into the single freeform
    `source_type: str`, with retrieval items taking values like "vector" /
    "keyword" / "graph" / "structural" — causing consumers to branch on
    "is this a fact?" via ad-hoc unions (tactical union constant; see
    TD-scanner-3 closure in local/IMPLEMENTED-PR-6-merge.md for the T-3
    history). The split makes both questions answerable independently:
    - `source_type == "fact"`  → is it fact-class?
    - `retrieval_source`       → which retrieval path produced it? (None for
      non-retrieval items: goals, procedures, artifacts)

    Artifacts intentionally get `source_type="artifact", retrieval_source=None`
    — the `retrieval_source` field is about FACT retrieval paths; artifacts
    are a distinct DataPoint type that happens to flow through the same
    retrieval pipeline.
    """
    id: str
    source_type: Literal[
        "fact", "artifact", "goal", "persistent_goal", "procedure",
    ]
    retrieval_source: Literal["structural", "keyword", "vector", "graph"] | None = None
    source_id: uuid.UUID
    text: str
    scores: WorkingSetScores = Field(default_factory=WorkingSetScores)
    token_size: int = Field(default=0, ge=0)
    system_prompt_eligible: bool = False
    must_inject: bool = False
    evidence_ref_ids: list[uuid.UUID] = Field(default_factory=list)
    # Phase 5 metadata (GAP-1) — populated from FactAssertion during conversion
    confidence: float = 1.0
    use_count: int = 0
    successful_use_count: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_used_at: datetime | None = None
    category: str = "general"
    goal_ids: list[uuid.UUID] = Field(default_factory=list)
    goal_relevance_tags: dict[str, str] = Field(default_factory=dict)


class WorkingSetSnapshot(BaseModel):
    """A point-in-time snapshot of the working set after scoring."""
    snapshot_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    session_id: uuid.UUID
    items: list[WorkingSetItem] = Field(default_factory=list)
    token_budget: int = Field(ge=0)
    tokens_used: int = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    weights_used: ScoringWeights = Field(default_factory=ScoringWeights)
    gateway_id: str = ""


class ScoringContext(BaseModel):
    """All pre-computed data needed by ScoringEngine for a single build_working_set call."""
    model_config = {"arbitrary_types_allowed": True}

    turn_text: str = ""
    turn_embedding: list[float] = Field(default_factory=list)
    session_goals: list = Field(default_factory=list)  # list[GoalState]
    global_goals: list = Field(default_factory=list)  # list[GoalState]
    goal_embeddings: dict[str, list[float]] = Field(default_factory=dict)
    compact_state_ids: set[str] = Field(default_factory=set)
    weights: ScoringWeights = Field(default_factory=ScoringWeights)
    now: datetime = Field(default_factory=lambda: datetime.now(UTC))
    token_budget: int = 8000
    # Pre-computed indices from Cypher queries
    evidence_index: dict[str, int] = Field(default_factory=dict)
    verification_index: dict[str, str] = Field(default_factory=dict)
    conflict_pairs: set[tuple[str, str]] = Field(default_factory=set)
    conflict_edge_types: dict[tuple[str, str], str] = Field(default_factory=dict)
    item_embeddings: dict[str, list[float]] = Field(default_factory=dict)
    # Config objects (GAP-8)
    verification_multipliers: VerificationMultipliers = Field(default_factory=VerificationMultipliers)
    conflict_config: ConflictDetectionConfig = Field(default_factory=ConflictDetectionConfig)
    scoring_config: ScoringConfig = Field(default_factory=ScoringConfig)
    # Pre-computed evidence ID mapping (fact_id → list[evidence_id])
    evidence_ids: dict[str, list[str]] = Field(default_factory=dict)
    # Enrichment fields
    goal_relevance_tags: dict[str, dict[str, str]] = Field(default_factory=dict)
    pairwise_redundancy: dict = Field(default_factory=dict)
    llm_contradiction_flags: dict = Field(default_factory=dict)
