"""Scoring framework schemas."""
from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class ScoringDimension(StrEnum):
    """The 11 scoring dimensions used in working-set competition."""
    TURN_RELEVANCE = "turn_relevance"
    SESSION_GOAL_RELEVANCE = "session_goal_relevance"
    GLOBAL_GOAL_RELEVANCE = "global_goal_relevance"
    RECENCY = "recency"
    SUCCESSFUL_USE_PRIOR = "successful_use_prior"
    CONFIDENCE = "confidence"
    EVIDENCE_STRENGTH = "evidence_strength"
    NOVELTY = "novelty"
    REDUNDANCY_PENALTY = "redundancy_penalty"
    CONTRADICTION_PENALTY = "contradiction_penalty"
    COST_PENALTY = "cost_penalty"


class WeightPreset(BaseModel):
    """Named weight preset for a profile or scenario."""
    name: str = Field(min_length=1)
    description: str = ""
    weights: dict[ScoringDimension, float] = Field(default_factory=dict)


class TuningDelta(BaseModel):
    """A requested adjustment to scoring weights based on feedback."""
    dimension: ScoringDimension
    delta: float
    reason: str = ""
    source_actor_id: str | None = None
