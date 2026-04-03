"""Scoring tuner interface."""
from __future__ import annotations

from abc import ABC, abstractmethod

from elephantbroker.schemas.scoring import TuningDelta
from elephantbroker.schemas.working_set import ScoringWeights


class IScoringTuner(ABC):
    """Tunes scoring weights based on feedback and usage patterns."""

    @abstractmethod
    async def get_weights(
        self, profile_name: str,
        org_id: str | None = None,
        gateway_id: str | None = None,
    ) -> ScoringWeights:
        """Get effective weights: base profile + org override + tuning deltas."""
        ...

    @abstractmethod
    async def apply_feedback(
        self, profile_name: str,
        deltas: list[TuningDelta],
        org_id: str | None = None,
        gateway_id: str | None = None,
    ) -> None:
        """Apply tuning deltas from consolidation Stage 9. Persists to store."""
        ...

    @abstractmethod
    async def run_tuning_cycle(
        self, org_id: str, gateway_id: str,
    ) -> dict[str, list[TuningDelta]]:
        """Run tuning for all profiles in an org. Returns deltas per profile_id."""
        ...
