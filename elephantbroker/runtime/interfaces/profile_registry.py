"""Profile registry interface."""
from __future__ import annotations

from abc import ABC, abstractmethod

from elephantbroker.schemas.profile import ProfilePolicy
from elephantbroker.schemas.working_set import ScoringWeights


class IProfileRegistry(ABC):
    """Resolves and manages profile policies."""

    @abstractmethod
    async def resolve_profile(self, profile_name: str, org_id: str | None = None) -> ProfilePolicy:
        """Resolve a profile by name, applying inheritance and org overrides."""
        ...

    @abstractmethod
    async def get_effective_policy(self, profile_name: str, org_id: str | None = None) -> ProfilePolicy:
        """Get the fully resolved policy for a profile."""
        ...

    @abstractmethod
    async def get_scoring_weights(self, profile_name: str, org_id: str | None = None) -> ScoringWeights:
        """Get the scoring weights for a profile."""
        ...
