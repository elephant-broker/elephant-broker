"""Business tier capability matrix."""
from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class BusinessTier(StrEnum):
    """Available business tiers."""
    MEMORY_ONLY = "memory_only"
    CONTEXT_ONLY = "context_only"
    FULL = "full"


# Maps tier -> set of enabled module interface names
TIER_CAPABILITIES: dict[BusinessTier, set[str]] = {
    BusinessTier.MEMORY_ONLY: {
        "IActorRegistry",
        "IGoalManager",
        "IMemoryStoreFacade",
        "IRetrievalOrchestrator",
        "IRerankOrchestrator",
        "IToolArtifactStore",
        "ITraceLedger",
        "IProfileRegistry",
        "IEvidenceAndVerificationEngine",
        "IProcedureEngine",
    },
    BusinessTier.CONTEXT_ONLY: {
        "IActorRegistry",
        "IGoalManager",
        "IWorkingSetManager",
        "IContextAssembler",
        "ICompactionEngine",
        "IRedLineGuardEngine",
        "IStatsAndTelemetryEngine",
        "ITraceLedger",
        "IProfileRegistry",
        "IScoringTuner",
    },
    BusinessTier.FULL: {
        "IActorRegistry",
        "IGoalManager",
        "IMemoryStoreFacade",
        "IWorkingSetManager",
        "IContextAssembler",
        "ICompactionEngine",
        "IProcedureEngine",
        "IEvidenceAndVerificationEngine",
        "IRedLineGuardEngine",
        "IToolArtifactStore",
        "IRetrievalOrchestrator",
        "IRerankOrchestrator",
        "IStatsAndTelemetryEngine",
        "IConsolidationEngine",
        "IProfileRegistry",
        "ITraceLedger",
        "IScoringTuner",
    },
}


class TierCapabilityMatrix(BaseModel):
    """Queryable capability matrix for business tiers."""
    tier: BusinessTier
    enabled_modules: set[str] = Field(default_factory=set)

    @classmethod
    def for_tier(cls, tier: BusinessTier) -> TierCapabilityMatrix:
        """Create a capability matrix for the given tier."""
        return cls(tier=tier, enabled_modules=TIER_CAPABILITIES[tier])

    def is_enabled(self, module_name: str) -> bool:
        """Check if a module is enabled for this tier."""
        return module_name in self.enabled_modules
