"""Stats and telemetry engine interface."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod


class IStatsAndTelemetryEngine(ABC):
    """Records injection/usage stats and provides per-profile telemetry."""

    @abstractmethod
    async def record_injection(self, session_id: uuid.UUID, fact_id: uuid.UUID, tokens: int) -> None:
        """Record that a fact was injected into context."""
        ...

    @abstractmethod
    async def record_use(self, session_id: uuid.UUID, fact_id: uuid.UUID, was_useful: bool) -> None:
        """Record whether an injected fact was actually used."""
        ...

    @abstractmethod
    async def get_stats_by_profile(self, profile_name: str) -> dict[str, float]:
        """Get aggregated stats for a profile."""
        ...
