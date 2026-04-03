"""Profile registry with inheritance engine and org-level overrides.

Resolution order (3-layer):
    Layer 1 — Base presets from ``presets.py`` (base + 5 named profiles)
    Layer 2 — Org overrides from SQLite via ``OrgOverrideStore``
    Layer 3 — (Phase 9) Tuning deltas from ``ScoringTuner``

Resolved profiles are cached in-memory with configurable TTL (default 5 min).
Cache is keyed by ``(profile_id, org_id)`` and invalidated on override changes.
"""
from __future__ import annotations

import logging
import time
from copy import deepcopy

from elephantbroker.runtime.interfaces.profile_registry import IProfileRegistry
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.profiles.inheritance import ProfileInheritanceEngine
from elephantbroker.runtime.profiles.org_override_store import OrgOverrideStore
from elephantbroker.runtime.profiles.presets import PROFILE_PRESETS
from elephantbroker.schemas.profile import ProfilePolicy
from elephantbroker.schemas.trace import TraceEvent, TraceEventType
from elephantbroker.schemas.working_set import ScoringWeights

logger = logging.getLogger(__name__)


class ProfileRegistry(IProfileRegistry):
    """Resolves profiles with inheritance, org overrides, and TTL caching."""

    def __init__(
        self,
        trace_ledger: ITraceLedger,
        org_store: OrgOverrideStore | None = None,
        cache_ttl_seconds: int = 300,
        metrics=None,
    ) -> None:
        self._presets = PROFILE_PRESETS
        self._engine = ProfileInheritanceEngine()
        self._org_store = org_store
        self._trace = trace_ledger
        self._cache: dict[tuple[str, str | None], tuple[ProfilePolicy, float]] = {}
        self._cache_ttl = cache_ttl_seconds
        self._metrics = metrics

    async def resolve_profile(self, profile_name: str, org_id: str | None = None) -> ProfilePolicy:
        """Resolve a profile by name with inheritance and optional org overrides.

        1. Check in-memory cache by ``(profile_name, org_id)`` — return deep copy if fresh.
        2. Look up profile in presets — ``KeyError`` if not found.
        3. Flatten inheritance chain via engine.
        4. Load org override from SQLite (if org_id and store available).
        5. Apply org override via engine.
        6. Cache result with timestamp.
        7. Return deep copy (immutable for session lifetime).
        """
        cache_key = (profile_name, org_id)

        # 1. Cache check
        cached = self._cache.get(cache_key)
        if cached is not None:
            policy, ts = cached
            if time.monotonic() - ts < self._cache_ttl:
                logger.debug("Profile cache HIT: %s/org=%s (age=%.0fs)", profile_name, org_id, time.monotonic() - ts)
                if self._metrics:
                    self._metrics.inc_profile_cache("hit")
                return deepcopy(policy)
            else:
                logger.debug("Profile cache EXPIRED: %s/org=%s", profile_name, org_id)
                if self._metrics:
                    self._metrics.inc_profile_cache("expired")
                del self._cache[cache_key]

        # 2. Preset lookup
        preset = self._presets.get(profile_name)
        if preset is None:
            raise KeyError(f"Unknown profile: {profile_name}")

        # 3. Load org override
        org_overrides: dict | None = None
        if org_id and self._org_store:
            try:
                org_overrides = await self._org_store.get_override(org_id, profile_name)
            except Exception:
                logger.warning("Failed to load org override for org=%s profile=%s", org_id, profile_name, exc_info=True)

        # 4. Flatten inheritance + apply org override
        resolved = self._engine.flatten(preset, self._presets, org_overrides=org_overrides)

        # 5. Cache
        self._cache[cache_key] = (resolved, time.monotonic())

        # 6. Metrics + trace
        if self._metrics:
            self._metrics.inc_profile_resolve(profile_name, org_overrides is not None)
            self._metrics.inc_profile_cache("miss")
        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.PROFILE_RESOLVED,
                payload={
                    "profile": profile_name,
                    "org_id": org_id or "",
                    "has_org_override": org_overrides is not None,
                    "chain": [profile_name, preset.extends] if preset.extends else [profile_name],
                },
            )
        )

        # 7. Return deep copy
        return deepcopy(resolved)

    async def get_effective_policy(self, profile_name: str, org_id: str | None = None) -> ProfilePolicy:
        """Alias for ``resolve_profile`` with org context."""
        return await self.resolve_profile(profile_name, org_id=org_id)

    async def get_scoring_weights(self, profile_name: str, org_id: str | None = None) -> ScoringWeights:
        """Get resolved scoring weights for a profile."""
        policy = await self.resolve_profile(profile_name, org_id=org_id)
        return policy.scoring_weights

    async def list_profiles(self) -> list[str]:
        """List all available profile IDs (excluding 'base')."""
        return [k for k in self._presets if k != "base"]

    async def register_org_override(
        self,
        org_id: str,
        profile_id: str,
        overrides: dict,
        actor_id: str | None = None,
    ) -> None:
        """Register organization-specific overrides. Persists to SQLite.

        Invalidates cache for ``(profile_id, org_id)``.
        """
        if profile_id not in self._presets:
            raise KeyError(f"Unknown profile: {profile_id}")
        if self._org_store is None:
            raise RuntimeError("Org override store not configured")
        await self._org_store.set_override(org_id, profile_id, overrides, actor_id)
        self._cache.pop((profile_id, org_id), None)
        logger.info("Invalidated cache for %s/org=%s (override changed)", profile_id, org_id)

    async def delete_org_override(self, org_id: str, profile_id: str) -> None:
        """Remove organization-specific override. Invalidates cache."""
        if self._org_store:
            await self._org_store.delete_override(org_id, profile_id)
        self._cache.pop((profile_id, org_id), None)
        logger.info("Invalidated cache for %s/org=%s (override deleted)", profile_id, org_id)
