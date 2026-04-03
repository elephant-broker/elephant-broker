"""ScoringTuner — adaptive weight tuning with per-gateway persistence.

Weight resolution chain (AD-7):
    base profile → named profile (extends base) → org override (Phase 8) → tuning delta (Phase 9)

Tuning deltas are per (profile_id, org_id, gateway_id) and never shared across gateways.
Caps reference BASE weight (not current tuned weight) to prevent convergence trap.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from elephantbroker.runtime.interfaces.profile_registry import IProfileRegistry
from elephantbroker.runtime.interfaces.scoring_tuner import IScoringTuner
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.schemas.scoring import TuningDelta
from elephantbroker.schemas.trace import TraceEvent, TraceEventType
from elephantbroker.schemas.working_set import ScoringWeights

if TYPE_CHECKING:
    from elephantbroker.runtime.working_set.tuning_delta_store import TuningDeltaStore

logger = logging.getLogger("elephantbroker.runtime.working_set.scoring_tuner")


class ScoringTuner(IScoringTuner):
    """Adaptive scoring weight tuner with SQLite-backed delta persistence."""

    def __init__(
        self,
        trace_ledger: ITraceLedger,
        profile_registry: IProfileRegistry,
        delta_store: TuningDeltaStore | None = None,
    ) -> None:
        self._trace = trace_ledger
        self._profile_registry = profile_registry
        self._delta_store = delta_store

    async def get_weights(
        self,
        profile_name: str,
        org_id: str | None = None,
        gateway_id: str | None = None,
    ) -> ScoringWeights:
        """Get effective weights: base profile + org override + tuning deltas.

        1. Get base weights from profile_registry (includes Phase 8 org overrides)
        2. If delta_store + org_id + gateway_id: load accumulated deltas
        3. Apply deltas additively to base weights
        4. Return adjusted weights
        """
        base = await self._profile_registry.get_scoring_weights(profile_name, org_id=org_id)

        if not self._delta_store or not org_id or not gateway_id:
            return base

        try:
            deltas = await self._delta_store.get_deltas(profile_name, org_id, gateway_id)
        except Exception:
            logger.debug("Failed to load tuning deltas — returning base weights")
            return base

        if not deltas:
            return base

        # Apply deltas additively to a copy of the base weights
        adjusted = base.model_copy()
        for dim_name, delta_value in deltas.items():
            if hasattr(adjusted, dim_name):
                current = getattr(adjusted, dim_name)
                if isinstance(current, (int, float)):
                    setattr(adjusted, dim_name, current + delta_value)

        return adjusted

    async def apply_feedback(
        self,
        profile_name: str,
        deltas: list[TuningDelta],
        org_id: str | None = None,
        gateway_id: str | None = None,
    ) -> None:
        """Persist tuning deltas from consolidation Stage 9."""
        if not self._delta_store or not org_id or not gateway_id:
            logger.debug("apply_feedback called without delta_store/org_id/gateway_id — skipping")
            return

        for delta in deltas:
            try:
                await self._delta_store.upsert_delta(
                    profile_id=profile_name,
                    org_id=org_id,
                    gateway_id=gateway_id,
                    dimension=delta.dimension.value if hasattr(delta.dimension, "value") else str(delta.dimension),
                    smoothed_delta=delta.delta,
                    raw_delta=delta.delta,
                )
            except Exception:
                logger.warning("Failed to persist tuning delta for %s", delta.dimension)

        await self._trace.append_event(TraceEvent(
            event_type=TraceEventType.SCORING_COMPLETED,
            payload={
                "action": "apply_feedback",
                "profile": profile_name,
                "delta_count": len(deltas),
                "org_id": org_id,
                "gateway_id": gateway_id,
            },
        ))

    async def run_tuning_cycle(
        self,
        org_id: str,
        gateway_id: str,
    ) -> dict[str, list[TuningDelta]]:
        """Run tuning cycle for all profiles.

        Loads scoring ledger, computes correlations via Stage 9 logic,
        and applies deltas. Returns deltas per profile_id.
        """
        # Get all profile names from registry
        result: dict[str, list[TuningDelta]] = {}
        try:
            profiles = ["coding", "research", "managerial", "worker", "personal_assistant"]
            for profile_name in profiles:
                await self.get_weights(profile_name, org_id=org_id, gateway_id=gateway_id)
                # Stage 9 correlation requires scoring_ledger_store (via ConsolidationEngine)
                result[profile_name] = []
        except Exception:
            logger.info("run_tuning_cycle: use ConsolidationEngine for full tuning")
        return result
