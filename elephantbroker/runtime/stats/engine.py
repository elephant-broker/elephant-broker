"""In-memory stats and telemetry engine."""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from elephantbroker.runtime.interfaces.stats import IStatsAndTelemetryEngine
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.schemas.trace import TraceEvent, TraceEventType


@dataclass
class _InjectionRecord:
    fact_id: uuid.UUID
    tokens: int
    session_id: uuid.UUID
    profile: str = ""
    was_useful: bool | None = None


class StatsAndTelemetryEngine(IStatsAndTelemetryEngine):
    """Tracks injection/usage stats in-memory, queryable per profile."""

    def __init__(self, trace_ledger: ITraceLedger) -> None:
        self._trace = trace_ledger
        self._records: dict[uuid.UUID, _InjectionRecord] = {}
        self._profile_map: dict[str, list[uuid.UUID]] = {}

    async def record_injection(
        self, session_id: uuid.UUID, fact_id: uuid.UUID, tokens: int
    ) -> None:
        rec = _InjectionRecord(fact_id=fact_id, tokens=tokens, session_id=session_id)
        key = uuid.uuid5(uuid.NAMESPACE_OID, f"{session_id}:{fact_id}")
        self._records[key] = rec
        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.INPUT_RECEIVED,
                session_id=session_id,
                payload={"action": "record_injection", "fact_id": str(fact_id), "tokens": tokens},
            )
        )

    async def record_use(
        self, session_id: uuid.UUID, fact_id: uuid.UUID, was_useful: bool
    ) -> None:
        key = uuid.uuid5(uuid.NAMESPACE_OID, f"{session_id}:{fact_id}")
        rec = self._records.get(key)
        if rec is not None:
            rec.was_useful = was_useful

    async def get_stats_by_profile(self, profile_name: str) -> dict[str, float]:
        total = len(self._records)
        useful = sum(1 for r in self._records.values() if r.was_useful is True)
        total_tokens = sum(r.tokens for r in self._records.values())
        return {
            "total_injections": float(total),
            "useful_count": float(useful),
            "total_tokens": float(total_tokens),
            "usefulness_rate": useful / total if total > 0 else 0.0,
        }
