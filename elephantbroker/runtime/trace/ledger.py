"""In-memory append-only trace ledger with optional gateway auto-enrichment and OTEL log bridge."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.schemas.trace import SessionListItem, SessionListResponse, TraceEvent, TraceQuery

if TYPE_CHECKING:
    from elephantbroker.schemas.config import TraceConfig


class TraceLedger(ITraceLedger):
    """Append-only in-memory event store with optional OTEL log export.

    Events are stored in insertion order and filtered on read.
    When ``gateway_id``, ``agent_key``, or ``agent_id`` are set,
    ``append_event()`` auto-enriches events missing those fields.

    Phase 9 additions:
    - Circular buffer eviction (memory_max_events + memory_ttl_seconds)
    - Optional OTEL LogRecord emission for durable persistence in ClickHouse
    """

    def __init__(
        self,
        gateway_id: str | None = None,
        otel_logger=None,
        config: TraceConfig | None = None,
        agent_key: str | None = None,
        agent_id: str | None = None,
    ) -> None:
        self._events: list[TraceEvent] = []
        self._gateway_id = gateway_id
        self._agent_key = agent_key
        self._agent_id = agent_id
        self._otel_logger = otel_logger
        # Lazy import to avoid circular — config may be None for backward compat
        if config is not None:
            self._max_events = config.memory_max_events
            self._ttl_seconds = config.memory_ttl_seconds
        else:
            self._max_events = 10_000
            self._ttl_seconds = 3600

    def set_agent_identity(self, agent_key: str, agent_id: str) -> None:
        """Update agent identity (called after bootstrap resolves agent_key)."""
        self._agent_key = agent_key
        self._agent_id = agent_id

    async def append_event(self, event: TraceEvent) -> TraceEvent:
        if self._gateway_id and not event.gateway_id:
            event.gateway_id = self._gateway_id
        if self._agent_key and not event.agent_key:
            event.agent_key = self._agent_key
        if self._agent_id and not event.agent_id:
            event.agent_id = self._agent_id
        self._events.append(event)
        self._evict_stale()

        # OTEL log export (durable persistence to ClickHouse via OTEL Collector)
        if self._otel_logger:
            try:
                self._emit_otel_log(event)
            except Exception:
                pass  # Never let OTEL export failure block trace recording

        return event

    def _evict_stale(self) -> None:
        """Enforce circular buffer (max events) and TTL-based eviction."""
        # Size cap — evict oldest when over limit
        while len(self._events) > self._max_events:
            self._events.pop(0)
        # TTL cap — prune events older than ttl_seconds (lazy, on each append)
        if self._events and self._ttl_seconds > 0:
            cutoff = datetime.now(UTC) - timedelta(seconds=self._ttl_seconds)
            while self._events and self._events[0].timestamp < cutoff:
                self._events.pop(0)

    def _emit_otel_log(self, event: TraceEvent) -> None:
        """Emit a TraceEvent as an OTEL LogRecord for durable storage."""
        try:
            from opentelemetry.sdk._logs import LogRecord
            from opentelemetry.trace import StatusCode  # noqa: F401 — used for severity
        except ImportError:
            return

        self._otel_logger.emit(LogRecord(
            body=event.model_dump_json(),
            attributes={
                "event_type": event.event_type.value,
                "session_id": str(event.session_id) if event.session_id else "",
                "session_key": event.session_key or "",
                "gateway_id": event.gateway_id or "",
                "agent_key": event.agent_key or "",
            },
        ))

    async def query_trace(self, query: TraceQuery) -> list[TraceEvent]:
        results: list[TraceEvent] = []
        for ev in self._events:
            if query.event_types and ev.event_type not in query.event_types:
                continue
            if query.session_id and ev.session_id != query.session_id:
                continue
            if query.actor_ids and not set(ev.actor_ids).intersection(query.actor_ids):
                continue
            if query.from_timestamp and ev.timestamp < query.from_timestamp:
                continue
            if query.to_timestamp and ev.timestamp > query.to_timestamp:
                continue
            results.append(ev)

        if query.session_key is not None:
            results = [e for e in results if e.session_key == query.session_key]
        if query.gateway_id is not None:
            results = [e for e in results if e.gateway_id == query.gateway_id]

        results = results[query.offset:]
        return results[: query.limit]

    async def list_sessions(
        self,
        gateway_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> SessionListResponse:
        """Return unique sessions for a gateway, sorted by most recent activity.

        Scans in-memory events to find unique (session_id, session_key) pairs,
        computes first/last event timestamps and event counts, then applies
        pagination via offset/limit.
        """
        # Collect per-session stats keyed by session_id
        session_map: dict[uuid.UUID, dict] = {}
        for ev in self._events:
            if ev.session_id is None:
                continue
            if gateway_id is not None and ev.gateway_id != gateway_id:
                continue
            sid = ev.session_id
            if sid not in session_map:
                session_map[sid] = {
                    "session_id": sid,
                    "session_key": ev.session_key or "",
                    "first_event_at": ev.timestamp,
                    "last_event_at": ev.timestamp,
                    "event_count": 0,
                }
            entry = session_map[sid]
            entry["event_count"] += 1
            if ev.timestamp < entry["first_event_at"]:
                entry["first_event_at"] = ev.timestamp
            if ev.timestamp > entry["last_event_at"]:
                entry["last_event_at"] = ev.timestamp
            # Keep the most informative session_key (non-empty wins)
            if ev.session_key and not entry["session_key"]:
                entry["session_key"] = ev.session_key

        # Sort by most recent activity first
        sessions_sorted = sorted(
            session_map.values(),
            key=lambda s: s["last_event_at"],
            reverse=True,
        )
        total_count = len(sessions_sorted)
        page = sessions_sorted[offset: offset + limit]
        return SessionListResponse(
            sessions=[SessionListItem(**s) for s in page],
            total_count=total_count,
        )

    async def get_evidence_chain(self, target_id: uuid.UUID) -> list[TraceEvent]:
        chain: list[TraceEvent] = []
        for ev in self._events:
            refs = ev.actor_ids + ev.artifact_ids + ev.claim_ids + ev.procedure_ids + ev.goal_ids
            if target_id in refs:
                chain.append(ev)
        return chain
