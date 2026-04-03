"""Redline index refresh pipeline — reloads guard rules on procedure changes (Phase 7 — §7.7)."""
from __future__ import annotations

import logging
import uuid

from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger(__name__)


class RedlineIndexRefreshPipeline:
    """Orchestrates guard rule reloading when procedures change.

    NOT a Cognee Task pipeline (rules are config, not knowledge).
    """

    def __init__(self, guard_engine, graph=None, profile_registry=None,
                 pipeline_runner=None, trace_ledger=None) -> None:
        self._guard_engine = guard_engine
        self._graph = graph
        self._profiles = profile_registry
        self._pipeline_runner = pipeline_runner
        self._trace = trace_ledger

    async def run(
        self,
        session_id: uuid.UUID,
        profile_name: str,
        active_procedure_ids: list[uuid.UUID] | None = None,
        *,
        session_key: str = "",
        agent_id: str = "",
    ) -> None:
        """Reload guard rules for a session."""
        await self._guard_engine.load_session_rules(
            session_id=session_id,
            profile_name=profile_name,
            active_procedure_ids=active_procedure_ids,
            session_key=session_key,
            agent_id=agent_id,
        )
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.TOOL_INVOKED,
                payload={
                    "action": "redline_refresh",
                    "profile": profile_name,
                    "procedures": len(active_procedure_ids or []),
                },
            ))
        logger.info("Redline index refreshed for session %s: profile=%s, procedures=%d",
                     session_id, profile_name, len(active_procedure_ids or []))
