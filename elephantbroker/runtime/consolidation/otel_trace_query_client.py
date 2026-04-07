"""OtelTraceQueryClient — queries ClickHouse for cross-session trace analytics.

Used by Stage 7 to detect repeated tool call sequences across sessions.
Graceful degradation: returns empty results when ClickHouse not configured.

F6 (TODO-3-611): when ClickHouse is unavailable (missing dependency, failed
connection, query error) the client now emits ``DEGRADED_OPERATION`` trace
events and bumps the ``eb_degraded_operations_total`` counter so operators
can see *why* Stage 7 fell back to the SQLite-only path. Previously the
warning logs were the only signal and were silently lost on hosts that
shipped logs to a sink with no warning-level filter.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from elephantbroker.schemas.trace import TraceEvent, TraceEventType

if TYPE_CHECKING:
    from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
    from elephantbroker.runtime.metrics import MetricsContext
    from elephantbroker.schemas.config import ClickHouseConfig

logger = logging.getLogger("elephantbroker.runtime.consolidation.otel_trace_query_client")

_COMPONENT = "clickhouse_trace_query"


class OtelTraceQueryClient:
    """Queries ClickHouse for cross-session trace analytics (AD-6)."""

    def __init__(
        self,
        config: ClickHouseConfig | None,
        trace_ledger: ITraceLedger | None = None,
        metrics: MetricsContext | None = None,
    ) -> None:
        self._client = None
        self._trace = trace_ledger
        self._metrics = metrics
        self._init_failure: tuple[str, str] | None = None  # (operation, reason) for lazy emit
        self._init_failure_emitted = False
        self._table = config.logs_table if config else "otel_logs"
        if config and config.enabled:
            try:
                import clickhouse_connect
                self._client = clickhouse_connect.get_client(
                    host=config.host,
                    port=config.port,
                    database=config.database,
                    username=config.user,
                    password=config.password,
                )
                logger.info("ClickHouse client connected (%s:%d/%s)", config.host, config.port, config.database)
            except ImportError:
                logger.warning("clickhouse-connect not installed — Stage 7 ClickHouse analytics unavailable")
                self._record_init_failure("connect_import", "clickhouse_connect_not_installed")
            except Exception as exc:
                logger.warning("ClickHouse connection failed", exc_info=True)
                self._record_init_failure("connect", f"connection_failed: {str(exc)[:120]}")

    def _record_init_failure(self, operation: str, reason: str) -> None:
        """Bump the degraded-op metric (sync) and stash the reason for the first async query.

        We can't emit a TraceEvent from __init__ because it's sync and the
        TraceLedger is async. Instead, the next ``get_tool_sequences()`` call
        emits a one-shot DEGRADED_OPERATION event so the trace shows the
        original failure context the first time something actually depended
        on this client.
        """
        self._init_failure = (operation, reason)
        if self._metrics is not None:
            try:
                self._metrics.inc_degraded_op(component=_COMPONENT, operation=operation)
            except Exception:
                pass

    async def _emit_init_failure_event(self) -> None:
        if self._init_failure is None or self._init_failure_emitted or self._trace is None:
            return
        operation, reason = self._init_failure
        self._init_failure_emitted = True
        try:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.DEGRADED_OPERATION,
                payload={
                    "component": _COMPONENT,
                    "operation": operation,
                    "reason": reason,
                },
            ))
        except Exception:
            pass

    @property
    def available(self) -> bool:
        return self._client is not None

    async def get_tool_sequences(
        self,
        gateway_id: str,
        days: int = 7,
        min_sessions: int = 3,
    ) -> list[dict]:
        """Find tool call sequences from OTEL log records in ClickHouse.

        Queries the otel_logs table populated by OTEL Collector's clickhouse exporter.
        LogAttributes contain event_type and gateway_id from TraceLedger emission.
        Body contains the full TraceEvent JSON.
        """
        if not self._client:
            await self._emit_init_failure_event()
            return []

        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()

        try:
            # ClickHouse SQL — parameterized query
            query = f"""
                SELECT
                    JSONExtractString(Body, 'session_key') AS session_key,
                    groupArray(JSONExtractString(Body, 'payload', 'tool_name')) AS tools
                FROM {self._table}
                WHERE LogAttributes['event_type'] = 'tool_invoked'
                  AND LogAttributes['gateway_id'] = %(gw)s
                  AND Timestamp >= %(cutoff)s
                GROUP BY session_key
                HAVING length(tools) >= 3
                ORDER BY length(tools) DESC
            """
            result = self._client.query(query, parameters={"gw": gateway_id, "cutoff": cutoff})
            rows = []
            for row in result.result_rows:
                session_key = row[0]
                tools = row[1] if isinstance(row[1], list) else json.loads(row[1])
                rows.append({"session_key": session_key, "tools": tools})
            return rows
        except Exception as exc:
            logger.warning("ClickHouse tool sequence query failed", exc_info=True)
            if self._metrics is not None:
                try:
                    self._metrics.inc_degraded_op(component=_COMPONENT, operation="query")
                except Exception:
                    pass
            if self._trace is not None:
                try:
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.DEGRADED_OPERATION,
                        payload={
                            "component": _COMPONENT,
                            "operation": "query",
                            "reason": f"query_failed: {str(exc)[:120]}",
                            "gateway_id": gateway_id,
                        },
                    ))
                except Exception:
                    pass
            return []

    def close(self) -> None:
        """Close ClickHouse connection."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
