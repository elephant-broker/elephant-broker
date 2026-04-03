"""OtelTraceQueryClient — queries ClickHouse for cross-session trace analytics.

Used by Stage 7 to detect repeated tool call sequences across sessions.
Graceful degradation: returns empty results when ClickHouse not configured.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elephantbroker.schemas.config import ClickHouseConfig

logger = logging.getLogger("elephantbroker.runtime.consolidation.otel_trace_query_client")


class OtelTraceQueryClient:
    """Queries ClickHouse for cross-session trace analytics (AD-6)."""

    def __init__(self, config: ClickHouseConfig) -> None:
        self._client = None
        self._table = config.logs_table if config else "otel_logs"
        if config and config.enabled:
            try:
                import clickhouse_connect
                self._client = clickhouse_connect.get_client(
                    host=config.host,
                    port=config.port,
                    database=config.database,
                )
                logger.info("ClickHouse client connected (%s:%d/%s)", config.host, config.port, config.database)
            except ImportError:
                logger.warning("clickhouse-connect not installed — Stage 7 ClickHouse analytics unavailable")
            except Exception:
                logger.warning("ClickHouse connection failed", exc_info=True)

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
        except Exception:
            logger.warning("ClickHouse tool sequence query failed", exc_info=True)
            return []

    def close(self) -> None:
        """Close ClickHouse connection."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
