"""Stage 7: Refine Procedures from Patterns — detect repeated tool sequences.

LLM calls bounded by max_patterns_per_run and context.llm_calls_cap.
Primary source: ClickHouse via OtelTraceQueryClient.
Fallback: ProcedureAuditStore (SQLite) for procedure-bound patterns only.
Never auto-activates — suggestions queued for human/supervisor review.
"""
from __future__ import annotations

import logging
import uuid
from collections import Counter
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.procedure import ProcedureSuggestion

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.llm.client import LLMClient
    from elephantbroker.runtime.audit.procedure_audit import ProcedureAuditStore
    from elephantbroker.runtime.consolidation.otel_trace_query_client import OtelTraceQueryClient
    from elephantbroker.schemas.consolidation import ConsolidationConfig, ConsolidationContext

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.refine_procedures")

_PROCEDURE_PROMPT = """Based on the following repeated tool call pattern observed \
across {sessions} sessions, generate a procedure definition.

Pattern: {sequence}
Description: {description}

Return a JSON object with:
- name: short procedure name
- description: what this procedure accomplishes
- steps: array of {{instruction: str, order: int}}

Return ONLY valid JSON."""


class RefineProceduresStage:
    """Detect repeated multi-step patterns and generate procedure drafts.

    Algorithm:
    1. Query tool sequences from ClickHouse (or fallback to ProcedureAuditStore)
    2. Group by session, extract ordered tool sequences
    3. Find sequences of length >= min_steps appearing in >= threshold sessions
    4. For each detected pattern: LLM generates ProcedureDefinition draft
    5. Store as ProcedureSuggestion with approval_status="pending"
    """

    def __init__(
        self,
        llm_client: LLMClient | None,
        trace_query_client: OtelTraceQueryClient,
        procedure_audit_store: ProcedureAuditStore | None,
        config: ConsolidationConfig,
    ) -> None:
        self._llm = llm_client
        self._trace_client = trace_query_client
        self._audit_store = procedure_audit_store
        self._recurrence = config.pattern_recurrence_threshold
        self._min_steps = config.pattern_min_steps
        self._max_patterns = config.max_patterns_per_run

    @traced
    async def run(
        self, gateway_id: str, context: ConsolidationContext,
    ) -> list[ProcedureSuggestion]:
        # 1. Get tool sequences
        sequences = await self._load_sequences(gateway_id)
        if not sequences:
            return []

        # 2-3. Find recurring patterns
        patterns = self._find_patterns(sequences)
        if not patterns:
            return []

        # 4. Generate drafts (bounded by caps)
        suggestions: list[ProcedureSuggestion] = []
        for seq_tuple, session_count in patterns[: self._max_patterns]:
            if not self._llm or context.llm_calls_used >= context.llm_calls_cap:
                logger.warning("LLM cap reached — stopping pattern generation")
                break

            seq_list = list(seq_tuple)
            desc = f"Repeated sequence: {' → '.join(seq_list)} (seen in {session_count} sessions)"
            try:
                await self._llm.complete(  # draft_text — parse into ProcedureDefinition later
                    system_prompt="You are a procedure definition generator.",
                    user_prompt=_PROCEDURE_PROMPT.format(
                        sessions=session_count,
                        sequence=" → ".join(seq_list),
                        description=desc,
                    ),
                    max_tokens=500,
                )
                context.llm_calls_used += 1
            except Exception:
                logger.warning("LLM draft generation failed", exc_info=True)
                continue

            suggestions.append(ProcedureSuggestion(
                id=uuid.uuid4(),
                pattern_description=desc,
                tool_sequence=seq_list,
                sessions_observed=session_count,
                draft_procedure=None,  # Would parse draft_text into ProcedureDefinition
                confidence=min(0.9, 0.3 + 0.1 * session_count),
                approval_status="pending",
                created_at=datetime.now(UTC),
                gateway_id=gateway_id,
            ))

        logger.info(
            "Stage 7: %d patterns found, %d suggestions generated (gateway=%s)",
            len(patterns), len(suggestions), gateway_id,
        )
        return suggestions

    async def _load_sequences(self, gateway_id: str) -> list[list[str]]:
        """Load tool sequences from ClickHouse or fallback to audit store."""
        # Primary: ClickHouse
        if self._trace_client and self._trace_client.available:
            try:
                results = await self._trace_client.get_tool_sequences(gateway_id)
                if results:
                    return [r.get("tools", []) for r in results if r.get("tools")]
            except Exception:
                logger.warning("ClickHouse query failed — falling back", exc_info=True)

        # Fallback: ProcedureAuditStore
        if self._audit_store:
            logger.info("ClickHouse not available — using ProcedureAuditStore fallback")
            try:
                events = await self._audit_store.get_procedure_events("*")
                # Group by session_key, extract step sequences
                sessions: dict[str, list[str]] = {}
                for ev in events:
                    sk = ev.get("session_key", "")
                    step = ev.get("step_instruction") or ev.get("event_type", "")
                    sessions.setdefault(sk, []).append(step)
                return list(sessions.values())
            except Exception:
                logger.warning("ProcedureAuditStore fallback failed", exc_info=True)

        logger.warning("No data source for Stage 7 — no tool sequence analysis")
        return []

    def _find_patterns(
        self, sequences: list[list[str]],
    ) -> list[tuple[tuple[str, ...], int]]:
        """Find subsequences appearing in >= threshold distinct sessions."""
        pattern_counts: Counter[tuple[str, ...]] = Counter()

        for seq in sequences:
            if len(seq) < self._min_steps:
                continue
            # Extract all contiguous subsequences of length >= min_steps
            seen_in_session: set[tuple[str, ...]] = set()
            for start in range(len(seq)):
                for end in range(start + self._min_steps, len(seq) + 1):
                    subseq = tuple(seq[start:end])
                    seen_in_session.add(subseq)
            for subseq in seen_in_session:
                pattern_counts[subseq] += 1

        # Filter by recurrence threshold, sort by count descending
        recurring = [
            (pat, count) for pat, count in pattern_counts.items()
            if count >= self._recurrence
        ]
        recurring.sort(key=lambda x: (-x[1], -len(x[0])))
        return recurring
