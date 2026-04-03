"""Compaction engine — continuous, goal-aware context compaction (Phase 6)."""
from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from elephantbroker.runtime.interfaces.compaction_engine import ICompactionEngine
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.runtime.observability import GatewayLoggerAdapter
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.config import ContextAssemblyConfig
from elephantbroker.schemas.context import (
    AgentMessage,
    CompactResult,
    CompactResultDetail,
    CompactionContext,
    SessionCompactState,
    content_as_text,
)
from elephantbroker.schemas.fact import MemoryClass
from elephantbroker.schemas.profile import CompactionPolicy
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.runtime.compaction.engine")

__all__ = ["CADENCE_MULTIPLIERS", "estimate_tokens", "CompactionEngine"]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CADENCE_MULTIPLIERS: dict[str, float] = {
    "aggressive": 1.5,
    "balanced": 2.0,
    "minimal": 3.0,
}

_PHATIC_RE = re.compile(
    r"^(hi|hello|hey|thanks|thank you|ok|okay|sure|yes|no|yep|nope|got it|alright|sounds good)[\s!.]*$",
    re.IGNORECASE,
)

_DECISION_RE = re.compile(
    r"\b(decided|decision:)\b",
    re.IGNORECASE,
)

_EVIDENCE_RE = re.compile(
    r"\b(claim[_-]?id|proof|evidence|verified|receipt)\b",
    re.IGNORECASE,
)

# Memory class hierarchy for _highest_class (highest durability first)
_MEMORY_CLASS_RANK: dict[str, int] = {
    MemoryClass.POLICY: 4,
    MemoryClass.PROCEDURAL: 3,
    MemoryClass.SEMANTIC: 2,
    MemoryClass.EPISODIC: 1,
    MemoryClass.WORKING_MEMORY: 0,
}

# Rough token estimate: 1 token ~ 4 chars
_CHARS_PER_TOKEN = 4


def estimate_tokens(text: str) -> int:
    """Rough token estimate for a text string."""
    return max(1, len(text) // _CHARS_PER_TOKEN)


# ---------------------------------------------------------------------------
# CompactionEngine
# ---------------------------------------------------------------------------


class CompactionEngine(ICompactionEngine):
    """Continuous, goal-aware context compaction.

    Classifies messages by memory-class linkage and compaction policy,
    preserves goal-relevant and decision-bearing messages, compresses
    the remainder via LLM summarization, and persists compact state
    in Redis for downstream assembly.
    """

    def __init__(
        self,
        trace_ledger: ITraceLedger,
        llm_client: Any | None = None,
        redis: Any | None = None,
        config: ContextAssemblyConfig | None = None,
        gateway_id: str = "local",
        redis_keys: RedisKeyBuilder | None = None,
        metrics: MetricsContext | None = None,
        ttl_seconds: int = 172800,
    ) -> None:
        self._trace = trace_ledger
        self._llm = llm_client
        self._redis = redis
        self._config = config or ContextAssemblyConfig()
        self._gateway_id = gateway_id
        self._keys = redis_keys or RedisKeyBuilder(gateway_id)
        self._metrics = metrics or MetricsContext(gateway_id)
        self._log = GatewayLoggerAdapter(
            logger, {"gateway_id": gateway_id, "agent_key": ""},
        )
        self._ttl = ttl_seconds

    # ------------------------------------------------------------------
    # Legacy interface (backward compat)
    # ------------------------------------------------------------------

    async def compact(
        self,
        session_id: uuid.UUID,
        token_budget: int,
        force: bool = False,
    ) -> CompactResult:
        """Backward-compat: delegates to compact_with_context with minimal context."""
        ctx = CompactionContext(
            session_key="",
            session_id=str(session_id),
            messages=[],
            token_budget=token_budget,
            force=force,
        )
        return await self.compact_with_context(ctx)

    async def get_compact_state(self, session_id: uuid.UUID) -> CompactResult:
        """Backward-compat: return a no-compaction result."""
        return CompactResult(ok=True, compacted=False, reason="no compaction state")

    async def merge_overlapping(self, session_id: uuid.UUID) -> int:
        """Backward-compat: no-op merge."""
        return 0

    # ------------------------------------------------------------------
    # Phase 6: compact_with_context
    # ------------------------------------------------------------------

    async def compact_with_context(self, context: CompactionContext) -> CompactResult:
        """Run compaction with full context (goals, messages, profile).

        Steps:
        1. Check trigger threshold via _should_trigger
        2. Classify messages into preserve / compress / drop
        3. LLM summarize compress bucket (single call) if llm_client available
        4. Build SessionCompactState
        5. Write compact state to Redis
        6. Return CompactResult with detail
        """
        profile = context.profile
        policy = profile.compaction if profile else CompactionPolicy()
        cadence = policy.cadence if policy.cadence in CADENCE_MULTIPLIERS else "balanced"

        current_tokens = context.current_token_count
        if current_tokens is None:
            current_tokens = sum(
                estimate_tokens(content_as_text(m)) for m in context.messages
            )

        # Step 1 — trigger check
        triggered = self._should_trigger(
            current_tokens=current_tokens,
            policy=policy,
            force=context.force,
            compaction_target=policy.target_tokens,
        )
        if not triggered:
            self._log.debug(
                "Compaction not triggered: tokens=%d, target=%d, cadence=%s",
                current_tokens, policy.target_tokens, cadence,
            )
            return CompactResult(ok=True, compacted=False, reason="below threshold")

        trigger_reason = "force" if context.force else context.trigger_reason if context.trigger_reason != "explicit" else "threshold"
        self._metrics.inc_compaction_triggered(cadence, trigger_reason)
        self._metrics.observe_compaction_tokens("before", current_tokens)
        self._log.info(
            "Compaction triggered: tokens=%d, target=%d, cadence=%s, force=%s",
            current_tokens, policy.target_tokens, cadence, context.force,
        )

        # Step 2 — classify messages
        preserve, compress, drop = self._classify_messages(
            context.messages, context.current_goals, policy,
        )
        for _ in preserve:
            self._metrics.inc_compaction_classification("preserve")
        for _ in compress:
            self._metrics.inc_compaction_classification("compress")
        for _ in drop:
            self._metrics.inc_compaction_classification("drop")

        self._log.info(
            "Classification: preserve=%d, compress=%d, drop=%d",
            len(preserve), len(compress), len(drop),
        )

        # Step 3 — LLM summarize
        summary = ""
        if compress:
            summary = await self._summarize(compress, context.current_goals)

        # Step 4 — build SessionCompactState
        decisions = [
            content_as_text(m) for m in preserve if _DECISION_RE.search(content_as_text(m))
        ]
        open_questions = [
            content_as_text(m) for m in preserve
            if "?" in content_as_text(m) and self._is_open_question(m, context.messages)
        ]
        evidence_refs = [
            content_as_text(m) for m in preserve if self._contains_evidence(m)
        ]
        preserved_ids = [
            m.metadata.get("eb_message_id", "") for m in preserve
            if m.metadata.get("eb_message_id")
        ]

        goal_summary = ""
        if context.current_goals:
            goal_texts = []
            for g in context.current_goals:
                label = getattr(g, "label", None) or getattr(g, "title", None) or str(g)
                goal_texts.append(label)
            goal_summary = "; ".join(goal_texts)

        tokens_after = sum(estimate_tokens(content_as_text(m)) for m in preserve)
        if summary:
            tokens_after += estimate_tokens(summary)

        compact_state = SessionCompactState(
            session_key=context.session_key,
            session_id=context.session_id,
            goal_summary=goal_summary,
            decisions_made=decisions[:20],
            open_questions=open_questions[:10],
            active_blockers=[],
            constraint_digest=[],
            evidence_bundle_refs=evidence_refs[:10],
            actor_context_summary="",
            compressed_digest=summary,
            preserved_item_ids=preserved_ids,
            token_count=tokens_after,
            turn_count_at_compaction=len(context.messages),
            created_at=datetime.now(UTC),
        )

        # Compute compacted item IDs (items NOT in preserved set → AD-6 Phase 5 contract)
        compacted_item_ids = []
        for m in compress + drop:
            mid = m.metadata.get("eb_message_id", "")
            if mid:
                compacted_item_ids.append(mid)
            # Also include any linked fact IDs
            for fid in m.metadata.get("eb_fact_ids", "").split(","):
                fid = fid.strip()
                if fid:
                    compacted_item_ids.append(fid)

        # Step 5 — persist to Redis
        await self._persist_compact_state(
            context.session_key, context.session_id, compact_state,
            compacted_item_ids=compacted_item_ids,
        )

        self._metrics.observe_compaction_tokens("after", tokens_after)

        # Step 6 — trace event
        await self._trace.append_event(TraceEvent(
            event_type=TraceEventType.COMPACTION_ACTION,
            session_id=uuid.UUID(context.session_id) if context.session_id else None,
            session_key=context.session_key,
            gateway_id=self._gateway_id,
            payload={
                "trigger": trigger_reason,
                "cadence": cadence,
                "tokens_before": current_tokens,
                "tokens_after": tokens_after,
                "preserved": len(preserve),
                "compressed": len(compress),
                "dropped": len(drop),
                "has_summary": bool(summary),
            },
        ))

        detail = CompactResultDetail(
            summary=summary or None,
            first_kept_entry_id=preserved_ids[0] if preserved_ids else None,
            tokens_before=current_tokens,
            tokens_after=tokens_after,
            details=f"preserve={len(preserve)}, compress={len(compress)}, drop={len(drop)}",
        )

        self._log.info(
            "Compaction complete: %d -> %d tokens, preserved=%d",
            current_tokens, tokens_after, len(preserve),
        )

        return CompactResult(ok=True, compacted=True, reason="compaction applied", result=detail)

    # ------------------------------------------------------------------
    # Phase 6: get_session_compact_state
    # ------------------------------------------------------------------

    async def get_session_compact_state(
        self, sk: str, sid: str,
    ) -> SessionCompactState | None:
        """Get structured compact state for a session from Redis."""
        if not self._redis:
            return None

        key = self._keys.compact_state_obj(sk, sid)
        try:
            raw = await self._redis.get(key)
            if raw is None:
                return None
            data = json.loads(raw)
            return SessionCompactState(**data)
        except Exception:
            self._log.warning("Failed to load compact state for %s/%s", sk, sid, exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Trigger logic
    # ------------------------------------------------------------------

    def _should_trigger(
        self,
        current_tokens: int,
        policy: CompactionPolicy,
        force: bool,
        compaction_target: int,
    ) -> bool:
        """Determine whether compaction should fire.

        Triggers if forced OR current_tokens > target * cadence_multiplier.
        """
        if force:
            return True
        cadence = policy.cadence if policy.cadence in CADENCE_MULTIPLIERS else "balanced"
        multiplier = CADENCE_MULTIPLIERS[cadence]
        threshold = compaction_target * multiplier
        return current_tokens > threshold

    # ------------------------------------------------------------------
    # Message classification (AD-20 memory-class awareness)
    # ------------------------------------------------------------------

    def _classify_messages(
        self,
        messages: list[AgentMessage],
        goals: list[Any],
        policy: CompactionPolicy,
    ) -> tuple[list[AgentMessage], list[AgentMessage], list[AgentMessage]]:
        """Classify messages into (preserve, compress, drop).

        Classification rules (evaluated in order):
        1. Messages linked to POLICY/PROCEDURAL facts -> PRESERVE always
        2. preserve_goal_state: messages referencing active goals -> PRESERVE
        3. preserve_open_questions: messages with ? and no answer -> PRESERVE
        4. Messages with "decided"/"decision:" -> PRESERVE
        5. Messages with evidence keywords -> PRESERVE (if preserve_evidence_refs)
        6. Messages marked eb_compacted=true -> DROP
        7. User messages, token<20, no ? -> DROP (phatic)
        8. Everything else -> COMPRESS
        """
        preserve: list[AgentMessage] = []
        compress: list[AgentMessage] = []
        drop: list[AgentMessage] = []

        for msg in messages:
            classification = self._classify_single(msg, messages, goals, policy)
            if classification == "preserve":
                preserve.append(msg)
            elif classification == "drop":
                drop.append(msg)
            else:
                compress.append(msg)

        return preserve, compress, drop

    def _classify_single(
        self,
        msg: AgentMessage,
        all_messages: list[AgentMessage],
        goals: list[Any],
        policy: CompactionPolicy,
    ) -> str:
        """Classify a single message. Returns 'preserve', 'compress', or 'drop'."""
        # Rule: already compacted -> DROP
        if msg.metadata.get("eb_compacted") == "true":
            return "drop"

        # Rule: phatic user messages -> DROP
        if self._is_phatic(msg):
            return "drop"

        # Rule: linked to POLICY or PROCEDURAL facts -> PRESERVE
        facts = self._get_facts_for_message(msg)
        if facts:
            highest = self._highest_class(facts)
            if highest in (MemoryClass.POLICY, MemoryClass.PROCEDURAL):
                return "preserve"

        # Rule: references active goals -> PRESERVE (if policy says so)
        if policy.preserve_goal_state and self._references_active_goal(msg, goals):
            return "preserve"

        # Rule: open questions -> PRESERVE (if policy says so)
        if policy.preserve_open_questions and self._is_open_question(msg, all_messages):
            return "preserve"

        # Rule: decision messages -> PRESERVE
        if self._is_decision(msg):
            return "preserve"

        # Rule: evidence references -> PRESERVE (if policy says so)
        if policy.preserve_evidence_refs and self._contains_evidence(msg):
            return "preserve"

        # Default -> COMPRESS
        return "compress"

    # ------------------------------------------------------------------
    # Helper predicates
    # ------------------------------------------------------------------

    @staticmethod
    def _is_phatic(msg: AgentMessage) -> bool:
        """Check if message is phatic (greeting/thanks/ok) under 20 chars, no question."""
        if msg.role != "user":
            return False
        content = content_as_text(msg).strip()
        if len(content) >= 20:
            return False
        if "?" in content:
            return False
        return bool(_PHATIC_RE.match(content))

    @staticmethod
    def _is_decision(msg: AgentMessage) -> bool:
        """Check if message contains decision language."""
        return bool(_DECISION_RE.search(content_as_text(msg)))

    @staticmethod
    def _references_active_goal(msg: AgentMessage, goals: list[Any]) -> bool:
        """Check if message content has keyword overlap with active goals."""
        if not goals:
            return False
        content_lower = content_as_text(msg).lower()
        content_words = set(re.findall(r"\w{4,}", content_lower))
        if not content_words:
            return False

        for goal in goals:
            label = getattr(goal, "label", None) or getattr(goal, "title", None) or str(goal)
            goal_words = set(re.findall(r"\w{4,}", label.lower()))
            if content_words & goal_words:
                return True
        return False

    @staticmethod
    def _is_open_question(msg: AgentMessage, all_messages: list[AgentMessage]) -> bool:
        """Check if message contains '?' with no subsequent answer.

        A question is considered open if no message after it from a different
        role contains content that could be an answer (non-phatic, non-empty).
        """
        if "?" not in content_as_text(msg):
            return False

        # Find position of this message
        try:
            idx = all_messages.index(msg)
        except ValueError:
            return True  # can't determine, assume open

        # Look for an answer in subsequent messages from a different role
        for later in all_messages[idx + 1:]:
            if later.role != msg.role and len(content_as_text(later).strip()) > 5:
                return False
        return True

    @staticmethod
    def _contains_evidence(msg: AgentMessage) -> bool:
        """Check if message references evidence (claim-ID, proof keywords)."""
        return bool(_EVIDENCE_RE.search(content_as_text(msg)))

    @staticmethod
    def _highest_class(facts: list[dict[str, Any]]) -> str:
        """Return the highest memory class from a list of fact metadata dicts."""
        best_rank = -1
        best_class = MemoryClass.WORKING_MEMORY
        for fact in facts:
            mc = fact.get("memory_class", MemoryClass.WORKING_MEMORY)
            rank = _MEMORY_CLASS_RANK.get(mc, 0)
            if rank > best_rank:
                best_rank = rank
                best_class = mc
        return best_class

    @staticmethod
    def _get_facts_for_message(msg: AgentMessage) -> list[dict[str, Any]]:
        """Extract linked fact metadata from message metadata.

        Messages carry eb_fact_ids (comma-separated) and optionally
        eb_fact_classes (parallel comma-separated memory classes).
        """
        fact_ids_raw = msg.metadata.get("eb_fact_ids", "")
        if not fact_ids_raw:
            return []

        fact_ids = [fid.strip() for fid in fact_ids_raw.split(",") if fid.strip()]
        fact_classes_raw = msg.metadata.get("eb_fact_classes", "")
        fact_classes = [fc.strip() for fc in fact_classes_raw.split(",") if fc.strip()]

        facts: list[dict[str, Any]] = []
        for i, fid in enumerate(fact_ids):
            mc = fact_classes[i] if i < len(fact_classes) else MemoryClass.WORKING_MEMORY
            facts.append({"id": fid, "memory_class": mc})
        return facts

    # ------------------------------------------------------------------
    # LLM summarization
    # ------------------------------------------------------------------

    async def _summarize(
        self,
        compress_messages: list[AgentMessage],
        goals: list[Any],
    ) -> str:
        """Summarize compress-bucket messages via LLM or fallback truncation."""
        if not compress_messages:
            return ""

        conversation_text = "\n".join(
            f"[{m.role}]: {content_as_text(m)}" for m in compress_messages
        )

        if not self._llm:
            # Fallback: truncate to target
            max_chars = self._config.compaction_summary_max_tokens * _CHARS_PER_TOKEN
            if len(conversation_text) <= max_chars:
                return conversation_text
            return conversation_text[:max_chars] + "..."

        # Build goal-aware system prompt
        goal_section = ""
        if goals:
            goal_lines = []
            for g in goals:
                label = getattr(g, "label", None) or getattr(g, "title", None) or str(g)
                goal_lines.append(f"- {label}")
            goal_section = (
                "\n\nActive goals (preserve information relevant to these):\n"
                + "\n".join(goal_lines)
            )

        system_prompt = (
            "You are a context compaction assistant. Your job is to produce a "
            "concise summary of the conversation below, preserving:\n"
            "- Key decisions and their rationale\n"
            "- Open questions that remain unanswered\n"
            "- Technical details and code references\n"
            "- Evidence and proof references\n"
            "- Information relevant to active goals\n\n"
            "Discard greetings, acknowledgments, and redundant exchanges. "
            "Output only the summary, no preamble."
            f"{goal_section}"
        )

        user_prompt = (
            f"Summarize this conversation segment ({len(compress_messages)} messages, "
            f"~{estimate_tokens(conversation_text)} tokens) into a compact digest:\n\n"
            f"{conversation_text}"
        )

        try:
            self._metrics.inc_compaction_llm_call()
            summary = await self._llm.complete(
                system_prompt,
                user_prompt,
                max_tokens=self._config.compaction_summary_max_tokens,
                temperature=0.2,
            )
            self._log.debug("LLM compaction summary: %d chars", len(summary))
            return summary.strip()
        except Exception:
            self._log.warning("LLM summarization failed, falling back to truncation", exc_info=True)
            max_chars = self._config.compaction_summary_max_tokens * _CHARS_PER_TOKEN
            if len(conversation_text) <= max_chars:
                return conversation_text
            return conversation_text[:max_chars] + "..."

    # ------------------------------------------------------------------
    # Redis persistence
    # ------------------------------------------------------------------

    async def _persist_compact_state(
        self,
        session_key: str,
        session_id: str,
        state: SessionCompactState,
        compacted_item_ids: list[str] | None = None,
    ) -> None:
        """Write compact state to Redis.

        - compact_state_obj key: JSON of SessionCompactState
        - compact_state key: SET of compacted working-set item IDs
          (Phase 5 ScoringEngine.compute_novelty reads this via SMEMBERS)
        """
        if not self._redis:
            self._log.debug("No Redis client — skipping compact state persistence")
            return

        obj_key = self._keys.compact_state_obj(session_key, session_id)
        set_key = self._keys.compact_state(session_key, session_id)
        state_json = state.model_dump_json()

        try:
            await self._redis.setex(obj_key, self._ttl, state_json)
            # Write compacted item IDs to SET (AD-6 — Phase 5 contract)
            if compacted_item_ids:
                await self._redis.sadd(set_key, *compacted_item_ids)
                await self._redis.expire(set_key, self._ttl)
            self._log.debug("Persisted compact state: %s (%d item IDs)",
                            obj_key, len(compacted_item_ids or []))
        except Exception:
            self._log.warning("Failed to persist compact state to Redis", exc_info=True)
