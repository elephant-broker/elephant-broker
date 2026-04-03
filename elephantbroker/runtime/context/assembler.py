"""Context assembler -- 4-block assembly pipeline (Phase 6).

Blocks:
  1. systemPromptAddition  -- constraints, procedures, guard rules
  2. prependSystemContext   -- goals with blockers highlighted
  3. prependContext          -- working-set items ordered by class then score
  4. appendSystemContext     -- evidence reference citations
"""
from __future__ import annotations

import logging
import uuid

from elephantbroker.runtime.interfaces.context_assembler import IContextAssembler
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.interfaces.working_set import IWorkingSetManager
from elephantbroker.schemas.config import ContextAssemblyConfig
from elephantbroker.schemas.context import AgentMessage, AssembleResult, SubagentPacket, SystemPromptOverlay, content_as_text
from elephantbroker.schemas.goal import GoalState
from elephantbroker.schemas.profile import ProfilePolicy
from elephantbroker.schemas.trace import TraceEvent, TraceEventType
from elephantbroker.schemas.working_set import WorkingSetItem, WorkingSetSnapshot

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Class ordering for Block 3 item placement.
# Lower value = higher priority (injected first).
# ---------------------------------------------------------------------------
CLASS_PRIORITY: dict[str, int] = {
    "policy": 0,
    "procedural": 1,
    "semantic": 2,
    "episodic": 3,
    "working_memory": 4,
}


def _estimate_tokens(text: str) -> int:
    """Cheap token estimate: 1 token ~= 4 characters."""
    return len(text) // 4


# ---------------------------------------------------------------------------
# Assembler
# ---------------------------------------------------------------------------


class ContextAssembler(IContextAssembler):
    """4-block context assembly pipeline.

    Constructs the prompt overlay from a scored ``WorkingSetSnapshot``:

    * **Block 1** (``system_prompt_addition``): policy constraints, active
      procedures, and guard rules injected as a system-prompt addendum.
    * **Block 2** (``prepend_system_context``): session goals with blockers
      highlighted so the model stays on track.
    * **Block 3** (``prepend_context``): scored working-set items ordered by
      memory class priority (policy first) then by descending score.
    * **Block 4** (``append_system_context``): evidence reference citations
      so the model can ground its answers.
    """

    def __init__(
        self,
        working_set_manager: IWorkingSetManager,
        trace_ledger: ITraceLedger,
        llm_client: object | None = None,
        config: ContextAssemblyConfig | None = None,
    ) -> None:
        self._working_set = working_set_manager
        self._trace = trace_ledger
        self._llm_client = llm_client
        self._config = config or ContextAssemblyConfig()

    # ------------------------------------------------------------------
    # Backward-compatible methods (kept from the stub)
    # ------------------------------------------------------------------

    async def assemble(
        self,
        session_id: uuid.UUID,
        messages: list[AgentMessage],
        token_budget: int,
        session_key: str = "",
        gateway_id: str = "",
    ) -> AssembleResult:
        """Return *messages* as-is with an estimated token count.

        This is the legacy entry point preserved for callers that have not
        migrated to :meth:`assemble_from_snapshot`.
        """
        estimated = sum(len(content_as_text(m)) // 4 for m in messages)
        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.CONTEXT_ASSEMBLED,
                session_id=session_id,
                session_key=session_key,
                gateway_id=gateway_id,
                payload={
                    "action": "assemble",
                    "messages": len(messages),
                    "tokens": estimated,
                },
            )
        )
        return AssembleResult(messages=list(messages), estimated_tokens=estimated)

    async def build_system_overlay(self, session_id: uuid.UUID) -> SystemPromptOverlay:
        """Return an empty overlay (legacy stub)."""
        return SystemPromptOverlay()

    async def build_subagent_packet(
        self,
        parent_session_key: str,
        child_session_key: str,
    ) -> SubagentPacket:
        """Return a minimal subagent packet (legacy stub)."""
        return SubagentPacket(
            parent_session_key=parent_session_key,
            child_session_key=child_session_key,
        )

    # ------------------------------------------------------------------
    # Phase 6: full 4-block assembly
    # ------------------------------------------------------------------

    async def assemble_from_snapshot(
        self,
        snapshot: WorkingSetSnapshot,
        effective_budget: int,
        session_goals: list[GoalState],
        profile: ProfilePolicy,
        guard_constraints: list[str] | None = None,
        session_key: str = "",
    ) -> AssembleResult:
        """Assemble context from a scored working-set snapshot.

        Budget allocation (fractions of *effective_budget*):
            Block 1 (system prompt addition) : 20 %, capped by profile max
            Block 2 (goal context)           : 10 %
            Block 4 (evidence refs)          : 5 %, capped by config max
            Block 3 (working-set items)      : remainder
        """
        # --- budget split ------------------------------------------------
        block1_budget = min(
            int(effective_budget * 0.20),
            profile.budgets.max_system_overlay_tokens,
        )
        block2_budget = int(effective_budget * 0.10)
        block4_budget = min(
            int(effective_budget * 0.05),
            self._config.evidence_budget_max_tokens,
        )
        block3_budget = effective_budget - block1_budget - block2_budget - block4_budget

        # --- Block 1: systemPromptAddition (constraints + procedures + guards)
        block1_parts: list[str] = []

        # Constraints from snapshot items flagged system_prompt_eligible
        constraint_items = [
            it for it in snapshot.items if it.system_prompt_eligible
        ]
        for item in constraint_items:
            block1_parts.append(f"- {item.text}")

        # Guard constraints passed by the caller (red-line rules)
        if guard_constraints:
            if block1_parts:
                block1_parts.append("")
            block1_parts.append("## Guard Rules")
            for gc in guard_constraints:
                block1_parts.append(f"- {gc}")

        block1_text = _truncate_to_budget("\n".join(block1_parts), block1_budget)

        # --- Block 3: prependContext (working-set items) ------------------
        # Items that were NOT consumed by Block 1
        context_items = [
            it for it in snapshot.items if not it.system_prompt_eligible
        ]
        ordered = _order_by_class_then_score(context_items)

        block3_parts: list[str] = []
        block3_tokens = 0
        for item in ordered:
            rendered = _render_item_block(item)
            item_tokens = _estimate_tokens(rendered)
            if block3_tokens + item_tokens > block3_budget:
                break
            block3_parts.append(rendered)
            block3_tokens += item_tokens

        block3_text = "\n\n".join(block3_parts)

        # --- total token estimate -----------------------------------------
        total_estimated = (
            _estimate_tokens(block1_text)
            + _estimate_tokens(block3_text)
        )

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.CONTEXT_ASSEMBLED,
                session_id=snapshot.session_id,
                session_key=session_key,
                gateway_id=snapshot.gateway_id,
                payload={
                    "action": "assemble_from_snapshot",
                    "snapshot_id": str(snapshot.snapshot_id),
                    "items_total": len(snapshot.items),
                    "items_block1": len(constraint_items),
                    "items_block3": len(block3_parts),
                    "budget": effective_budget,
                    "tokens_estimated": total_estimated,
                },
            )
        )

        # NOTE: messages are NOT set here — lifecycle owns message transformation
        # (Surface A). Block 3 goes to Surface B via build_system_overlay_from_items().
        return AssembleResult(
            estimated_tokens=total_estimated,
            system_prompt_addition=block1_text if block1_text else None,
        )

    async def build_system_overlay_from_items(
        self,
        constraints: list[WorkingSetItem],
        goals: list[GoalState],
        block3_text: str,
        profile: ProfilePolicy,
    ) -> SystemPromptOverlay:
        """Build a ``SystemPromptOverlay`` from pre-processed items.

        * Block 2 (``prepend_system_context``): goals rendered with blockers.
        * Block 4 (``append_system_context``): evidence reference citations.
        * Block 3 (``prepend_context``): the caller-provided *block3_text*.
        """
        budget = profile.budgets.max_system_overlay_tokens

        # Block 2: goals with blockers
        block2_budget = int(budget * 0.40)
        block2_text = _render_goal_block(goals, block2_budget)

        # Block 4: evidence refs from constraint items
        block4_budget = min(
            int(budget * 0.20),
            self._config.evidence_budget_max_tokens,
        )
        block4_text = _render_evidence_block(constraints, block4_budget)

        return SystemPromptOverlay(
            prepend_system_context=block2_text if block2_text else None,
            prepend_context=block3_text if block3_text else None,
            append_system_context=block4_text if block4_text else None,
        )

    async def build_subagent_packet_from_context(
        self,
        parent_snapshot: WorkingSetSnapshot,
        child_goal: GoalState,
        budget: int,
        llm_client: object | None = None,
    ) -> SubagentPacket:
        """Build a ``SubagentPacket`` from a parent working-set snapshot.

        If an *llm_client* (or the instance-level one) is available, attempt
        LLM-based summarization of the inherited context.  On failure, fall
        back to the deterministic strategy: all ``must_inject`` items plus the
        top-3 items by score.
        """
        client = llm_client or self._llm_client

        # Deterministic fallback selection
        must_items = [it for it in parent_snapshot.items if it.must_inject]
        remaining = sorted(
            [it for it in parent_snapshot.items if not it.must_inject],
            key=lambda it: it.scores.final,
            reverse=True,
        )
        fallback_items = must_items + remaining[:3]
        fallback_parts: list[str] = []
        tokens_used = 0
        for item in fallback_items:
            rendered = _render_item_block(item)
            item_tokens = _estimate_tokens(rendered)
            if tokens_used + item_tokens > budget:
                break
            fallback_parts.append(rendered)
            tokens_used += item_tokens

        fallback_summary = "\n\n".join(fallback_parts)

        # Attempt LLM summarization if a client is available
        context_summary = fallback_summary
        if client is not None:
            try:
                all_text = "\n".join(it.text for it in fallback_items)
                prompt = (
                    "Summarize the following context for a child agent. "
                    f"The child's goal is: {child_goal.title}\n\n"
                    f"Context:\n{all_text}"
                )
                # Duck-typed async LLM client: must support
                # ``await client.complete(prompt)`` returning a string.
                raw = await client.complete(prompt)  # type: ignore[union-attr]
                summary_text = str(raw).strip()
                if summary_text:
                    context_summary = _truncate_to_budget(summary_text, budget)
            except Exception:
                logger.warning(
                    "LLM summarization failed for subagent packet; using fallback",
                    exc_info=True,
                )

        inherited_goal_ids = [child_goal.id]
        inherited_facts = len(fallback_items)

        return SubagentPacket(
            parent_session_key="",  # caller fills in
            child_session_key="",   # caller fills in
            context_summary=context_summary,
            inherited_goals=inherited_goal_ids,
            inherited_facts_count=inherited_facts,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------


# ======================================================================
# Module-level helpers (stateless, easily testable)
# ======================================================================

_ARTIFACT_PLACEHOLDER_THRESHOLD = 400  # characters (~100 tokens)


def _render_item_block(item: WorkingSetItem) -> str:
    """Render a single working-set item for prompt injection.

    Large items are replaced with an artifact placeholder to save tokens;
    small items are inlined verbatim.
    """
    if len(item.text) > _ARTIFACT_PLACEHOLDER_THRESHOLD:
        # Artifact placeholder -- the model can call ``artifact_search``
        # to retrieve the full content on demand.
        summary = item.text[:120].replace("\n", " ").strip()
        return (
            f"[Memory ({item.source_type}): {summary}...]\n"
            f" -> Call artifact_search(\"{item.id}\") for full content"
        )
    return f"[{item.source_type}] {item.text}"


def _render_goal_block(goals: list[GoalState], budget: int) -> str:
    """Render goals with blockers highlighted, within a token budget."""
    if not goals:
        return ""

    parts: list[str] = ["## Active Goals"]
    tokens_used = _estimate_tokens(parts[0])

    for goal in goals:
        line = f"- **{goal.title}**"
        if goal.description:
            line += f": {goal.description}"
        if goal.status:
            line += f" [{goal.status}]"

        # Highlight blockers so the model prioritises unblocking
        if goal.blockers:
            blocker_text = "; ".join(goal.blockers)
            line += f"\n  BLOCKERS: {blocker_text}"

        line_tokens = _estimate_tokens(line)
        if tokens_used + line_tokens > budget:
            break
        parts.append(line)
        tokens_used += line_tokens

    return "\n".join(parts)


def _render_evidence_block(items: list[WorkingSetItem], budget: int) -> str:
    """Render evidence reference citations from working-set items."""
    refs: list[str] = []
    tokens_used = 0

    for item in items:
        if not item.evidence_ref_ids:
            continue
        for ref_id in item.evidence_ref_ids:
            ref_line = f"[Evidence {ref_id}] supports: {item.text[:80]}"
            ref_tokens = _estimate_tokens(ref_line)
            if tokens_used + ref_tokens > budget:
                return "\n".join(refs)
            refs.append(ref_line)
            tokens_used += ref_tokens

    return "\n".join(refs)


def _order_by_class_then_score(items: list[WorkingSetItem]) -> list[WorkingSetItem]:
    """Sort items by memory-class priority (policy first), then descending score.

    Items whose ``category`` does not appear in ``CLASS_PRIORITY`` are
    sorted last (priority 99).
    """
    return sorted(
        items,
        key=lambda it: (
            CLASS_PRIORITY.get(it.category, 99),
            -it.scores.final,
        ),
    )


def _truncate_to_budget(text: str, budget_tokens: int) -> str:
    """Truncate *text* so its estimated token count stays within *budget_tokens*.

    Prefers line boundaries, then word boundaries, to avoid mid-word cuts.
    The "..." suffix is accounted for in the budget so the final output
    (text + suffix) never exceeds ``budget_tokens * 4`` chars.
    """
    max_chars = budget_tokens * 4
    if len(text) <= max_chars:
        return text
    suffix = "..."
    cut_at = max_chars - len(suffix)
    truncated = text[:cut_at]
    # Try line boundary first
    nl_pos = truncated.rfind("\n")
    if nl_pos > cut_at // 2:
        return truncated[:nl_pos] + suffix
    # Fall back to word boundary
    sp_pos = truncated.rfind(" ")
    if sp_pos > cut_at // 2:
        return truncated[:sp_pos] + suffix
    return truncated + suffix
