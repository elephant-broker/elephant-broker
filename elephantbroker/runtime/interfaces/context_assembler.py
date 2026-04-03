"""Context assembler interface."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.context import AgentMessage, AssembleResult, SubagentPacket, SystemPromptOverlay
from elephantbroker.schemas.goal import GoalState
from elephantbroker.schemas.profile import ProfilePolicy
from elephantbroker.schemas.working_set import WorkingSetItem, WorkingSetSnapshot


class IContextAssembler(ABC):
    """Assembles context from the working set into prompt-ready format."""

    @abstractmethod
    async def assemble(
        self, session_id: uuid.UUID, messages: list[AgentMessage], token_budget: int,
        session_key: str = "", gateway_id: str = "",
    ) -> AssembleResult:
        """Assemble context for the given messages within the token budget."""
        ...

    @abstractmethod
    async def build_system_overlay(self, session_id: uuid.UUID) -> SystemPromptOverlay:
        """Build the system prompt overlay for before_prompt_build hook."""
        ...

    @abstractmethod
    async def build_subagent_packet(
        self, parent_session_key: str, child_session_key: str
    ) -> SubagentPacket:
        """Build a context packet for a subagent spawn."""
        ...

    # Phase 6 additions

    async def assemble_from_snapshot(
        self,
        snapshot: WorkingSetSnapshot,
        effective_budget: int,
        session_goals: list[GoalState],
        profile: ProfilePolicy,
        guard_constraints: list[str] | None = None,
        session_key: str = "",
    ) -> AssembleResult:
        """Assemble context from a pre-built working set snapshot."""
        return AssembleResult(estimated_tokens=0)

    async def build_system_overlay_from_items(
        self,
        constraints: list[WorkingSetItem],
        goals: list[GoalState],
        block3_text: str,
        profile: ProfilePolicy,
    ) -> SystemPromptOverlay:
        """Build system prompt overlay from pre-processed items."""
        return SystemPromptOverlay()

    async def build_subagent_packet_from_context(
        self,
        parent_snapshot: WorkingSetSnapshot,
        child_goal: GoalState,
        budget: int,
        llm_client=None,
    ) -> SubagentPacket:
        """Build a subagent packet from a working set snapshot."""
        return SubagentPacket(parent_session_key="", child_session_key="")
