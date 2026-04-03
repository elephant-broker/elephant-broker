"""Consolidation engine interface."""
from __future__ import annotations

from abc import ABC, abstractmethod

from elephantbroker.schemas.consolidation import (
    ConsolidationContext,
    ConsolidationReport,
    StageResult,
)


class IConsolidationEngine(ABC):
    """9-stage 'sleep' pipeline for memory consolidation."""

    @abstractmethod
    async def run_consolidation(
        self, org_id: str, gateway_id: str, profile_id: str | None = None,
    ) -> ConsolidationReport:
        """Run the full consolidation pipeline. Scoped to (org_id, gateway_id) pair."""
        ...

    @abstractmethod
    async def run_stage(
        self, stage_num: int, org_id: str, gateway_id: str,
        context: ConsolidationContext,
    ) -> StageResult:
        """Run a single consolidation stage (for testing/debugging)."""
        ...

    @abstractmethod
    async def get_consolidation_report(self, report_id: str) -> ConsolidationReport | None:
        """Retrieve a previous consolidation report from SQLite."""
        ...
