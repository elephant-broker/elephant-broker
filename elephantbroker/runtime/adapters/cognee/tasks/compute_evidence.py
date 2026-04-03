"""Task: compute evidence scores for claims. Stub — full logic in Phase 3-4."""
from __future__ import annotations

from elephantbroker.schemas.evidence import ClaimRecord


async def compute_evidence(claims: list[ClaimRecord]) -> list[ClaimRecord]:
    """Compute evidence strength and update claim statuses.

    Phase 2 stub: returns claims unchanged. Full evidence computation
    and status updates will be implemented in Phase 3-4.
    """
    return claims
