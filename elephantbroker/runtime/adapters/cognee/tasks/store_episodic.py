"""Task: store episodic facts in the graph. Stub — full logic in Phase 4."""
from __future__ import annotations

from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
from elephantbroker.schemas.fact import FactAssertion


async def store_episodic(
    facts: list[FactAssertion],
) -> list[str]:
    """Store episodic facts as graph entities via add_data_points.

    Phase 2 stub updated for Cognee-first storage. Returns the list of stored
    entity IDs.
    """
    ids: list[str] = []
    for fact in facts:
        dp = FactDataPoint.from_schema(fact)
        await add_data_points([dp])
        ids.append(dp.eb_id)
    return ids
