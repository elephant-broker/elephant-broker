"""Task: update the knowledge graph with new datapoints. Stub — full logic in Phase 4."""
from __future__ import annotations

from typing import Any

from cognee.tasks.storage import add_data_points


async def update_graph(
    datapoints: list[Any],
) -> int:
    """Add or merge datapoints into the knowledge graph via add_data_points.

    Phase 2 stub updated for Cognee-first storage.
    Returns the count of entities processed.
    """
    count = 0
    for dp in datapoints:
        await add_data_points([dp])
        count += 1
    return count
