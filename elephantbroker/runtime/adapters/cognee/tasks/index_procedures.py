"""Task: index procedures into the knowledge graph. Stub — full logic in Phase 4."""
from __future__ import annotations

from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import ProcedureDataPoint
from elephantbroker.schemas.procedure import ProcedureDefinition


async def index_procedures(
    procs: list[ProcedureDefinition],
) -> list[str]:
    """Store procedure definitions as graph entities via add_data_points.

    Phase 2 stub updated for Cognee-first storage. Returns the list of stored
    entity IDs.
    """
    ids: list[str] = []
    for proc in procs:
        dp = ProcedureDataPoint.from_schema(proc)
        await add_data_points([dp])
        ids.append(dp.eb_id)
    return ids
