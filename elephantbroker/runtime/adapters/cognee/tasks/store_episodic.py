"""Task: store episodic facts in the graph. Stub — full logic in Phase 4."""
from __future__ import annotations

import logging

from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
from elephantbroker.schemas.fact import FactAssertion

logger = logging.getLogger(__name__)


async def store_episodic(
    facts: list[FactAssertion],
) -> list[str]:
    """Store episodic facts as graph entities via add_data_points.

    Phase 2 stub updated for Cognee-first storage. Returns the list of stored
    entity IDs.

    TODO-5-008: cognee_data_id is passed explicitly as None here with a
    WARNING log. This task has no graph adapter in scope, so it cannot
    round-trip to fetch existing pointers. The stub is intended for
    fresh-insert episodic paths where no prior Cognee Data row exists —
    cognee_data_id=None is the correct value for first-store. If this
    task is ever wired to MERGE an already-stored fact, the WARNING will
    flag that the graph pointer will be wiped; callers on that path must
    migrate to the full facade.store() path which captures the data_id
    from cognee.add() and passes it to from_schema() explicitly.
    """
    ids: list[str] = []
    for fact in facts:
        logger.warning(
            "store_episodic task called for fact_id=%s — stub path. "
            "cognee_data_id=None will be MERGE'd; if this fact already "
            "exists in the graph its cognee_data_id pointer will be wiped. "
            "See TODO-5-008; prefer facade.store() for cascade-safe writes.",
            getattr(fact, "id", "<unknown>"),
        )
        dp = FactDataPoint.from_schema(fact, cognee_data_id=None)
        await add_data_points([dp])
        ids.append(dp.eb_id)
    return ids
