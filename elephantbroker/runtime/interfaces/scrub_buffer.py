"""IScrubBuffer — narrowed Protocol for facade.delete()'s recent-facts scrub.

TODO-5-316: `MemoryStoreFacade` is the sole declared-type consumer of
`IIngestBuffer`, and it only calls one of its seven methods
(`scrub_fact_from_recent` on GDPR delete). Declaring a 7-method ABC for a
1-method dependency violates interface segregation and makes the facade's
coupling look heavier than it is.

Other `IngestBuffer` consumers (TurnIngest pipeline, API routes) duck-type
on the concrete class and never touch `IIngestBuffer`, so narrowing the
facade-side contract to a Protocol is a safe, container-free refactor.

`@runtime_checkable` enables isinstance() contract tests; `IngestBuffer`
satisfies this Protocol structurally without inheriting from it.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class IScrubBuffer(Protocol):
    """Narrow contract for GDPR-delete recent-facts scrubbing."""

    async def scrub_fact_from_recent(self, session_key: str, fact_id: str) -> int:
        """Remove a fact from the recent-facts window on GDPR delete.

        Returns the number of entries removed (0 if key missing or id not
        present). facade.delete() relies on this to prevent re-extraction
        of deleted facts inside the TTL window.
        """
        ...
