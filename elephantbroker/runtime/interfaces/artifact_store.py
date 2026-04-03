"""Tool artifact store interface."""
from __future__ import annotations

from abc import ABC, abstractmethod

from elephantbroker.schemas.artifact import ArtifactHash, ToolArtifact


class IToolArtifactStore(ABC):
    """Stores and retrieves tool output artifacts."""

    @abstractmethod
    async def store_artifact(self, artifact: ToolArtifact) -> ToolArtifact:
        """Store a tool artifact."""
        ...

    @abstractmethod
    async def search_artifacts(self, query: str, max_results: int = 10) -> list[ToolArtifact]:
        """Search stored artifacts by query."""
        ...

    @abstractmethod
    async def get_by_hash(self, content_hash: ArtifactHash) -> ToolArtifact | None:
        """Retrieve an artifact by its content hash."""
        ...
