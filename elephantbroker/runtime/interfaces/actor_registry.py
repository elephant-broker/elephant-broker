"""Actor registry interface."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from elephantbroker.schemas.actor import ActorRef, ActorRelationship


class IActorRegistry(ABC):
    """Registry for actor resolution, relationships, and authority chains."""

    @abstractmethod
    async def resolve_actor(self, actor_id: uuid.UUID) -> ActorRef | None:
        """Resolve an actor by ID, returning None if not found."""
        ...

    @abstractmethod
    async def register_actor(self, actor: ActorRef) -> ActorRef:
        """Register a new actor, returning the stored reference."""
        ...

    @abstractmethod
    async def get_authority_chain(self, actor_id: uuid.UUID) -> list[ActorRef]:
        """Get the authority chain for an actor (supervisor -> ... -> root)."""
        ...

    @abstractmethod
    async def get_relationships(self, actor_id: uuid.UUID) -> list[ActorRelationship]:
        """Get all relationships involving this actor."""
        ...
