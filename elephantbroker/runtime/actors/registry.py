"""Actor registry — CRUD + authority chain traversal via Neo4j."""
from __future__ import annotations

import uuid

import cognee
from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import ActorDataPoint
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.identity_utils import assert_same_gateway
from elephantbroker.runtime.interfaces.actor_registry import IActorRegistry
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.schemas.actor import ActorRef, ActorRelationship, ActorType, RelationshipType
from elephantbroker.schemas.trace import TraceEvent, TraceEventType


class ActorRegistry(IActorRegistry):

    def __init__(self, graph: GraphAdapter, trace_ledger: ITraceLedger,
                 dataset_name: str = "elephantbroker", gateway_id: str = "") -> None:
        self._graph = graph
        self._trace = trace_ledger
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id

    async def register_actor(self, actor: ActorRef) -> ActorRef:
        actor.gateway_id = actor.gateway_id or self._gateway_id
        dp = ActorDataPoint.from_schema(actor)
        await add_data_points([dp])

        # Phase 8: Create MEMBER_OF edges for each team
        for team_id in actor.team_ids:
            try:
                # R2-P7 / link-spam guard: validate team belongs to the
                # caller's gateway. PermissionError surfaces as 403 via
                # R2-P5 middleware; runtime errors stay best-effort
                # (silent skip per pre-existing contract).
                await assert_same_gateway(self._graph, str(team_id), self._gateway_id)
                await self._graph.add_relation(str(actor.id), str(team_id), "MEMBER_OF")
            except PermissionError:
                await self._trace.append_event(TraceEvent(
                    event_type=TraceEventType.AUTHORITY_CHECK_FAILED,
                    payload={"action": "register_actor", "target": str(team_id), "gateway_id": self._gateway_id},
                ))
                raise
            except Exception:
                pass  # Edge creation is best-effort

        actor_text = f"Actor: {actor.display_name} (type: {actor.type.value})"
        if actor.handles:
            actor_text += f" handles: {', '.join(actor.handles)}"
        await cognee.add(actor_text, dataset_name=self._dataset_name)

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.INPUT_RECEIVED,
                actor_ids=[actor.id],
                payload={"action": "register_actor", "display_name": actor.display_name},
            )
        )
        return actor

    async def resolve_by_handle(self, handle: str) -> ActorRef | None:
        """Look up an actor by platform-qualified handle (e.g. 'telegram:user_tg')."""
        cypher = (
            "MATCH (a:ActorDataPoint) "
            "WHERE $handle IN a.handles AND a.gateway_id = $gateway_id "
            "RETURN properties(a) AS props LIMIT 1"
        )
        records = await self._graph.query_cypher(
            cypher, {"handle": handle, "gateway_id": self._gateway_id}
        )
        if not records:
            await self._trace.append_event(
                TraceEvent(
                    event_type=TraceEventType.HANDLE_RESOLVED,
                    payload={"handle": handle, "result": "not_found"},
                )
            )
            return None
        entity = records[0].get("props", {})
        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.HANDLE_RESOLVED,
                actor_ids=[uuid.UUID(entity.get("eb_id", ""))],
                payload={"handle": handle, "result": "found", "actor": entity.get("display_name", "")},
            )
        )
        raw_team_ids = entity.get("team_ids", [])
        if not raw_team_ids and entity.get("team_id"):
            raw_team_ids = [entity["team_id"]]
        return ActorRef(
            id=uuid.UUID(entity["eb_id"]),
            type=ActorType(entity["actor_type"]),
            display_name=entity["display_name"],
            authority_level=entity.get("authority_level", 0),
            handles=entity.get("handles", []),
            org_id=uuid.UUID(entity["org_id"]) if entity.get("org_id") else None,
            team_ids=[uuid.UUID(t) if not isinstance(t, uuid.UUID) else t for t in raw_team_ids],
            trust_level=entity.get("trust_level", 0.5),
            tags=entity.get("tags", []),
            gateway_id=entity.get("gateway_id", ""),
        )

    async def resolve_actor(self, actor_id: uuid.UUID) -> ActorRef | None:
        entity = await self._graph.get_entity(str(actor_id))
        if entity is None:
            return None
        # Reconstruct ActorRef from graph properties
        # Backward compat: old Neo4j nodes have "team_id" (string), new have "team_ids" (list)
        raw_team_ids = entity.get("team_ids", [])
        if not raw_team_ids and entity.get("team_id"):
            raw_team_ids = [entity["team_id"]]
        return ActorRef(
            id=uuid.UUID(entity["eb_id"]),
            type=ActorType(entity["actor_type"]),
            display_name=entity["display_name"],
            authority_level=entity.get("authority_level", 0),
            handles=entity.get("handles", []),
            org_id=uuid.UUID(entity["org_id"]) if entity.get("org_id") else None,
            team_ids=[uuid.UUID(t) if not isinstance(t, uuid.UUID) else t for t in raw_team_ids],
            trust_level=entity.get("trust_level", 0.5),
            tags=entity.get("tags", []),
            gateway_id=entity.get("gateway_id", ""),
        )

    async def get_authority_chain(self, actor_id: uuid.UUID) -> list[ActorRef]:
        # Traverse SUPERVISES/REPORTS_TO edges upward
        cypher = (
            "MATCH path = (start {eb_id: $actor_id})-[:REPORTS_TO|SUPERVISES*1..10]->(supervisor) "
            "WHERE start.gateway_id = $gateway_id "
            "RETURN properties(supervisor) AS props "
            "ORDER BY length(path)"
        )
        records = await self._graph.query_cypher(cypher, {"actor_id": str(actor_id), "gateway_id": self._gateway_id})
        chain: list[ActorRef] = []
        for rec in records:
            props = rec["props"]
            raw_tids = props.get("team_ids", [])
            if not raw_tids and props.get("team_id"):
                raw_tids = [props["team_id"]]
            chain.append(ActorRef(
                id=uuid.UUID(props["eb_id"]),
                type=ActorType(props["actor_type"]),
                display_name=props["display_name"],
                authority_level=props.get("authority_level", 0),
                handles=props.get("handles", []),
                org_id=uuid.UUID(props["org_id"]) if props.get("org_id") else None,
                team_ids=[uuid.UUID(t) if not isinstance(t, uuid.UUID) else t for t in raw_tids],
                trust_level=props.get("trust_level", 0.5),
                tags=props.get("tags", []),
                gateway_id=props.get("gateway_id", ""),
            ))
        return chain

    async def get_relationships(self, actor_id: uuid.UUID) -> list[ActorRelationship]:
        cypher = (
            "MATCH (a {eb_id: $actor_id})-[r]->(b) "
            "WHERE a.gateway_id = $gateway_id "
            "RETURN a.eb_id AS source, b.eb_id AS target, type(r) AS rel_type, properties(r) AS props "
            "UNION "
            "MATCH (b)-[r]->(a {eb_id: $actor_id}) "
            "WHERE a.gateway_id = $gateway_id "
            "RETURN b.eb_id AS source, a.eb_id AS target, type(r) AS rel_type, properties(r) AS props"
        )
        records = await self._graph.query_cypher(cypher, {"actor_id": str(actor_id), "gateway_id": self._gateway_id})
        relationships: list[ActorRelationship] = []
        for rec in records:
            try:
                rel_type = RelationshipType(rec["rel_type"].lower())
            except ValueError:
                continue
            relationships.append(ActorRelationship(
                source_actor_id=uuid.UUID(rec["source"]),
                target_actor_id=uuid.UUID(rec["target"]),
                relationship_type=rel_type,
            ))
        return relationships
