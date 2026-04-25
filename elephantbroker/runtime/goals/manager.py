"""Goal manager — CRUD + hierarchy via Neo4j."""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

logger = logging.getLogger("elephantbroker.runtime.goals.manager")

import cognee
from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import GoalDataPoint
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.graph_utils import clean_graph_props
from elephantbroker.runtime.identity_utils import assert_same_gateway
from elephantbroker.runtime.interfaces.goal_manager import IGoalManager
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.schemas.goal import GoalHierarchy, GoalState, GoalStatus
from elephantbroker.schemas.trace import TraceEvent, TraceEventType


class GoalManager(IGoalManager):

    def __init__(self, graph: GraphAdapter, trace_ledger: ITraceLedger,
                 dataset_name: str = "elephantbroker", gateway_id: str = "") -> None:
        self._graph = graph
        self._trace = trace_ledger
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id

    async def set_goal(
        self, goal: GoalState, org_id: str | None = None, team_id: str | None = None,
    ) -> GoalState:
        goal.gateway_id = goal.gateway_id or self._gateway_id
        # Phase 8: auto-populate org_id/team_id for scoped goals
        if not goal.org_id and org_id:
            import uuid as _uuid
            goal.org_id = _uuid.UUID(org_id)
        if not goal.team_id and team_id:
            import uuid as _uuid
            goal.team_id = _uuid.UUID(team_id)
        dp = GoalDataPoint.from_schema(goal)
        await add_data_points([dp])  # CREATE — also add text

        goal_text = f"Goal: {goal.title}"
        if goal.description:
            goal_text += f" — {goal.description}"
        if goal.success_criteria:
            goal_text += f" criteria: {', '.join(goal.success_criteria)}"
        await cognee.add(goal_text, dataset_name=self._dataset_name)

        if goal.parent_goal_id:
            # R2-P7 / link-spam guard: validate parent goal belongs to
            # the same gateway. PermissionError → 403 via R2-P5
            # middleware. Best-effort: skips check if parent missing.
            await assert_same_gateway(self._graph, str(goal.parent_goal_id), self._gateway_id)
            await self._graph.add_relation(str(goal.id), str(goal.parent_goal_id), "CHILD_OF")
        # Create OWNS_GOAL edges for owner actors (best-effort)
        for owner_id in goal.owner_actor_ids:
            try:
                # R2-P7 / link-spam guard: validate owner actor belongs
                # to the same gateway. PermissionError surfaces here
                # (NOT swallowed by the inner except — re-raised below)
                # so the cross-gateway link attempt becomes a 403.
                await assert_same_gateway(self._graph, str(owner_id), self._gateway_id)
                await self._graph.add_relation(str(owner_id), str(goal.id), "OWNS_GOAL")
            except PermissionError:
                # Re-raise PermissionError unswallowed — security-policy
                # rejection must surface as 403, not a silent skip.
                raise
            except Exception as exc:
                logger.warning("Failed to create OWNS_GOAL edge: actor=%s goal=%s error=%s", owner_id, goal.id, exc)
        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.INPUT_RECEIVED,
                goal_ids=[goal.id],
                payload={"action": "set_goal", "title": goal.title},
            )
        )
        return goal

    async def resolve_active_goals(self, session_id: uuid.UUID) -> list[GoalState]:
        cypher = (
            "MATCH (g:GoalDataPoint) WHERE g.status = 'active' AND g.gateway_id = $gateway_id "
            "RETURN properties(g) AS props"
        )
        records = await self._graph.query_cypher(cypher, {"gateway_id": self._gateway_id})
        goals: list[GoalState] = []
        for rec in records:
            dp = GoalDataPoint(**clean_graph_props(rec["props"]))
            goals.append(dp.to_schema())
        return goals

    async def get_goal_hierarchy(self, root_goal_id: uuid.UUID) -> GoalHierarchy:
        root_entity = await self._graph.get_entity(str(root_goal_id))
        if root_entity is None:
            return GoalHierarchy()

        root_props = clean_graph_props(root_entity)
        root_dp = GoalDataPoint(**root_props)
        root_goal = root_dp.to_schema()

        # Find children
        cypher = (
            "MATCH (child:GoalDataPoint)-[:CHILD_OF]->(parent {eb_id: $root_id}) "
            "WHERE child.gateway_id = $gateway_id "
            "RETURN properties(child) AS props"
        )
        records = await self._graph.query_cypher(cypher, {"root_id": str(root_goal_id), "gateway_id": self._gateway_id})
        children: list[GoalState] = []
        for rec in records:
            dp = GoalDataPoint(**clean_graph_props(rec["props"]))
            children.append(dp.to_schema())

        hierarchy = GoalHierarchy(root_goals=[root_goal])
        if children:
            hierarchy.children[str(root_goal_id)] = children
        return hierarchy

    async def update_goal_status(self, goal_id: uuid.UUID, status: GoalStatus,
                                 confidence: float | None = None) -> GoalState:
        entity = await self._graph.get_entity(str(goal_id))
        if entity is None:
            raise KeyError(f"Goal not found: {goal_id}")

        props = clean_graph_props(entity)
        dp = GoalDataPoint(**props)
        goal = dp.to_schema()
        goal.status = status
        if confidence is not None:
            goal.confidence = confidence
        goal.updated_at = datetime.now(UTC)
        goal.gateway_id = goal.gateway_id or self._gateway_id

        updated_dp = GoalDataPoint.from_schema(goal)
        await add_data_points([updated_dp])  # UPDATE — no cognee.add()
        payload = {"action": "goal_status_updated", "status": status.value}
        if confidence is not None:
            payload["confidence"] = confidence
        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.SESSION_GOAL_UPDATED,
                goal_ids=[goal_id],
                payload=payload,
            )
        )
        return goal
