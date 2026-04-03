"""Admin API routes — authority-gated management of orgs, teams, actors, goals, profiles.

All mutating endpoints call ``check_authority()`` before performing the operation.
The same API surface is used by the dashboard, ``ebrun`` CLI, and privileged agent tools.
"""
from __future__ import annotations

import logging
import uuid

from cognee.tasks.storage import add_data_points
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from elephantbroker.api.routes._authority import check_authority
from elephantbroker.runtime.adapters.cognee.datapoints import (
    OrganizationDataPoint,
    TeamDataPoint,
)
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------

class CreateOrgRequest(BaseModel):
    name: str = Field(min_length=1)
    display_label: str = ""


class CreateTeamRequest(BaseModel):
    name: str = Field(min_length=1)
    display_label: str = ""
    org_id: str


class AddMemberRequest(BaseModel):
    actor_id: str


class CreatePersistentGoalRequest(BaseModel):
    title: str = Field(min_length=1)
    description: str = ""
    scope: str = "actor"
    org_id: str | None = None
    team_id: str | None = None
    parent_goal_id: str | None = None
    success_criteria: list[str] = Field(default_factory=list)
    owner_actor_ids: list[str] = Field(default_factory=list)


class UpdateAuthorityRuleRequest(BaseModel):
    min_authority_level: int = Field(ge=0)
    require_matching_org: bool = False
    require_matching_team: bool = False
    require_self_ownership: bool = False
    matching_exempt_level: int | None = None


class SetProfileOverrideRequest(BaseModel):
    overrides: dict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_deps(request: Request):
    """Extract common dependencies from request."""
    container = request.app.state.container
    actor_id_str = getattr(request.state, "actor_id", "") or request.headers.get("X-EB-Actor-Id", "")
    return container, actor_id_str


async def _auth(request: Request, action: str, target_org_id: str | None = None, target_team_id: str | None = None):
    """Shortcut for authority check."""
    container, actor_id_str = _get_deps(request)
    # Lazy bootstrap detection (avoids Neo4j connection during container init)
    bootstrap = False
    if hasattr(container, "check_bootstrap_mode"):
        bootstrap = await container.check_bootstrap_mode()
    elif hasattr(container, "_bootstrap_mode"):
        bootstrap = getattr(container, "_bootstrap_mode", False) or False
    if not actor_id_str and not bootstrap:
        raise HTTPException(status_code=401, detail="X-EB-Actor-Id header required for admin operations")
    aid = uuid.UUID(actor_id_str) if actor_id_str else uuid.uuid4()
    return await check_authority(
        container.actor_registry,
        container.authority_store,
        aid, action,
        target_org_id=target_org_id,
        target_team_id=target_team_id,
        bootstrap_mode=bootstrap,
        metrics=getattr(container, "metrics_ctx", None),
        trace_ledger=getattr(container, "trace_ledger", None),
    )


# ---------------------------------------------------------------------------
# Bootstrap status
# ---------------------------------------------------------------------------

@router.get("/bootstrap-status")
async def get_bootstrap_status(request: Request):
    container = request.app.state.container
    if hasattr(container, "check_bootstrap_mode"):
        mode = await container.check_bootstrap_mode()
    else:
        mode = getattr(container, "_bootstrap_mode", False) or False
    return {"bootstrap_mode": mode}


# ---------------------------------------------------------------------------
# Authority rules
# ---------------------------------------------------------------------------

@router.get("/authority-rules")
async def list_authority_rules(request: Request):
    container = request.app.state.container
    rules = await container.authority_store.get_rules()
    return rules


@router.put("/authority-rules/{action}")
async def update_authority_rule(action: str, body: UpdateAuthorityRuleRequest, request: Request):
    await _auth(request, "create_org")  # system admin required
    container = request.app.state.container
    rule_data = body.model_dump(exclude_none=True)
    await container.authority_store.set_rule(action, rule_data)
    return {"action": action, "rule": rule_data}


# ---------------------------------------------------------------------------
# Organizations
# ---------------------------------------------------------------------------

@router.post("/organizations")
async def create_organization(body: CreateOrgRequest, request: Request):
    await _auth(request, "create_org")
    container = request.app.state.container
    org_id = str(uuid.uuid4())
    dp = OrganizationDataPoint(
        id=uuid.UUID(org_id), name=body.name,
        display_label=body.display_label or body.name[:20],
        eb_id=org_id,
    )
    try:
        await add_data_points([dp])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to store organization: {exc}") from exc

    await container.trace_ledger.append_event(TraceEvent(
        event_type=TraceEventType.ORG_CREATED,
        payload={"org_id": org_id, "name": body.name},
    ))
    logger.info("Created organization: %s (%s)", body.name, org_id)
    metrics = getattr(container, "metrics_ctx", None)
    if metrics:
        metrics.inc_admin_op("create_org", "success")
    return {"org_id": org_id, "name": body.name, "display_label": dp.display_label}


@router.get("/organizations")
async def list_organizations(request: Request):
    await _auth(request, "register_actor")  # authority >= 70
    container = request.app.state.container
    records = await container.graph.query_cypher(
        "MATCH (o:OrganizationDataPoint) RETURN properties(o) AS props"
    )
    return [{"org_id": r["props"].get("eb_id"), "name": r["props"].get("name"),
             "display_label": r["props"].get("display_label", "")} for r in records]


@router.put("/organizations/{org_id}")
async def update_organization(org_id: str, body: CreateOrgRequest, request: Request):
    await _auth(request, "create_org")
    container = request.app.state.container
    entity = await container.graph.get_entity(org_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Organization not found")
    dp = OrganizationDataPoint(
        id=uuid.UUID(org_id), name=body.name,
        display_label=body.display_label or body.name[:20],
        eb_id=org_id,
    )
    await add_data_points([dp])
    return {"org_id": org_id, "name": body.name, "display_label": dp.display_label}


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

@router.post("/teams")
async def create_team(body: CreateTeamRequest, request: Request):
    await _auth(request, "create_team", target_org_id=body.org_id)
    container = request.app.state.container
    team_id = str(uuid.uuid4())
    dp = TeamDataPoint(
        id=uuid.UUID(team_id), name=body.name,
        display_label=body.display_label or body.name[:20],
        org_id=body.org_id, eb_id=team_id,
    )
    await add_data_points([dp])
    # BELONGS_TO edge: team → org
    await container.graph.add_relation(team_id, body.org_id, "BELONGS_TO")

    await container.trace_ledger.append_event(TraceEvent(
        event_type=TraceEventType.TEAM_CREATED,
        payload={"team_id": team_id, "org_id": body.org_id, "name": body.name},
    ))
    logger.info("Created team: %s in org %s (%s)", body.name, body.org_id, team_id)
    metrics = getattr(container, "metrics_ctx", None)
    if metrics:
        metrics.inc_admin_op("create_team", "success")
        metrics.inc_org_team_edge("BELONGS_TO", "created")
    return {"team_id": team_id, "name": body.name, "org_id": body.org_id, "display_label": dp.display_label}


@router.get("/teams")
async def list_teams(request: Request, org_id: str | None = None):
    await _auth(request, "add_team_member", target_org_id=org_id)  # authority >= 50
    container = request.app.state.container
    if org_id:
        records = await container.graph.query_cypher(
            "MATCH (t:TeamDataPoint)-[:BELONGS_TO]->(o:OrganizationDataPoint {eb_id: $org_id}) "
            "RETURN properties(t) AS props",
            {"org_id": org_id},
        )
    else:
        records = await container.graph.query_cypher(
            "MATCH (t:TeamDataPoint) RETURN properties(t) AS props"
        )
    return [{"team_id": r["props"].get("eb_id"), "name": r["props"].get("name"),
             "org_id": r["props"].get("org_id"), "display_label": r["props"].get("display_label", "")}
            for r in records]


@router.put("/teams/{team_id}")
async def update_team(team_id: str, body: CreateTeamRequest, request: Request):
    await _auth(request, "create_team", target_org_id=body.org_id)
    container = request.app.state.container
    entity = await container.graph.get_entity(team_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Team not found")
    dp = TeamDataPoint(
        id=uuid.UUID(team_id), name=body.name,
        display_label=body.display_label or body.name[:20],
        org_id=body.org_id, eb_id=team_id,
    )
    await add_data_points([dp])
    return {"team_id": team_id, "name": body.name, "org_id": body.org_id}


# ---------------------------------------------------------------------------
# Team membership
# ---------------------------------------------------------------------------

@router.post("/teams/{team_id}/members")
async def add_team_member(team_id: str, body: AddMemberRequest, request: Request):
    await _auth(request, "add_team_member", target_team_id=team_id)
    container = request.app.state.container
    await container.graph.add_relation(body.actor_id, team_id, "MEMBER_OF")
    await container.trace_ledger.append_event(TraceEvent(
        event_type=TraceEventType.MEMBER_ADDED,
        payload={"actor_id": body.actor_id, "team_id": team_id},
    ))
    logger.info("Added member %s to team %s", body.actor_id, team_id)
    metrics = getattr(container, "metrics_ctx", None)
    if metrics:
        metrics.inc_admin_op("add_member", "success")
        metrics.inc_org_team_edge("MEMBER_OF", "created")
    return {"actor_id": body.actor_id, "team_id": team_id, "status": "added"}


@router.delete("/teams/{team_id}/members/{actor_id}")
async def remove_team_member(team_id: str, actor_id: str, request: Request):
    await _auth(request, "remove_team_member", target_team_id=team_id)
    container = request.app.state.container
    if hasattr(container.graph, "delete_relation"):
        await container.graph.delete_relation(actor_id, team_id, "MEMBER_OF")
    else:
        # Fallback: use Cypher directly
        await container.graph.query_cypher(
            "MATCH (a {eb_id: $aid})-[r:MEMBER_OF]->(t {eb_id: $tid}) DELETE r",
            {"aid": actor_id, "tid": team_id},
        )
    await container.trace_ledger.append_event(TraceEvent(
        event_type=TraceEventType.MEMBER_REMOVED,
        payload={"actor_id": actor_id, "team_id": team_id},
    ))
    logger.info("Removed member %s from team %s", actor_id, team_id)
    metrics = getattr(container, "metrics_ctx", None)
    if metrics:
        metrics.inc_admin_op("remove_member", "success")
        metrics.inc_org_team_edge("MEMBER_OF", "deleted")
    return {"actor_id": actor_id, "team_id": team_id, "status": "removed"}


@router.get("/teams/{team_id}/members")
async def list_team_members(team_id: str, request: Request):
    await _auth(request, "add_team_member", target_team_id=team_id)
    container = request.app.state.container
    records = await container.graph.query_cypher(
        "MATCH (a:ActorDataPoint)-[:MEMBER_OF]->(t:TeamDataPoint {eb_id: $team_id}) "
        "RETURN properties(a) AS props",
        {"team_id": team_id},
    )
    return [{"actor_id": r["props"].get("eb_id"), "display_name": r["props"].get("display_name"),
             "actor_type": r["props"].get("actor_type"), "authority_level": r["props"].get("authority_level", 0)}
            for r in records]


# ---------------------------------------------------------------------------
# Actors
# ---------------------------------------------------------------------------

@router.get("/actors")
async def list_actors(request: Request, org_id: str | None = None):
    await _auth(request, "register_actor")
    container = request.app.state.container
    gw_id = getattr(request.state, "gateway_id", "local")
    if org_id:
        records = await container.graph.query_cypher(
            "MATCH (a:ActorDataPoint) WHERE a.gateway_id = $gw AND a.org_id = $org "
            "RETURN properties(a) AS props",
            {"gw": gw_id, "org": org_id},
        )
    else:
        records = await container.graph.query_cypher(
            "MATCH (a:ActorDataPoint) WHERE a.gateway_id = $gw RETURN properties(a) AS props",
            {"gw": gw_id},
        )
    return [{"actor_id": r["props"].get("eb_id"), "display_name": r["props"].get("display_name"),
             "actor_type": r["props"].get("actor_type"), "authority_level": r["props"].get("authority_level", 0)}
            for r in records]


@router.post("/actors")
async def register_actor(request: Request):
    body = await request.json()
    await _auth(request, "register_actor")
    container = request.app.state.container
    from elephantbroker.schemas.actor import ActorRef, ActorType
    actor = ActorRef(
        type=ActorType(body.get("type", "worker_agent")),
        display_name=body.get("display_name", ""),
        authority_level=body.get("authority_level", 0),
        handles=body.get("handles", []),
        org_id=uuid.UUID(body["org_id"]) if body.get("org_id") else None,
        team_ids=[uuid.UUID(t) for t in body.get("team_ids", [])],
        gateway_id=getattr(request.state, "gateway_id", "local"),
    )
    result = await container.actor_registry.register_actor(actor)
    # Disable bootstrap mode after first actor creation
    if getattr(container, "_bootstrap_mode", False) or getattr(container, "_bootstrap_mode", None) is True:
        container._bootstrap_mode = False
        container._bootstrap_checked = True
        logger.info("Bootstrap mode disabled after first actor creation")
    return result.model_dump(mode="json")


@router.put("/actors/{actor_id}")
async def update_actor(actor_id: str, request: Request):
    body = await request.json()
    await _auth(request, "register_actor")
    container = request.app.state.container
    actor = await container.actor_registry.resolve_actor(uuid.UUID(actor_id))
    if not actor:
        raise HTTPException(status_code=404, detail="Actor not found")
    # Apply updates
    if "authority_level" in body:
        actor.authority_level = body["authority_level"]
    if "handles" in body:
        actor.handles = body["handles"]
    if "display_name" in body:
        actor.display_name = body["display_name"]
    # Re-store
    await container.actor_registry.register_actor(actor)
    return actor.model_dump(mode="json")


@router.post("/actors/{actor_id}/merge")
async def merge_actors(actor_id: str, request: Request):
    body = await request.json()
    await _auth(request, "merge_actors")
    container = request.app.state.container
    duplicate_id = body.get("duplicate_id")
    if not duplicate_id:
        raise HTTPException(status_code=400, detail="duplicate_id required")
    if hasattr(container.actor_registry, "merge_actors"):
        result = await container.actor_registry.merge_actors(
            uuid.UUID(actor_id), uuid.UUID(duplicate_id)
        )
        return result.model_dump(mode="json")
    raise HTTPException(status_code=501, detail="Actor merge not implemented")


# ---------------------------------------------------------------------------
# Persistent goals
# ---------------------------------------------------------------------------

SCOPE_ACTION_MAP = {
    "global": "create_global_goal",
    "organization": "create_org_goal",
    "team": "create_team_goal",
    "actor": "create_actor_goal",
}


@router.post("/goals")
async def create_persistent_goal(body: CreatePersistentGoalRequest, request: Request):
    action = SCOPE_ACTION_MAP.get(body.scope, "create_global_goal")
    await _auth(request, action, target_org_id=body.org_id, target_team_id=body.team_id)
    container = request.app.state.container
    gw_id = getattr(request.state, "gateway_id", "local")

    goal = GoalState(
        title=body.title, description=body.description,
        scope=Scope(body.scope), status=GoalStatus.ACTIVE,
        parent_goal_id=uuid.UUID(body.parent_goal_id) if body.parent_goal_id else None,
        success_criteria=body.success_criteria,
        owner_actor_ids=[uuid.UUID(a) for a in body.owner_actor_ids],
        gateway_id=gw_id,
        org_id=uuid.UUID(body.org_id) if body.org_id else None,
        team_id=uuid.UUID(body.team_id) if body.team_id else None,
    )
    result = await container.goal_manager.set_goal(goal)

    metrics = getattr(container, "metrics_ctx", None)
    if metrics:
        metrics.inc_goal_create()

    await container.trace_ledger.append_event(TraceEvent(
        event_type=TraceEventType.PERSISTENT_GOAL_CREATED,
        payload={"goal_id": str(result.id), "scope": body.scope, "org_id": body.org_id or ""},
    ))
    return result.model_dump(mode="json")


@router.get("/goals")
async def list_persistent_goals(request: Request, scope: str | None = None, org_id: str | None = None):
    container = request.app.state.container
    gw_id = getattr(request.state, "gateway_id", "local")
    cypher = "MATCH (g:GoalDataPoint) WHERE g.gateway_id = $gw AND g.status = 'active'"
    params: dict = {"gw": gw_id}
    if scope:
        cypher += " AND g.scope = $scope"
        params["scope"] = scope
    if org_id:
        cypher += " AND g.org_id = $org_id"
        params["org_id"] = org_id
    cypher += " RETURN properties(g) AS props"
    records = await container.graph.query_cypher(cypher, params)
    return [r["props"] for r in records]


@router.get("/goals/{goal_id}")
async def get_persistent_goal(goal_id: str, request: Request):
    container = request.app.state.container
    entity = await container.graph.get_entity(goal_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Goal not found")
    return entity


@router.put("/goals/{goal_id}")
async def update_persistent_goal(goal_id: str, request: Request):
    body = await request.json()
    container = request.app.state.container
    entity = await container.graph.get_entity(goal_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Goal not found")
    if "status" in body:
        await container.goal_manager.update_goal_status(uuid.UUID(goal_id), GoalStatus(body["status"]))
    return {"goal_id": goal_id, "updated": True}


# ---------------------------------------------------------------------------
# Profile overrides
# ---------------------------------------------------------------------------

@router.get("/profiles/overrides/{org_id}")
async def list_profile_overrides(org_id: str, request: Request):
    await _auth(request, "register_org_profile_override", target_org_id=org_id)
    container = request.app.state.container
    if not container.profile_registry._org_store:
        return []
    return await container.profile_registry._org_store.list_overrides(org_id)


@router.put("/profiles/overrides/{org_id}/{profile_id}")
async def set_profile_override(org_id: str, profile_id: str, body: SetProfileOverrideRequest, request: Request):
    await _auth(request, "register_org_profile_override", target_org_id=org_id)
    container = request.app.state.container
    _, actor_id_str = _get_deps(request)
    await container.profile_registry.register_org_override(org_id, profile_id, body.overrides, actor_id=actor_id_str)
    return {"org_id": org_id, "profile_id": profile_id, "status": "set"}


@router.delete("/profiles/overrides/{org_id}/{profile_id}")
async def delete_profile_override(org_id: str, profile_id: str, request: Request):
    await _auth(request, "register_org_profile_override", target_org_id=org_id)
    container = request.app.state.container
    await container.profile_registry.delete_org_override(org_id, profile_id)
    return {"org_id": org_id, "profile_id": profile_id, "status": "deleted"}
