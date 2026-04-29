"""Authority check helper for admin API routes.

This is NOT middleware — it's a per-route helper called only where authorization
is needed. Most routes (memory search, working set build) don't need it.
"""
from __future__ import annotations

import uuid

from fastapi import HTTPException

from elephantbroker.runtime.interfaces.actor_registry import IActorRegistry
from elephantbroker.runtime.profiles.authority_store import AuthorityRuleStore
from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

# Actions allowed during bootstrap mode (empty actor graph).
# R2-P7: add ``add_team_member`` / ``remove_team_member`` so the
# bootstrap workflow is complete — pre-fix you could create a team
# in bootstrap mode but not assign anyone to it (no admin actor yet
# exists to authorize the assignment, but you can't bootstrap one
# *into* the team either). The R2-P7 link-spam guard now provides
# the cross-gateway rejection at the route layer regardless of
# bootstrap state, so the security posture is preserved.
BOOTSTRAP_ACTIONS = frozenset({
    "create_org",
    "create_team",
    "register_actor",
    "add_team_member",
    "remove_team_member",
})


async def check_authority(
    actor_registry: IActorRegistry,
    authority_store: AuthorityRuleStore,
    actor_id: uuid.UUID | str,
    action: str,
    target_org_id: str | None = None,
    target_team_id: str | None = None,
    bootstrap_mode: bool = False,
    metrics=None,
    trace_ledger=None,
) -> ActorRef:
    """Resolve actor and check authority against the rule store.

    Returns the resolved ``ActorRef`` on success.
    Raises ``HTTPException(403)`` on insufficient authority.
    Raises ``HTTPException(404)`` if actor not found.

    Parameters
    ----------
    actor_registry : IActorRegistry
        Used to resolve the calling actor by ID.
    authority_store : AuthorityRuleStore
        Provides the rule for the requested action.
    actor_id : uuid.UUID or str
        The calling actor's ID (from ``X-EB-Actor-Id`` header or CLI config).
    action : str
        The action being attempted (e.g. ``"create_org"``, ``"add_team_member"``).
    target_org_id : str, optional
        The org being acted upon (for ``require_matching_org`` checks).
    target_team_id : str, optional
        The team being acted upon (for ``require_matching_team`` checks).
    bootstrap_mode : bool
        If ``True`` and action is in ``BOOTSTRAP_ACTIONS``, skip all checks.
    """
    # Bootstrap exception — first admin creation on empty graph
    if bootstrap_mode and action in BOOTSTRAP_ACTIONS:
        return ActorRef(
            id=uuid.UUID(str(actor_id)) if not isinstance(actor_id, uuid.UUID) else actor_id,
            type=ActorType.HUMAN_COORDINATOR,
            display_name="bootstrap-admin",
            authority_level=90,
        )

    # Resolve actor
    aid = uuid.UUID(str(actor_id)) if not isinstance(actor_id, uuid.UUID) else actor_id
    actor = await actor_registry.resolve_actor(aid)
    if actor is None:
        if metrics:
            metrics.inc_authority_check(action, "denied")
        if trace_ledger:
            await trace_ledger.append_event(TraceEvent(
                event_type=TraceEventType.AUTHORITY_CHECK_FAILED,
                actor_ids=[aid],
                payload={"action": action, "reason": "actor_not_found"},
            ))
        raise HTTPException(status_code=404, detail=f"Actor not found: {actor_id}")

    # Load rule
    rule = await authority_store.get_rule(action)
    min_level = rule.get("min_authority_level", 90)

    # Level check
    if actor.authority_level < min_level:
        if metrics:
            metrics.inc_authority_check(action, "denied")
        if trace_ledger:
            await trace_ledger.append_event(TraceEvent(
                event_type=TraceEventType.AUTHORITY_CHECK_FAILED,
                actor_ids=[aid],
                payload={"action": action, "reason": "insufficient_level", "required_level": min_level, "actor_level": actor.authority_level},
            ))
        raise HTTPException(
            status_code=403,
            detail=f"Requires authority_level >= {min_level} for action '{action}' "
                   f"(actor has {actor.authority_level})",
        )

    # Exempt check — high-authority actors bypass matching constraints
    exempt_level = rule.get("matching_exempt_level", 999)
    if actor.authority_level >= exempt_level:
        if metrics:
            metrics.inc_authority_check(action, "allowed")
        return actor

    # Org matching
    if rule.get("require_matching_org") and target_org_id:
        actor_org = str(actor.org_id) if actor.org_id else ""
        if actor_org != target_org_id:
            if metrics:
                metrics.inc_authority_check(action, "denied")
            if trace_ledger:
                await trace_ledger.append_event(TraceEvent(
                    event_type=TraceEventType.AUTHORITY_CHECK_FAILED,
                    actor_ids=[aid],
                    payload={"action": action, "reason": "org_mismatch", "actor_org": actor_org, "target_org": target_org_id},
                ))
            raise HTTPException(
                status_code=403,
                detail=f"Actor not in target org: actor_org={actor_org}, target={target_org_id}",
            )

    # Team matching
    if rule.get("require_matching_team") and target_team_id:
        actor_team_ids = [str(t) for t in actor.team_ids]
        if target_team_id not in actor_team_ids:
            if metrics:
                metrics.inc_authority_check(action, "denied")
            if trace_ledger:
                await trace_ledger.append_event(TraceEvent(
                    event_type=TraceEventType.AUTHORITY_CHECK_FAILED,
                    actor_ids=[aid],
                    payload={"action": action, "reason": "team_mismatch", "actor_teams": actor_team_ids, "target_team": target_team_id},
                ))
            raise HTTPException(
                status_code=403,
                detail=f"Actor not on target team: actor_teams={actor_team_ids}, target={target_team_id}",
            )

    if metrics:
        metrics.inc_authority_check(action, "allowed")
    return actor
