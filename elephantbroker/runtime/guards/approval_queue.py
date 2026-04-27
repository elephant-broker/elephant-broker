"""Approval queue — Redis-backed HITL approval request management (Phase 7 — §7.19)."""
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import UTC, datetime, timedelta

from elephantbroker.runtime.observability import GatewayLoggerAdapter
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.config import HitlConfig
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.guards import ApprovalRequest, ApprovalStatus, AutonomyLevel
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger(__name__)


class ApprovalQueue:
    """Redis-backed approval request queue with auto-goal integration."""

    def __init__(self, redis, redis_keys: RedisKeyBuilder, config: HitlConfig | None = None,
                 gateway_id: str = "", trace_ledger=None) -> None:
        self._redis = redis
        self._keys = redis_keys
        self._config = config or HitlConfig()
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})
        self._trace = trace_ledger

    async def create(
        self,
        request: ApprovalRequest,
        agent_id: str,
        *,
        session_goal_store=None,
        session_key: str = "",
        session_id: uuid.UUID | None = None,
    ) -> ApprovalRequest:
        """Store request in Redis + session index. Optionally create blocker auto-goal."""
        if not agent_id:
            self._log.warning("ApprovalQueue called with empty agent_id — keys will use empty segment")

        # Lazy sweep expired approvals before creating new ones (B2-O23)
        try:
            await self.sweep_expired(agent_id, request.session_id)
        except Exception as exc:
            self._log.warning("Lazy sweep failed (non-fatal): %s", exc)

        # Serialize and store (timeout_at already set by model_post_init from request.timeout_seconds)
        key = self._keys.approval(agent_id, str(request.id))
        ttl = request.timeout_seconds + 60
        await self._redis.setex(key, ttl, request.model_dump_json())

        # Add to session index
        idx_key = self._keys.approvals_by_session(agent_id, str(request.session_id))
        await self._redis.sadd(idx_key, str(request.id))
        await self._redis.expire(idx_key, ttl)

        # Auto-goal creation
        if session_goal_store and session_key and session_id:
            goal = GoalState(
                title=f"Pending approval: {request.action_summary[:80]}",
                description=f"Domain: {request.decision_domain}. Waiting for human approval.",
                status=GoalStatus.ACTIVE,
                gateway_id=self._log.extra.get("gateway_id", ""),
                blockers=[f"Guard: {', '.join(request.matched_rules[:3])} (event: {request.guard_event_id})"],
                metadata={
                    "source_type": "auto",
                    "source_system": "guard_approval",
                    "source_id": str(request.guard_event_id),
                    "approval_request_id": str(request.id),
                    "resolved_by_runtime": "false",
                },
            )
            try:
                await session_goal_store.add_goal(session_key, session_id, goal)
                if self._trace:
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.SESSION_GOAL_CREATED,
                        session_id=session_id,
                        session_key=session_key,
                        gateway_id=self._log.extra.get("gateway_id", ""),
                        goal_ids=[goal.id],
                        payload={
                            "source_type": "auto",
                            "source_system": "guard_approval",
                            "approval_request_id": str(request.id),
                            "session_key": session_key,
                        },
                    ))
            except Exception as exc:
                self._log.warning("Failed to create approval auto-goal: %s", exc)

        self._log.info("Created approval request %s for agent=%s domain=%s",
                       request.id, agent_id, request.decision_domain)
        return request

    async def get(self, request_id: uuid.UUID, agent_id: str) -> ApprovalRequest | None:
        key = self._keys.approval(agent_id, str(request_id))
        data = await self._redis.get(key)
        if data is None:
            return None
        return ApprovalRequest.model_validate_json(data)

    async def get_for_session(self, session_id: uuid.UUID, agent_id: str) -> list[ApprovalRequest]:
        idx_key = self._keys.approvals_by_session(agent_id, str(session_id))
        req_ids = await self._redis.smembers(idx_key)
        results = []
        for rid in req_ids:
            rid_str = rid.decode() if isinstance(rid, bytes) else rid
            req = await self.get(uuid.UUID(rid_str), agent_id)
            if req:
                results.append(req)
        return results

    async def approve(
        self,
        request_id: uuid.UUID,
        agent_id: str,
        *,
        message: str | None = None,
        approved_by: str | None = None,
        session_goal_store=None,
        session_key: str = "",
    ) -> ApprovalRequest | None:
        req = await self.get(request_id, agent_id)
        if req is None:
            return None
        req.status = ApprovalStatus.APPROVED
        req.resolved_at = datetime.now(UTC)
        req.resolved_by = approved_by
        req.approval_message = message
        key = self._keys.approval(agent_id, str(request_id))
        remaining_ttl = await self._redis.ttl(key)
        if remaining_ttl > 0:
            await self._redis.setex(key, remaining_ttl, req.model_dump_json())
        else:
            self._log.warning("Approval %s TTL expired during approve — update dropped", request_id)
        await self.resolve_approval_goal(req, session_goal_store, session_key, GoalStatus.COMPLETED)
        self._log.info("Approved request %s by=%s", request_id, approved_by or "unknown")
        return req

    async def reject(
        self,
        request_id: uuid.UUID,
        agent_id: str,
        *,
        reason: str,
        rejected_by: str | None = None,
        session_goal_store=None,
        session_key: str = "",
    ) -> ApprovalRequest | None:
        req = await self.get(request_id, agent_id)
        if req is None:
            return None
        req.status = ApprovalStatus.REJECTED
        req.resolved_at = datetime.now(UTC)
        req.resolved_by = rejected_by
        req.rejection_reason = reason
        key = self._keys.approval(agent_id, str(request_id))
        remaining_ttl = await self._redis.ttl(key)
        if remaining_ttl > 0:
            await self._redis.setex(key, remaining_ttl, req.model_dump_json())
        else:
            self._log.warning("Approval %s TTL expired during reject — update dropped", request_id)
        await self.resolve_approval_goal(req, session_goal_store, session_key, GoalStatus.ABANDONED)
        self._log.info("Rejected request %s by=%s reason=%s", request_id, rejected_by or "unknown", reason[:80])
        return req

    async def cancel(
        self,
        request_id: uuid.UUID,
        agent_id: str,
        *,
        reason: str = "Session ended",
        session_goal_store=None,
        session_key: str = "",
    ) -> ApprovalRequest | None:
        """Cancel a pending approval (e.g., on session end). Uses CANCELLED status."""
        req = await self.get(request_id, agent_id)
        if req is None:
            return None
        req.status = ApprovalStatus.CANCELLED
        req.resolved_at = datetime.now(UTC)
        req.resolved_by = "system"
        req.rejection_reason = reason
        key = self._keys.approval(agent_id, str(request_id))
        remaining_ttl = await self._redis.ttl(key)
        if remaining_ttl > 0:
            await self._redis.setex(key, remaining_ttl, req.model_dump_json())
        await self.resolve_approval_goal(req, session_goal_store, session_key, GoalStatus.ABANDONED)
        self._log.info("Cancelled request %s reason=%s", request_id, reason[:80])
        return req

    async def check_timeout(
        self,
        request_id: uuid.UUID,
        agent_id: str,
        timeout_action: AutonomyLevel = AutonomyLevel.HARD_STOP,
        *,
        session_goal_store=None,
        session_key: str = "",
    ) -> ApprovalRequest | None:
        """Lazy timeout check with 4 distinct behaviors per timeout_action.

        HARD_STOP / APPROVE_FIRST: permanently block → TIMED_OUT
        INFORM: downgrade to inform, action auto-proceeds → TIMED_OUT (guard maps to INFORM)
        AUTONOMOUS: auto-approve on timeout (silence = consent) → APPROVED
        """
        req = await self.get(request_id, agent_id)
        if req is None or req.status != ApprovalStatus.PENDING:
            return req
        if req.timeout_at and datetime.now(UTC) > req.timeout_at:
            req.resolved_at = datetime.now(UTC)
            req.resolved_by = "timeout"

            if timeout_action == AutonomyLevel.AUTONOMOUS:
                # Silence = consent: auto-approve
                req.status = ApprovalStatus.APPROVED
                req.approval_message = "Auto-approved on timeout (autonomous)"
                goal_status = GoalStatus.COMPLETED
            else:
                # HARD_STOP, APPROVE_FIRST, INFORM: mark as timed out
                # (guard engine maps INFORM timeout to GuardOutcome.INFORM at Layer 0)
                req.status = ApprovalStatus.TIMED_OUT
                goal_status = GoalStatus.ABANDONED

            key = self._keys.approval(agent_id, str(request_id))
            await self._redis.setex(key, 300, req.model_dump_json())
            await self.resolve_approval_goal(req, session_goal_store, session_key, goal_status)
        return req

    async def find_matching(
        self,
        session_id: uuid.UUID,
        action_summary: str,
        agent_id: str,
    ) -> ApprovalRequest | None:
        """Find existing approval for same action in session. Prevents duplicate requests."""
        requests = await self.get_for_session(session_id, agent_id)
        summary_hash = _action_hash(action_summary)
        for req in requests:
            req_hash = _action_hash(req.action_summary)
            if req_hash == summary_hash:
                return await self.check_timeout(req.id, agent_id)
        return None

    async def resolve_approval_goal(
        self,
        req: ApprovalRequest,
        session_goal_store,
        session_key: str,
        new_status: GoalStatus,
    ) -> None:
        """Find and resolve the auto-goal for this approval."""
        if not session_goal_store or not session_key:
            return
        try:
            goals = await session_goal_store.get_goals(session_key, req.session_id)
            for goal in goals:
                if (goal.metadata.get("source_type") == "auto"
                        and goal.metadata.get("source_system") == "guard_approval"
                        and goal.metadata.get("approval_request_id") == str(req.id)):
                    updates = {
                        "status": new_status,
                        "metadata": {**goal.metadata, "resolved_by_runtime": "true"},
                    }
                    if new_status == GoalStatus.ABANDONED and req.rejection_reason:
                        updates["blockers"] = [f"Rejected: {req.rejection_reason}"]
                    await session_goal_store.update_goal(session_key, req.session_id, goal.id, updates)
                    if self._trace:
                        await self._trace.append_event(TraceEvent(
                            event_type=TraceEventType.SESSION_GOAL_UPDATED,
                            session_id=req.session_id,
                            session_key=session_key,
                            gateway_id=self._log.extra.get("gateway_id", ""),
                            goal_ids=[goal.id],
                            payload={
                                "source_type": "auto",
                                "source_system": "guard_approval",
                                "approval_request_id": str(req.id),
                                "new_status": new_status.value,
                                "session_key": session_key,
                            },
                        ))
                    break
        except Exception as exc:
            self._log.warning("Failed to resolve approval goal: %s", exc)


    async def sweep_expired(self, agent_id: str, session_id: uuid.UUID) -> int:
        """Lazy sweep: check and resolve expired approvals for a session."""
        requests = await self.get_for_session(session_id, agent_id)
        swept = 0
        for req in requests:
            if req.status == ApprovalStatus.PENDING and req.timeout_at:
                if datetime.now(UTC) > req.timeout_at:
                    await self.check_timeout(req.id, agent_id)
                    swept += 1
        if swept:
            self._log.info("Swept %d expired approvals for agent=%s session=%s", swept, agent_id, session_id)
        return swept


def _action_hash(action_summary: str) -> str:
    """Deterministic hash for action dedup."""
    return hashlib.sha256(action_summary.strip().lower().encode()).hexdigest()[:16]
