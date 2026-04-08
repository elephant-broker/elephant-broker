"""SessionGoalStore — Redis two-bucket session goals."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime

import cognee
from cognee.tasks.storage import add_data_points
from redis.exceptions import WatchError

from elephantbroker.runtime.adapters.cognee.datapoints import GoalDataPoint
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.observability import GatewayLoggerAdapter, traced
from elephantbroker.schemas.config import ScoringConfig
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.runtime.working_set.session_goals")


class SessionGoalStore:
    """Redis-backed session goals with Cognee flush on session end."""

    def __init__(self, redis, config: ScoringConfig | None = None,
                 trace_ledger: ITraceLedger | None = None,
                 graph=None, dataset_name: str = "elephantbroker",
                 gateway_id: str = "", redis_keys=None,
                 metrics=None) -> None:
        self._redis = redis
        self._config = config or ScoringConfig()
        self._trace = trace_ledger
        self._graph = graph
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id
        self._keys = redis_keys
        self._metrics = metrics
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})

    def _key(self, session_key: str, session_id: uuid.UUID | None = None) -> str:
        if self._keys:
            return self._keys.session_goals(session_key)
        self._log.warning("RedisKeyBuilder not configured — using fallback key without gateway scoping")
        return f"eb:session_goals:{session_key}"

    @traced
    async def get_goals(self, session_key: str, session_id: uuid.UUID) -> list[GoalState]:
        try:
            raw = await self._redis.get(self._key(session_key, session_id))
            if raw:
                data = json.loads(raw)
                return [GoalState(**g) for g in data]
        except Exception as exc:
            self._log.warning("Failed to get session goals (%s/%s): %s", session_key, session_id, exc)
        return []

    @traced
    async def set_goals(self, session_key: str, session_id: uuid.UUID,
                        goals: list[GoalState]) -> None:
        data = [g.model_dump(mode="json") for g in goals]
        await self._redis.setex(
            self._key(session_key, session_id),
            self._config.session_goals_ttl_seconds,
            json.dumps(data),
        )
        if self._metrics:
            self._metrics.set_session_goals_count(len(goals))

    @traced
    async def add_goal(self, session_key: str, session_id: uuid.UUID,
                       goal: GoalState) -> GoalState:
        goals = await self.get_goals(session_key, session_id)
        goals.append(goal)
        await self.set_goals(session_key, session_id, goals)
        return goal

    @traced
    async def update_goal(self, session_key: str, session_id: uuid.UUID,
                          goal_id: uuid.UUID, updates: dict) -> GoalState | None:
        goals = await self.get_goals(session_key, session_id)
        for g in goals:
            if g.id == goal_id:
                # Phase 7: Auto-goal enforcement — agent cannot manually complete/abandon
                if g.metadata.get("source_type") == "auto":
                    new_status = updates.get("status")
                    if new_status in (GoalStatus.COMPLETED, GoalStatus.ABANDONED):
                        # Only the runtime (resolved_by_runtime=true) can complete/abandon auto-goals
                        new_meta = updates.get("metadata", {})
                        if new_meta.get("resolved_by_runtime") != "true" and g.metadata.get("resolved_by_runtime") != "true":
                            raise ValueError(
                                "This goal is managed by the runtime. It will be resolved when "
                                "the underlying procedure step is completed with required proof. "
                                "Use procedure_complete_step to provide evidence."
                            )
                _IMMUTABLE_FIELDS = {"id", "created_at"}
                for k, v in updates.items():
                    if k in _IMMUTABLE_FIELDS:
                        continue
                    # Append-mode for evidence list (B2-O15)
                    if k == "_append_evidence" and isinstance(v, str):
                        g.evidence.append(v)
                        continue
                    if hasattr(g, k):
                        setattr(g, k, v)
                # Auto-clear blockers on terminal states (B2-O14)
                # Preserve rejection-reason blockers on ABANDONED (approval_queue sets these)
                if g.status == GoalStatus.COMPLETED:
                    g.blockers = []
                elif g.status == GoalStatus.ABANDONED:
                    g.blockers = [b for b in g.blockers if b.startswith("Rejected:")]
                g.updated_at = datetime.now(UTC)
                await self.set_goals(session_key, session_id, goals)
                return g
        return None

    @traced
    async def add_blocker(self, session_key: str, session_id: uuid.UUID,
                          goal_id: uuid.UUID, blocker: str) -> GoalState | None:
        """Atomically add a blocker to a goal using WATCH/MULTI to prevent races (B2-BUG05)."""
        key = self._key(session_key, session_id)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if not raw:
                        return None
                    goals_data = json.loads(raw)
                    goals = [GoalState(**g) for g in goals_data]
                    target = None
                    for g in goals:
                        if g.id == goal_id:
                            g.blockers.append(blocker)
                            g.updated_at = datetime.now(UTC)
                            target = g
                            break
                    if target is None:
                        return None
                    data = [g.model_dump(mode="json") for g in goals]
                    pipe.multi()
                    pipe.setex(key, self._config.session_goals_ttl_seconds, json.dumps(data))
                    await pipe.execute()
                    if self._metrics:
                        self._metrics.set_session_goals_count(len(goals))
                    return target
            except WatchError:
                if attempt < max_retries - 1:
                    continue
                self._log.warning("Atomic add_blocker WatchError after %d retries", max_retries)
                break
            except Exception as exc:
                self._log.warning("Atomic add_blocker failed (attempt %d): %s", attempt + 1, exc)
                break
        # Fallback: non-atomic (better than failing)
        goals = await self.get_goals(session_key, session_id)
        for g in goals:
            if g.id == goal_id:
                g.blockers.append(blocker)
                g.updated_at = datetime.now(UTC)
                await self.set_goals(session_key, session_id, goals)
                return g
        return None

    @traced
    async def remove_goal(self, session_key: str, session_id: uuid.UUID,
                          goal_id: uuid.UUID) -> bool:
        goals = await self.get_goals(session_key, session_id)
        # Auto-goal protection: runtime-managed goals cannot be removed directly
        for g in goals:
            if (g.id == goal_id
                    and g.metadata.get("source_type") == "auto"
                    and g.metadata.get("resolved_by_runtime") != "true"):
                raise ValueError(
                    f"Cannot remove auto-goal {goal_id} — must be resolved by runtime"
                )
        new_goals = [g for g in goals if g.id != goal_id]
        if len(new_goals) == len(goals):
            return False
        await self.set_goals(session_key, session_id, new_goals)
        return True

    @traced
    async def flush_to_cognee(self, session_key: str, session_id: uuid.UUID,
                              agent_key: str | None = None) -> int:
        """Flush session goals to Cognee graph on session end."""
        goals = await self.get_goals(session_key, session_id)
        if not goals:
            self._log.info("No session goals for %s — nothing to flush", session_key)
            return 0

        # Derive agent actor UUID for OWNS_GOAL edge fallback
        from elephantbroker.runtime.identity import deterministic_uuid_from
        fallback_owner_id = None
        identity = agent_key or self._gateway_id
        if identity:
            try:
                fallback_owner_id = deterministic_uuid_from(identity)
            except Exception:
                pass

        count = 0
        for goal in goals:
            try:
                goal.gateway_id = goal.gateway_id or self._gateway_id
                # Stamp owner if empty so OWNS_GOAL edges get created
                if not goal.owner_actor_ids and fallback_owner_id:
                    goal.owner_actor_ids = [fallback_owner_id]
                    self._log.info("Stamped fallback owner %s on goal %s", fallback_owner_id, goal.id)
                dp = GoalDataPoint.from_schema(goal)
                await add_data_points([dp])
                self._log.info(
                    "Goal %s flushed to graph (eb_id=%s, owner_actor_ids=%s, parent_goal_id=%s)",
                    goal.id, dp.eb_id if hasattr(dp, 'eb_id') else 'NO_EB_ID',
                    goal.owner_actor_ids, goal.parent_goal_id,
                )
                goal_text = f"Goal: {goal.title}"
                if goal.description:
                    goal_text += f" — {goal.description}"
                if goal.status == GoalStatus.COMPLETED and goal.success_criteria:
                    goal_text += f" [COMPLETED: {', '.join(goal.success_criteria)}]"
                elif goal.status == GoalStatus.ABANDONED:
                    goal_text += " [ABANDONED]"
                elif goal.blockers:
                    goal_text += f" [BLOCKED: {', '.join(goal.blockers)}]"
                elif goal.success_criteria:
                    goal_text += f" criteria: {', '.join(goal.success_criteria)}"
                await cognee.add(goal_text, dataset_name=self._dataset_name)

                # Create CHILD_OF edges for sub-goals
                if goal.parent_goal_id and self._graph:
                    try:
                        self._log.info("add_relation CHILD_OF: source=%s target=%s", str(goal.id), str(goal.parent_goal_id))
                        await self._graph.add_relation(
                            str(goal.id), str(goal.parent_goal_id), "CHILD_OF",
                        )
                    except Exception as exc:
                        self._log.warning(
                            "Failed to create CHILD_OF edge %s → %s: %s",
                            str(goal.id), str(goal.parent_goal_id), exc,
                        )

                # Create OWNS_GOAL edges
                if self._graph:
                    for owner_id in goal.owner_actor_ids:
                        try:
                            self._log.info("add_relation OWNS_GOAL: source=%s target=%s", str(owner_id), str(goal.id))
                            await self._graph.add_relation(
                                str(owner_id), str(goal.id), "OWNS_GOAL",
                            )
                        except Exception as exc:
                            self._log.warning(
                                "Failed to create OWNS_GOAL edge %s → %s: %s",
                                str(owner_id), str(goal.id), exc,
                            )

                count += 1
            except Exception as exc:
                self._log.warning("Failed to flush goal %s: %s", goal.id, exc)

        # Delete Redis key
        try:
            await self._redis.delete(self._key(session_key, session_id))
        except Exception:
            pass

        # Emit trace event
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.SESSION_BOUNDARY,
                session_id=session_id,
                session_key=session_key,
                payload={
                    "action": "goals_flushed",
                    "session_key": session_key,
                    "goals_flushed": count,
                    "goals_total": len(goals),
                    "completed": sum(1 for g in goals if g.status == GoalStatus.COMPLETED),
                    "abandoned": sum(1 for g in goals if g.status == GoalStatus.ABANDONED),
                    "active": sum(1 for g in goals if g.status == GoalStatus.ACTIVE),
                },
            ))

        if self._metrics:
            for _ in range(count):
                self._metrics.inc_session_goals_flushed()
            self._metrics.set_session_goals_count(0)

        self._log.info("Flushed %d/%d session goals for %s", count, len(goals), session_key)
        return count
