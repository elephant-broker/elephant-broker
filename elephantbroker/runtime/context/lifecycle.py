"""ContextLifecycle — central coordinator for the context engine lifecycle (AD-1)."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
import uuid
from datetime import UTC, datetime

from elephantbroker.runtime.compaction.engine import CADENCE_MULTIPLIERS, estimate_tokens
from elephantbroker.runtime.context._utils import STOP_WORDS, _extract_key_phrases
from elephantbroker.runtime.observability import GatewayLoggerAdapter
from elephantbroker.runtime.redis_keys import touch_session_keys
from elephantbroker.schemas.artifact import SessionArtifact
from elephantbroker.schemas.context import (
    AfterTurnParams,
    AgentMessage,
    AssembleParams,
    content_as_text,
    AssembleResult,
    BootstrapParams,
    BootstrapResult,
    CompactionContext,
    CompactParams,
    CompactResult,
    IngestBatchParams,
    IngestBatchResult,
    IngestParams,
    IngestResult,
    SessionCompactState,
    SessionContext,
    SubagentEndedParams,
    SubagentSpawnParams,
    SubagentSpawnResult,
    SystemPromptOverlay,
)
from elephantbroker.schemas.profile import SuccessfulUseThresholds
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

TOOL_ALIASES: dict[str, str] = {
    "psql": "postgresql", "pg_dump": "postgresql", "pg_restore": "postgresql",
    "mysql": "mysql", "mongosh": "mongodb", "redis-cli": "redis",
    "npm": "node", "yarn": "node", "pnpm": "node",
    "pip": "python", "poetry": "python", "pytest": "python",
    "docker": "docker", "kubectl": "kubernetes", "helm": "kubernetes",
    "git": "git", "gh": "github", "curl": "http", "wget": "http",
}

PROGRESS_SIGNALS: dict[str, list[str]] = {
    "completed": [r"(?:done|finished|completed|fixed|resolved|implemented|shipped|merged|deployed)"],
    "blocked": [r"(?:can't|cannot|unable|blocked|waiting|stuck|need\s+\w+\s+(?:to|before))"],
    "progressing": [r"(?:working on|started|making progress|almost|nearly)"],
}


class ContextLifecycle:
    """Central coordinator for the context engine lifecycle."""

    def __init__(
        self,
        working_set_manager=None,
        context_assembler=None,
        compaction_engine=None,
        guard_engine=None,
        memory_store=None,
        turn_ingest=None,
        artifact_ingest=None,
        session_goal_store=None,
        hint_processor=None,
        actor_registry=None,
        profile_registry=None,
        trace_ledger=None,
        llm_client=None,
        redis=None,
        config=None,
        gateway_id: str = "",
        redis_keys=None,
        metrics=None,
        session_context_store=None,
        session_artifact_store=None,
        procedure_engine=None,
        async_analyzer=None,
        successful_use_task=None,
    ) -> None:
        self._wsm = working_set_manager
        self._assembler = context_assembler
        self._compaction = compaction_engine
        self._guard = guard_engine
        self._memory_store = memory_store
        self._turn_ingest = turn_ingest
        self._artifact_ingest = artifact_ingest
        self._session_goal_store = session_goal_store
        self._hint_processor = hint_processor
        self._actor_registry = actor_registry
        self._profile_registry = profile_registry
        self._procedure_engine = procedure_engine
        self._async_analyzer = async_analyzer
        self._successful_use_task = successful_use_task
        self._trace = trace_ledger
        self._llm = llm_client
        self._redis = redis
        self._config = config
        self._gateway_id = gateway_id
        self._keys = redis_keys
        self._metrics = metrics
        self._session_store = session_context_store
        self._artifact_store = session_artifact_store
        self._agent_key = ""
        self._log = GatewayLoggerAdapter(
            logging.getLogger("elephantbroker.runtime.context.lifecycle"),
            {"gateway_id": gateway_id},
        )
        self._ingest_degraded_warned = False
        self._fallback_session_ids: dict[str, str] = {}
        self._bootstrap_session_ids: dict[str, str] = {}  # Keyed by session_key, used as fallback for dispose

    # ------------------------------------------------------------------
    # bootstrap
    # ------------------------------------------------------------------

    def _ensure_session_id(self, sid: str, session_key: str) -> str:
        """Return *sid* if non-empty, otherwise reuse or generate a fallback UUID.

        Fallbacks are cached per session_key so that assemble, after_turn,
        and dispose within the same session window all share the same UUID.
        Cache is bounded to 128 entries to prevent unbounded growth.
        """
        if sid:
            return sid
        if session_key in self._fallback_session_ids:
            return self._fallback_session_ids[session_key]
        # Cap cache size to prevent unbounded growth
        if len(self._fallback_session_ids) >= 128:
            oldest = next(iter(self._fallback_session_ids))
            del self._fallback_session_ids[oldest]
        fallback = str(uuid.uuid4())
        self._fallback_session_ids[session_key] = fallback
        self._log.warning(
            "Empty session_id for %s, generated fallback: %s", session_key, fallback,
        )
        return fallback

    async def _trace_fallback_session_id(self, session_key: str, fallback_id: str) -> None:
        """Emit a DEGRADED_OPERATION trace event when a fallback session_id is generated."""
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.DEGRADED_OPERATION,
                session_key=session_key,
                session_id=fallback_id,
                gateway_id=self._gateway_id,
                payload={
                    "reason": "empty_session_id",
                    "fallback_session_id": fallback_id,
                    "session_key": session_key,
                },
            ))

    async def bootstrap(self, params: BootstrapParams) -> BootstrapResult:
        t0 = time.monotonic()
        original_sid = params.session_id
        params.session_id = self._ensure_session_id(params.session_id, params.session_key)
        if not original_sid:
            await self._trace_fallback_session_id(params.session_key, params.session_id)
        # Only clear fallback cache if a real session_id was provided
        if original_sid:
            self._fallback_session_ids.pop(params.session_key, None)
        profile_name = params.profile_name or "coding"

        # Resolve profile (Phase 8: pass org_id for org-specific overrides)
        gw_config = getattr(self._config, "gateway", None)
        org_id = getattr(gw_config, "org_id", "") or "" if gw_config else ""
        try:
            profile = await self._profile_registry.resolve_profile(profile_name, org_id=org_id or None)
        except (KeyError, Exception):
            profile = await self._profile_registry.resolve_profile("coding", org_id=org_id or None)
            profile_name = "coding"

        # Subagent detection (AD-9): explicit flag + Redis fallback (safety net)
        parent_sk = params.parent_session_key
        if not parent_sk and self._redis and self._keys:
            try:
                parent_sk = await self._redis.get(self._keys.session_parent(params.session_key))
                if parent_sk:
                    self._log.info("Subagent auto-detected via Redis: %s → parent %s",
                                   params.session_key, parent_sk)
            except Exception:
                pass

        # Store session_id for dispose fallback (keyed by session_key for multi-session)
        if len(self._bootstrap_session_ids) >= 128:
            oldest = next(iter(self._bootstrap_session_ids))
            del self._bootstrap_session_ids[oldest]
        self._bootstrap_session_ids[params.session_key] = params.session_id

        # Update agent_key from bootstrap params if provided
        if params.agent_key:
            self._agent_key = params.agent_key
            agent_id = params.agent_key.split(":")[-1] if ":" in params.agent_key else ""
            self._log.info("agent_key assigned: %s", params.agent_key)
            # Auto-enrich all future trace events with agent identity
            if self._trace:
                self._trace.set_agent_identity(params.agent_key, agent_id)

        # GF-15: Reuse existing SessionContext if it survived dispose.
        # TODO(BL-303): This early-return skips guard rule refresh, profile
        # re-resolution, and procedure execution restore. This is mitigated by:
        # (1) guard rules are loaded once per session and persist in Redis,
        # (2) profiles are immutable per session (resolved at first bootstrap),
        # (3) procedure executions are restored from Redis on first access.
        # If per-turn profile/guard refresh is needed, add it here.
        if self._session_store:
            existing_ctx = await self._session_store.get(params.session_key, params.session_id)
            if existing_ctx is not None:
                self._log.info(
                    "Reusing existing SessionContext (turn_count=%d, compact_count=%d)",
                    existing_ctx.turn_count, existing_ctx.compact_count,
                )
                if self._trace:
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.BOOTSTRAP_COMPLETED,
                        session_key=params.session_key,
                        session_id=params.session_id,
                        gateway_id=self._gateway_id,
                        payload={
                            "session_key": params.session_key,
                            "session_id": params.session_id,
                            "profile_name": existing_ctx.profile_name,
                            "is_reuse": True,
                            "turn_count": existing_ctx.turn_count,
                        },
                    ))
                if self._metrics:
                    self._metrics.observe_lifecycle_duration("bootstrap", existing_ctx.profile_name, time.monotonic() - t0)
                return BootstrapResult(bootstrapped=True)

        # Build SessionContext (gw_config already resolved above for profile org_id)
        session_ctx = SessionContext(
            session_key=params.session_key,
            session_id=params.session_id,
            profile_name=profile_name,
            profile=profile,
            gateway_id=params.gateway_id or self._gateway_id,
            agent_key=params.agent_key or self._agent_key,
            org_id=getattr(gw_config, "org_id", "") or "",
            team_ids=[gw_config.team_id] if gw_config and gw_config.team_id else [],
            parent_session_key=parent_sk,
        )

        # Phase 8: Resolve org/team display labels for logging/traces
        graph = getattr(self, "_graph", None) or getattr(self._wsm, "_graph", None)
        if session_ctx.org_id and graph:
            try:
                org_entity = await graph.get_entity(session_ctx.org_id)
                if org_entity:
                    session_ctx.org_label = org_entity.get("display_label", "")
            except Exception:
                pass
        if session_ctx.team_ids and graph:
            try:
                team_entity = await graph.get_entity(session_ctx.team_ids[0])
                if team_entity:
                    session_ctx.team_label = team_entity.get("display_label", "")
            except Exception:
                pass

        # Prior state restoration (AD-12)
        if params.prior_session_id and self._session_store:
            prior_state = await self._session_store.get_compact_state(
                params.session_key, params.prior_session_id,
            )
            if prior_state:
                self._log.info("Restored prior compact state from %s", params.prior_session_id)
                # Carry forward key state from prior session
                session_ctx.fact_last_injection_turn = {}
                session_ctx.goal_inject_history = {}

        # Restore procedure executions from Redis (TD-6)
        if self._procedure_engine and hasattr(self._procedure_engine, "restore_executions"):
            try:
                await self._procedure_engine.restore_executions(params.session_key, params.session_id)
            except Exception as exc:
                self._log.debug("Procedure execution restore skipped: %s", exc)

        # Guard init (Phase 7)
        if self._guard and hasattr(self._guard, "load_session_rules"):
            try:
                active_proc_ids = []
                if self._procedure_engine and hasattr(self._procedure_engine, "get_active_execution_ids"):
                    sid_uuid = uuid.UUID(params.session_id) if isinstance(params.session_id, str) else params.session_id
                    active_proc_ids = await self._procedure_engine.get_active_execution_ids(
                        params.session_key, sid_uuid)
                sid_uuid = uuid.UUID(params.session_id) if isinstance(params.session_id, str) else params.session_id
                agent_id = self._agent_key.split(":")[-1] if self._agent_key and ":" in self._agent_key else ""
                await self._guard.load_session_rules(
                    session_id=sid_uuid,
                    profile_name=profile_name,
                    active_procedure_ids=active_proc_ids or None,
                    session_key=params.session_key,
                    agent_id=agent_id,
                    org_id=org_id,
                )
            except Exception as exc:
                self._log.warning("Guard rule loading failed: %s", exc)

        # Save session context
        if self._session_store:
            await self._session_store.save(session_ctx)

        # Trace
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.BOOTSTRAP_COMPLETED,
                session_key=params.session_key,
                session_id=params.session_id,
                gateway_id=self._gateway_id,
                payload={
                    "session_key": params.session_key,
                    "session_id": params.session_id,
                    "profile_name": profile_name,
                    "is_subagent": params.is_subagent,
                    "parent_session_key": parent_sk,
                },
            ))

        if self._metrics:
            self._metrics.inc_lifecycle_call("bootstrap", profile_name)
            self._metrics.observe_lifecycle_duration("bootstrap", profile_name, time.monotonic() - t0)
            # TD-65 follow-up: session_start metric moved to the HTTP route
            # (sessions.py POST /sessions/start) to match the session_end
            # pairing. Bootstrap is a context-engine event (potentially fires
            # multiple times per session on re-bootstrap after dispose — see
            # TF-FN-011 GF-15 history) so it was a poor proxy for "session
            # started" — observer Layer B/C reverify confirmed the
            # session_start time series was missing in practice.

        return BootstrapResult(bootstrapped=True)

    # ------------------------------------------------------------------
    # ingest (single message — degraded mode AD-29)
    # ------------------------------------------------------------------

    async def ingest(self, params: IngestParams) -> IngestResult:
        if not self._ingest_degraded_warned:
            self._log.warning("ingest() called — degraded mode. Use ingest_batch() instead.")
            self._ingest_degraded_warned = True

        batch_params = IngestBatchParams(
            session_id=params.session_id,
            session_key=params.session_key,
            messages=[params.message],
            is_heartbeat=params.is_heartbeat,
        )
        result = await self.ingest_batch(batch_params)
        return IngestResult(ingested=result.ingested_count > 0)

    # ------------------------------------------------------------------
    # ingest_batch
    # ------------------------------------------------------------------

    async def ingest_batch(self, params: IngestBatchParams, *, _called_from_after_turn: bool = False) -> IngestBatchResult:
        t0 = time.monotonic()
        original_sid = params.session_id
        params.session_id = self._ensure_session_id(params.session_id, params.session_key)
        if not original_sid:
            await self._trace_fallback_session_id(params.session_key, params.session_id)
        else:
            self._fallback_session_ids.pop(params.session_key, None)
        sk, sid = params.session_key, params.session_id

        # Emit INPUT_RECEIVED trace event (skip when called from after_turn to avoid duplicate)
        if self._trace and not _called_from_after_turn:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.INPUT_RECEIVED,
                session_key=sk,
                session_id=sid,
                gateway_id=self._gateway_id,
                payload={"action": "ingest_batch", "message_count": len(params.messages)},
            ))

        # Load or auto-bootstrap session context
        session_ctx = await self._load_session_context(sk, sid)
        if session_ctx is None:
            self._log.warning("No SessionContext — auto-bootstrapping with default profile")
            await self.bootstrap(BootstrapParams(
                session_key=sk, session_id=sid, profile_name=params.profile_name,
            ))
            session_ctx = await self._load_session_context(sk, sid)

        # Annotate messages with turn number
        if session_ctx:
            for msg in params.messages:
                msg.metadata["eb_turn"] = str(session_ctx.turn_count)
            # NOTE: turn_count is incremented ONLY in after_turn(), which is the
            # canonical "turn completed" signal in both live and simulation modes.
            # ingest_batch() annotates messages with the current turn_count but does
            # NOT increment it, avoiding double-increment when both are called in
            # the same turn (e.g., simulation mode). See ISSUE-23, PR #11 R1 TODO-2.

        # Store messages in Redis LIST for compact() to read
        if self._redis and self._keys:
            try:
                key = self._keys.session_messages(sk, sid)
                values = [msg.model_dump_json() for msg in params.messages]
                if values:
                    await self._redis.rpush(key, *values)
                    ttl = self._session_store._effective_ttl(session_ctx.profile) if self._session_store and session_ctx else 86400
                    await self._redis.expire(key, ttl)
            except Exception as exc:
                self._log.warning("Failed to store messages in Redis: %s", exc)

        # Refresh TTL on all session keys (Amendment 6.1)
        if self._redis and self._keys and session_ctx:
            try:
                ttl = (self._session_store._effective_ttl(session_ctx.profile)
                       if self._session_store else 86400)
                is_subagent = bool(session_ctx.parent_session_key)
                touched = await touch_session_keys(
                    self._keys, self._redis, sk, sid, ttl,
                    include_parent=is_subagent,
                )
                self._log.debug("Touched %d session keys (ttl=%d)", touched, ttl)
                if self._metrics:
                    self._metrics.inc_session_ttl_touch()
                    self._metrics.observe_session_ttl_touch_keys(touched)
            except Exception as exc:
                self._log.debug("Failed to touch session keys: %s", exc)

        # Auto-capture tool artifacts (AD-17)
        if self._artifact_store and session_ctx:
            for msg in params.messages:
                if msg.role == "tool" and self._should_capture_artifact(msg):
                    content_hash = hashlib.sha256(content_as_text(msg).encode()).hexdigest()
                    existing = await self._artifact_store.get_by_hash(sk, sid, content_hash)
                    if existing is None:
                        artifact = SessionArtifact(
                            tool_name=msg.name or "unknown",
                            content=content_as_text(msg),
                            summary=content_as_text(msg)[:200],
                            content_hash=content_hash,
                            session_key=sk,
                            session_id=sid,
                            token_estimate=len(content_as_text(msg)) // 4,
                        )
                        await self._artifact_store.store(sk, sid, artifact, profile=session_ctx.profile)

        # Delegate to turn ingest pipeline
        pipeline_result = None
        if self._turn_ingest:
            try:
                pipeline_result = await self._turn_ingest.run(
                    session_key=sk,
                    messages=params.messages,
                    session_id=sid,
                    profile_name=params.profile_name,
                    gateway_id=self._gateway_id,
                    agent_key=session_ctx.agent_key if session_ctx else "",
                )
            except Exception as exc:
                self._log.warning("Turn ingest failed: %s", exc, exc_info=True)

        # Save updated context (skip when called from after_turn to avoid double-save).
        #
        # Contract note (TODO-6-107, Round 1 Business Logic Reviewer, LOW):
        # When `_called_from_after_turn=True`, this code path skips the
        # session save — the caller (`after_turn()`) is responsible for
        # the save at its happy-path exit (currently `lifecycle.py:954`).
        # If `after_turn()` raises between this `ingest_batch()` call and
        # its own save, any session-context mutations made here — notably
        # the auto-bootstrap at line ~378 and `eb_turn` stamping at line
        # ~388 — are lost. Blast radius is small today because the fact-
        # update branch inside `_track_successful_use` (line ~1403) wraps
        # per-item updates in try/except and the auto-compaction precheck
        # at `lifecycle.py:~1030` also catches locally, but the gap is
        # real: scanner init, analyzer spawn, and other between-steps
        # exceptions would propagate past this save. A future tightening
        # would wrap `after_turn()`'s body in try/finally that always
        # saves. For now: any caller that passes `_called_from_after_turn
        # =True` MUST guarantee `self._session_store.save(session_ctx)` on
        # every exit path (happy + exceptional). Today only `after_turn()`
        # exercises this branch.
        if session_ctx and self._session_store and not _called_from_after_turn:
            await self._session_store.save(session_ctx)

        # Trace
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.INGEST_BUFFER_FLUSH,
                session_key=sk,
                session_id=sid,
                gateway_id=self._gateway_id,
                payload={"action": "ingest_batch", "session_key": sk,
                         "message_count": len(params.messages),
                         "facts_stored": pipeline_result.facts_stored if pipeline_result else 0},
            ))

        if self._metrics:
            self._metrics.inc_lifecycle_call("ingest_batch", params.profile_name)
            self._metrics.observe_lifecycle_duration("ingest_batch", params.profile_name, time.monotonic() - t0)

        return IngestBatchResult(
            ingested_count=len(params.messages),
            facts_stored=pipeline_result.facts_stored if pipeline_result else 0,
        )

    # ------------------------------------------------------------------
    # assemble
    # ------------------------------------------------------------------

    async def assemble(self, params: AssembleParams) -> AssembleResult:
        t0 = time.monotonic()
        original_sid = params.session_id
        params.session_id = self._ensure_session_id(params.session_id, params.session_key)
        if not original_sid:
            await self._trace_fallback_session_id(params.session_key, params.session_id)
        else:
            self._fallback_session_ids.pop(params.session_key, None)
        sk, sid = params.session_key, params.session_id

        session_ctx = await self._load_session_context(sk, sid)
        if session_ctx is None:
            self._log.warning("No SessionContext for assemble — auto-bootstrapping")
            await self.bootstrap(BootstrapParams(session_key=sk, session_id=sid, profile_name=params.profile_name))
            session_ctx = await self._load_session_context(sk, sid)

        profile = session_ctx.profile if session_ctx else None
        if profile is None:
            return AssembleResult(messages=list(params.messages), estimated_tokens=0)

        # Resolve effective budget (AD-8)
        openclaw_budget = params.token_budget
        ctx_window = params.context_window_tokens or (session_ctx.context_window_tokens if session_ctx else None)
        effective_budget, budget_source = self._resolve_effective_budget(
            profile, openclaw_budget, ctx_window,
        )
        self._log.info("Budget: profile=%d, openclaw=%s, window=%s → effective=%d (source=%s)",
                        profile.budgets.max_prompt_tokens, openclaw_budget, ctx_window, effective_budget, budget_source)

        # Extract query
        query = params.query
        if not query and params.messages:
            for msg in reversed(params.messages):
                if msg.role == "user":
                    query = content_as_text(msg)[:500]
                    break

        # Guard preflight (Phase 7)
        guard_constraints: list[str] = []
        if self._guard:
            try:
                sid_uuid = uuid.UUID(sid) if isinstance(sid, str) else sid
                guard_result = await self._guard.preflight_check(sid_uuid, params.messages)
                # Handle GuardResult (Phase 7) vs list[str] (backward compat)
                if hasattr(guard_result, 'outcome'):
                    from elephantbroker.schemas.guards import GuardOutcome
                    if guard_result.outcome != GuardOutcome.PASS:
                        constraints_raw = await self._guard.reinject_constraints(sid_uuid)
                        guard_constraints = constraints_raw if isinstance(constraints_raw, list) else []
                    else:
                        force_inject = (session_ctx and session_ctx.profile
                                        and session_ctx.profile.guards.force_system_constraint_injection)
                        if force_inject:
                            constraints_raw = await self._guard.reinject_constraints(sid_uuid)
                            guard_constraints = constraints_raw if isinstance(constraints_raw, list) else []
                elif isinstance(guard_result, list):
                    guard_constraints = guard_result
            except Exception as exc:
                self._log.warning("Guard check failed: %s", exc)

        # Build working set
        snapshot = None
        if self._wsm:
            try:
                snapshot = await self._wsm.build_working_set(
                    session_id=uuid.UUID(sid) if isinstance(sid, str) else sid,
                    session_key=sk,
                    profile_name=session_ctx.profile_name if session_ctx else "coding",
                    query=query,
                    org_id=session_ctx.org_id or None if session_ctx else None,
                    team_ids=session_ctx.team_ids or None if session_ctx else None,
                )
            except Exception as exc:
                self._log.warning("Working set build failed: %s", exc)

        # Load session goals
        session_goals = []
        if self._session_goal_store and session_ctx:
            try:
                session_goals = await self._session_goal_store.get_goals(sk, uuid.UUID(sid))
            except Exception:
                pass

        # Filter goals for injection (smart cadence)
        filtered_goals = self._filter_goals_for_injection(session_goals, session_ctx, profile.assembly_placement) if session_ctx else session_goals

        # Message transformation (AD-4: these belong on ContextLifecycle, NOT ContextAssembler)
        transformed_messages = list(params.messages)
        dedup_count = 0
        if profile.assembly_placement.replace_tool_outputs and self._artifact_store:
            transformed_messages = await self._replace_old_tool_outputs(
                transformed_messages, sk, sid, profile.assembly_placement,
            )
        if snapshot and profile.assembly_placement.conversation_dedup_enabled:
            transformed_messages, dedup_count = self._deduplicate_conversation(
                transformed_messages, snapshot.items, profile.assembly_placement,
            )

        # Assemble — assembler produces system_prompt_addition (Block 1), lifecycle provides messages
        result = AssembleResult(messages=transformed_messages, estimated_tokens=0)
        if snapshot and self._assembler:
            try:
                assembly = await self._assembler.assemble_from_snapshot(
                    snapshot, effective_budget, filtered_goals, profile,
                    guard_constraints=guard_constraints,
                    session_key=sk,
                )
                # Merge: lifecycle owns message transformation (Surface A messages),
                # assembler owns system_prompt_addition (Surface A block 1)
                result = AssembleResult(
                    messages=transformed_messages,
                    estimated_tokens=assembly.estimated_tokens + sum(len(content_as_text(m)) // 4 for m in transformed_messages),
                    system_prompt_addition=assembly.system_prompt_addition,
                )
            except Exception as exc:
                self._log.warning("Assembly failed, returning raw messages: %s", exc)
                result = AssembleResult(
                    messages=transformed_messages,
                    estimated_tokens=sum(len(content_as_text(m)) // 4 for m in transformed_messages),
                )

        # Update fact_last_injection_turn (AD-7)
        if snapshot and session_ctx:
            for item in snapshot.items:
                item_id = str(item.id)
                if item_id not in session_ctx.fact_last_injection_turn:
                    session_ctx.fact_last_injection_turn[item_id] = session_ctx.turn_count

        # Touch last_used_at on injected facts (Phase 9 forward-compat)
        # T-3: source_type is now clean DataPoint-type semantic; retrieval-
        # sourced facts carry source_type="fact" with retrieval_source stamped
        # on the separate field. Simple `== "fact"` check replaces the prior
        # retrieval-path union (tactical frozenset removed under TD-scanner-3;
        # see local/IMPLEMENTED-PR-6-merge.md for the T-3 history).
        if snapshot and self._memory_store:
            now_iso = datetime.now(UTC).isoformat()
            for item in snapshot.items:
                if item.source_type == "fact":
                    try:
                        await self._memory_store.update(item.source_id, {"last_used_at": now_iso})
                    except Exception:
                        pass

        # Increment artifact injected_count
        if snapshot and self._artifact_store:
            for item in snapshot.items:
                if item.source_type == "artifact":
                    try:
                        await self._artifact_store.increment_injected(sk, sid, str(item.source_id))
                    except Exception:
                        pass

        # Cache snapshot to Redis for build_overlay() and after_turn() to read
        if snapshot and self._redis and self._keys:
            try:
                ttl = self._session_store._effective_ttl(session_ctx.profile) if self._session_store and session_ctx else 86400
                await self._redis.setex(
                    self._keys.ws_snapshot(sk, sid), ttl, snapshot.model_dump_json(),
                )
            except Exception as exc:
                self._log.debug("Failed to cache snapshot to Redis: %s", exc)

        # Async injection analysis (AD-24) — non-blocking background task
        if self._async_analyzer and snapshot:
            task = asyncio.create_task(self._async_analyzer.analyze(
                snapshot, params.messages, sk, sid,
            ))
            def _on_analyzer_done(t):
                exc = t.exception() if not t.cancelled() else None
                if exc:
                    self._log.warning("AsyncInjectionAnalyzer task failed: %s", exc)
            task.add_done_callback(_on_analyzer_done)

        # Cache snapshot ID in session context
        if snapshot and session_ctx:
            session_ctx.last_snapshot_id = str(snapshot.snapshot_id)

        # Save context
        if session_ctx and self._session_store:
            await self._session_store.save(session_ctx)

        # Trace
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.CONTEXT_ASSEMBLED,
                session_key=sk,
                session_id=sid,
                gateway_id=self._gateway_id,
                payload={
                    "effective_budget": effective_budget,
                    "budget_source": budget_source,
                    "tokens_used": result.estimated_tokens,
                    "items_count": len(snapshot.items) if snapshot else 0,
                    "goals_injected": len(filtered_goals),
                },
            ))

        if self._metrics:
            self._metrics.inc_lifecycle_call("assemble", session_ctx.profile_name if session_ctx else "coding")
            self._metrics.observe_lifecycle_duration("assemble", session_ctx.profile_name if session_ctx else "coding", time.monotonic() - t0)
            if self._metrics and budget_source:
                self._metrics.observe_budget_resolution(budget_source, effective_budget)

        return result

    # ------------------------------------------------------------------
    # build_overlay
    # ------------------------------------------------------------------

    async def build_overlay(self, sk: str, sid: str) -> SystemPromptOverlay:
        session_ctx = await self._load_session_context(sk, sid)
        if session_ctx is None or self._assembler is None:
            return SystemPromptOverlay()

        # Load cached snapshot from Redis
        snapshot = None
        if self._redis and self._keys:
            try:
                raw = await self._redis.get(self._keys.ws_snapshot(sk, sid))
                if raw:
                    from elephantbroker.schemas.working_set import WorkingSetSnapshot
                    snapshot = WorkingSetSnapshot.model_validate_json(raw)
            except Exception:
                pass

        if snapshot is None:
            return SystemPromptOverlay()

        # Load session goals
        session_goals = []
        if self._session_goal_store:
            try:
                session_goals = await self._session_goal_store.get_goals(sk, uuid.UUID(sid))
            except Exception:
                pass

        constraints = [item for item in snapshot.items if getattr(item, "category", "") == "constraint" and getattr(item, "must_inject", False)]
        block3_text = "\n".join(item.text for item in snapshot.items if item.text)

        try:
            return await self._assembler.build_system_overlay_from_items(
                constraints, session_goals, block3_text, session_ctx.profile,
            )
        except Exception:
            return SystemPromptOverlay()

    # ------------------------------------------------------------------
    # compact
    # ------------------------------------------------------------------

    async def compact(
        self, params: CompactParams, *, _cached_messages: list[AgentMessage] | None = None,
    ) -> CompactResult:
        t0 = time.monotonic()
        original_sid = params.session_id
        params.session_id = self._ensure_session_id(params.session_id, params.session_key)
        if not original_sid:
            await self._trace_fallback_session_id(params.session_key, params.session_id)
        else:
            self._fallback_session_ids.pop(params.session_key, None)
        sk, sid = params.session_key, params.session_id

        session_ctx = await self._load_session_context(sk, sid)
        if session_ctx is None:
            return CompactResult(ok=True, compacted=False, reason="no session context")

        profile = session_ctx.profile

        # Use pre-read messages if available (avoids double Redis read from auto-compaction)
        messages: list[AgentMessage] = []
        if _cached_messages is not None:
            messages = _cached_messages
        elif self._redis and self._keys:
            try:
                raw_list = await self._redis.lrange(self._keys.session_messages(sk, sid), 0, -1)
                messages = [AgentMessage.model_validate_json(r) for r in (raw_list or [])]
            except Exception as exc:
                self._log.warning("Failed to read messages from Redis: %s", exc)

        if not messages:
            return CompactResult(ok=True, compacted=False, reason="no messages to compact")

        # Load session goals
        session_goals = []
        if self._session_goal_store:
            try:
                session_goals = await self._session_goal_store.get_goals(sk, uuid.UUID(sid))
            except Exception:
                pass

        # Build CompactionContext
        context = CompactionContext(
            session_key=sk,
            session_id=sid,
            messages=messages,
            current_goals=session_goals,
            token_budget=params.token_budget or profile.compaction.target_tokens,
            force=params.force,
            current_token_count=params.current_token_count,
            profile=profile,
            trigger_reason=params.trigger_reason,
        )

        # Delegate to compaction engine
        result = CompactResult(ok=True, compacted=False, reason="no compaction engine")
        if self._compaction:
            try:
                result = await self._compaction.compact_with_context(context)
            except Exception as exc:
                self._log.warning("Compaction failed: %s", exc)
                result = CompactResult(ok=False, compacted=False, reason=str(exc))

        # Post-compaction state reset
        if result.compacted and session_ctx:
            session_ctx.fact_last_injection_turn = {}
            session_ctx.goal_inject_history = {}
            session_ctx.compact_count += 1

        # Save context
        if session_ctx and self._session_store:
            await self._session_store.save(session_ctx)

        # Trace
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.COMPACTION_ACTION,
                session_key=sk,
                session_id=sid,
                gateway_id=self._gateway_id,
                payload={
                    "compacted": result.compacted,
                    "reason": result.reason,
                    "force": params.force,
                },
            ))

        if self._metrics:
            self._metrics.inc_lifecycle_call("compact", session_ctx.profile_name if session_ctx else "coding")
            self._metrics.observe_lifecycle_duration("compact", session_ctx.profile_name if session_ctx else "coding", time.monotonic() - t0)

        return result

    # ------------------------------------------------------------------
    # after_turn
    # ------------------------------------------------------------------

    async def after_turn(self, params: AfterTurnParams) -> None:
        t0 = time.monotonic()
        original_sid = params.session_id
        params.session_id = self._ensure_session_id(params.session_id, params.session_key)
        if not original_sid:
            await self._trace_fallback_session_id(params.session_key, params.session_id)
        else:
            self._fallback_session_ids.pop(params.session_key, None)
        sk, sid = params.session_key, params.session_id

        session_ctx = await self._load_session_context(sk, sid)
        if session_ctx is None:
            self._log.info("No session context for after_turn — skipping")
            return

        # --- Fact extraction: afterTurn is the only hook OpenClaw calls in FULL mode ---
        # ingest_batch() is never called directly by OpenClaw when afterTurn is defined,
        # so we delegate here to ensure fact extraction runs on every turn.
        # ingest_batch() does NOT increment turn_count (see comment below).
        if params.messages:
            try:
                await self.ingest_batch(IngestBatchParams(
                    session_key=params.session_key,
                    session_id=params.session_id,
                    messages=params.messages,
                    profile_name=session_ctx.profile_name if session_ctx else "coding",
                ), _called_from_after_turn=True)
            except Exception as exc:
                self._log.warning("after_turn ingest_batch failed (turn_count still incremented): %s", exc)

        # Increment turn_count — after_turn is the canonical "turn completed" signal
        # (OpenClaw calls afterTurn, not ingestBatch, when afterTurn is defined)
        session_ctx.turn_count += 1
        session_ctx.last_turn_at = datetime.now(UTC)

        # Load last snapshot
        snapshot = None
        if session_ctx.last_snapshot_id and self._redis and self._keys:
            try:
                raw = await self._redis.get(self._keys.ws_snapshot(sk, sid))
                if raw:
                    from elephantbroker.schemas.working_set import WorkingSetSnapshot
                    snapshot = WorkingSetSnapshot.model_validate_json(raw)
            except Exception:
                pass

        # Determine response window (P4: hybrid A+C)
        # A: Honor plugin signal when OpenClaw emitted it (hot path, zero cost).
        # C: Fall back to tail-walker when the plugin stayed silent — we scan
        #    backward for the last user-role message and slice after it so that
        #    downstream scanners (successful-use, goal-progress) only see the
        #    response side. Tail-walker is defense-in-depth; OpenClaw should be
        #    emitting the count in steady state.
        if not params.messages:
            response_messages = []
            boundary_source = "empty"
        elif params.pre_prompt_message_count is not None:
            response_messages = params.messages[params.pre_prompt_message_count:]
            boundary_source = "plugin"
        else:
            response_messages = self._extract_response_delta(params.messages)
            boundary_source = "derived"

        # TODO-6-201 / TODO-6-302 (cluster C-boundary-source): surface the
        # branch decision on both the log channel (DEBUG — ephemeral, for
        # operators tailing journalctl during a session) and the metric
        # channel (Prometheus counter — for alertmanager rules). See
        # CONFIGURATION.md §3 "after_turn_completed payload" for the
        # observability semantics; `source="derived"` is operator-
        # actionable (plugin stopped emitting prePromptMessageCount).
        self._log.debug(
            "P4 boundary_source=%s response_delta=%d total=%d",
            boundary_source, len(response_messages), len(params.messages),
        )
        if self._metrics:
            self._metrics.inc_after_turn_boundary_source(boundary_source)

        # Successful-use tracking (AD-7)
        # The cheap heuristic path (S1/S2/Jaccard) always runs when we have
        # a snapshot and response messages.  The expensive LLM-based RT-1 batch
        # evaluation below is separately gated on config.successful_use.enabled.
        #
        # T-2 (2026-04-21): resolve per-profile scanner thresholds once per
        # turn from `session_ctx.profile` (immutable per-session per
        # CLAUDE.md). Per-turn resolve — not stashed on `self` — because a
        # single ContextLifecycle instance serves multiple concurrent
        # session_keys each with its own profile. See M-1/P-1 design.
        _registry = self._profile_registry
        if _registry is not None and session_ctx.profile is not None:
            thresholds = _registry.effective_successful_use_thresholds(session_ctx.profile)
        else:
            thresholds = SuccessfulUseThresholds()
        updated_count = 0
        signals_by_item: dict[str, dict] = {}
        if snapshot and response_messages:
            updated_count, signals_by_item = await self._track_successful_use(
                snapshot, response_messages, session_ctx, thresholds,
            )
            if self._trace and signals_by_item:
                await self._trace.append_event(TraceEvent(
                    event_type=TraceEventType.SUCCESSFUL_USE_TRACKED,
                    session_key=sk,
                    session_id=sid,
                    gateway_id=self._gateway_id,
                    payload={
                        "session_key": sk,
                        "items_tracked": updated_count,
                        "signals_summary": signals_by_item,
                    },
                ))

        # Goal progress regex (AD-26/T1-4)
        if response_messages and self._session_goal_store:
            session_goals = []
            try:
                session_goals = await self._session_goal_store.get_goals(sk, uuid.UUID(sid))
            except Exception:
                pass
            if session_goals:
                self._detect_goal_progress(response_messages, session_goals)

        # Save context
        if self._session_store:
            await self._session_store.save(session_ctx)

        # Emit injection effectiveness metrics (AD-26/T1-1)
        if snapshot and signals_by_item and self._metrics:
            # J-1 + T-2: aligns with the same use_confidence_gate used by
            # the fact-update branch in _track_successful_use so metric
            # counts track actual fact-update behavior per-profile.
            referenced = sum(
                1 for s in signals_by_item.values()
                if s.get("confidence", 0) > thresholds.use_confidence_gate
            )
            ignored = sum(1 for s in signals_by_item.values() if s.get("method") == "ignored")
            for item in snapshot.items:
                item_id = str(item.id)
                sig = signals_by_item.get(item_id, {})
                cat = getattr(item, "category", "general")
                mc = getattr(item, "memory_class", "episodic") if hasattr(item, "memory_class") else "episodic"
                # Option C stamping: `retrieval_source or source_type` preserves
                # the pre-T-3 dashboard cardinality contract. See metrics.py
                # `source_type` label union-semantics comment (at the
                # eb_injection_referenced_total / eb_injection_ignored_total
                # declarations) for the full rationale.
                st = item.retrieval_source or item.source_type
                if sig.get("confidence", 0) > thresholds.use_confidence_gate:
                    self._metrics.inc_injection_referenced(cat, mc, st)
                elif sig.get("method") == "ignored":
                    self._metrics.inc_injection_ignored(cat, mc, st)

        # Phase 9: RT-1 — LLM-based successful-use reasoning (batch trigger)
        if self._successful_use_task and self._config:
            su_cfg = getattr(self._config, "successful_use", None)
            if su_cfg and su_cfg.enabled and snapshot:
                session_ctx.rt1_turn_counter += 1
                from datetime import UTC as _utc
                from datetime import datetime as _dt
                should_fire = session_ctx.rt1_turn_counter >= su_cfg.batch_size
                if not should_fire and session_ctx.rt1_last_batch_at:
                    elapsed = (_dt.now(_utc) - session_ctx.rt1_last_batch_at).total_seconds()
                    should_fire = elapsed >= su_cfg.batch_timeout_seconds
                if should_fire:
                    session_ctx.rt1_turn_counter = 0
                    session_ctx.rt1_last_batch_at = _dt.now(_utc)
                    import asyncio
                    asyncio.create_task(self._successful_use_task.evaluate_batch(
                        injected_facts=list(getattr(snapshot, "items", [])),
                        turn_messages=[],
                        session_goals=[],
                        gateway_id=self._gateway_id,
                    ))

        # Auto-trigger compaction check
        if self._compaction and self._redis and self._keys:
            try:
                raw_list = await self._redis.lrange(self._keys.session_messages(sk, sid), 0, -1)
                if raw_list:
                    msgs = [AgentMessage.model_validate_json(r) for r in raw_list]
                    total_tokens = sum(estimate_tokens(content_as_text(m)) for m in msgs)
                    policy = session_ctx.profile.compaction
                    cadence = policy.cadence if policy.cadence in CADENCE_MULTIPLIERS else "balanced"
                    threshold = policy.target_tokens * CADENCE_MULTIPLIERS[cadence]
                    if total_tokens > threshold:
                        self._log.info(
                            "Auto-compaction triggered in after_turn: tokens=%d, threshold=%d",
                            total_tokens, int(threshold),
                        )
                        result = await self.compact(
                            CompactParams(
                                session_key=sk,
                                session_id=sid,
                                force=False,
                                current_token_count=total_tokens,
                                trigger_reason="auto",
                            ),
                            _cached_messages=msgs,
                        )
                        if not result.ok:
                            self._log.warning("Auto-compaction failed: %s", result.reason)
                            if self._metrics:
                                self._metrics.inc_lifecycle_error("compact", "auto_compaction")
            except Exception as exc:
                self._log.warning("Auto-compaction check failed: %s", exc)
                if self._metrics:
                    self._metrics.inc_lifecycle_error("compact", "auto_compaction_precheck")

        # Trace
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.AFTER_TURN_COMPLETED,
                session_key=sk,
                session_id=sid,
                gateway_id=self._gateway_id,
                payload={
                    "turn_count": session_ctx.turn_count,
                    "updated_count": updated_count,
                    "response_messages": len(response_messages),
                    "total_messages": len(params.messages),
                    "boundary_source": boundary_source,
                    "snapshot_available": snapshot is not None,
                    "signals_summary": {
                        k: v.get("method", "none") for k, v in signals_by_item.items()
                    } if signals_by_item else {},
                },
            ))

        if self._metrics:
            self._metrics.inc_lifecycle_call("after_turn", session_ctx.profile_name)
            self._metrics.observe_lifecycle_duration("after_turn", session_ctx.profile_name, time.monotonic() - t0)

    # ------------------------------------------------------------------
    # prepare_subagent_spawn
    # ------------------------------------------------------------------

    async def prepare_subagent_spawn(self, params: SubagentSpawnParams) -> SubagentSpawnResult:
        parent_sk = params.parent_session_key
        child_sk = params.child_session_key
        rollback_key = ""

        if self._redis and self._keys:
            try:
                ttl = params.ttl_ms // 1000 if params.ttl_ms else 86400
                key = self._keys.session_parent(child_sk)

                # Safety check: warn if child already has a different parent
                existing = await self._redis.get(key)
                if existing and existing != parent_sk:
                    self._log.warning("Child %s already has parent %s, overwriting with %s",
                                       child_sk, existing, parent_sk)

                await self._redis.setex(key, ttl, parent_sk)
                rollback_key = key

                # Reverse lookup
                children_key = self._keys.session_children(parent_sk)
                await self._redis.sadd(children_key, child_sk)
                await self._redis.expire(children_key, ttl)
            except Exception as exc:
                self._log.warning("Subagent parent mapping failed: %s", exc)
                return SubagentSpawnResult(
                    parent_session_key=parent_sk, child_session_key=child_sk,
                    parent_mapping_stored=False,
                )

        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.SUBAGENT_PARENT_MAPPED,
                session_key=child_sk,
                gateway_id=self._gateway_id,
                payload={"parent_session_key": parent_sk, "child_session_key": child_sk},
            ))

        if self._metrics:
            self._metrics.inc_subagent_spawn()

        return SubagentSpawnResult(
            parent_session_key=parent_sk,
            child_session_key=child_sk,
            rollback_key=rollback_key,
            parent_mapping_stored=True,
        )

    # ------------------------------------------------------------------
    # on_subagent_ended
    # ------------------------------------------------------------------

    async def on_subagent_ended(self, params: SubagentEndedParams) -> None:
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.SUBAGENT_ENDED,
                session_key=params.child_session_key,
                gateway_id=self._gateway_id,
                payload={"reason": params.reason, "child_session_key": params.child_session_key},
            ))

    # ------------------------------------------------------------------
    # dispose
    # ------------------------------------------------------------------

    async def refresh_guard_rules(self, sk: str, sid: str, profile_name: str = "coding") -> None:
        """Reload guard rules after procedure activate/complete/abandon. Called by API routes."""
        sid = self._ensure_session_id(sid, sk)
        if not self._guard or not hasattr(self._guard, "load_session_rules"):
            return
        try:
            sid_uuid = uuid.UUID(sid) if isinstance(sid, str) else sid
            active_proc_ids = []
            if self._procedure_engine and hasattr(self._procedure_engine, "get_active_execution_ids"):
                active_proc_ids = await self._procedure_engine.get_active_execution_ids(sk, sid_uuid)
            agent_id = self._agent_key.split(":")[-1] if self._agent_key and ":" in self._agent_key else ""
            gw_cfg = getattr(self._config, "gateway", None)
            lc_org_id = getattr(gw_cfg, "org_id", "") or "" if gw_cfg else ""
            await self._guard.load_session_rules(
                session_id=sid_uuid,
                profile_name=profile_name,
                active_procedure_ids=active_proc_ids or None,
                session_key=sk,
                agent_id=agent_id,
                org_id=lc_org_id,
            )
            self._log.info("Guard rules refreshed for session %s (procedures=%d)", sid, len(active_proc_ids))
        except Exception as exc:
            self._log.warning("Guard rule refresh failed: %s", exc)

    async def dispose(self, sk: str, sid: str) -> None:
        """Engine teardown — safe to call on every turn (GF-15).

        On the Python side this only cleans up the fallback session_id cache
        and emits a SESSION_BOUNDARY trace event (action=engine_teardown).
        The TS plugin clears its per-turn in-memory buffers separately.
        Does NOT delete session state from Redis, flush goals, or unload guards.
        For actual session cleanup, use session_end().
        """
        sid = self._ensure_session_id(sid, sk)
        self._fallback_session_ids.pop(sk, None)

        # Intentionally emitted per-turn (GF-15): OpenClaw calls dispose() after
        # every run, so this traces each engine teardown for diagnostics.
        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.SESSION_BOUNDARY,
                session_key=sk,
                session_id=sid,
                gateway_id=self._gateway_id,
                payload={"event": "engine_teardown", "session_key": sk, "session_id": sid},
            ))

    async def session_end(
        self,
        sk: str,
        sid: str,
        *,
        agent_id: str | None = None,
        agent_key: str | None = None,
    ) -> dict:
        """Actual session cleanup — called on real session end only (GF-15).

        Flushes goals to Cognee, unloads guards, deletes SessionContext from Redis.
        Does NOT delete session artifacts (TTL-based expiry for Phase 9).

        The ``agent_id`` and ``agent_key`` kwargs are plumbed through from the route
        layer so the lifecycle's SESSION_BOUNDARY emission carries the same top-level
        identity fields as the route's own emission (TD-65 observer-reverify
        follow-ups). When omitted, falls back to empty string on the trace event
        (not ``None``) to keep the field type stable. The container's
        ``self._agent_key`` is not used here because lifecycle is a
        gateway-wide singleton and has no per-request identity — the route
        (which has request.state) is the authoritative source.
        """
        if not sid and sk in self._bootstrap_session_ids:
            sid = self._bootstrap_session_ids.pop(sk)
            self._log.info("session_end using stored bootstrap session_id for %s: %s", sk, sid)
        else:
            sid = self._ensure_session_id(sid, sk)
            self._bootstrap_session_ids.pop(sk, None)  # clean up stored entry
        self._fallback_session_ids.pop(sk, None)

        # Flush session goals to Cognee before cleanup
        goals_flushed = 0
        if self._session_goal_store:
            try:
                sid_uuid = uuid.UUID(sid) if isinstance(sid, str) else sid
                goals_flushed = await self._session_goal_store.flush_to_cognee(sk, sid_uuid, agent_key=self._agent_key)
                self._log.info("Flushed %d session goals for %s/%s", goals_flushed, sk, sid)
            except Exception as exc:
                self._log.warning("Failed to flush session goals on session_end: %s", exc)

        # Phase 7: Unload guard session state
        if self._guard and hasattr(self._guard, "unload_session"):
            try:
                sid_uuid = uuid.UUID(sid) if isinstance(sid, str) else sid
                await self._guard.unload_session(sid_uuid)
            except Exception as exc:
                self._log.warning("Guard unload failed: %s", exc)

        if self._session_store:
            await self._session_store.delete(sk, sid)

        if self._trace:
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.SESSION_BOUNDARY,
                session_key=sk,
                session_id=sid,
                gateway_id=self._gateway_id,
                agent_id=agent_id or "",
                agent_key=agent_key or "",
                payload={"event": "lifecycle_session_end", "session_key": sk, "session_id": sid,
                         "goals_flushed": goals_flushed},
            ))

        if self._metrics:
            self._metrics.inc_session_boundary("session_end")

        return {"goals_flushed": goals_flushed}

    # ==================================================================
    # Helper methods
    # ==================================================================

    async def _load_session_context(self, sk: str, sid: str) -> SessionContext | None:
        if self._session_store:
            return await self._session_store.get(sk, sid)
        return None

    async def _save_session_context(self, ctx: SessionContext) -> None:
        if self._session_store:
            await self._session_store.save(ctx)

    def _resolve_effective_budget(self, profile, openclaw_budget, context_window_tokens) -> tuple[int, str]:
        config = self._config
        sources: list[tuple[int, str]] = [
            (profile.budgets.max_prompt_tokens, "profile"),
        ]
        if openclaw_budget and openclaw_budget > 0:
            sources.append((openclaw_budget, "openclaw"))
        if context_window_tokens:
            ca = getattr(config, "context_assembly", None)
            frac = ca.max_context_window_fraction if ca else 0.15
            sources.append((int(context_window_tokens * frac), "window"))
        elif config and getattr(config, "context_assembly", None) and config.context_assembly.enable_dynamic_budget:
            sources.append((int(config.context_assembly.fallback_context_window * config.context_assembly.max_context_window_fraction), "window"))
        budget, source = min(sources, key=lambda x: x[0])
        return budget, source

    def _should_capture_artifact(self, msg: AgentMessage) -> bool:
        """Check if a tool message should be auto-captured as session artifact."""
        config = self._config
        if config and hasattr(config, "artifact_capture"):
            ac = config.artifact_capture
            if not ac.enabled:
                return False
            if len(content_as_text(msg)) < ac.min_content_chars:
                return False
            if len(content_as_text(msg)) > ac.max_content_chars:
                return False
            if msg.name and msg.name in ac.skip_tools:
                return False
        else:
            if len(content_as_text(msg)) < 200:
                return False
        return True

    async def _replace_old_tool_outputs(self, messages: list[AgentMessage], sk: str, sid: str, policy) -> list[AgentMessage]:
        """Replace consumed tool outputs with artifact placeholders. Keep recent ones intact."""
        if not policy.replace_tool_outputs:
            return messages
        tool_indices = [i for i, m in enumerate(messages) if m.role == "tool"]
        keep_count = policy.keep_last_n_tool_outputs if policy.keep_last_n_tool_outputs > 0 else len(tool_indices)
        keep_indices = set(tool_indices[-keep_count:]) if keep_count < len(tool_indices) else set(tool_indices)

        result: list[AgentMessage] = []
        for i, msg in enumerate(messages):
            if (msg.role == "tool"
                    and i not in keep_indices
                    and len(content_as_text(msg)) // 4 > policy.replace_tool_output_min_tokens):
                content_hash = hashlib.sha256(content_as_text(msg).encode()).hexdigest()
                artifact = await self._artifact_store.get_by_hash(sk, sid, content_hash)
                if artifact:
                    tool_name = msg.name or "tool"
                    replacement = AgentMessage(
                        role="tool",
                        content=(
                            f'[Captured output: {tool_name} — {artifact.summary}\n'
                            f' → artifact_search("{artifact.artifact_id}") for full output]'
                        ),
                        name=msg.name,
                        metadata={**msg.metadata, "eb_replaced": "true",
                                  "eb_artifact_id": str(artifact.artifact_id)},
                    )
                    result.append(replacement)
                    if self._metrics:
                        self._metrics.inc_tool_replacement(tool_name)
                    continue
            result.append(msg)
        return result

    def _deduplicate_conversation(self, messages: list[AgentMessage], block3_items, policy) -> tuple[list[AgentMessage], int]:
        """Remove tool messages whose content is already covered by Block 3 items."""
        if not policy.conversation_dedup_enabled or not block3_items:
            return messages, 0

        block3_tokens_set = set()
        for item in block3_items:
            for word in item.text.lower().split():
                if word not in STOP_WORDS and len(word) > 2:
                    block3_tokens_set.add(word)

        if not block3_tokens_set:
            return messages, 0

        result: list[AgentMessage] = []
        removed = 0
        for msg in messages:
            if msg.role == "tool" and content_as_text(msg) and not msg.metadata.get("eb_replaced"):
                msg_tokens = {w.lower() for w in content_as_text(msg).split() if w.lower() not in STOP_WORDS and len(w) > 2}
                if msg_tokens:
                    overlap = len(msg_tokens & block3_tokens_set) / len(msg_tokens)
                    if overlap > policy.conversation_dedup_threshold:
                        removed += 1
                        continue
            result.append(msg)
        return result, removed

    def _filter_goals_for_injection(self, goals, session_ctx, placement) -> list:
        """Smart cadence filtering for goal injection."""
        if not goals:
            return []
        if placement.goal_injection_cadence == "always":
            return list(goals)

        filtered = []
        for goal in goals:
            goal_id = str(goal.id)
            history = session_ctx.goal_inject_history.get(goal_id, {})
            last_turn = history.get("turn", -1)
            last_status = history.get("status", "")
            turn_count = session_ctx.turn_count

            # Always inject if: first turn, has blockers, status changed, or reminder interval
            should_inject = (
                last_turn < 0
                or (hasattr(goal, "blockers") and goal.blockers)
                or (hasattr(goal, "status") and str(goal.status) != last_status)
                or (turn_count - last_turn >= placement.goal_reminder_interval)
            )

            if should_inject:
                filtered.append(goal)
                session_ctx.goal_inject_history[goal_id] = {
                    "turn": turn_count,
                    "status": str(getattr(goal, "status", "")),
                }

        return filtered

    def _extract_response_delta(self, messages: list[AgentMessage]) -> list[AgentMessage]:
        """P4 fallback: slice after the last user-role message.

        Walks backward from the end to find the most recent user turn, then
        returns everything after it (the model's response + any tool traffic).

        No-user-role fallback (TODO-6-105 / TODO-6-306,
        cluster C-response-delta-no-user): if the envelope contains no
        ``role=="user"`` message — e.g. a heartbeat/sweep turn, a subagent
        delta, a malformed buffer replay, or a future OpenClaw shape that
        omits user echoes — the whole list is returned as response delta.
        Scanner blast radius is bounded (S1/S3 filter to
        ``role=="assistant"`` downstream) but the semantic inversion is
        quiet, so this branch additionally emits a WARN log and increments
        ``eb_response_delta_no_user_total{gateway_id}`` for operator
        visibility (trace: journalctl, alertmanager: ``rate(...) > 0``).
        """
        for idx in range(len(messages) - 1, -1, -1):
            if messages[idx].role == "user":
                return messages[idx + 1:]
        # No user-role message found — return the full list as a defensive
        # fallback so downstream scanners still have something to iterate,
        # but surface the incident on both observability channels.
        self._log.warning(
            "P4 _extract_response_delta: no user-role message in %d-message "
            "envelope — treating entire list as response delta (potential "
            "scanner over-attribution)",
            len(messages),
        )
        if self._metrics:
            self._metrics.inc_response_delta_no_user_boundary()
        return list(messages)

    async def _track_successful_use(
        self,
        snapshot,
        response_messages,
        session_ctx,
        thresholds: SuccessfulUseThresholds | None = None,
    ) -> tuple[int, dict]:
        """Multi-signal successful-use tracking: S1+S2+S6+Jaccard.

        T-2 (2026-04-21): ``thresholds`` carries the per-profile scanner
        thresholds (S1/S2/S3/use-confidence gate + S6 floor). Falls back
        to ``SuccessfulUseThresholds()`` (module defaults = J-1 baseline
        0.15/0.3/0.15/0.15/3) when caller didn't resolve them — keeps
        direct-call unit tests backward-compatible.
        """
        t = thresholds or SuccessfulUseThresholds()
        updated = 0
        signals_by_item: dict[str, dict] = {}

        for item in snapshot.items:
            item_id = str(item.id)
            injection_turn = session_ctx.fact_last_injection_turn.get(item_id, 0)
            turns_since = session_ctx.turn_count - injection_turn
            signals: list[tuple[str, float]] = []

            # S1: Direct quote detection
            is_quote, quote_conf = self._detect_direct_quote(
                item, response_messages, injection_turn, t,
            )
            if is_quote:
                signals.append(("direct_quote", quote_conf))

            # S2: Tool correlation
            is_tool, tool_conf = self._detect_tool_correlation(item, response_messages, t)
            if is_tool:
                signals.append(("tool_correlation", tool_conf))

            # Running Jaccard
            # J-1: threshold calibrated from 0.3 → 0.15 (H-alt-2). Jaccard at
            # 0.3 required near-complete token overlap to register; 0.15 captures
            # paraphrased references while still rejecting coincidental matches.
            # T-2: threshold now reads from resolved profile thresholds.
            jaccard_score = self._compute_running_jaccard(item, response_messages, injection_turn)
            if jaccard_score > t.s3_jaccard_score:
                signals.append(("jaccard", jaccard_score))
                if self._metrics:
                    self._metrics.observe_successful_use_jaccard(jaccard_score)

            # Determine confidence
            if signals:
                use_confidence = max(s[1] for s in signals)
                method = max(signals, key=lambda s: s[1])[0]
            else:
                use_confidence = 0.0
                method = "ignored"

            signal_entry: dict = {"confidence": use_confidence, "method": method}
            # S6: Track ignored_turns for Phase 9 weight tuning
            if method == "ignored" and turns_since >= t.s6_ignored_turns_floor:
                signal_entry["ignored_turns"] = turns_since

            signals_by_item[item_id] = signal_entry

            # Update fact
            # T-3: source_type is now the DataPoint-type semantic; the gate
            # fires for fact-class items only. retrieval_source exposes the
            # retrieval path (structural/keyword/vector/graph) for fact items,
            # None for non-retrieval items.
            # J-1 heritage: use_confidence gate 0.3 → 0.15 (H-alt-2/H-alt-4).
            # T-2: gate + S1/S3 thresholds resolved per-profile via the
            # SuccessfulUseThresholds resolver.
            #
            # TODO-6-102 (Business Logic Reviewer, MEDIUM) — empirical
            # calibration note: S1 direct-quote ratio, S3 Jaccard score,
            # and use_confidence_gate all default to 0.15. DIAG-I1 + K-2
            # live-fire probes observed realistic agent paraphrases clearing
            # the scanner with confidences in the 0.14-0.18 range (K-2
            # TimescaleDB case fired at confidence=0.190). The 0.15 gate
            # sits at the LOWER edge of that band — intentional to catch
            # paraphrases near the median. Known tradeoff: signals in the
            # 0.14-0.15 band fall below the gate and update `use_count`
            # silently (NOT `successful_use_count`), so Phase-9 strengthening
            # input under-counts the low-signal tail. Raising the default
            # (e.g., to 0.17) would block legitimate paraphrases near the
            # empirical median. Per-profile tuning via
            # `ProfilePolicy.successful_use_thresholds` is the right
            # precision/recall knob for operators with use-case-specific
            # telemetry; global default left at 0.15 (matches operator's
            # Option C "reset-all-presets-to-defaults" landing — see
            # commit 252c7d3). The paraphrase-fragility class is addressed
            # long-term by TD-scanner-4 (embedding-based S4 scanner), not
            # by a threshold bump here.
            if self._memory_store and item.source_type == "fact":
                now_iso = datetime.now(UTC).isoformat()
                try:
                    if use_confidence > t.use_confidence_gate:
                        new_suc = (item.successful_use_count or 0) + 1
                        new_use = (item.use_count or 0) + 1
                        await self._memory_store.update(item.source_id, {
                            "successful_use_count": new_suc,
                            "use_count": new_use,
                            "last_used_at": now_iso,
                        })
                        updated += 1
                        if self._metrics:
                            self._metrics.inc_successful_use_update(method)
                    else:
                        await self._memory_store.update(item.source_id, {
                            "use_count": (item.use_count or 0) + 1,
                            "last_used_at": now_iso,
                        })
                except Exception:
                    pass

        return updated, signals_by_item

    def _detect_direct_quote(
        self,
        item,
        messages,
        injection_turn,
        thresholds: SuccessfulUseThresholds | None = None,
    ) -> tuple[bool, float]:
        """S1: Check if item's key phrases appear in post-injection assistant messages.

        T-2 (2026-04-21): ``thresholds`` carries the per-profile S1 ratio
        threshold. Defaults to ``SuccessfulUseThresholds()`` (0.15) when
        callers don't resolve — preserves direct-call unit-test ergonomics.
        """
        t = thresholds or SuccessfulUseThresholds()
        phrases = _extract_key_phrases(item.text)
        if not phrases:
            return False, 0.0

        # Only search assistant messages AFTER the fact's injection turn
        post_injection = [
            m for m in messages
            if m.role == "assistant" and int(m.metadata.get("eb_turn", "0")) >= injection_turn
        ]
        if not post_injection:
            return False, 0.0

        combined = " ".join(content_as_text(m).lower() for m in post_injection)
        matches = sum(1 for p in phrases if p in combined)
        ratio = matches / len(phrases) if phrases else 0.0
        # J-1: S1 direct-quote threshold calibrated from 0.4 → 0.15 (H-alt-2).
        # At 0.4 the scanner required ~40% of a fact's extracted key-phrases to appear
        # verbatim in the response to register — too strict given that agents often
        # quote a single canonical token (e.g., TimescaleDB) rather than a phrase set.
        # 0.15 registers a single-phrase hit on a ~7-phrase fact while still requiring
        # at least one real match. T-2: per-profile resolvable via
        # ``ProfilePolicy.successful_use_thresholds`` (defaults to 0.15 across all
        # presets after the Option C reset).
        return ratio > t.s1_direct_quote_ratio, min(ratio, 1.0)

    def _detect_tool_correlation(
        self,
        item,
        messages,
        thresholds: SuccessfulUseThresholds | None = None,
    ) -> tuple[bool, float]:
        """S2: Check keyword overlap with tool message args, with alias expansion.

        T-2 (2026-04-21): ``thresholds`` carries the per-profile S2 overlap
        threshold. Defaults to ``SuccessfulUseThresholds()`` (0.3) when
        callers don't resolve — preserves direct-call unit-test ergonomics.
        """
        t = thresholds or SuccessfulUseThresholds()
        item_words = {w.lower() for w in item.text.split() if w.lower() not in STOP_WORDS and len(w) > 2}
        expanded = set(item_words)
        for word in item_words:
            alias = TOOL_ALIASES.get(word)
            if alias:
                expanded.add(alias)

        tool_messages = [m for m in messages if m.role == "tool"]
        if not tool_messages:
            return False, 0.0

        tool_words = set()
        for m in tool_messages:
            tool_words.update(w.lower() for w in content_as_text(m).split() if len(w) > 2)
            if m.name:
                tool_words.add(m.name.lower())
                alias = TOOL_ALIASES.get(m.name.lower())
                if alias:
                    tool_words.add(alias)

        if not expanded or not tool_words:
            return False, 0.0

        overlap = len(expanded & tool_words) / len(expanded) if expanded else 0.0
        return overlap > t.s2_tool_correlation_overlap, min(overlap, 1.0)

    def _compute_running_jaccard(self, item, messages, injection_turn) -> float:
        """Max Jaccard score across post-injection assistant messages."""
        item_tokens = {w.lower() for w in item.text.split() if w.lower() not in STOP_WORDS and len(w) > 2}
        if not item_tokens:
            return 0.0

        max_score = 0.0
        for msg in messages:
            if msg.role != "assistant":
                continue
            # Only consider messages at or after injection turn
            msg_turn = int(msg.metadata.get("eb_turn", "0"))
            if msg_turn < injection_turn:
                continue
            msg_tokens = {w.lower() for w in content_as_text(msg).split() if w.lower() not in STOP_WORDS and len(w) > 2}
            if not msg_tokens:
                continue
            intersection = item_tokens & msg_tokens
            union = item_tokens | msg_tokens
            score = len(intersection) / len(union) if union else 0.0
            max_score = max(max_score, score)

        return max_score

    def _detect_goal_progress(self, messages, session_goals) -> None:
        """Detect goal progress signals from response messages.

        Amendment 7.2: Currently detection-only (logs + metrics).
        Automatic goal status updates deferred to Phase 10 (requires
        confidence scoring and false-positive mitigation).
        """
        if not messages:
            return

        last_assistant = None
        for msg in reversed(messages):
            if msg.role == "assistant":
                last_assistant = msg
                break

        if not last_assistant:
            return

        content = content_as_text(last_assistant).lower()
        detected = {}
        for status, patterns in PROGRESS_SIGNALS.items():
            for pattern in patterns:
                if re.search(pattern, content):
                    detected[status] = True
                    self._log.debug("Goal progress signal: %s", status)
                    break

        if detected and self._metrics:
            for status in detected:
                self._metrics.inc_lifecycle_call("goal_progress_detected", status)
