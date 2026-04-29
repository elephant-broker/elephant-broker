"""Turn ingest pipeline -- extracts facts from conversation turns."""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

import cognee

from elephantbroker.runtime.adapters.cognee.tasks.classify_memory import classify_memory
from elephantbroker.runtime.adapters.cognee.tasks.extract_facts import extract_facts
from elephantbroker.runtime.adapters.cognee.tasks.resolve_actors import resolve_actors
from elephantbroker.runtime.memory.facade import DedupSkipped
from elephantbroker.runtime.metrics import inc_cognify, inc_pipeline
from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.context import AgentMessage
from elephantbroker.schemas.fact import FactAssertion
from elephantbroker.schemas.pipeline import TurnIngestResult
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.pipelines.turn_ingest")


# TODO-8-R1-010 — dual-metric pattern acknowledgment.
#
# The metric call sites in this file follow a dual pattern:
#
#     if self._metrics:
#         self._metrics.inc_pipeline("turn_ingest", "success")  # MetricsContext (gateway-aware)
#     else:
#         inc_pipeline("turn_ingest", "success")                # free function (gateway_id="")
#
# In production, ``self._metrics`` is ALWAYS set (the container wires
# ``c.metrics_ctx`` into every pipeline at construction time — see
# ``container.py: from_config``). The free-function fallback is reachable
# only by unit tests that intentionally pass ``metrics=None`` to exercise
# error paths without the MetricsContext wrapper. If the fallback ever
# fired in production it would emit ``gateway_id=""`` on the Prometheus
# label, which would be a tenant-isolation regression — but it cannot
# fire in production because there is no construction path that yields
# ``self._metrics is None``.
#
# A future cleanup may replace the fallback with a ``NullMetrics()``
# object so the production path is unconditional and the test path can
# inject a no-op. That refactor is intentionally NOT taken in this PR
# because it would invalidate every pipeline test that constructs the
# pipeline with ``metrics=None`` (~10 sites in test_artifact_ingest.py,
# test_procedure_ingest.py, test_turn_ingest.py). Tracked as
# follow-up architectural debt.


def _to_dict(msg: AgentMessage | dict) -> dict:
    """Normalize pipeline input. ``AgentMessage.model_dump(mode='json')`` preserves
    extra fields (e.g. ``actor_id``) and stringifies any typed UUID fields, keeping
    the ``uuid.UUID(msg['actor_id'])`` precondition downstream intact (TD-28)."""
    return msg if isinstance(msg, dict) else msg.model_dump(mode="json")


class TurnIngestPipeline:
    """Orchestrates fact extraction, classification, embedding, and storage for a turn."""

    def __init__(
        self, memory_facade, actor_registry, embedding_service, llm_client,
        trace_ledger, config, profile_registry=None, profile_policy=None, buffer=None,
        graph=None, session_goal_store=None, hint_processor=None, goal_manager=None,
        goal_injection_config=None, gateway_id: str = "", metrics=None,
        org_id: str = "", dataset_name: str = "elephantbroker",
    ):
        self._facade = memory_facade
        self._actors = actor_registry
        self._embeddings = embedding_service
        self._llm = llm_client
        self._trace = trace_ledger
        self._config = config
        self._profile_registry = profile_registry
        self._profile = profile_policy
        self._buffer = buffer
        self._graph = graph
        self._session_goal_store = session_goal_store
        self._hint_processor = hint_processor
        self._goal_manager = goal_manager
        self._goal_injection_config = goal_injection_config
        self._gateway_id = gateway_id
        self._metrics = metrics
        self._org_id = org_id
        self._dataset_name = dataset_name

    async def _resolve_profile(self, profile_name: str):
        """Resolve profile policy — use injected one, or look up from registry."""
        if self._profile is not None:
            return self._profile
        if self._profile_registry is not None:
            try:
                return await self._profile_registry.resolve_profile(profile_name, org_id=self._org_id or None)
            except Exception:
                pass
        return None

    @traced
    async def run(
        self, session_key: str, messages: list[AgentMessage | dict], session_id=None,
        profile_name: str = "coding", goal_ids=None,
        gateway_id: str | None = None, agent_key: str | None = None,
    ) -> TurnIngestResult:
        """Run the full turn ingest pipeline."""
        try:
            messages = [_to_dict(m) for m in messages]
            gw = gateway_id or self._gateway_id

            if not messages:
                if self._trace:
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.FACT_EXTRACTED,
                        session_id=session_id,
                        session_key=session_key,
                        gateway_id=gw,
                        payload={"facts_count": 0, "profile_name": profile_name,
                                 "session_key": session_key, "reason": "empty_messages"},
                    ))
                return TurnIngestResult()

            # Resolve profile for autorecall settings
            profile = await self._resolve_profile(profile_name)
            autorecall = profile.autorecall if profile else None

            # 1. Resolve actors (query registry for known actors)
            known_actors: list = []
            if self._actors:
                try:
                    known_actors = await self._actors.get_relationships(session_key)
                except Exception:
                    known_actors = []
            try:
                resolved = await resolve_actors(messages, known_actors)
            except Exception:
                resolved = []

            # Determine source_actor_id from user-role messages
            source_actor_id = None
            for msg in messages:
                if msg.get("role") == "user" and msg.get("actor_id"):
                    source_actor_id = uuid.UUID(msg["actor_id"])
                    break

            # 2. Load recent facts from buffer
            recent_facts: list[dict] = []
            if self._buffer:
                recent_facts = await self._buffer.load_recent_facts(session_key)

            # 2b. Load session goals from Redis (if available)
            session_goals_list = []  # list[GoalState]
            active_session_goal_dicts: list[dict] | None = None
            persistent_goal_dicts: list[dict] | None = None

            if self._session_goal_store and session_id:
                try:
                    session_goals_list = await self._session_goal_store.get_goals(
                        session_key, uuid.UUID(session_id) if isinstance(session_id, str) else session_id,
                    )
                    if session_goals_list:
                        active_session_goal_dicts = [
                            {"title": g.title, "id": str(g.id)} for g in session_goals_list
                        ]
                except Exception as exc:
                    logger.debug("Failed to load session goals: %s", exc)

            # Load persistent (global/org-scope) goals from GoalManager (read-only context)
            if self._goal_manager and session_id:
                try:
                    sid = uuid.UUID(session_id) if isinstance(session_id, str) else session_id
                    persistent = await self._goal_manager.resolve_active_goals(sid)
                    # Filter to non-session scopes for persistent context
                    from elephantbroker.schemas.base import Scope
                    persistent_filtered = [
                        g for g in persistent
                        if g.scope in (Scope.GLOBAL, Scope.ORGANIZATION, Scope.TEAM, Scope.ACTOR)
                    ]
                    if persistent_filtered:
                        persistent_goal_dicts = [
                            {"title": g.title, "id": str(g.id)} for g in persistent_filtered
                        ]
                except Exception as exc:
                    logger.debug("Failed to load persistent goals: %s", exc)

            # 3. Extract facts via LLM
            extraction_focus = autorecall.extraction_focus if autorecall else []
            custom_categories = autorecall.custom_categories if autorecall else []
            extraction_result = await extract_facts(
                messages, recent_facts, self._llm, self._config,
                extraction_focus=extraction_focus,
                custom_categories=custom_categories,
                profile_name=profile_name,
                active_session_goals=active_session_goal_dicts,
                persistent_goals=persistent_goal_dicts,
                goal_injection_config=self._goal_injection_config,
            )

            # extract_facts now returns a dict with "facts" and "goal_status_hints"
            raw_facts = extraction_result.get("facts", [])
            goal_status_hints = extraction_result.get("goal_status_hints", [])

            if not raw_facts:
                # Still dispatch any goal_status_hints even if no facts extracted
                if goal_status_hints and self._hint_processor and session_goals_list and session_id:
                    try:
                        sid = uuid.UUID(session_id) if isinstance(session_id, str) else session_id
                        await self._hint_processor.process_hints(
                            goal_status_hints, session_goals_list,
                            session_key=session_key, session_id=sid,
                            recent_messages=messages,
                        )
                    except Exception as exc:
                        logger.debug("Hint processing failed (no facts path): %s", exc)
                if self._trace:
                    await self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.FACT_EXTRACTED,
                        session_id=session_id,
                        session_key=session_key,
                        gateway_id=gw,
                        payload={"facts_count": 0, "profile_name": profile_name,
                                 "session_key": session_key, "reason": "llm_no_facts"},
                    ))
                return TurnIngestResult(actors_resolved=resolved)

            # 4. Build supersession + contradiction maps (decay deferred to after store)
            superseded_factor = autorecall.superseded_confidence_factor if autorecall else 0.3
            facts_superseded = 0
            supersession_map: list[tuple[dict, str]] = []  # (raw_fact, old_fact_id)
            contradiction_map: list[tuple[dict, str]] = []  # (raw_fact, old_fact_id)
            for rf in raw_facts:
                sup_idx = rf.get("supersedes_index", -1)
                if sup_idx >= 0 and sup_idx < len(recent_facts):
                    old_fact_id = recent_facts[sup_idx].get("id")
                    if old_fact_id:
                        supersession_map.append((rf, old_fact_id))
                # 4b. Handle contradicts_index (no decay on old fact)
                con_idx = rf.get("contradicts_index", -1)
                if con_idx >= 0 and con_idx < len(recent_facts):
                    old_fact_id = recent_facts[con_idx].get("id")
                    if old_fact_id:
                        contradiction_map.append((rf, old_fact_id))

            # 5. Build FactAssertions
            now = datetime.now(UTC)
            assertions: list[FactAssertion] = []
            for rf in raw_facts:
                # Map goal_relevance goal_index -> goal_id -> goal_relevance_tags
                relevance_tags: dict[str, str] = {}
                for gr in rf.get("goal_relevance", []):
                    gi = gr.get("goal_index")
                    strength = gr.get("strength", "none")
                    if (
                        isinstance(gi, int)
                        and session_goals_list
                        and 0 <= gi < len(session_goals_list)
                        and strength != "none"
                    ):
                        goal_id_str = str(session_goals_list[gi].id)
                        relevance_tags[goal_id_str] = strength

                # Per-message attribution (GAP-6): determine source_actor for this fact
                fact_source = source_actor_id
                if agent_key:
                    from elephantbroker.runtime.identity import deterministic_uuid_from
                    agent_actor_id = deterministic_uuid_from(agent_key)
                    source_turns = rf.get("source_turns", [])
                    if source_turns:
                        turn_idx = source_turns[0]
                        if turn_idx < len(messages):
                            role = messages[turn_idx].get("role", "")
                            if role in ("assistant", "tool"):
                                fact_source = agent_actor_id
                                if self._metrics:
                                    self._metrics.inc_fact_attribution(role)
                            elif role == "user" and messages[turn_idx].get("actor_id"):
                                fact_source = uuid.UUID(messages[turn_idx]["actor_id"])
                                if self._metrics:
                                    self._metrics.inc_fact_attribution(role)
                            # User messages without a resolvable actor_id intentionally
                            # do NOT increment eb_fact_attribution_total — fact_source
                            # silently falls back to the request-level source_actor_id,
                            # so no attribution actually happened. Counting the
                            # fall-through would inflate the metric with non-events.

                fact = FactAssertion(
                    text=rf["text"],
                    category=rf.get("category", "general"),
                    session_key=session_key,
                    session_id=uuid.UUID(session_id) if session_id else None,
                    source_actor_id=fact_source,
                    goal_ids=[uuid.UUID(g) for g in (goal_ids or [])],
                    provenance_refs=[f"{session_key}:turn:{i}" for i in rf.get("source_turns", [])],
                    goal_relevance_tags=relevance_tags,
                    created_at=now,
                    updated_at=now,
                    gateway_id=gw,
                    decision_domain=rf.get("decision_domain"),
                )
                assertions.append(fact)

            # 5b. Dispatch goal_status_hints to hint_processor (if available)
            if goal_status_hints and self._hint_processor and session_goals_list and session_id:
                try:
                    sid = uuid.UUID(session_id) if isinstance(session_id, str) else session_id
                    await self._hint_processor.process_hints(
                        goal_status_hints, session_goals_list,
                        session_key=session_key, session_id=sid,
                        recent_messages=messages,
                    )
                except Exception as exc:
                    logger.debug("Hint processing failed: %s", exc)

            # 6. Classify memory class
            classified = await classify_memory(assertions, self._llm)
            class_counts: dict[str, int] = {}
            for fact, mc in classified:
                fact.memory_class = mc
                class_counts[mc.value] = class_counts.get(mc.value, 0) + 1

            if class_counts:
                await self._trace.append_event(TraceEvent(
                    event_type=TraceEventType.MEMORY_CLASS_ASSIGNED,
                    session_key=session_key,
                    session_id=uuid.UUID(session_id) if session_id else None,
                    gateway_id=gw,
                    payload={"classes": class_counts, "profile_name": profile_name},
                ))

            # 7. Batch embed
            texts = [f.text for f in assertions]
            embeddings: list = []
            if texts:
                try:
                    embeddings = await self._embeddings.embed_batch(texts)
                except Exception:
                    embeddings = [None] * len(texts)

            # 8. Store facts via facade
            dedup_threshold = autorecall.dedup_similarity if autorecall else 0.95
            facts_stored = 0
            stored_assertions: list[FactAssertion] = []
            for i, fact in enumerate(assertions):
                emb = embeddings[i] if i < len(embeddings) else None
                try:
                    stored_fact = await self._facade.store(
                        fact, dedup_threshold=dedup_threshold, precomputed_embedding=emb,
                        profile_name=profile_name,
                    )
                    if stored_fact is not None:
                        facts_stored += 1
                        stored_assertions.append(stored_fact)
                except DedupSkipped:
                    pass  # Expected: near-duplicate skipped
                except Exception as exc:
                    logger.warning("Failed to store fact: %s", exc)

            # 8b. Create SUPERSEDES/CONTRADICTS edges + deferred decay (stored facts only)
            stored_ids = {f.id for f in stored_assertions}
            raw_to_fact = {id(rf): assertions[i] for i, rf in enumerate(raw_facts) if i < len(assertions) and assertions[i].id in stored_ids}

            # Decay superseded old facts only when new fact was actually stored
            for rf, old_id in supersession_map:
                new_fact = raw_to_fact.get(id(rf))
                if new_fact:
                    try:
                        await self._facade.decay(uuid.UUID(old_id), superseded_factor)
                        facts_superseded += 1
                        await self._trace.append_event(TraceEvent(
                            event_type=TraceEventType.FACT_SUPERSEDED,
                            session_key=session_key,
                            session_id=uuid.UUID(session_id) if session_id else None,
                            gateway_id=gw,
                            payload={"old_fact_id": old_id, "new_fact_text": rf["text"][:50], "decay_factor": superseded_factor},
                        ))
                    except Exception:
                        pass

            if self._graph:
                for rf, old_id in supersession_map:
                    new_fact = raw_to_fact.get(id(rf))
                    if new_fact:
                        try:
                            await self._graph.add_relation(str(new_fact.id), old_id, "SUPERSEDES")
                        except Exception:
                            pass
                for rf, old_id in contradiction_map:
                    new_fact = raw_to_fact.get(id(rf))
                    if new_fact:
                        try:
                            await self._graph.add_relation(str(new_fact.id), old_id, "CONTRADICTS")
                        except Exception:
                            pass

            # 9. Update recent facts in buffer (only stored facts — skip dedup-skipped)
            if self._buffer:
                new_recent = [
                    {"id": str(f.id), "text": f.text, "category": f.category}
                    for f in stored_assertions
                ]
                await self._buffer.update_recent_facts(
                    session_key, recent_facts + new_recent,
                    max_count=self._config.extraction_context_facts,
                )

            # 9b. Phase 7: Write fact decision_domains to Redis for guard Tier 2 classification
            #
            # TODO-8-R1-019 — private-member access acknowledgment.
            # This block reaches into ``self._buffer._keys`` and
            # ``self._buffer._redis`` because IngestBuffer was originally
            # a thin Redis-key wrapper and the pipeline grew to need
            # adjacent Redis primitives (LPUSH/LTRIM/EXPIRE) on the same
            # key family. A clean fix would add a typed
            # ``IngestBuffer.write_fact_domains(session_key, sid, domains)``
            # method on the IIngestBuffer ABC + IngestBuffer implementation,
            # but that would change a Phase 4 public interface and require
            # ABC-parity-test updates for two-name kwargs. Tracked as
            # follow-up architectural debt; the coupling is documented
            # here so a future refactor knows the two attributes form a
            # logical pair (the Redis client AND the key builder, not just
            # one of them).
            if self._buffer and self._buffer._keys and session_id:
                try:
                    domains = [f.decision_domain for f in assertions if f.decision_domain]
                    if domains:
                        sid_str = str(session_id)
                        key = self._buffer._keys.fact_domains(session_key, sid_str)
                        await self._buffer._redis.lpush(key, *domains)
                        await self._buffer._redis.ltrim(key, 0, 19)
                        await self._buffer._redis.expire(key, 86400)
                except Exception as exc:
                    logger.debug("Failed to write fact domains to Redis: %s", exc)

            # 10. Cognee ingest
            try:
                for fact in assertions:
                    await cognee.add(fact.text, dataset_name=self._dataset_name)
                await cognee.cognify(datasets=[self._dataset_name])
                if self._metrics:
                    self._metrics.inc_cognify("success")
                else:
                    inc_cognify("success")
                await self._trace.append_event(TraceEvent(
                    event_type=TraceEventType.COGNEE_COGNIFY_COMPLETED,
                    session_id=session_id,
                    gateway_id=gw,
                    payload={"session_key": session_key, "facts_indexed": len(assertions)},
                ))
            except Exception as exc:
                if self._metrics:
                    self._metrics.inc_cognify("error")
                else:
                    inc_cognify("error")
                logger.warning("Cognee cognify failed: %s", exc)

            if self._metrics:
                self._metrics.inc_pipeline("turn_ingest", "success")
            else:
                inc_pipeline("turn_ingest", "success")

            trace_event = TraceEvent(
                event_type=TraceEventType.FACT_EXTRACTED,
                session_id=session_id,
                session_key=session_key,
                gateway_id=gw,
                payload={
                    "facts_count": len(assertions),
                    "profile_name": profile_name,
                    "session_key": session_key,
                },
            )
            await self._trace.append_event(trace_event)

            return TurnIngestResult(
                facts_extracted=assertions,
                facts_stored=facts_stored,
                facts_superseded=facts_superseded,
                actors_resolved=resolved,
                memory_classes_assigned=class_counts,
                trace_event_id=trace_event.id,
            )
        except Exception:
            if self._metrics:
                self._metrics.inc_pipeline("turn_ingest", "error")
            else:
                inc_pipeline("turn_ingest", "error")
            raise
