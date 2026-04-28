"""Tests for session lifecycle routes."""
import uuid
from unittest.mock import AsyncMock

from elephantbroker.runtime.identity import deterministic_uuid_from
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.trace import TraceEventType


class TestSessionRoutes:
    async def test_session_start_returns_ok(self, client):
        r = await client.post(
            "/sessions/start",
            json={"session_key": "agent:main:main", "session_id": "abc-123"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["session_key"] == "agent:main:main"
        assert data["session_id"] == "abc-123"
        # G1 extension: response exposes gateway identity so the TS plugin can verify
        # the server-side derivation matches its local stamping (#573)
        assert "agent_key" in data
        assert "agent_actor_id" in data

    async def test_session_end_returns_summary(self, client):
        r = await client.post(
            "/sessions/end",
            json={"session_key": "agent:main:main", "session_id": "abc-123"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["session_key"] == "agent:main:main"
        assert data["facts_count"] == 0
        assert data["goals_flushed"] == 0
        assert data["messages_flushed"] == 0
        assert "trace_event_id" in data
        assert data["trace_event_id"] is not None

    async def test_session_start_with_parent(self, client):
        r = await client.post(
            "/sessions/start",
            json={
                "session_key": "agent:worker:task1",
                "session_id": "def-456",
                "parent_session_key": "agent:main:main",
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data["session_key"] == "agent:worker:task1"

    async def test_session_start_parent_uses_config_ttl(self, client, container):
        """BUG-5: parent TTL must come from config, not hardcoded 86400."""
        redis_mock = AsyncMock()
        container.redis = redis_mock
        container.config = ElephantBrokerConfig(consolidation_min_retention_seconds=259200)

        r = await client.post("/sessions/start", json={
            "session_key": "agent:child:main",
            "session_id": "sid-123",
            "parent_session_key": "agent:parent:main",
        })
        assert r.status_code == 200
        redis_mock.setex.assert_called()
        ttl_arg = redis_mock.setex.call_args[0][1]
        assert ttl_arg == 259200  # NOT 86400

    # ------------------------------------------------------------------
    # TF-FN-013 additions
    # ------------------------------------------------------------------

    async def test_session_start_registers_actor_datapoint(self, client, monkeypatch):
        """G2 (#559): POST /sessions/start upserts an ActorDataPoint via add_data_points with
        type=WORKER_AGENT, id=deterministic_uuid_from(agent_key), and handles=[agent_key].

        Overrides the autouse fixture's fake_add_data_points with a capturing spy so we can
        inspect the actual DataPoint instance passed on the single call.
        """
        calls: list = []

        async def capture(data_points, context=None, custom_edges=None, embed_triplets=False):
            calls.append(list(data_points))
            return list(data_points)

        monkeypatch.setattr("elephantbroker.api.routes.sessions.add_data_points", capture)
        r = await client.post("/sessions/start", json={
            "session_key": "agent:main:main",
            "session_id": "abc-123",
            "agent_id": "main",
        })
        assert r.status_code == 200
        assert len(calls) == 1, "Expected exactly one add_data_points call (agent ActorRef upsert)"
        assert len(calls[0]) == 1, "Expected a single-item DataPoint list"
        dp = calls[0][0]
        assert dp.actor_type == "worker_agent"
        # handles must include the server-side-derived agent_key; body.agent_id="main" +
        # default middleware gateway_id yields agent_key like "<gw>:main"
        assert any("main" in h for h in dp.handles), f"handles={dp.handles!r} missing 'main'"
        # eb_id is the deterministic UUID derived from agent_key
        expected = deterministic_uuid_from(r.json()["agent_key"])
        assert dp.eb_id == str(expected)

    async def test_session_start_merges_agent_identity(self, client, container):
        """G3 (#560): POST /sessions/start issues a Cypher MERGE on AgentIdentity keyed on
        agent_key. Idempotent: ON CREATE SET registered_at; ON MATCH SET last_seen_at.
        """
        container.graph.query_cypher = AsyncMock(return_value=[])
        r = await client.post("/sessions/start", json={
            "session_key": "agent:main:main",
            "session_id": "abc-123",
            "agent_id": "main",
        })
        assert r.status_code == 200
        container.graph.query_cypher.assert_awaited()
        cypher = container.graph.query_cypher.call_args[0][0]
        assert "MERGE (n:AgentIdentity" in cypher
        assert "agent_key" in cypher

    async def test_session_start_second_call_same_agent_key_is_idempotent(self, client, monkeypatch):
        """G4 (#559): Calling /sessions/start twice with the same agent_key yields ActorDataPoint
        instances with identical eb_id (deterministic UUID from agent_key). No duplicate-upsert
        creates a second actor.
        """
        calls: list = []

        async def capture(data_points, context=None, custom_edges=None, embed_triplets=False):
            calls.append(list(data_points))
            return list(data_points)

        monkeypatch.setattr("elephantbroker.api.routes.sessions.add_data_points", capture)
        body = {"session_key": "agent:main:main", "session_id": "s1", "agent_id": "main"}
        r1 = await client.post("/sessions/start", json=body)
        r2 = await client.post("/sessions/start", json={**body, "session_id": "s2"})
        assert r1.status_code == 200 and r2.status_code == 200
        assert len(calls) == 2
        assert calls[0][0].eb_id == calls[1][0].eb_id, "eb_id must be deterministic on agent_key"

    async def test_session_start_emits_session_boundary_with_event_start(self, client, container):
        """G5+G9 (start half): /sessions/start emits SESSION_BOUNDARY with payload.event='start'.

        Pins D1 from Step 0 (commit 3526837) — the plan previously expected
        payload.action='start'; code ships payload.event='start' (sessions.py:113).

        TD-65 extension: also asserts top-level TraceEvent.session_id is set (not None)
        so POST /trace/query can filter by session_id.
        """
        sid = str(uuid.uuid4())
        await client.post("/sessions/start", json={
            "session_key": "agent:main:main",
            "session_id": sid,
            "agent_id": "main",
        })
        events = [
            e for e in container.trace_ledger._events
            if e.event_type == TraceEventType.SESSION_BOUNDARY
        ]
        assert len(events) >= 1
        ev = events[-1]
        assert ev.payload["event"] == "start"
        assert ev.payload["session_key"] == "agent:main:main"
        assert "agent_key" in ev.payload
        # TD-65: top-level session_id must be set (not None) for trace_query filtering.
        assert ev.session_id == uuid.UUID(sid)

    async def test_session_end_emits_session_boundary_with_event_end(self, client, container):
        """G5+G9 (end half): /sessions/end emits SESSION_BOUNDARY with payload.event='end'.

        Pins D1 from Step 0 — matches sessions.py:273.

        TD-65 extension: also asserts top-level TraceEvent.session_id is set. The
        `event="end"` emission comes from the route handler (distinct from lifecycle's
        `lifecycle_session_end` and goal store's `goals_flushed`). We filter for the
        route event specifically by its unique payload keys (`reason`, `facts_count`).
        """
        sid = str(uuid.uuid4())
        await client.post(
            "/sessions/end",
            headers={"X-EB-Agent-Id": "main"},
            json={"session_key": "agent:main:main", "session_id": sid},
        )
        events = [
            e for e in container.trace_ledger._events
            if e.event_type == TraceEventType.SESSION_BOUNDARY
        ]
        assert len(events) >= 1
        # Find the route-level "end" emission (distinguished by payload.event + reason field).
        route_events = [e for e in events if e.payload.get("event") == "end"]
        assert route_events, "Expected at least one SESSION_BOUNDARY with event='end' from the route"
        ev = route_events[-1]
        assert ev.payload["session_key"] == "agent:main:main"
        # TD-65: top-level session_id must be set (not None) for trace_query filtering.
        assert ev.session_id == uuid.UUID(sid)
        # TD-65 follow-up (observer reverify catch): agent_id must also be set as a
        # top-level field on the /end emission. Previously only /start populated it,
        # leaving end events with agent_id=None — inconsistent with the start emission
        # and breaking per-agent trace_query filtering on session-end signals.
        assert ev.agent_id == "main"

    async def test_session_start_header_gateway_id_wins_body_ignored_security_fix(self, client, container):
        """Pins security-corrected behavior post TODO-3-030 / Bucket A-R3 / TD-41:
        middleware-set gateway_id wins UNCONDITIONALLY over body.gateway_id.
        The original PROD risk #1515 (body-wins-allowing-tenant-spoofing) was
        RESOLVED by this fix. Body.gateway_id is now ignored — the route only
        reads request.state.gateway_id. If middleware is not installed, the
        route returns HTTP 500 (deployment bug, not runtime condition). If a
        future change reverts to body-wins, that's a tenant-spoofing regression
        — update this test, the TF-FN-013 plan, and reopen #1515.
        """
        # Body claims one gateway; header (middleware) claims another. Header wins.
        r = await client.post(
            "/sessions/start",
            headers={"X-EB-Gateway-ID": "gw-target"},
            json={
                "session_key": "agent:main:main",
                "session_id": "abc-123",
                "agent_id": "main",
                "gateway_id": "gw-attacker",  # body value -- MUST be ignored
            },
        )
        assert r.status_code == 200
        # Scan the SESSION_BOUNDARY event emitted on start and verify the stamped
        # gateway_id is the middleware value, not the body value.
        events = [
            e for e in container.trace_ledger._events
            if e.event_type == TraceEventType.SESSION_BOUNDARY
        ]
        assert len(events) >= 1
        ev = events[-1]
        assert ev.gateway_id == "gw-target", (
            f"Header gateway_id must win over body; got {ev.gateway_id!r} "
            f"(body claimed 'gw-attacker', header claimed 'gw-target')"
        )

    async def test_session_start_accepts_agent_id_in_body(self, client):
        """G7 (#573): /sessions/start accepts agent_id in the request body (not just headers).

        Body-level redundancy — TS plugins can send agent identity via body OR header.
        Response's agent_key must include the body-supplied agent_id.
        """
        r = await client.post("/sessions/start", json={
            "session_key": "agent:main:main-worker",
            "session_id": "abc-123",
            "agent_id": "main-worker",
        })
        assert r.status_code == 200
        assert "main-worker" in r.json()["agent_key"]

    async def test_session_start_increments_session_boundary_metric(self, client, container):
        """TD-65 follow-up (observer-reverify catch): POST /sessions/start now increments
        `eb_session_boundary_total{event="session_start"}` so the metric fires 1:1 with the
        session_end increment in the session_end route (M1 moved from lifecycle to route).

        Previously the start increment was inside ContextLifecycle.bootstrap, which was a
        poor proxy — bootstrap can fire multiple times per session on re-bootstrap after
        dispose (see TF-FN-011 GF-15), and in some deployment modes it doesn't fire at all.
        Observer Layer B/C reverify confirmed the start time series was missing in
        practice; this pin ensures the HTTP-layer signal is the authoritative source.
        """
        # MetricsContext from the api conftest is a real instance with .inc_session_boundary;
        # wrap it so we can count calls without breaking the underlying counter.
        from unittest.mock import MagicMock
        original = container.metrics_ctx.inc_session_boundary
        spy = MagicMock(wraps=original)
        container.metrics_ctx.inc_session_boundary = spy

        await client.post("/sessions/start", json={
            "session_key": "agent:main:main",
            "session_id": "abc-123",
            "agent_id": "main",
        })
        calls = [c.args[0] for c in spy.call_args_list if c.args]
        assert "session_start" in calls, (
            f"POST /sessions/start must call inc_session_boundary('session_start'); "
            f"observed calls: {calls!r}"
        )

    async def test_session_end_does_not_verify_gateway_id_documented_prod_risk(self, client):
        """Pins PROD risk #1507 — /sessions/end does not verify body.gateway_id matches the
        session's stored gateway. Cross-gateway session end is possible.

        Note: the route ignores body.gateway_id entirely (middleware wins per TD-41 — see
        sessions.py:208-213). There is also no lookup against a stored session gateway for
        cross-gateway verification. The test only asserts that /sessions/end returns 200
        regardless of body-supplied gateway_id (i.e., no authorization check performed).
        If gateway-match authorization is added, update this test and file a TD.
        """
        r = await client.post("/sessions/end", json={
            "session_key": "any",
            "session_id": "x",
            "gateway_id": "gw-attacker",
        })
        assert r.status_code == 200

    async def test_session_end_fires_session_boundary_metric(self, client, container):
        """M1: POST /sessions/end increments eb_session_boundary_total{event="session_end"}
        unconditionally from the HTTP route, regardless of tier.

        Pre-fix: the metric lived inside ContextLifecycle.session_end(), only
        reachable in FULL tier. MEMORY_ONLY deployments never incremented the
        session_end counter, causing the start/end pair to diverge perpetually.
        """
        from unittest.mock import MagicMock
        original = container.metrics_ctx.inc_session_boundary
        spy = MagicMock(wraps=original)
        container.metrics_ctx.inc_session_boundary = spy

        await client.post("/sessions/end", json={
            "session_key": "agent:main:main",
            "session_id": "abc-123",
        })
        calls = [c.args[0] for c in spy.call_args_list if c.args]
        assert "session_end" in calls, (
            f"POST /sessions/end must call inc_session_boundary('session_end'); "
            f"observed calls: {calls!r}"
        )

    async def test_session_end_metric_fires_without_context_lifecycle(self, client, container):
        """M1-bis: in MEMORY_ONLY tier (no context_lifecycle), the session_end
        metric still fires from the route — this was the missing path pre-fix.
        """
        from unittest.mock import MagicMock
        container.context_lifecycle = None
        original = container.metrics_ctx.inc_session_boundary
        spy = MagicMock(wraps=original)
        container.metrics_ctx.inc_session_boundary = spy

        await client.post("/sessions/end", json={
            "session_key": "agent:main:main",
            "session_id": "abc-123",
        })
        calls = [c.args[0] for c in spy.call_args_list if c.args]
        assert "session_end" in calls, (
            f"MEMORY_ONLY: session_end metric must fire from route; "
            f"observed calls: {calls!r}"
        )

    async def test_session_end_force_flush_emits_buffer_flush_metric_and_trace(
        self, client, container,
    ):
        """TODO-8-R1-013 — sessions/end force_flush observability completeness.

        B2.2 wired ``inc_buffer_flush`` + ``INGEST_BUFFER_FLUSH`` trace at the
        three batch-size flush sites (memory.py route, lifecycle.afterTurn,
        buffer timer) but missed this fourth site — the MEMORY_ONLY-tier
        session-end force flush. In MEMORY_ONLY mode this is the *only*
        flush call site, so a missing metric here meant deployments without
        a context engine were silently undercounting flushes and the
        per-session timeline missed the INGEST_BUFFER_FLUSH event at session
        boundary.

        Test setup: MEMORY_ONLY-style (context_lifecycle=None) so the
        force_flush branch is reachable. Stub the ingest_buffer to return
        a non-empty list of messages so the new conditional fires (an
        empty force_flush is intentionally NOT counted as a flush — see
        the route comment).
        """
        from unittest.mock import MagicMock
        # Force the MEMORY_ONLY branch.
        container.context_lifecycle = None

        # Stub a buffer that returns 2 messages on force_flush.
        buffer_stub = AsyncMock()
        buffer_stub.force_flush = AsyncMock(return_value=[
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ])
        container.ingest_buffer = buffer_stub

        # Spy on inc_buffer_flush with an explicit MagicMock (don't wrap
        # the real one — its `gateway_id=""` argument shape is awkward to
        # reason about and not relevant to this test).
        original_inc = container.metrics_ctx.inc_buffer_flush
        spy_inc = MagicMock(wraps=original_inc)
        container.metrics_ctx.inc_buffer_flush = spy_inc

        # Spy on trace ledger to capture INGEST_BUFFER_FLUSH events.
        ledger_events = []
        original_append = container.trace_ledger.append_event

        async def capture_append(ev):
            ledger_events.append(ev)
            return await original_append(ev)

        container.trace_ledger.append_event = capture_append

        sid = uuid.uuid4()
        r = await client.post("/sessions/end", json={
            "session_key": "agent:main:main",
            "session_id": str(sid),
        })
        assert r.status_code == 200

        # Metric: inc_buffer_flush("session_end") fired exactly once.
        flush_calls = [c.args[0] for c in spy_inc.call_args_list if c.args]
        assert "session_end" in flush_calls, (
            f"force_flush must call inc_buffer_flush('session_end'); "
            f"observed calls: {flush_calls!r}"
        )
        # Trace: INGEST_BUFFER_FLUSH with trigger=session_end + identity.
        flush_events = [
            ev for ev in ledger_events
            if ev.event_type == TraceEventType.INGEST_BUFFER_FLUSH
            and ev.payload.get("trigger") == "session_end"
        ]
        assert len(flush_events) == 1
        ev = flush_events[0]
        assert ev.session_key == "agent:main:main"
        assert ev.session_id == sid
        assert ev.payload.get("message_count") == 2

    async def test_session_end_force_flush_empty_does_not_emit_flush_metric(
        self, client, container,
    ):
        """TODO-8-R1-013 — empty buffer force_flush is NOT a flush event.

        Companion to ``test_session_end_force_flush_emits_buffer_flush_metric_and_trace``:
        when ``buffer.force_flush()`` returns an empty list (the common case
        in FULL mode and in MEMORY_ONLY mode when no messages were buffered),
        we must NOT emit ``inc_buffer_flush`` or ``INGEST_BUFFER_FLUSH``. An
        empty flush is not a meaningful event and would distort flush-rate
        dashboards.
        """
        from unittest.mock import MagicMock
        container.context_lifecycle = None
        buffer_stub = AsyncMock()
        buffer_stub.force_flush = AsyncMock(return_value=[])
        container.ingest_buffer = buffer_stub

        original_inc = container.metrics_ctx.inc_buffer_flush
        spy_inc = MagicMock(wraps=original_inc)
        container.metrics_ctx.inc_buffer_flush = spy_inc

        ledger_events = []
        original_append = container.trace_ledger.append_event

        async def capture_append(ev):
            ledger_events.append(ev)
            return await original_append(ev)

        container.trace_ledger.append_event = capture_append

        await client.post("/sessions/end", json={
            "session_key": "agent:main:main",
            "session_id": str(uuid.uuid4()),
        })

        flush_calls = [c.args[0] for c in spy_inc.call_args_list if c.args]
        assert "session_end" not in flush_calls, (
            f"empty force_flush must NOT fire inc_buffer_flush; observed: {flush_calls!r}"
        )
        flush_events = [
            ev for ev in ledger_events
            if ev.event_type == TraceEventType.INGEST_BUFFER_FLUSH
            and ev.payload.get("trigger") == "session_end"
        ]
        assert flush_events == []
