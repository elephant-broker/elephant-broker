"""Tests for Stage 2: Canonicalize Stable Facts (LLM Smart Merge)."""
from __future__ import annotations

import sys
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.schemas.consolidation import (
    CanonicalResult,
    ConsolidationConfig,
    ConsolidationContext,
    DuplicateCluster,
)
from tests.fixtures.factories import make_fact_assertion


@pytest.fixture(autouse=True)
def _mock_cognee_for_canonicalize(monkeypatch):
    """Mock cognee and add_data_points for all canonicalize tests."""
    mock_adp = AsyncMock()
    mock_cognee = MagicMock()
    mock_cognee.add = AsyncMock()
    monkeypatch.setattr("cognee.tasks.storage.add_data_points", mock_adp)
    # Ensure cognee module is available for inline import
    if "cognee" not in sys.modules:
        sys.modules["cognee"] = mock_cognee
    else:
        monkeypatch.setattr("cognee.add", AsyncMock())
    return mock_adp


def _make_stage(llm_text="merged fact", llm_fail=False, trace=None, metrics=None, gateway_id="gw"):
    from elephantbroker.runtime.consolidation.stages.canonicalize import CanonicalizationStage

    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    llm = AsyncMock()
    if llm_fail:
        llm.complete = AsyncMock(side_effect=RuntimeError("LLM down"))
    else:
        llm.complete = AsyncMock(return_value=llm_text)
    config = ConsolidationConfig()
    stage = CanonicalizationStage(
        graph, vector, llm, embeddings, config,
        dataset_name="gw__elephantbroker",
        trace_ledger=trace, metrics=metrics,
        gateway_id=gateway_id,
    )
    return stage, graph, vector, llm


def _make_context(**kw):
    defaults = {"org_id": "org", "gateway_id": "gw", "llm_calls_cap": 50}
    defaults.update(kw)
    return ConsolidationContext(**defaults)


def _make_cluster(facts, avg_sim=0.95):
    return DuplicateCluster(
        fact_ids=[str(f.id) for f in facts],
        canonical_candidate_id=str(facts[0].id),
        avg_similarity=avg_sim,
        session_keys=list({f.session_key for f in facts if f.session_key}),
    )


class TestCanonicalize:
    async def test_creates_new_canonical_fact(self):
        stage, graph, vector, llm = _make_stage(llm_text="User prefers TypeScript for backend projects")
        facts = [
            make_fact_assertion(text="User prefers TypeScript", confidence=0.9, session_key="s1"),
            make_fact_assertion(text="User likes TypeScript for backend", confidence=0.7, session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 1
        assert results[0].canonical_text == "User prefers TypeScript for backend projects"
        assert results[0].llm_used is True

    async def test_archives_all_originals(self):
        stage, graph, vector, llm = _make_stage()
        facts = [
            make_fact_assertion(text="fact A", confidence=0.8, session_key="s1"),
            make_fact_assertion(text="fact B", confidence=0.6, session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results[0].archived_fact_ids) == 2

    async def test_merges_use_counts(self):
        stage, *_ = _make_stage()
        facts = [
            make_fact_assertion(text="a", use_count=5, successful_use_count=3, session_key="s1"),
            make_fact_assertion(text="b", use_count=3, successful_use_count=2, session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert results[0].merged_use_count == 8
        assert results[0].merged_successful_use_count == 5

    async def test_merges_provenance_from_all_versions(self):
        stage, *_ = _make_stage()
        facts = [
            make_fact_assertion(text="a", provenance_refs=["ref1", "ref2"], session_key="s1"),
            make_fact_assertion(text="b", provenance_refs=["ref2", "ref3"], session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert set(results[0].merged_provenance) == {"ref1", "ref2", "ref3"}

    async def test_merges_goal_ids(self):
        stage, *_ = _make_stage()
        g1, g2 = uuid.uuid4(), uuid.uuid4()
        facts = [
            make_fact_assertion(text="a", goal_ids=[g1], session_key="s1"),
            make_fact_assertion(text="b", goal_ids=[g2], session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results[0].merged_goal_ids) == 2

    async def test_deterministic_merge_for_identical_texts(self):
        stage, _, _, llm = _make_stage()
        facts = [
            make_fact_assertion(text="identical text", confidence=0.9, session_key="s1"),
            make_fact_assertion(text="identical text", confidence=0.7, session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 1
        assert results[0].llm_used is False
        llm.complete.assert_not_called()

    async def test_respects_llm_calls_cap(self):
        stage, _, _, llm = _make_stage()
        facts = [
            make_fact_assertion(text="a", session_key="s1"),
            make_fact_assertion(text="b", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context(llm_calls_used=50, llm_calls_cap=50)
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 0
        llm.complete.assert_not_called()

    async def test_creates_superseded_by_edges(self):
        stage, graph, vector, _ = _make_stage()
        facts = [
            make_fact_assertion(text="identical", session_key="s1"),
            make_fact_assertion(text="identical", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        await stage.run([cluster], facts, "gw", ctx)
        assert graph.add_relation.call_count == 2
        for call in graph.add_relation.call_args_list:
            assert call[0][2] == "SUPERSEDED_BY"

    async def test_deletes_qdrant_embeddings_on_archive(self):
        stage, graph, vector, _ = _make_stage()
        facts = [
            make_fact_assertion(text="identical", session_key="s1"),
            make_fact_assertion(text="identical", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        await stage.run([cluster], facts, "gw", ctx)
        assert vector.delete_embedding.call_count == 2

    async def test_llm_failure_skips_cluster(self):
        stage, *_ = _make_stage(llm_fail=True)
        facts = [
            make_fact_assertion(text="a", session_key="s1"),
            make_fact_assertion(text="b", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 0

    async def test_uses_broadest_scope(self):
        stage, *_ = _make_stage()
        facts = [
            make_fact_assertion(text="identical", scope="session", session_key="s1"),
            make_fact_assertion(text="identical", scope="actor", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 1

    async def test_empty_clusters_returns_empty(self):
        stage, *_ = _make_stage()
        ctx = _make_context()
        results = await stage.run([], [], "gw", ctx)
        assert results == []

    # --- PR #5 TODO 5-201: canonicalize TD-50 cognee_data_id capture ---
    # Three tests pinning the C8 fix:
    #   1. Success path — cognee.add() return is captured and threaded onto
    #      the canonical FactDataPoint via `from_schema(..., cognee_data_id=…)`
    #      BEFORE add_data_points() runs the MERGE (so the canonical node is
    #      storable with a cascade-reachable pointer, not the pre-fix silent
    #      None). Post-C21 (`95457e4`) the field lives only on `FactDataPoint`
    #      (storage layer); `FactAssertion` no longer carries it.
    #   2. Capture failure — when cognee.add() returns a malformed shape the
    #      facade cannot extract a data_id from, the DEGRADED_OPERATION trace
    #      and the eb_cognee_data_id_capture_failures_total metric (operation
    #      label "canonicalize") both fire. The canonical FactDataPoint is
    #      still persisted with cognee_data_id=None (graceful degradation —
    #      same contract as facade.store()/update()).
    #   3. Superseded cascade — each pre-existing fact whose FactDataPoint
    #      carried its own cognee_data_id has that id enqueued for the
    #      Cognee-side delete so the old documents don't silently accumulate
    #      as orphans after the canonical merge supersedes them.

    async def test_canonicalize_captures_cognee_data_id_on_new_fact(self, monkeypatch):
        """Success path: cognee.add() returns a valid shape → new_fact.cognee_data_id is set
        BEFORE add_data_points() runs (so the MERGEd canonical node points at the new doc).

        Note: add_data_points is called 3× in this flow (1 canonical + 2 archived
        originals). We capture ALL calls and inspect the FIRST one — the canonical —
        because archived originals legitimately have cognee_data_id=None in this
        factory setup and would mask the canonical's data_id on a last-write capture.
        """
        from unittest.mock import MagicMock as _MagicMock

        captured_dps: list = []

        async def capture_adp(dps):
            if dps:
                captured_dps.append(dps[0])

        monkeypatch.setattr("cognee.tasks.storage.add_data_points", AsyncMock(side_effect=capture_adp))

        fake_data_id = uuid.uuid4()
        fake_result = _MagicMock()
        fake_result.data_ingestion_info = [{"data_id": fake_data_id}]
        monkeypatch.setattr("cognee.add", AsyncMock(return_value=fake_result))

        stage, *_ = _make_stage(llm_text="merged canonical text")
        facts = [
            make_fact_assertion(text="a", session_key="s1"),
            make_fact_assertion(text="b", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 1
        # The first add_data_points call is the canonical dp — it must carry the
        # captured data_id, not the pre-fix silent None.
        assert len(captured_dps) >= 1
        canonical_dp = captured_dps[0]
        assert canonical_dp.cognee_data_id == str(fake_data_id)

    async def test_canonicalize_emits_degraded_operation_on_capture_failure(self, monkeypatch):
        """Malformed cognee.add() return → DEGRADED_OPERATION trace + capture-failure metric.
        The canonical fact is still persisted with cognee_data_id=None."""
        from unittest.mock import MagicMock as _MagicMock

        # Malformed shape: data_ingestion_info missing → KeyError on extraction
        bad_result = _MagicMock()
        bad_result.data_ingestion_info = []  # IndexError on [0]
        monkeypatch.setattr("cognee.add", AsyncMock(return_value=bad_result))

        trace_calls: list = []

        class FakeTrace:
            async def append_event(self, event):
                trace_calls.append(event)

        class FakeMetrics:
            def __init__(self):
                self.capture_calls: list[str] = []
            def inc_cognee_capture_failure(self, operation):
                self.capture_calls.append(operation)

        fake_metrics = FakeMetrics()
        fake_trace = FakeTrace()

        captured_dps: list = []

        async def capture_adp(dps):
            if dps:
                captured_dps.append(dps[0])

        monkeypatch.setattr("cognee.tasks.storage.add_data_points", AsyncMock(side_effect=capture_adp))

        stage, *_ = _make_stage(
            llm_text="merged canonical text",
            trace=fake_trace,
            metrics=fake_metrics,
        )
        facts = [
            make_fact_assertion(text="a", session_key="s1"),
            make_fact_assertion(text="b", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)

        assert len(results) == 1
        # Capture failure metric fired with operation="canonicalize"
        assert fake_metrics.capture_calls == ["canonicalize"]
        # DEGRADED_OPERATION trace emitted
        from elephantbroker.schemas.trace import TraceEventType
        degraded = [e for e in trace_calls if e.event_type == TraceEventType.DEGRADED_OPERATION]
        assert len(degraded) == 1
        assert degraded[0].payload["operation"] == "canonicalize"
        assert degraded[0].payload["failure"] == "cognee_data_id_capture"
        # First add_data_points call is the canonical dp — it must carry cognee_data_id=None
        # under graceful-degradation contract (pre-fix silent None is now accompanied by
        # metric + trace, but the persist still succeeds with the missing id).
        assert len(captured_dps) >= 1
        canonical_dp = captured_dps[0]
        assert canonical_dp.cognee_data_id is None

    async def test_canonicalize_capture_failure_on_non_uuid_data_id(self, monkeypatch):
        """TODO-5-003 / TODO-5-211: cognee.add() returns a non-UUID-parseable
        data_id → ValueError routed through _emit_capture_failure exactly
        like a shape mismatch. Canonical fact persisted with
        cognee_data_id=None; metric + DEGRADED_OPERATION fire.
        Pre-fix the canonicalize except tuple omitted ValueError so this
        crashed the stage with an unhandled exception."""
        from unittest.mock import MagicMock as _MagicMock

        bad_result = _MagicMock()
        bad_result.data_ingestion_info = [{"data_id": "definitely-not-a-uuid"}]
        monkeypatch.setattr("cognee.add", AsyncMock(return_value=bad_result))

        trace_calls: list = []

        class FakeTrace:
            async def append_event(self, event):
                trace_calls.append(event)

        class FakeMetrics:
            def __init__(self):
                self.capture_calls: list[str] = []
            def inc_cognee_capture_failure(self, operation):
                self.capture_calls.append(operation)

        fake_metrics = FakeMetrics()
        fake_trace = FakeTrace()
        captured_dps: list = []

        async def capture_adp(dps):
            if dps:
                captured_dps.append(dps[0])

        monkeypatch.setattr("cognee.tasks.storage.add_data_points", AsyncMock(side_effect=capture_adp))

        stage, *_ = _make_stage(
            llm_text="merged canonical text",
            trace=fake_trace,
            metrics=fake_metrics,
        )
        facts = [
            make_fact_assertion(text="a", session_key="s1"),
            make_fact_assertion(text="b", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)

        assert len(results) == 1
        assert fake_metrics.capture_calls == ["canonicalize"]
        from elephantbroker.schemas.trace import TraceEventType
        degraded = [e for e in trace_calls if e.event_type == TraceEventType.DEGRADED_OPERATION]
        assert len(degraded) == 1
        assert degraded[0].payload["exception_type"] == "ValueError"
        assert degraded[0].payload["failure"] == "cognee_data_id_capture"
        assert len(captured_dps) >= 1
        assert captured_dps[0].cognee_data_id is None

    async def test_canonicalize_enqueues_superseded_cognee_data_ids_for_cascade(self, monkeypatch):
        """Each pre-existing fact with a cognee_data_id on its graph node has
        that id passed through to the Cognee delete cascade (so the old
        documents are not silently orphaned).

        TODO-5-307: cognee_data_id is no longer on FactAssertion. The stage
        fetches each superseded member's storage-backend id from the graph
        node via `graph.get_entity(str(member.id))`. This test mocks that
        per-member lookup to return the simulated legacy ids.
        """
        from unittest.mock import MagicMock as _MagicMock

        # cognee.add succeeds for canonical (so the new fact is storable and the
        # test focuses on the superseded-cascade branch only)
        fake_data_id = uuid.uuid4()
        fake_result = _MagicMock()
        fake_result.data_ingestion_info = [{"data_id": fake_data_id}]
        monkeypatch.setattr("cognee.add", AsyncMock(return_value=fake_result))

        # Stub Cognee internals bound into the shared cascade helper's
        # namespace (TODO-5-314: cascade now lives in
        # elephantbroker.runtime.memory.cascade_helper, so module-level
        # `from ... import X` bindings are what the patches must target).
        fake_user = _MagicMock()
        fake_user.id = uuid.uuid4()
        fake_dataset = _MagicMock()
        fake_dataset.id = uuid.uuid4()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_default_user",
            AsyncMock(return_value=fake_user),
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_datasets_by_name",
            AsyncMock(return_value=[fake_dataset]),
        )

        delete_calls: list = []

        async def capture_delete(**kwargs):
            delete_calls.append(kwargs)

        # cognee.datasets.delete_data is what the cascade calls
        import cognee as _cognee
        fake_datasets_mod = _MagicMock()
        fake_datasets_mod.delete_data = AsyncMock(side_effect=capture_delete)
        monkeypatch.setattr(_cognee, "datasets", fake_datasets_mod, raising=False)

        stage, graph, *_ = _make_stage(llm_text="merged canonical text")

        old_data_id_a = uuid.uuid4()
        old_data_id_b = uuid.uuid4()
        facts = [
            make_fact_assertion(text="identical", session_key="s1"),
            make_fact_assertion(text="identical", session_key="s2"),
        ]
        # Per-member graph lookup returns each fact's storage-backend id.
        member_ids_by_fact: dict[str, str] = {
            str(facts[0].id): str(old_data_id_a),
            str(facts[1].id): str(old_data_id_b),
        }

        async def _get_entity(node_id: str):
            return {"cognee_data_id": member_ids_by_fact.get(node_id)}

        graph.get_entity = AsyncMock(side_effect=_get_entity)

        cluster = _make_cluster(facts)
        ctx = _make_context()
        await stage.run([cluster], facts, "gw", ctx)

        # Both superseded originals' cognee_data_ids cascaded
        assert len(delete_calls) == 2
        cascaded_ids = {call["data_id"] for call in delete_calls}
        assert cascaded_ids == {old_data_id_a, old_data_id_b}
        # All cascades use soft-delete mode, preserving the dataset
        assert all(call["mode"] == "soft" for call in delete_calls)
        assert all(call["delete_dataset_if_empty"] is False for call in delete_calls)

    async def test_superseded_cascade_recovers_from_qdrant_404(self, monkeypatch):
        """TODO-5-314 regression: the shared cascade helper's TD-Cognee-
        Qdrant-404 recovery branch now covers the canonicalize path too.

        Pre-extraction, canonicalize carried an intentionally-duplicated
        cascade body WITHOUT the 404 branch — if a cluster included a
        member whose Cognee Data row was added but never cognify()'d, the
        vector collection would 404 mid-cascade and Cognee's outer
        delete_data would abort before unbinding the Data↔Dataset. This
        test pins that canonicalize now invokes the shared helper which
        manually completes the metadata removal on 404, returning
        "ok_idempotent" rather than "failed".
        """
        from unittest.mock import MagicMock as _MagicMock
        from httpx import Headers
        from qdrant_client.http.exceptions import UnexpectedResponse

        # cognee.add for the canonical succeeds so we reach the cascade loop
        fake_data_id = uuid.uuid4()
        fake_result = _MagicMock()
        fake_result.data_ingestion_info = [{"data_id": fake_data_id}]
        monkeypatch.setattr("cognee.add", AsyncMock(return_value=fake_result))

        fake_user = _MagicMock()
        fake_user.id = uuid.uuid4()
        fake_dataset = _MagicMock()
        fake_ds_id = uuid.uuid4()
        fake_dataset.id = fake_ds_id
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_default_user",
            AsyncMock(return_value=fake_user),
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_datasets_by_name",
            AsyncMock(return_value=[fake_dataset]),
        )

        # Inner Qdrant delete raises 404 — the exact shape we see when a
        # Data row was added but never cognify()'d (no derived vector
        # collection exists).
        qdrant_404 = UnexpectedResponse(
            status_code=404,
            reason_phrase="Not Found",
            content=b'{"status":{"error":"Collection not found"}}',
            headers=Headers({}),
        )
        import cognee as _cognee
        fake_datasets_mod = _MagicMock()
        fake_datasets_mod.delete_data = AsyncMock(side_effect=qdrant_404)
        monkeypatch.setattr(_cognee, "datasets", fake_datasets_mod, raising=False)

        # Recovery path: helper re-fetches Data rows and calls inner
        # delete_data to complete the Data↔Dataset unbind manually.
        old_data_id = uuid.uuid4()
        fake_data_row = type("Data", (), {"id": old_data_id, "__tablename__": "data"})()
        get_dataset_data_mock = AsyncMock(return_value=[fake_data_row])
        delete_data_row_mock = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_dataset_data",
            get_dataset_data_mock,
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper._delete_data_row",
            delete_data_row_mock,
        )

        stage, graph, *_ = _make_stage(llm_text="merged canonical text")

        facts = [
            make_fact_assertion(text="identical", session_key="s1"),
            make_fact_assertion(text="identical", session_key="s2"),
        ]
        member_ids_by_fact: dict[str, str] = {
            str(facts[0].id): str(old_data_id),
            str(facts[1].id): str(uuid.uuid4()),
        }

        async def _get_entity(node_id: str):
            return {"cognee_data_id": member_ids_by_fact.get(node_id)}

        graph.get_entity = AsyncMock(side_effect=_get_entity)

        cluster = _make_cluster(facts)
        ctx = _make_context()
        # Must not raise — helper returns status strings; canonicalize's
        # per-member cascade loop swallows results best-effort.
        await stage.run([cluster], facts, "gw", ctx)

        # Recovery fired for the member whose data_id we set up:
        # get_dataset_data was called with the dataset id, and the inner
        # delete_data_row was awaited to complete the Data↔Dataset unbind.
        get_dataset_data_mock.assert_awaited()
        delete_data_row_mock.assert_awaited()
        # Assert the recovery call targeted the correct row.
        called_with_row = delete_data_row_mock.await_args_list[0].args
        assert called_with_row[0] is fake_data_row
        assert called_with_row[1] == fake_ds_id

    # --- PR #5 TODO 5-113 / 5-509: preload + gateway_id observability ---

    async def test_preload_failure_emits_warning_metric_trace(self, monkeypatch, caplog):
        """TODO-5-113: graph.get_entity raising during the preload loop
        fires the observability trio (WARNING log + capture-failure metric
        with operation='canonicalize_preload' + DEGRADED_OPERATION trace)
        rather than the pre-fix silent logger.debug + None fallback. The
        canonicalize run itself continues — the failing member is simply
        excluded from the superseded-cognee-data cascade."""
        from unittest.mock import MagicMock as _MagicMock

        fake_data_id = uuid.uuid4()
        fake_result = _MagicMock()
        fake_result.data_ingestion_info = [{"data_id": fake_data_id}]
        monkeypatch.setattr("cognee.add", AsyncMock(return_value=fake_result))

        trace_calls: list = []

        class FakeTrace:
            async def append_event(self, event):
                trace_calls.append(event)

        class FakeMetrics:
            def __init__(self):
                self.capture_calls: list[str] = []

            def inc_cognee_capture_failure(self, operation):
                self.capture_calls.append(operation)

        fake_metrics = FakeMetrics()
        fake_trace = FakeTrace()

        stage, graph, *_ = _make_stage(
            llm_text="merged canonical text",
            trace=fake_trace,
            metrics=fake_metrics,
        )

        facts = [
            make_fact_assertion(text="identical", session_key="s1"),
            make_fact_assertion(text="identical", session_key="s2"),
        ]

        bad_member_id = str(facts[0].id)

        async def _get_entity(node_id: str):
            if node_id == bad_member_id:
                raise RuntimeError("neo4j transient error")
            return {"cognee_data_id": str(uuid.uuid4())}

        graph.get_entity = AsyncMock(side_effect=_get_entity)

        cluster = _make_cluster(facts)
        ctx = _make_context()
        import logging as _logging
        with caplog.at_level(_logging.WARNING, logger="elephantbroker.runtime.consolidation.stages.canonicalize"):
            await stage.run([cluster], facts, "gw", ctx)

        assert fake_metrics.capture_calls == ["canonicalize_preload"]

        from elephantbroker.schemas.trace import TraceEventType
        degraded = [e for e in trace_calls if e.event_type == TraceEventType.DEGRADED_OPERATION]
        assert len(degraded) == 1
        assert degraded[0].payload["operation"] == "canonicalize_preload"
        assert degraded[0].payload["failure"] == "get_entity_exception"
        assert degraded[0].payload["fact_id"] == bad_member_id
        assert degraded[0].payload["exception_type"] == "RuntimeError"

        assert any(
            "Preload of cognee_data_id failed for member" in rec.message
            for rec in caplog.records
        )

    async def test_emit_capture_failure_threads_gateway_id_on_bare_path(self, monkeypatch):
        """TODO-5-509: when `self._metrics` is None (bare-function metric
        path), `_emit_capture_failure` must pass `gateway_id=self._gateway_id`
        to `inc_cognee_capture_failure` so the Prometheus label matches the
        MetricsContext-wrapped path. Pre-fix the bare call elided the kwarg,
        defaulting the label to the empty string."""
        captured_calls: list = []

        def fake_inc(operation, gateway_id=""):
            captured_calls.append((operation, gateway_id))

        monkeypatch.setattr(
            "elephantbroker.runtime.consolidation.stages.canonicalize.inc_cognee_capture_failure",
            fake_inc,
        )

        stage, *_ = _make_stage(gateway_id="gw-5-509")
        # Directly invoke the helper — its own contract is the unit under test.
        await stage._emit_capture_failure(
            fact_id=uuid.uuid4(), exc=RuntimeError("boom"),
        )
        await stage._emit_preload_failure(
            fact_id=uuid.uuid4(), exc=RuntimeError("boom"),
        )

        assert ("canonicalize", "gw-5-509") in captured_calls
        assert ("canonicalize_preload", "gw-5-509") in captured_calls
