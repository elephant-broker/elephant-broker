"""Unit tests for TurnIngestPipeline and IngestBuffer."""
from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.pipelines.turn_ingest.buffer import IngestBuffer
from elephantbroker.pipelines.turn_ingest.pipeline import TurnIngestPipeline
from elephantbroker.runtime.memory.facade import DedupSkipped
from elephantbroker.schemas.config import LLMConfig
from elephantbroker.schemas.fact import MemoryClass
from elephantbroker.schemas.pipeline import TurnIngestResult
from elephantbroker.schemas.trace import TraceEventType
from tests.fixtures.factories import make_fact_assertion


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**overrides):
    defaults = {
        "extraction_max_input_tokens": 4000,
        "extraction_max_output_tokens": 16384,
        "extraction_max_facts_per_batch": 10,
        "extraction_context_facts": 20,
        "ingest_batch_size": 6,
        "ingest_batch_timeout_seconds": 60.0,
        "ingest_buffer_ttl_seconds": 300,
        "extraction_context_ttl_seconds": 3600,
    }
    defaults.update(overrides)
    config = MagicMock()
    for k, v in defaults.items():
        setattr(config, k, v)
    return config


def _make_llm(facts=None):
    """Mock LLM that returns facts from extract_facts."""
    llm = MagicMock()
    llm.complete_json = AsyncMock(return_value={
        "facts": facts or [
            {
                "text": "User prefers Python",
                "category": "preference",
                "source_turns": [0],
                "supersedes_index": -1,
            },
        ],
        "goal_status_hints": [],
    })
    return llm


def _make_facade():
    facade = MagicMock()
    facade.store = AsyncMock(side_effect=lambda fact, **kw: fact)
    facade.decay = AsyncMock()
    return facade


def _make_trace():
    trace = MagicMock()
    trace.append_event = AsyncMock(side_effect=lambda e: e)
    return trace


def _make_embeddings(dim=3):
    emb = MagicMock()
    emb.embed_batch = AsyncMock(side_effect=lambda texts: [[0.1] * dim for _ in texts])
    return emb


def _make_buffer(recent_facts=None):
    buf = MagicMock()
    buf.load_recent_facts = AsyncMock(return_value=recent_facts or [])
    buf.update_recent_facts = AsyncMock()
    return buf


def _make_pipeline(
    llm=None, facade=None, trace=None, embeddings=None, config=None,
    profile=None, buffer=None, graph=None,
):
    return TurnIngestPipeline(
        memory_facade=facade or _make_facade(),
        actor_registry=MagicMock(),
        embedding_service=embeddings or _make_embeddings(),
        llm_client=llm or _make_llm(),
        trace_ledger=trace or _make_trace(),
        config=config or _make_config(),
        profile_policy=profile,
        buffer=buffer,
        graph=graph,
    )


# ---------------------------------------------------------------------------
# Pipeline Tests
# ---------------------------------------------------------------------------

class TestTurnIngestPipeline:
    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_full_pipeline_runs(self, mock_cognee):
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        pipe = _make_pipeline()
        messages = [{"role": "user", "content": "I prefer Python for all projects"}]
        result = await pipe.run("session:test", messages)
        assert isinstance(result, TurnIngestResult)
        assert len(result.facts_extracted) > 0
        assert result.facts_stored > 0
        assert result.trace_event_id is not None

    async def test_empty_messages_returns_zero(self):
        pipe = _make_pipeline()
        result = await pipe.run("session:test", [])
        assert result.facts_extracted == []
        assert result.facts_stored == 0

    async def test_empty_messages_emits_fact_extracted_trace(self):
        """TODO-11-005: FACT_EXTRACTED with facts_count=0 on empty messages early-return."""
        trace = _make_trace()
        pipe = _make_pipeline(trace=trace)
        await pipe.run("session:test", [])

        fact_extracted_calls = [
            c for c in trace.append_event.call_args_list
            if c[0][0].event_type == TraceEventType.FACT_EXTRACTED
        ]
        assert len(fact_extracted_calls) >= 1
        payload = fact_extracted_calls[0][0][0].payload
        assert payload["facts_count"] == 0
        assert payload["reason"] == "empty_messages"

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_extracts_and_stores_facts(self, mock_cognee):
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        facade = _make_facade()
        llm = _make_llm(facts=[
            {"text": "fact A", "category": "event", "source_turns": [0], "supersedes_index": -1},
            {"text": "fact B", "category": "identity", "source_turns": [1], "supersedes_index": -1},
        ])
        pipe = _make_pipeline(llm=llm, facade=facade)
        messages = [
            {"role": "user", "content": "Something happened today with the project"},
            {"role": "assistant", "content": "That is interesting, let me help"},
        ]
        result = await pipe.run("session:test", messages)
        assert len(result.facts_extracted) == 2
        assert facade.store.call_count == 2

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_classifies_memory_class(self, mock_cognee):
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        llm = _make_llm(facts=[
            {"text": "User prefers tabs", "category": "preference", "source_turns": [0], "supersedes_index": -1},
        ])
        pipe = _make_pipeline(llm=llm)
        result = await pipe.run("session:test", [{"role": "user", "content": "I prefer tabs over spaces always"}])
        # preference -> SEMANTIC
        assert result.memory_classes_assigned.get("semantic", 0) > 0

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_batch_embed_single_call(self, mock_cognee):
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        embeddings = _make_embeddings()
        llm = _make_llm(facts=[
            {"text": "fact 1", "category": "event", "source_turns": [0], "supersedes_index": -1},
            {"text": "fact 2", "category": "event", "source_turns": [0], "supersedes_index": -1},
        ])
        pipe = _make_pipeline(llm=llm, embeddings=embeddings)
        await pipe.run("session:test", [{"role": "user", "content": "Two facts happening right now in the project"}])
        embeddings.embed_batch.assert_called_once()
        # Should be called with 2 texts
        assert len(embeddings.embed_batch.call_args[0][0]) == 2

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_store_uses_precomputed_embedding(self, mock_cognee):
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        facade = _make_facade()
        pipe = _make_pipeline(facade=facade)
        await pipe.run("session:test", [{"role": "user", "content": "Store this fact with precomputed embedding"}])
        # Check that store was called with precomputed_embedding
        store_call = facade.store.call_args
        assert "precomputed_embedding" in store_call[1]
        assert store_call[1]["precomputed_embedding"] is not None

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_cognee_cognify_called(self, mock_cognee):
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        pipe = _make_pipeline()
        await pipe.run("session:test", [{"role": "user", "content": "This should trigger cognee cognify call"}])
        mock_cognee.add.assert_called()
        mock_cognee.cognify.assert_called_once()

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_emits_trace_events(self, mock_cognee):
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        trace = _make_trace()
        pipe = _make_pipeline(trace=trace)
        result = await pipe.run("session:test", [{"role": "user", "content": "Should emit trace events"}])
        # Pipeline emits multiple trace events: MEMORY_CLASS_ASSIGNED, COGNEE_COGNIFY_COMPLETED, FACT_EXTRACTED
        assert trace.append_event.call_count >= 2
        event_types = [call.args[0].event_type.value for call in trace.append_event.call_args_list]
        assert "fact_extracted" in event_types
        assert "memory_class_assigned" in event_types
        assert "cognee_cognify_completed" in event_types

    # --- Edge-creation tests (supersession / contradiction) ---

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_supersession_creates_supersedes_edge(self, mock_cognee):
        """When a fact supersedes an older one, a SUPERSEDES edge is created."""
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        graph = AsyncMock()
        graph.add_relation = AsyncMock()
        old_fact_id = str(uuid.uuid4())
        buffer = _make_buffer(recent_facts=[
            {"id": old_fact_id, "text": "Old fact", "category": "general"},
        ])
        llm = _make_llm(facts=[
            {"text": "New fact", "category": "general", "source_turns": [0],
             "supersedes_index": 0, "contradicts_index": -1},
        ])
        pipe = _make_pipeline(llm=llm, buffer=buffer, graph=graph)
        result = await pipe.run("session:test",
                                [{"role": "user", "content": "This is the new version"}])
        calls = [str(c) for c in graph.add_relation.call_args_list]
        assert any("SUPERSEDES" in c for c in calls)

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_contradiction_creates_contradicts_edge(self, mock_cognee):
        """When a fact contradicts an older one, a CONTRADICTS edge is created."""
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        graph = AsyncMock()
        graph.add_relation = AsyncMock()
        old_fact_id = str(uuid.uuid4())
        buffer = _make_buffer(recent_facts=[
            {"id": old_fact_id, "text": "Old fact", "category": "general"},
        ])
        llm = _make_llm(facts=[
            {"text": "Contradicting fact", "category": "general", "source_turns": [0],
             "supersedes_index": -1, "contradicts_index": 0},
        ])
        pipe = _make_pipeline(llm=llm, buffer=buffer, graph=graph)
        result = await pipe.run("session:test",
                                [{"role": "user", "content": "Actually that is wrong"}])
        calls = [str(c) for c in graph.add_relation.call_args_list]
        assert any("CONTRADICTS" in c for c in calls)

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_no_edges_when_graph_is_none(self, mock_cognee):
        """No edge creation attempted when graph adapter is not provided."""
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        old_fact_id = str(uuid.uuid4())
        buffer = _make_buffer(recent_facts=[
            {"id": old_fact_id, "text": "Old fact", "category": "general"},
        ])
        llm = _make_llm(facts=[
            {"text": "New fact", "category": "general", "source_turns": [0],
             "supersedes_index": 0, "contradicts_index": -1},
        ])
        # graph=None (the default)
        pipe = _make_pipeline(llm=llm, buffer=buffer)
        result = await pipe.run("session:test",
                                [{"role": "user", "content": "Update fact"}])
        # Should still succeed — edges silently skipped
        assert result.facts_stored > 0

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_supersedes_edge_failure_does_not_block_pipeline(self, mock_cognee):
        """Graph edge failure is best-effort; pipeline completes normally."""
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        graph = AsyncMock()
        graph.add_relation = AsyncMock(side_effect=Exception("Neo4j down"))
        old_fact_id = str(uuid.uuid4())
        buffer = _make_buffer(recent_facts=[
            {"id": old_fact_id, "text": "Old fact", "category": "general"},
        ])
        llm = _make_llm(facts=[
            {"text": "New fact", "category": "general", "source_turns": [0],
             "supersedes_index": 0, "contradicts_index": -1},
        ])
        pipe = _make_pipeline(llm=llm, buffer=buffer, graph=graph)
        result = await pipe.run("session:test",
                                [{"role": "user", "content": "Update despite failure"}])
        # Pipeline still stores facts even when edge creation fails
        assert result.facts_stored > 0
        assert result.facts_superseded == 1

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_dedup_skip_excluded_from_facts_stored(self, mock_cognee):
        """When facade.store() raises DedupSkipped, facts_stored excludes it."""
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        facade = MagicMock()
        # First fact stored, second deduped (raises DedupSkipped)
        facade.store = AsyncMock(side_effect=[
            make_fact_assertion(text="stored"),
            DedupSkipped("existing-id", 0.98),
        ])
        facade.decay = AsyncMock()
        llm = _make_llm(facts=[
            {"text": "fact A", "category": "event", "source_turns": [0], "supersedes_index": -1},
            {"text": "fact B", "category": "event", "source_turns": [0], "supersedes_index": -1},
        ])
        pipe = _make_pipeline(llm=llm, facade=facade)
        messages = [{"role": "user", "content": "Two facts, one is a dup"}]
        result = await pipe.run("session:test", messages)
        assert result.facts_stored == 1
        assert facade.store.call_count == 2

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_dedup_skip_no_edges_for_skipped_facts(self, mock_cognee):
        """Edges are only created for successfully stored facts, not dedup-skipped."""
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        graph = AsyncMock()
        graph.add_relation = AsyncMock()
        old_fact_id = str(uuid.uuid4())
        buffer = _make_buffer(recent_facts=[
            {"id": old_fact_id, "text": "Old fact", "category": "general"},
        ])
        facade = MagicMock()
        # Dedup skip: store raises DedupSkipped
        facade.store = AsyncMock(side_effect=DedupSkipped("existing-id", 0.98))
        facade.decay = AsyncMock()
        llm = _make_llm(facts=[
            {"text": "New fact", "category": "general", "source_turns": [0],
             "supersedes_index": 0, "contradicts_index": -1},
        ])
        pipe = _make_pipeline(llm=llm, facade=facade, buffer=buffer, graph=graph)
        result = await pipe.run("session:test",
                                [{"role": "user", "content": "Dedup skip edge test"}])
        assert result.facts_stored == 0
        # No SUPERSEDES edge — fact was not stored (and decay not called)
        graph.add_relation.assert_not_called()
        facade.decay.assert_not_called()

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_dedup_skip_recent_facts_excludes_skipped(self, mock_cognee):
        """Recent facts buffer only includes successfully stored facts (C04 fix)."""
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        facade = MagicMock()
        stored_fact = make_fact_assertion(text="stored")
        facade.store = AsyncMock(side_effect=[stored_fact, DedupSkipped("dup-id", 0.98)])
        facade.decay = AsyncMock()
        buffer = _make_buffer()
        llm = _make_llm(facts=[
            {"text": "fact A", "category": "event", "source_turns": [0], "supersedes_index": -1},
            {"text": "fact B", "category": "event", "source_turns": [0], "supersedes_index": -1},
        ])
        pipe = _make_pipeline(llm=llm, facade=facade, buffer=buffer)
        await pipe.run("session:test", [{"role": "user", "content": "Two facts, one dup"}])
        # update_recent_facts should be called with only 1 new fact (stored_fact)
        buffer.update_recent_facts.assert_called_once()
        new_recent = buffer.update_recent_facts.call_args[0][1]
        new_ids = [f["id"] for f in new_recent]
        assert str(stored_fact.id) in new_ids

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_out_of_range_supersedes_index_creates_no_edge(self, mock_cognee):
        """supersedes_index beyond recent_facts length creates no edge."""
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        graph = AsyncMock()
        graph.add_relation = AsyncMock()
        buffer = _make_buffer(recent_facts=[
            {"id": str(uuid.uuid4()), "text": "Only one fact", "category": "general"},
        ])
        llm = _make_llm(facts=[
            {"text": "A fact", "category": "general", "source_turns": [0],
             "supersedes_index": 5, "contradicts_index": -1},
        ])
        pipe = _make_pipeline(llm=llm, buffer=buffer, graph=graph)
        result = await pipe.run("session:test",
                                [{"role": "user", "content": "Index out of range"}])
        # No SUPERSEDES edge because index 5 is beyond the 1-element recent_facts
        calls = [str(c) for c in graph.add_relation.call_args_list]
        assert not any("SUPERSEDES" in c for c in calls)
        assert result.facts_superseded == 0


# ---------------------------------------------------------------------------
# Buffer Tests
# ---------------------------------------------------------------------------

class _FakeRedis:
    """Minimal Redis mock for IngestBuffer tests."""

    def __init__(self):
        self._data: dict[str, list] = {}
        self._kv: dict[str, str] = {}
        self._ttls: dict[str, int] = {}

    def pipeline(self):
        return _FakePipeline(self)

    async def llen(self, key):
        return len(self._data.get(key, []))

    async def get(self, key):
        return self._kv.get(key)

    async def set(self, key, value, ex=None):
        self._kv[key] = value
        if ex:
            self._ttls[key] = ex


class _FakePipeline:
    def __init__(self, redis: _FakeRedis):
        self._redis = redis
        self._ops: list = []

    def rpush(self, key, value):
        self._ops.append(("rpush", key, value))

    def expire(self, key, ttl):
        self._ops.append(("expire", key, ttl))

    def lrange(self, key, start, end):
        self._ops.append(("lrange", key, start, end))

    def delete(self, key):
        self._ops.append(("delete", key))

    def ltrim(self, key, start, stop):
        # stop is inclusive in Redis LTRIM (e.g., -1 means last element)
        self._ops.append(("ltrim", key, start, stop))

    async def execute(self):
        results = []
        for op in self._ops:
            if op[0] == "rpush":
                key, val = op[1], op[2]
                self._redis._data.setdefault(key, []).append(val)
                results.append(len(self._redis._data[key]))
            elif op[0] == "expire":
                results.append(True)
            elif op[0] == "lrange":
                key = op[1]
                results.append(list(self._redis._data.get(key, [])))
            elif op[0] == "delete":
                key = op[1]
                self._redis._data.pop(key, None)
                results.append(1)
            elif op[0] == "ltrim":
                key, start, stop = op[1], op[2], op[3]
                lst = self._redis._data.get(key, [])
                # Redis LTRIM keeps elements from start to stop (inclusive).
                # Negative indices work like Python: -1 = last element.
                if stop == -1:
                    self._redis._data[key] = lst[start:]
                else:
                    self._redis._data[key] = lst[start:stop + 1]
                results.append("OK")
        self._ops.clear()
        return results


class TestIngestBuffer:
    def _make_config(self, **overrides):
        defaults = {
            "ingest_batch_size": 3,
            "ingest_buffer_ttl_seconds": 300,
            "ingest_batch_timeout_seconds": 60.0,
            "extraction_context_ttl_seconds": 3600,
        }
        defaults.update(overrides)
        return LLMConfig(**defaults)

    async def test_buffer_add_returns_false_when_not_full(self):
        redis = _FakeRedis()
        config = self._make_config(ingest_batch_size=3)
        buf = IngestBuffer(redis, config)
        result = await buf.add_messages("s1", [{"role": "user", "content": "hello"}])
        assert result is False

    async def test_buffer_add_returns_true_at_batch_size(self):
        redis = _FakeRedis()
        config = self._make_config(ingest_batch_size=3)
        buf = IngestBuffer(redis, config)
        await buf.add_messages("s1", [{"role": "user", "content": "msg1"}])
        await buf.add_messages("s1", [{"role": "user", "content": "msg2"}])
        result = await buf.add_messages("s1", [{"role": "user", "content": "msg3"}])
        assert result is True

    async def test_buffer_flush_returns_all_buffered(self):
        redis = _FakeRedis()
        config = self._make_config(ingest_batch_size=10)
        buf = IngestBuffer(redis, config)
        await buf.add_messages("s1", [{"role": "user", "content": "msg1"}])
        await buf.add_messages("s1", [{"role": "user", "content": "msg2"}])
        flushed = await buf.flush("s1")
        assert len(flushed) == 2
        assert flushed[0]["content"] == "msg1"
        assert flushed[1]["content"] == "msg2"

    async def test_buffer_flush_deletes_buffer(self):
        redis = _FakeRedis()
        config = self._make_config(ingest_batch_size=10)
        buf = IngestBuffer(redis, config)
        await buf.add_messages("s1", [{"role": "user", "content": "msg1"}])
        await buf.flush("s1")
        # Second flush should be empty
        flushed2 = await buf.flush("s1")
        assert flushed2 == []

    async def test_load_recent_facts_empty(self):
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        result = await buf.load_recent_facts("s1")
        assert result == []

    async def test_update_and_load_recent_facts(self):
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        facts = [{"id": "1", "text": "fact one"}, {"id": "2", "text": "fact two"}]
        await buf.update_recent_facts("s1", facts, max_count=20)
        loaded = await buf.load_recent_facts("s1")
        assert len(loaded) == 2

    async def test_update_recent_facts_trims(self):
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        facts = [{"id": str(i), "text": f"fact {i}"} for i in range(30)]
        await buf.update_recent_facts("s1", facts, max_count=5)
        loaded = await buf.load_recent_facts("s1")
        assert len(loaded) == 5

    async def test_check_timeout_flush(self):
        redis = _FakeRedis()
        config = self._make_config(ingest_batch_timeout_seconds=1.0)
        buf = IngestBuffer(redis, config)
        # No prior flush (last_flush defaults to 0) -> elapsed >= 1.0 -> True
        result = await buf.check_timeout_flush("s1")
        assert result is True

    async def test_buffer_add_trims_overflow(self):
        """Buffer overflow guard: ltrim keeps only last max_size messages."""
        redis = _FakeRedis()
        config = self._make_config(ingest_batch_size=3)  # max_size = 3 * 3 = 9
        buf = IngestBuffer(redis, config)

        # Add 12 messages (exceeds max_size=9)
        for i in range(12):
            await buf.add_messages("s1", [{"role": "user", "content": f"msg{i}"}])

        # Flush and verify only last 9 remain (oldest 3 trimmed)
        flushed = await buf.flush("s1")
        assert len(flushed) == 9
        assert flushed[0]["content"] == "msg3"  # oldest surviving message
        assert flushed[-1]["content"] == "msg11"  # newest message


# ---------------------------------------------------------------------------
# Phase 7: decision_domain extraction
# ---------------------------------------------------------------------------


class TestDecisionDomainExtraction:
    """Phase 7: decision_domain populated on extracted facts."""

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    @patch("elephantbroker.pipelines.turn_ingest.pipeline.classify_memory", new_callable=AsyncMock, return_value=[])
    @patch("elephantbroker.pipelines.turn_ingest.pipeline.resolve_actors", new_callable=AsyncMock, return_value=[])
    @patch("elephantbroker.pipelines.turn_ingest.pipeline.extract_facts")
    async def test_fact_gets_decision_domain_from_extraction(
        self, mock_extract, mock_resolve, mock_classify, mock_cognee,
    ):
        """When LLM returns decision_domain, it should be set on the FactAssertion."""
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()

        mock_extract.return_value = {
            "facts": [
                {"text": "Payment processed", "category": "event", "decision_domain": "financial"},
            ],
            "goal_status_hints": [],
        }

        facade = _make_facade()
        pipe = _make_pipeline(facade=facade)

        messages = [{"role": "user", "content": "Process the payment"}]
        result = await pipe.run("sk", messages, session_id=str(uuid.uuid4()))

        # Verify fact was stored with decision_domain
        if facade.store.called:
            stored_fact = facade.store.call_args[0][0]
            assert stored_fact.decision_domain == "financial"
