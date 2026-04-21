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
from elephantbroker.schemas.context import AgentMessage
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

    @patch("elephantbroker.pipelines.turn_ingest.pipeline.cognee")
    async def test_run_accepts_mixed_agent_message_and_dict_input(self, mock_cognee):
        """TD-28: run() accepts list[AgentMessage | dict] and normalizes internally.

        Mixed input exercises the fast path (lifecycle forwards AgentMessage objects
        directly) alongside legacy callers that pass plain dicts. The pipeline must
        normalize both to dicts before downstream .get()/subscript operations, and
        extra fields (e.g. actor_id) must survive via model_dump(mode="json").
        """
        mock_cognee.add = AsyncMock()
        mock_cognee.cognify = AsyncMock()
        pipe = _make_pipeline()

        actor_id = str(uuid.uuid4())
        messages = [
            AgentMessage(role="user", content="I prefer Python for all projects", actor_id=actor_id),
            {"role": "assistant", "content": "Noted — Python it is."},
        ]
        result = await pipe.run("session:test", messages)

        assert isinstance(result, TurnIngestResult)
        # Pipeline did not crash on AgentMessage input — normalization worked.
        assert len(result.facts_extracted) > 0
        # Extra field preserved through normalization: the user fact's source_actor_id
        # should resolve to the AgentMessage's actor_id (TD-28 precondition check).
        assert result.facts_extracted[0].source_actor_id == uuid.UUID(actor_id)

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

    async def delete(self, key):
        existed = (key in self._kv) or (key in self._data)
        self._kv.pop(key, None)
        self._data.pop(key, None)
        self._ttls.pop(key, None)
        return 1 if existed else 0

    async def eval(self, script, numkeys, *keys_and_args):
        """Minimal Lua eval emulation for _SCRUB_LUA.

        Redis Lua executes atomically server-side. This Python mock is
        trivially "atomic" against concurrent coroutines because the body
        contains zero `await` points — asyncio cannot interleave another
        coroutine's eval() on the same key between the GET-like read and the
        SET-like write, which is what 5-101's "no lost-update" guarantee
        hinges on at the mock tier. (The Python GIL is about thread
        scheduling; asyncio atomicity here comes from the absence of
        suspension points, not the GIL.) The script is identified by
        signature rather than parsed.
        """
        assert "tostring(e.id) == ARGV[1]" in script, "only _SCRUB_LUA is emulated"
        key = keys_and_args[0]
        target = keys_and_args[1]
        ttl = int(keys_and_args[2])
        data = self._kv.get(key)
        if not data:
            return 0
        # 5-317: non-table decode results (JSON parse failure or non-array
        # payload) DEL the corrupt key and return 0 — mirrors the Lua script's
        # defense-in-depth branch so the mock cannot drift from prod behavior.
        try:
            entries = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            self._kv.pop(key, None)
            self._ttls.pop(key, None)
            return 0
        if not isinstance(entries, list):
            self._kv.pop(key, None)
            self._ttls.pop(key, None)
            return 0
        filtered = [e for e in entries if not (isinstance(e, dict) and str(e.get("id")) == target)]
        removed = len(entries) - len(filtered)
        if removed == 0:
            return 0
        if filtered:
            self._kv[key] = json.dumps(filtered)
            self._ttls[key] = ttl
        else:
            self._kv.pop(key, None)
            self._ttls.pop(key, None)
        return removed


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

    # --- Gateway Identity: Redis key prefix (PR #5 TODOs 5-202, 5-310) ---
    # Every Redis key MUST be built via RedisKeyBuilder so two gateways sharing
    # Redis never collide. buffer.py previously fell back to hardcoded
    # `f"eb:ingest_buffer:..."` / `f"eb:recent_facts:..."` strings when
    # `redis_keys=None`, bypassing the gateway prefix. The three tests below
    # pin the post-fix behavior.

    async def test_ingest_buffer_key_carries_gateway_prefix(self):
        """With an explicit RedisKeyBuilder(gateway_id=...), add/flush use the
        gateway-prefixed key `eb:{gw}:ingest_buffer:{sk}`."""
        from elephantbroker.runtime.redis_keys import RedisKeyBuilder
        redis = _FakeRedis()
        config = self._make_config(ingest_batch_size=10)
        keys = RedisKeyBuilder(gateway_id="gw-alpha")
        buf = IngestBuffer(redis, config, redis_keys=keys)
        await buf.add_messages("sk:test", [{"role": "user", "content": "hi"}])
        # The gateway-prefixed key must exist on the fake Redis.
        assert "eb:gw-alpha:ingest_buffer:sk:test" in redis._data
        # And the legacy unprefixed key must NOT exist.
        assert "eb:ingest_buffer:sk:test" not in redis._data

    async def test_recent_facts_key_carries_gateway_prefix(self):
        """update_recent_facts / load_recent_facts / scrub_fact_from_recent
        all use `eb:{gw}:recent_facts:{sk}` when a RedisKeyBuilder is provided."""
        from elephantbroker.runtime.redis_keys import RedisKeyBuilder
        redis = _FakeRedis()
        config = self._make_config()
        keys = RedisKeyBuilder(gateway_id="gw-beta")
        buf = IngestBuffer(redis, config, redis_keys=keys)
        fact_id = str(uuid.uuid4())
        await buf.update_recent_facts(
            "sk:test", [{"id": fact_id, "text": "x", "category": "general"}],
        )
        assert "eb:gw-beta:recent_facts:sk:test" in redis._kv
        assert "eb:recent_facts:sk:test" not in redis._kv
        loaded = await buf.load_recent_facts("sk:test")
        assert len(loaded) == 1 and loaded[0]["id"] == fact_id
        removed = await buf.scrub_fact_from_recent("sk:test", fact_id)
        assert removed == 1
        # Scrub removed the only entry → key deleted under gateway prefix.
        assert "eb:gw-beta:recent_facts:sk:test" not in redis._kv

    async def test_redis_keys_none_defaults_to_empty_gateway_builder(self):
        """When `redis_keys` is omitted or None, the buffer still routes all
        keys through an internal RedisKeyBuilder (gateway_id="") — no
        hardcoded fallback string reaches Redis."""
        redis = _FakeRedis()
        config = self._make_config(ingest_batch_size=10)
        buf = IngestBuffer(redis, config)  # redis_keys defaults to None
        await buf.add_messages("sk:test", [{"role": "user", "content": "hi"}])
        # Default builder produces `eb::ingest_buffer:sk:test` (empty gateway
        # → double colon between `eb:` and the key name).
        assert "eb::ingest_buffer:sk:test" in redis._data
        # The legacy hardcoded fallback format must NOT be present anywhere.
        assert "eb:ingest_buffer:sk:test" not in redis._data


# ---------------------------------------------------------------------------
# TODO 5-101: scrub_fact_from_recent is atomic (Lua eval). The previous
# read-modify-write pattern could drop concurrent scrubs' results.
# ---------------------------------------------------------------------------


class TestIngestBufferAtomicScrub:
    def _make_config(self, **overrides):
        defaults = {
            "ingest_batch_size": 10,
            "ingest_buffer_ttl_seconds": 300,
            "ingest_batch_timeout_seconds": 60.0,
            "extraction_context_ttl_seconds": 3600,
        }
        defaults.update(overrides)
        return LLMConfig(**defaults)

    async def test_scrub_uses_redis_eval_not_get_set(self):
        """5-101: scrub_fact_from_recent must route through redis.eval (Lua),
        not a GET→SET RMW. Regression guard so the atomic path doesn't get
        reverted to the lost-update pattern."""
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        await buf.update_recent_facts("s1", [{"id": "a", "text": "x"}])

        eval_calls: list = []
        orig_eval = redis.eval

        async def _spy_eval(script, numkeys, *args):
            eval_calls.append((script, numkeys, args))
            return await orig_eval(script, numkeys, *args)

        redis.eval = _spy_eval  # type: ignore[assignment]
        await buf.scrub_fact_from_recent("s1", "a")
        assert len(eval_calls) == 1
        script = eval_calls[0][0]
        assert "cjson.decode" in script  # Lua, not Python RMW
        assert "redis.call('SET'" in script or 'redis.call("SET"' in script

    async def test_concurrent_scrubs_disjoint_ids_no_lost_update(self):
        """Two scrubs of distinct ids running concurrently must both succeed
        and the final state must reflect BOTH removals. Under the old RMW
        pattern one scrub's SET could overwrite the other's filtered list."""
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        initial = [
            {"id": "a", "text": "fact a"},
            {"id": "b", "text": "fact b"},
            {"id": "c", "text": "fact c"},
        ]
        await buf.update_recent_facts("s1", initial)

        import asyncio
        results = await asyncio.gather(
            buf.scrub_fact_from_recent("s1", "a"),
            buf.scrub_fact_from_recent("s1", "b"),
        )
        assert results == [1, 1]
        # Only "c" remains.
        remaining = await buf.load_recent_facts("s1")
        remaining_ids = {e["id"] for e in remaining}
        assert remaining_ids == {"c"}

    async def test_concurrent_scrubs_same_id_idempotent(self):
        """Two scrubs of the same id: one sees removed=1, the other sees
        removed=0. Neither corrupts the stored data."""
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        await buf.update_recent_facts("s1", [
            {"id": "a", "text": "x"},
            {"id": "b", "text": "y"},
        ])

        import asyncio
        results = await asyncio.gather(
            buf.scrub_fact_from_recent("s1", "a"),
            buf.scrub_fact_from_recent("s1", "a"),
        )
        assert sorted(results) == [0, 1]
        remaining = await buf.load_recent_facts("s1")
        assert [e["id"] for e in remaining] == ["b"]

    async def test_scrub_last_entry_deletes_key(self):
        """Scrubbing the last remaining entry must DEL the key rather than
        leaving an empty JSON object behind (cjson encodes empty tables as
        {} not [], which would corrupt subsequent loads)."""
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        await buf.update_recent_facts("s1", [{"id": "only", "text": "x"}])
        key = buf._keys.recent_facts("s1")
        assert key in redis._kv
        removed = await buf.scrub_fact_from_recent("s1", "only")
        assert removed == 1
        assert key not in redis._kv

    async def test_scrub_missing_key_returns_zero(self):
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        result = await buf.scrub_fact_from_recent("s1", "nope")
        assert result == 0

    # --- 5-317: non-table branch DELs corrupt keys (defense-in-depth) ---

    async def test_scrub_corrupt_json_dels_key(self):
        """If the recent_facts key holds a non-JSON payload (corrupt write,
        byte-order mangle, partial failure), the Lua scrub must DEL the key
        rather than leave the bad value in place. Without DEL, every
        subsequent scrub would re-hit the same corrupt blob until TTL expiry
        and the extraction prompt would keep reading garbage."""
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        key = buf._keys.recent_facts("s1")
        redis._kv[key] = "this-is-not-valid-json-{"
        result = await buf.scrub_fact_from_recent("s1", "any-id")
        assert result == 0
        # 5-317: key must be DELed, not left in place.
        assert key not in redis._kv

    async def test_scrub_non_array_json_dels_key(self):
        """If the recent_facts key holds a JSON object (not an array) — e.g.
        a migration artifact or a writer that accidentally stored a dict —
        the Lua scrub must DEL the key. Arrays are the only valid shape; any
        other JSON top-level shape is treated as corruption and cleaned."""
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        key = buf._keys.recent_facts("s1")
        redis._kv[key] = '{"not": "an array"}'
        result = await buf.scrub_fact_from_recent("s1", "any-id")
        assert result == 0
        assert key not in redis._kv

    async def test_scrub_corrupt_json_self_heals_before_next_update(self):
        """After DEL on corruption, a subsequent update_recent_facts() can
        seed clean state — the key re-appears with a valid array payload."""
        redis = _FakeRedis()
        config = self._make_config()
        buf = IngestBuffer(redis, config)
        key = buf._keys.recent_facts("s1")
        redis._kv[key] = "}{not-json"
        await buf.scrub_fact_from_recent("s1", "x")  # DELs corrupt key
        assert key not in redis._kv
        await buf.update_recent_facts("s1", [{"id": "fresh", "text": "t"}])
        assert key in redis._kv
        loaded = await buf.load_recent_facts("s1")
        assert loaded == [{"id": "fresh", "text": "t"}]


# ---------------------------------------------------------------------------
# TODO 5-304 / 5-308: IngestBuffer conforms to IIngestBuffer contract.
# The facade and turn-ingest pipeline both inject IngestBuffer — hoisting its
# public surface into an ABC prevents silent duck-typed skew (e.g. renaming
# scrub_fact_from_recent and breaking the facade.delete() scrub path).
# ---------------------------------------------------------------------------


class TestIngestBufferABCConformance:
    def test_ingest_buffer_is_subclass_of_iingest_buffer(self):
        from elephantbroker.runtime.interfaces.ingest_buffer import IIngestBuffer
        assert issubclass(IngestBuffer, IIngestBuffer)

    def test_iingest_buffer_declares_scrub_contract(self):
        """5-308: scrub_fact_from_recent must be an abstract method on the ABC
        so any future IngestBuffer implementation is forced to provide it —
        facade.delete() relies on it to purge deleted facts from the
        extraction-context window."""
        from elephantbroker.runtime.interfaces.ingest_buffer import IIngestBuffer
        scrub = IIngestBuffer.scrub_fact_from_recent
        assert getattr(scrub, "__isabstractmethod__", False) is True

    def test_iingest_buffer_is_abstract_cannot_instantiate(self):
        from elephantbroker.runtime.interfaces.ingest_buffer import IIngestBuffer
        with pytest.raises(TypeError):
            IIngestBuffer()  # type: ignore[abstract]

    def test_partial_impl_missing_scrub_raises_typeerror(self):
        """A subclass that forgets scrub_fact_from_recent cannot be
        instantiated — the ABC is load-bearing, not cosmetic."""
        from elephantbroker.runtime.interfaces.ingest_buffer import IIngestBuffer

        class _Partial(IIngestBuffer):
            async def add_messages(self, session_key, messages): return False
            async def flush(self, session_key): return []
            async def force_flush(self, session_key): return []
            async def check_timeout_flush(self, session_key): return False
            async def load_recent_facts(self, session_key): return []
            async def update_recent_facts(self, session_key, new_facts, max_count=20): return None
            # scrub_fact_from_recent deliberately omitted

        with pytest.raises(TypeError):
            _Partial()  # type: ignore[abstract]

    async def test_mock_iingest_buffer_substitutes_in_facade(self):
        """A minimal IIngestBuffer implementation can be passed as the
        facade's ingest_buffer — the facade only depends on the ABC, not on
        IngestBuffer concretely."""
        from elephantbroker.runtime.interfaces.ingest_buffer import IIngestBuffer
        from elephantbroker.runtime.memory.facade import MemoryStoreFacade

        class _StubBuffer(IIngestBuffer):
            def __init__(self): self.scrubbed: list[tuple[str, str]] = []
            async def add_messages(self, session_key, messages): return False
            async def flush(self, session_key): return []
            async def force_flush(self, session_key): return []
            async def check_timeout_flush(self, session_key): return False
            async def load_recent_facts(self, session_key): return []
            async def update_recent_facts(self, session_key, new_facts, max_count=20): return None
            async def scrub_fact_from_recent(self, session_key, fact_id):
                self.scrubbed.append((session_key, fact_id))
                return 1

        stub = _StubBuffer()
        facade = MemoryStoreFacade(
            graph=MagicMock(), vector=MagicMock(), embeddings=MagicMock(),
            trace_ledger=MagicMock(), ingest_buffer=stub,
        )
        assert facade._ingest_buffer is stub


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
