"""Tests for graph_utils.clean_graph_props()."""
from elephantbroker.runtime.graph_utils import clean_graph_props


class TestCleanGraphProps:
    def test_strips_labels_key(self):
        assert clean_graph_props({"_labels": ["X"], "text": "hi"}) == {"text": "hi"}

    def test_strips_id_key(self):
        assert clean_graph_props({"id": "uuid", "eb_id": "x"}) == {"eb_id": "x"}

    def test_strips_underscore_prefixed(self):
        assert clean_graph_props({"_internal": 1, "name": "v"}) == {"name": "v"}

    def test_deserializes_json_dict(self):
        result = clean_graph_props({"meta": '{"a": 1}'})
        assert result == {"meta": {"a": 1}}

    def test_invalid_json_kept_as_str(self):
        result = clean_graph_props({"meta": "{not json"})
        assert result == {"meta": "{not json"}

    def test_preserves_primitives(self):
        result = clean_graph_props({"count": 5, "flag": True})
        assert result == {"count": 5, "flag": True}

    def test_preserves_lists(self):
        result = clean_graph_props({"tags": ["a", "b"]})
        assert result == {"tags": ["a", "b"]}

    def test_preserves_none_values(self):
        result = clean_graph_props({"ref": None})
        assert result == {"ref": None}

    def test_empty_dict(self):
        assert clean_graph_props({}) == {}

    def test_mixed_input(self):
        raw = {
            "_labels": ["Node"],
            "id": "some-uuid",
            "_internal": True,
            "eb_id": "my-id",
            "text": "hello",
            "meta": '{"key": "val"}',
            "bad_json": "{nope",
            "count": 3,
            "tags": ["a"],
            "ref": None,
        }
        result = clean_graph_props(raw)
        assert result == {
            "eb_id": "my-id",
            "text": "hello",
            "meta": {"key": "val"},
            "bad_json": "{nope",
            "count": 3,
            "tags": ["a"],
            "ref": None,
        }

    # TF-FN-020 G1 — pin the ``{``-prefix-only JSON deserialization behavior.
    # graph_utils.py:25 checks ``v.startswith("{")`` only, so JSON-encoded
    # *arrays* (``[...]``) pass through as-is even though they're structurally
    # the same kind of "Neo4j had to serialize this" data as dicts.
    def test_json_array_string_NOT_deserialized_pin_1163(self):
        """G1 (#1163 DEFENSIVE): pin that ``clean_graph_props`` only
        deserializes JSON *objects* (leading ``{``), not JSON *arrays*
        (leading ``[``). This is the current behavior at
        ``graph_utils.py:25``.

        Why it's DEFENSIVE, not LIVE: no current schema field is stored
        as a ``list[dict]`` that Neo4j would need to serialize. The
        codebase uses the ``*_json: str`` workaround pattern (see
        ``ProcedureDataPoint.steps_json`` at ``datapoints.py:265``, plus
        ``red_line_bindings_json`` and ``approval_requirements_json``)
        where the DataPoint class holds the raw JSON string and its
        ``to_schema()`` method explicitly calls ``json.loads()``.

        If a future schema introduces a plain ``list[dict]`` field that
        Neo4j auto-serializes on write, this test will pin the gap —
        the to_schema pipeline would receive a string instead of a list.
        Resolution would be either extend ``clean_graph_props`` to handle
        ``[``-prefix, or adopt the ``*_json: str`` pattern for that
        field.

        Paired with G4 in ``test_datapoint_reconstruction.py`` which
        pins ProcedureDataPoint's ``*_json`` pattern as the current
        workaround.
        """
        raw = {"tags": '["a", "b"]', "vals": '[1, 2, 3]'}
        result = clean_graph_props(raw)
        # Strings survive unchanged — no `[`-prefix deserialization.
        assert result == {"tags": '["a", "b"]', "vals": '[1, 2, 3]'}
