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

    # TF-FN-020 G1 — POST R2-P3 #1163 RESOLVED: ``clean_graph_props`` now
    # deserializes BOTH ``{``-prefix (objects) AND ``[``-prefix (arrays).
    # Selective opt-out via ``*_json`` suffix preserves the
    # ProcedureDataPoint workaround pattern (see
    # ``test_clean_graph_props_skips_json_suffix_keys_for_strings`` below).
    def test_json_array_string_deserialized_post_1163_fix(self):
        """G1 FLIPPED (#1163 RESOLVED — R2-P3): ``clean_graph_props`` now
        deserializes JSON arrays (``[``-prefix) the same way it
        deserializes JSON objects (``{``-prefix). Mirrors the symmetric
        treatment of structurally-equivalent "Neo4j had to serialize
        this" data.

        Pre-fix this test pinned the gap (``"NOT_deserialized_pin_1163"``).
        Post-fix the symmetry is restored. Fields that need to remain
        as JSON strings (the ``ProcedureDataPoint.steps_json`` pattern)
        opt out via the ``*_json`` suffix — pinned by
        ``test_clean_graph_props_skips_json_suffix_keys_for_strings``.
        """
        raw = {"tags": '["a", "b"]', "vals": '[1, 2, 3]'}
        result = clean_graph_props(raw)
        # Both arrays now deserialize.
        assert result == {"tags": ["a", "b"], "vals": [1, 2, 3]}

    def test_clean_graph_props_skips_json_suffix_keys_for_strings(self):
        """G1-extend (R2-P3 selective rule): keys ending in ``_json``
        opt out of deserialization, even when the value starts with
        ``{`` or ``[``. This preserves the
        ``ProcedureDataPoint.steps_json`` / ``red_line_bindings_json`` /
        ``approval_requirements_json`` workaround where the DataPoint
        class field is typed ``str`` and ``to_schema()`` calls
        ``json.loads()`` inside the method body.

        Pins the new selective rule alongside G1's flipped semantics:
        ``tags`` (no suffix) parses; ``steps_json`` (suffix) stays raw.
        If a future refactor drops the ``*_json`` opt-out, every
        ProcedureDataPoint reconstruction breaks because the str-typed
        field receives a list. This test surfaces the regression
        immediately.
        """
        raw = {
            "tags": '[1, 2]',                # no suffix → deserialized
            "steps_json": '[{"a": 1}]',      # suffix → preserved as str
            "red_line_bindings_json": '["x", "y"]',  # suffix → preserved
            "config": '{"k": "v"}',          # no suffix, dict → still works
        }
        result = clean_graph_props(raw)
        # Non-suffix keys: parsed.
        assert result["tags"] == [1, 2]
        assert result["config"] == {"k": "v"}
        # Suffix keys: preserved as raw JSON strings, untouched.
        assert result["steps_json"] == '[{"a": 1}]'
        assert result["red_line_bindings_json"] == '["x", "y"]'
