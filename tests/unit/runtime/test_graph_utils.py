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
