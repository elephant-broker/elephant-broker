"""Tests for TypeScript type codegen."""
import json
import tempfile
from pathlib import Path

from elephantbroker.codegen.generate_ts_types import generate_json_schemas, generate_enum_ts


class TestGenerateJsonSchemas:
    def test_generates_fact_assertion_schema(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = generate_json_schemas(tmpdir)
            assert "FactAssertion" in result
            schema_path = result["FactAssertion"]
            assert schema_path.exists()
            schema = json.loads(schema_path.read_text())
            assert schema["type"] == "object"

    def test_all_schemas_valid_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = generate_json_schemas(tmpdir)
            for name, path in result.items():
                schema = json.loads(path.read_text())
                assert "type" in schema or "$defs" in schema, f"{name} missing type"

    def test_schema_includes_memory_class(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = generate_json_schemas(tmpdir)
            schema = json.loads(result["FactAssertion"].read_text())
            schema_str = json.dumps(schema)
            assert "memory_class" in schema_str


class TestGenerateEnumTs:
    def test_generates_scope_type(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "enums.ts"
            generate_enum_ts(out)
            content = out.read_text()
            assert "Scope" in content
            assert '"global"' in content
            assert '"session"' in content

    def test_generates_memory_class_type(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "enums.ts"
            generate_enum_ts(out)
            content = out.read_text()
            assert "MemoryClass" in content
            assert '"episodic"' in content
            assert '"semantic"' in content
