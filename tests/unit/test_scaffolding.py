"""Tests for project scaffolding — verifies all expected files exist and are importable."""
import importlib
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent

SCHEMA_MODULES = [
    "elephantbroker.schemas.base",
    "elephantbroker.schemas.actor",
    "elephantbroker.schemas.goal",
    "elephantbroker.schemas.fact",
    "elephantbroker.schemas.procedure",
    "elephantbroker.schemas.evidence",
    "elephantbroker.schemas.trace",
    "elephantbroker.schemas.working_set",
    "elephantbroker.schemas.profile",
    "elephantbroker.schemas.scoring",
    "elephantbroker.schemas.artifact",
    "elephantbroker.schemas.context",
    "elephantbroker.schemas.config",
    "elephantbroker.schemas.lifecycle",
    "elephantbroker.schemas.tiers",
]

INTERFACE_MODULES = [
    "elephantbroker.runtime.interfaces.actor_registry",
    "elephantbroker.runtime.interfaces.goal_manager",
    "elephantbroker.runtime.interfaces.memory_store",
    "elephantbroker.runtime.interfaces.working_set",
    "elephantbroker.runtime.interfaces.context_assembler",
    "elephantbroker.runtime.interfaces.compaction_engine",
    "elephantbroker.runtime.interfaces.procedure_engine",
    "elephantbroker.runtime.interfaces.evidence_engine",
    "elephantbroker.runtime.interfaces.guard_engine",
    "elephantbroker.runtime.interfaces.artifact_store",
    "elephantbroker.runtime.interfaces.retrieval",
    "elephantbroker.runtime.interfaces.rerank",
    "elephantbroker.runtime.interfaces.stats",
    "elephantbroker.runtime.interfaces.consolidation",
    "elephantbroker.runtime.interfaces.profile_registry",
    "elephantbroker.runtime.interfaces.trace_ledger",
    "elephantbroker.runtime.interfaces.scoring_tuner",
]


class TestProjectStructure:
    def test_all_schema_files_exist(self):
        for mod_name in SCHEMA_MODULES:
            mod = importlib.import_module(mod_name)
            assert mod is not None, f"Failed to import {mod_name}"

    def test_all_interface_files_exist(self):
        for mod_name in INTERFACE_MODULES:
            mod = importlib.import_module(mod_name)
            assert mod is not None, f"Failed to import {mod_name}"

    def test_schemas_use_pydantic_v2(self):
        from pydantic import BaseModel

        for mod_name in SCHEMA_MODULES:
            mod = importlib.import_module(mod_name)
            for attr_name in dir(mod):
                attr = getattr(mod, attr_name)
                if isinstance(attr, type) and attr is not BaseModel:
                    if issubclass(attr, BaseModel):
                        assert hasattr(attr, "model_dump"), f"{mod_name}.{attr_name} is not pydantic v2"

    def test_pyproject_toml_exists(self):
        assert (REPO_ROOT / "pyproject.toml").exists()

    def test_gitignore_exists(self):
        assert (REPO_ROOT / ".gitignore").exists()

    def test_docker_compose_exists(self):
        assert (REPO_ROOT / "infrastructure" / "docker-compose.yml").exists()
