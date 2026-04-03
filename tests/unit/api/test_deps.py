"""Tests for API dependency injection helpers."""
from unittest.mock import MagicMock

from elephantbroker.api.deps import (
    get_actor_registry,
    get_artifact_store,
    get_container,
    get_evidence_engine,
    get_goal_manager,
    get_memory_store,
    get_procedure_engine,
    get_working_set_manager,
)


def _make_request():
    request = MagicMock()
    container = MagicMock()
    request.app.state.container = container
    return request, container


class TestDeps:
    def test_get_container_returns_container(self):
        request, container = _make_request()
        assert get_container(request) is container

    def test_get_memory_store(self):
        request, container = _make_request()
        assert get_memory_store(request) is container.memory_store

    def test_get_actor_registry(self):
        request, container = _make_request()
        assert get_actor_registry(request) is container.actor_registry

    def test_get_goal_manager(self):
        request, container = _make_request()
        assert get_goal_manager(request) is container.goal_manager

    def test_get_procedure_engine(self):
        request, container = _make_request()
        assert get_procedure_engine(request) is container.procedure_engine

    def test_get_evidence_engine(self):
        request, container = _make_request()
        assert get_evidence_engine(request) is container.evidence_engine

    def test_get_artifact_store(self):
        request, container = _make_request()
        assert get_artifact_store(request) is container.artifact_store

    def test_get_working_set_manager(self):
        request, container = _make_request()
        assert get_working_set_manager(request) is container.working_set_manager
