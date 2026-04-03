"""Tests for runtime/identity.py — deterministic UUID generation."""
import uuid

from elephantbroker.runtime.identity import EB_AGENT_NAMESPACE, deterministic_uuid_from


def test_deterministic_uuid_same_input_same_output():
    id1 = deterministic_uuid_from("gw-prod:main")
    id2 = deterministic_uuid_from("gw-prod:main")
    assert id1 == id2


def test_different_inputs_different_uuids():
    id1 = deterministic_uuid_from("gw-prod:main")
    id2 = deterministic_uuid_from("gw-staging:main")
    assert id1 != id2


def test_returns_uuid_type():
    result = deterministic_uuid_from("test:key")
    assert isinstance(result, uuid.UUID)


def test_namespace_is_fixed():
    assert EB_AGENT_NAMESPACE == uuid.UUID("e1e9b4a0-7c3d-4f8e-9a2b-1d5f6e8c0a3b")


def test_uuid_v5_version():
    result = deterministic_uuid_from("gw:agent")
    assert result.version == 5
