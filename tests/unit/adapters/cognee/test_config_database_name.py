"""R2-P1 / TD-64 #1187 RESOLVED — configure_cognee threads gateway_id into
the Cognee vector-db config's ``vector_db_name`` field.

These tests spy on ``cognee.config.set_vector_db_config`` via monkeypatch
and assert the payload carries ``vector_db_name=<gateway_id>`` (or is
absent when gateway_id is empty, preserving back-compat).

Paired with ``test_vector_gateway_filter.py`` (read side) +
``test_container_gateway_id_passed_to_cognee.py`` (wiring order).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.adapters.cognee.config import configure_cognee
from elephantbroker.schemas.config import CogneeConfig


async def _run_configure(monkeypatch, gateway_id: str) -> dict:
    """Drive configure_cognee with a mocked cognee module, returning the
    dict passed to ``set_vector_db_config``."""
    captured: dict[str, object] = {}

    mock_cognee = MagicMock()
    # Capture the kwargs passed to set_vector_db_config for assertion.
    def _capture(cfg: dict) -> None:
        captured.update(cfg)
    mock_cognee.config.set_graph_database_provider = MagicMock()
    mock_cognee.config.set_graph_db_config = MagicMock()
    mock_cognee.config.set_vector_db_provider = MagicMock()
    mock_cognee.config.set_vector_db_config = MagicMock(side_effect=_capture)
    mock_cognee.config.set_llm_config = MagicMock()
    monkeypatch.setattr("elephantbroker.runtime.adapters.cognee.config.cognee", mock_cognee, raising=False)
    # Also ensure the `import cognee` inside configure_cognee hits the mock.
    import sys
    monkeypatch.setitem(sys.modules, "cognee", mock_cognee)
    # Stub out community qdrant adapter registration (imported for side-effect)
    mock_register = MagicMock()
    monkeypatch.setitem(sys.modules, "cognee_community_vector_adapter_qdrant", mock_register)
    # Stub get_embedding_config — configure_cognee reads attributes then assigns.
    mock_embedding_cfg = MagicMock()
    monkeypatch.setattr(
        "cognee.infrastructure.databases.vector.embeddings.config.get_embedding_config",
        lambda: mock_embedding_cfg,
        raising=False,
    )
    # Stub QDrantAdapter import path so the monkey-patch block skips cleanly.
    monkeypatch.setitem(sys.modules, "cognee_community_vector_adapter_qdrant.qdrant_adapter", MagicMock())
    monkeypatch.setitem(sys.modules, "qdrant_client", MagicMock())

    config = CogneeConfig(neo4j_password="x")
    await configure_cognee(config, llm_config=None, gateway_id=gateway_id)
    return captured


async def test_configure_cognee_sets_vector_db_name_when_gateway_id_given(monkeypatch):
    """G1: ``configure_cognee(config, gateway_id="gw-prod")`` passes
    ``vector_db_name="gw-prod"`` in the dict handed to
    ``cognee.config.set_vector_db_config``. That populates Qdrant's
    per-tenant database field (indexed with ``is_tenant:true`` by the
    community adapter), so every point written via ``add_data_points()``
    carries the tenant id on its payload.
    """
    captured = await _run_configure(monkeypatch, gateway_id="gw-prod")
    assert captured.get("vector_db_url")  # sanity: other fields still wired
    assert captured.get("vector_db_name") == "gw-prod"


async def test_configure_cognee_omits_vector_db_name_when_gateway_id_empty(monkeypatch):
    """G2 (regression guard): empty gateway_id omits the tenant field.

    Preserves legacy single-tenant behavior (and pre-R2-P1 Qdrant data
    where ``database_name=""`` on every point). If a future refactor
    accidentally starts always-setting the field even for empty input,
    legacy points would become filter-invisible — this test surfaces it.
    """
    captured = await _run_configure(monkeypatch, gateway_id="")
    # vector_db_url still present; vector_db_name NOT present.
    assert captured.get("vector_db_url")
    assert "vector_db_name" not in captured
