"""Unit tests for the Cognee configuration adapter."""
from __future__ import annotations

import logging
import os
from unittest.mock import MagicMock, patch

from elephantbroker.runtime.adapters.cognee.config import configure_cognee
from elephantbroker.schemas.config import CogneeConfig, LLMConfig


def _cognee_mocks(mock_config_module, mock_embedding_cfg):
    """Build sys.modules dict that mocks cognee + its embedding config subpackage."""
    embedding_config_mod = MagicMock()
    embedding_config_mod.get_embedding_config = MagicMock(return_value=mock_embedding_cfg)
    return {
        "cognee": MagicMock(config=mock_config_module),
        "cognee.infrastructure": MagicMock(),
        "cognee.infrastructure.databases": MagicMock(),
        "cognee.infrastructure.databases.vector": MagicMock(),
        "cognee.infrastructure.databases.vector.embeddings": MagicMock(),
        "cognee.infrastructure.databases.vector.embeddings.config": embedding_config_mod,
        # Community Qdrant adapter — register import is a no-op in tests
        "cognee_community_vector_adapter_qdrant": MagicMock(),
    }


class TestConfigureCognee:
    async def test_sets_graph_provider_to_neo4j(self):
        mock_config_module = MagicMock()
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(CogneeConfig())
            mock_config_module.set_graph_database_provider.assert_called_once_with("neo4j")

    async def test_sets_vector_provider_to_qdrant(self):
        mock_config_module = MagicMock()
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(CogneeConfig())
            mock_config_module.set_vector_db_provider.assert_called_once_with("qdrant")

    async def test_passes_qdrant_url(self):
        mock_config_module = MagicMock()
        cfg = CogneeConfig(qdrant_url="http://qdrant.prod:6333")
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(cfg)
            call_args = mock_config_module.set_vector_db_config.call_args[0][0]
            assert call_args["vector_db_url"] == "http://qdrant.prod:6333"

    async def test_passes_neo4j_credentials(self):
        mock_config_module = MagicMock()
        cfg = CogneeConfig(neo4j_uri="bolt://prod:7687", neo4j_user="admin", neo4j_password="secret")
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(cfg)
            call_args = mock_config_module.set_graph_db_config.call_args[0][0]
            assert call_args["graph_database_url"] == "bolt://prod:7687"
            assert call_args["graph_database_username"] == "admin"
            assert call_args["graph_database_password"] == "secret"

    async def test_sets_access_control_env_var(self):
        mock_config_module = MagicMock()
        os.environ.pop("ENABLE_BACKEND_ACCESS_CONTROL", None)
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(CogneeConfig())
            assert os.environ.get("ENABLE_BACKEND_ACCESS_CONTROL") == "false"

    async def test_with_llm_config_sets_real_model(self):
        mock_config_module = MagicMock()
        llm = LLMConfig(model="openai/gemini/gemini-2.5-pro", endpoint="http://llm:8811/v1", api_key="sk-real")
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(CogneeConfig(), llm_config=llm)
            call_args = mock_config_module.set_llm_config.call_args[0][0]
            assert call_args["llm_model"] == "openai/gemini/gemini-2.5-pro"
            assert call_args["llm_endpoint"] == "http://llm:8811/v1"
            assert call_args["llm_api_key"] == "sk-real"

    async def test_without_llm_config_falls_back(self):
        mock_config_module = MagicMock()
        cfg = CogneeConfig(embedding_model="openai/text-embedding-3-large")
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(cfg)  # no llm_config
            call_args = mock_config_module.set_llm_config.call_args[0][0]
            assert call_args["llm_model"] == "openai/text-embedding-3-large"

    async def test_sets_embedding_config(self):
        mock_config_module = MagicMock()
        mock_embedding_cfg = MagicMock()
        cfg = CogneeConfig(
            embedding_provider="openai",
            embedding_model="openai/text-embedding-3-large",
            embedding_dimensions=1024,
            embedding_endpoint="http://embed:8811/v1",
            embedding_api_key="sk-embed",
        )
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, mock_embedding_cfg)):
            await configure_cognee(cfg)
            assert mock_embedding_cfg.embedding_provider == "openai"
            assert mock_embedding_cfg.embedding_model == "openai/text-embedding-3-large"
            assert mock_embedding_cfg.embedding_dimensions == 1024
            assert mock_embedding_cfg.embedding_endpoint == "http://embed:8811/v1"
            assert mock_embedding_cfg.embedding_api_key == "sk-embed"

    async def test_llm_provider_is_openai(self):
        mock_config_module = MagicMock()
        llm = LLMConfig(model="openai/gemini/gemini-2.5-pro")
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(CogneeConfig(), llm_config=llm)
            call_args = mock_config_module.set_llm_config.call_args[0][0]
            assert call_args["llm_provider"] == "openai"

    async def test_sets_telemetry_and_connection_env_vars(self, monkeypatch):
        """G1: configure_cognee sets COGNEE_DISABLE_TELEMETRY + COGNEE_SKIP_CONNECTION_TEST to 'true'
        via setdefault semantics (preserves operator-set custom values on re-entry).

        Uses ``monkeypatch`` for env-var manipulation so test teardown auto-restores the
        pre-test values -- the suite-wide ``COGNEE_DISABLE_TELEMETRY="true"`` (set at
        ``elephantbroker/__init__.py`` import time) must survive this test so that a
        later test (``tests/unit/test_deployment_fixes.py::TestCogneeTelemetryEnvVar``)
        still observes the import-time value.
        """
        # Fresh: both env vars absent -> configure_cognee sets them to "true"
        monkeypatch.delenv("COGNEE_DISABLE_TELEMETRY", raising=False)
        monkeypatch.delenv("COGNEE_SKIP_CONNECTION_TEST", raising=False)
        mock_config_module = MagicMock()
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(CogneeConfig())
        assert os.environ.get("COGNEE_DISABLE_TELEMETRY") == "true"
        assert os.environ.get("COGNEE_SKIP_CONNECTION_TEST") == "true"

        # setdefault semantics: operator-set custom values must be preserved on re-entry
        monkeypatch.setenv("COGNEE_DISABLE_TELEMETRY", "custom")
        monkeypatch.setenv("COGNEE_SKIP_CONNECTION_TEST", "custom")
        mock_config_module2 = MagicMock()
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module2, MagicMock())):
            await configure_cognee(CogneeConfig())
        assert os.environ.get("COGNEE_DISABLE_TELEMETRY") == "custom"
        assert os.environ.get("COGNEE_SKIP_CONNECTION_TEST") == "custom"

    async def test_init_sets_telemetry_env_var_at_import_time(self):
        """G2: elephantbroker.__init__ sets COGNEE_DISABLE_TELEMETRY at import time.

        Must happen in __init__.py (not at first configure_cognee call) because Cognee reads
        this env var at import time -- any cognee import before configure_cognee would phone home.
        Pins the import-time set in elephantbroker/__init__.py (DEPLOYMENT-FIXES.md §30).
        """
        import inspect

        import elephantbroker
        assert "COGNEE_DISABLE_TELEMETRY" in inspect.getsource(elephantbroker)

    async def test_fallback_emits_warning_log(self, caplog):
        """G3: When llm_config is None, fallback branch emits a WARNING log.

        Pins the F1 fix from commit 3526837 -- operators must see a loud warning that
        cognify() will likely fail because the LLM pool is the embedding endpoint.
        """
        mock_config_module = MagicMock()
        with caplog.at_level(logging.WARNING, logger="elephantbroker.adapters.cognee.config"):
            with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
                await configure_cognee(CogneeConfig())
        assert "LLM config not provided, falling back to embedding config" in caplog.text

    async def test_fallback_uses_unused_when_embedding_key_empty(self):
        """G4: Fallback branch uses api_key='unused' when embedding_api_key is empty."""
        mock_config_module = MagicMock()
        cfg = CogneeConfig(embedding_api_key="")
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, MagicMock())):
            await configure_cognee(cfg)  # no llm_config -> fallback
        call_args = mock_config_module.set_llm_config.call_args[0][0]
        assert call_args["llm_api_key"] == "unused"

    async def test_empty_embedding_endpoint_not_set(self):
        """G5a: Empty embedding_endpoint must NOT overwrite Cognee's existing default.

        The `if config.embedding_endpoint:` guard at config.py protects Cognee's default
        from being clobbered by an EB-side empty string.
        """
        mock_config_module = MagicMock()
        mock_embedding_cfg = MagicMock()
        mock_embedding_cfg.embedding_endpoint = "PRESERVED_DEFAULT"
        cfg = CogneeConfig(embedding_endpoint="")
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, mock_embedding_cfg)):
            await configure_cognee(cfg)
        assert mock_embedding_cfg.embedding_endpoint == "PRESERVED_DEFAULT"

    async def test_empty_embedding_api_key_not_set(self):
        """G5b: Empty embedding_api_key must NOT overwrite Cognee's existing default.

        Same guard pattern as embedding_endpoint -- empty EB config leaves Cognee default intact.
        """
        mock_config_module = MagicMock()
        mock_embedding_cfg = MagicMock()
        mock_embedding_cfg.embedding_api_key = "PRESERVED_DEFAULT"
        cfg = CogneeConfig(embedding_api_key="")
        with patch.dict("sys.modules", _cognee_mocks(mock_config_module, mock_embedding_cfg)):
            await configure_cognee(cfg)
        assert mock_embedding_cfg.embedding_api_key == "PRESERVED_DEFAULT"
