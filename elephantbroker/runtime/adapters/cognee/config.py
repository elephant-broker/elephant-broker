"""Map ElephantBroker config to Cognee SDK settings."""
from __future__ import annotations

import logging
import os
from importlib import metadata as _importlib_metadata

from elephantbroker.schemas.config import CogneeConfig, LLMConfig

# TODO-5-006: Cognee version pin is load-bearing.
# The TD-50 cascade in MemoryStoreFacade._cascade_cognee_data calls Cognee
# internal paths (cognee.modules.users.methods, cognee.modules.data.methods,
# cognee.datasets.delete_data) whose signatures are NOT stabilized across
# Cognee minor versions. Bumping this requires re-verifying each call site.
# See local/TECHNICAL-DEBT.md §"Load-bearing dependency pins".
_SUPPORTED_COGNEE_VERSION = "0.5.3"

_log = logging.getLogger("elephantbroker.adapters.cognee.config")


def _verify_cognee_pin() -> None:
    """Warn if installed Cognee version differs from the verified pin.

    Not an assertion: a mismatch should surface loudly on boot but not block
    startup in case an operator is deliberately testing a new version.
    """
    try:
        installed = _importlib_metadata.version("cognee")
    except _importlib_metadata.PackageNotFoundError:
        _log.warning("Cognee package metadata not found — cannot verify version pin")
        return
    if installed != _SUPPORTED_COGNEE_VERSION:
        _log.warning(
            "Cognee version %s differs from the verified pin %s — "
            "TD-50 cascade paths (MemoryStoreFacade._cascade_cognee_data) use "
            "Cognee internal APIs and MUST be re-verified before running on an "
            "unpinned version. See local/TECHNICAL-DEBT.md §Load-bearing "
            "dependency pins.",
            installed, _SUPPORTED_COGNEE_VERSION,
        )


async def configure_cognee(
    config: CogneeConfig,
    llm_config: LLMConfig | None = None,
    gateway_id: str = "",
) -> None:
    """Apply ElephantBroker config to the Cognee SDK.

    Graph: Neo4j (not the default Kuzu).
    Vector: Qdrant via cognee-community-vector-adapter-qdrant (not the default LanceDB).

    #1187 / TD-64 RESOLVED (R2-P1, path c): ``gateway_id`` is forwarded to
    Cognee's vector-db config as ``vector_db_name`` (Qdrant uses this as
    the tenant / database name on every point payload; the community
    adapter indexes it as a tenant field with ``is_tenant:true``). Points
    written to Qdrant via ``add_data_points()`` now carry
    ``database_name=<gateway_id>`` in their payload, which lets
    ``VectorAdapter.search_similar`` add a ``FieldCondition`` filter on
    the same key to guarantee cross-gateway isolation — closing the
    dedup-leak surface pinned in TF-FN-018 G10. Empty ``gateway_id``
    preserves legacy single-tenant behavior (pre-R2-P1 data stays
    readable under the empty default).
    """
    _verify_cognee_pin()
    import cognee
    from cognee.infrastructure.databases.vector.embeddings.config import get_embedding_config

    # Register the community Qdrant adapter before any Cognee vector operations.
    # This populates cognee's supported_databases registry with "qdrant".
    from cognee_community_vector_adapter_qdrant import register  # noqa: F401

    # Disable multi-user access control for dev/test
    os.environ.setdefault("ENABLE_BACKEND_ACCESS_CONTROL", "false")

    # Skip Cognee's LLM connection probe — EB validates backends independently
    os.environ.setdefault("COGNEE_SKIP_CONNECTION_TEST", "true")

    # Disable Cognee's built-in usage telemetry (phones home to Cognee servers)
    os.environ.setdefault("COGNEE_DISABLE_TELEMETRY", "true")

    # Graph database: Neo4j (not the default Kuzu)
    cognee.config.set_graph_database_provider("neo4j")
    cognee.config.set_graph_db_config({
        "graph_database_url": config.neo4j_uri,
        "graph_database_username": config.neo4j_user,
        "graph_database_password": config.neo4j_password,
    })

    # Vector database: Qdrant (not the default LanceDB)
    cognee.config.set_vector_db_provider("qdrant")
    vector_db_cfg: dict[str, object] = {
        "vector_db_url": config.qdrant_url,
    }
    # #1187 / TD-64 path (c): populate Qdrant's per-tenant database name
    # with the gateway_id so every point payload carries the tenant
    # identifier and search_similar can filter on it via the community
    # adapter's `is_tenant:true` payload index. Skip when gateway_id is
    # empty (dev / single-tenant fallback) — Cognee's default behavior
    # is unchanged.
    if gateway_id:
        vector_db_cfg["vector_db_name"] = gateway_id
    cognee.config.set_vector_db_config(vector_db_cfg)

    # Fix: community Qdrant adapter hardcodes port=6333, overriding URL.
    # Monkey-patch to use URL as-is (respecting our configured port).
    # Tested against cognee-community-vector-adapter-qdrant for Cognee v0.5.x.
    try:
        from cognee_community_vector_adapter_qdrant.qdrant_adapter import QDrantAdapter
        from qdrant_client import AsyncQdrantClient as _AQC

        if not getattr(QDrantAdapter, "_eb_patched", False):
            _orig_get_client = QDrantAdapter.get_qdrant_client

            def _patched_get_client(self):
                if self.url is not None:
                    return _AQC(url=self.url, api_key=self.api_key)
                return _orig_get_client(self)

            QDrantAdapter.get_qdrant_client = _patched_get_client
            QDrantAdapter._eb_patched = True
    except ImportError:
        pass  # adapter not installed
    except AttributeError:
        import logging
        logging.getLogger("elephantbroker.adapters.cognee.config").warning(
            "QDrantAdapter API changed — Qdrant port monkey-patch skipped. Verify Cognee adapter compatibility."
        )

    # LLM config — Cognee needs a real LLM for cognify() entity/relationship extraction
    if llm_config:
        cognee.config.set_llm_config({
            "llm_provider": "openai",
            "llm_model": llm_config.model,
            "llm_endpoint": llm_config.endpoint,
            "llm_api_key": llm_config.api_key,
        })
    else:
        _log.warning("LLM config not provided, falling back to embedding config")
        # Fallback: use embedding config (may fail on cognify but allows basic operations)
        cognee.config.set_llm_config({
            "llm_provider": config.embedding_provider,
            "llm_model": config.embedding_model,
            "llm_endpoint": config.embedding_endpoint,
            "llm_api_key": config.embedding_api_key or "unused",
        })

    # Embedding config — Cognee uses this for chunk/triplet embedding during cognify()
    embedding_cfg = get_embedding_config()
    embedding_cfg.embedding_provider = config.embedding_provider
    embedding_cfg.embedding_model = config.embedding_model
    embedding_cfg.embedding_dimensions = config.embedding_dimensions
    if config.embedding_endpoint:
        embedding_cfg.embedding_endpoint = config.embedding_endpoint
    if config.embedding_api_key:
        embedding_cfg.embedding_api_key = config.embedding_api_key
