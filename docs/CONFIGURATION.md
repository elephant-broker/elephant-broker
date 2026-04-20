# ElephantBroker -- Configuration Reference

> Complete configuration reference for ElephantBroker Cognitive Runtime.
> Generated 2026-03-28.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Environment Variables](#environment-variables)
3. [YAML Configuration](#yaml-configuration)
4. [Configuration Schemas](#configuration-schemas)
5. [Profile System](#profile-system)
6. [Infrastructure & Deployment](#infrastructure--deployment)
7. [TypeScript Plugin Configuration](#typescript-plugin-configuration)
8. [Redis Keys & TTLs](#redis-keys--ttls)
9. [Neo4j & Qdrant Configuration](#neo4j--qdrant-configuration)
10. [CLI Reference](#cli-reference)
11. [Observability & Metrics](#observability--metrics)
12. [Hardcoded Constants](#hardcoded-constants)
13. [Second Pass: Configuration Gap Analysis](#13-second-pass-configuration-gap-analysis)
14. [Third Pass: Configuration Error Behavior & Troubleshooting](#14-third-pass-configuration-error-behavior--troubleshooting)
15. [LLM Prompt Template Reference](#15-llm-prompt-template-reference)
16. [Configuration Value Guide](#16-configuration-value-guide)
17. [Production Security Hardening Guide](#17-production-security-hardening-guide)

---

## Quick Start

### Minimal `.env` for Local Development

```bash
# Infrastructure (if not using defaults)
EB_NEO4J_URI=bolt://localhost:7687
EB_NEO4J_USER=neo4j
EB_NEO4J_PASSWORD=elephant_dev
EB_QDRANT_URL=http://localhost:6333
EB_REDIS_URL=redis://localhost:6379

# LLM (required) — openai/ prefix REQUIRED (Cognee strips it before sending to LiteLLM)
EB_LLM_MODEL=openai/gemini/gemini-2.5-pro
EB_LLM_API_KEY=your-api-key-here
EB_LLM_ENDPOINT=http://localhost:8811/v1  # LiteLLM proxy

# Embedding (required) — must match what your LiteLLM proxy serves
EB_EMBEDDING_MODEL=gemini/text-embedding-004
EB_EMBEDDING_DIMENSIONS=768
EB_EMBEDDING_API_KEY=your-api-key-here

# Gateway identity (required)
EB_GATEWAY_ID=gw-dev-local
```

### Start Infrastructure

```bash
cd infrastructure/
docker compose up -d   # Neo4j, Qdrant, Redis
```

### Start the Runtime

```bash
python -m elephantbroker.api.app
```

### Configuration Loading Priority

1. **Environment variables** (`EB_*`) -- always checked, highest priority
2. **YAML file** (`--config path/to/config.yaml`) -- base configuration
3. **Hardcoded defaults** -- fallback values in Pydantic schemas

Use `ElephantBrokerConfig.load(path)` (or `load(None)` for the packaged default) — this is the single entry point after the F2/F3 unification. The legacy `from_env()` classmethod has been removed; the runtime, CLI, and tests all converge on `load()`, which always reads YAML first (the packaged `elephantbroker/config/default.yaml` when no path is given) and then applies every binding in `ENV_OVERRIDE_BINDINGS` on top.

See the [Environment Variables](#environment-variables) section for the complete list, or the [YAML Configuration](#yaml-configuration) section for file-based setup.

### Deployment Mode

ElephantBroker operates in **FULL mode** — both the Memory Plugin and Context Engine Plugin active. This is the recommended configuration for ~90% of production deployments and is the current default (the runtime unconditionally instantiates all modules; see `container.py` line 551).

| Mode | Plugins Installed | What You Get |
|------|-------------------|--------------|
| **FULL** ✓ recommended | `elephantbroker-memory` + `elephantbroker-context` | Complete stack: durable memory (Neo4j + Qdrant), working set scoring, context assembly, compaction, guards, consolidation |
| MEMORY_ONLY | `elephantbroker-memory` only | Memory storage + retrieval without context lifecycle. Edge case — useful for incremental rollout or low-resource deployments |
| CONTEXT_ONLY | `elephantbroker-context` only | Context assembly without Cognee-backed memory persistence. Edge case only |

**Today, always deploy FULL mode.** MEMORY_ONLY and CONTEXT_ONLY are architecturally supported (schemas and container wiring exist) but are not deployed or tested in production (see TD-15 in `local/TECHNICAL-DEBT.md`). To enable FULL mode: install both plugins and set no additional tier flag — it is the default.

See [Section 8: Tier Capability Gating](#8-tier-capability-gating) for the full module availability matrix per tier.


---


## ElephantBroker Environment Variable Reference

**Source of truth:** `elephantbroker/schemas/config.py` — specifically the `ENV_OVERRIDE_BINDINGS` registry (the single canonical list of every `EB_*` → YAML field mapping) and `ElephantBrokerConfig.load()` — plus the TS plugin source files and HITL middleware config.

**Resolution order:** env var (if set) > YAML value > model default. After the F2/F3 unification there is exactly one config load path: `load()` always reads YAML first (packaged default if no `--config` is given) and then applies every binding in `ENV_OVERRIDE_BINDINGS` on top. There is no longer a "curated subset" — all 72 bindings are honored in both env-only and YAML+env modes. The `Env override` column below records whether a given variable has a binding in the registry.

---

### 1. Identity

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_GATEWAY_ID` | **Yes** (TS plugins require it; Python defaults to `"local"`) | `"local"` (Python), none/fail (TS) | string | Python runtime, TS plugins, Docker, `ebrun` CLI | `gw-prod`, `gw-prod-assistant`, `local` | No | Yes |
| `EB_GATEWAY_SHORT_NAME` | No | First 8 chars of `EB_GATEWAY_ID` | string | Python runtime, TS plugins | `prod`, `admin` | No | Yes |
| `EB_ORG_ID` | No | `None` | string | Python runtime, Docker | `org-acme-uuid` | No | Yes |
| `EB_TEAM_ID` | No | `None` | string | Python runtime, Docker | `team-backend-uuid` | No | Yes |
| `EB_ACTOR_ID` | No | `""` (falls back to `~/.elephantbroker/config.json`) | string | `ebrun` CLI only | UUID | No | No |
| `EB_AGENT_AUTHORITY_LEVEL` | No | `0` | int | Python runtime | `0`, `1`, `5` | No | Yes |
| `EB_PROFILE` | No | `"coding"` | string | TS plugins only | `coding`, `research`, `managerial`, `worker`, `personal_assistant` | No | No |
| `EB_DEFAULT_PROFILE` | No | `"coding"` | string | Python runtime | `coding`, `research`, `managerial`, `worker`, `personal_assistant` | No | Yes |

### 2. Database

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_NEO4J_URI` | Recommended | `"bolt://localhost:7687"` | string | Python runtime, Docker | `bolt://10.10.0.10:7687` | No | Yes |
| `EB_NEO4J_USER` | No | `"neo4j"` | string | Python runtime | `neo4j` | No | Yes |
| `EB_NEO4J_PASSWORD` | No | `"elephant_dev"` | string | Python runtime | `elephant_dev`, `<production-password>` | **Yes** | Yes |
| `EB_QDRANT_URL` | Recommended | `"http://localhost:6333"` | string | Python runtime, Docker | `http://10.10.0.10:6333` | No | Yes |
| `EB_REDIS_URL` | Recommended | `"redis://localhost:6379"` | string | Python runtime, Docker | `redis://10.10.0.10:6379` | No | Yes |
| `EB_DEFAULT_DATASET` | No | `"elephantbroker"` | string | Python runtime | `elephantbroker` | No | Yes |

### 3. LLM (Primary -- extraction, classification, summarization)

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_LLM_API_KEY` | **Yes** (if endpoint needs auth) | `""` (falls back to `EB_EMBEDDING_API_KEY`) | string | Python runtime | API key | **Yes** | Yes |
| `EB_LLM_MODEL` | No | `"openai/gemini/gemini-2.5-pro"` | string | Python runtime | `openai/gemini/gemini-2.5-pro`, `openai/gpt-4o` | No | Yes |
| `EB_LLM_ENDPOINT` | No | `"http://localhost:8811/v1"` | string | Python runtime | `http://litellm:8811/v1` | No | Yes |
| `EB_LLM_MAX_TOKENS` | No | `8192` | int | Python runtime | `8192`, `16384` | No | Yes |
| `EB_LLM_TEMPERATURE` | No | `0.1` | float | Python runtime | `0.1`, `0.3` | No | Yes |
| `EB_LLM_EXTRACTION_MAX_INPUT_TOKENS` | No | `4000` | int | Python runtime | `4000` | No | Yes |
| `EB_LLM_EXTRACTION_MAX_OUTPUT_TOKENS` | No | `16384` | int | Python runtime | `16384` | No | Yes |
| `EB_LLM_EXTRACTION_MAX_FACTS` | No | `10` | int | Python runtime | `10`, `20` | No | Yes |
| `EB_LLM_SUMMARIZATION_MAX_OUTPUT_TOKENS` | No | `200` | int | Python runtime | `200` | No | Yes |
| `EB_LLM_SUMMARIZATION_MIN_CHARS` | No | `500` | int | Python runtime | `500` | No | Yes |

### 4. Embedding

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_EMBEDDING_API_KEY` | No | `""` | string | Python runtime | API key | **Yes** | Yes |
| `EB_EMBEDDING_PROVIDER` | No | `"openai"` | string | Python runtime | `openai` | No | Yes |
| `EB_EMBEDDING_MODEL` | No | `"gemini/text-embedding-004"` | string | Python runtime | `gemini/text-embedding-004`, `openai/text-embedding-3-large` | No | Yes |
| `EB_EMBEDDING_ENDPOINT` | No | `"http://localhost:8811/v1"` | string | Python runtime | `http://litellm:8811/v1` | No | Yes |
| `EB_EMBEDDING_DIMENSIONS` | No | `768` | int | Python runtime | `768`, `1024`, `3072` | No | Yes |
| `EB_EMBEDDING_CACHE_ENABLED` | No | `true` | bool | Python runtime | `true`, `false` | No | Yes |
| `EB_EMBEDDING_CACHE_TTL` | No | `3600` | int (seconds) | Python runtime | `3600`, `7200` | No | Yes |

### 5. Reranker

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_RERANKER_ENDPOINT` | No | `"http://localhost:1235"` | string | Python runtime | `http://reranker:1235` | No | Yes |
| `EB_RERANKER_API_KEY` | No | `""` | string | Python runtime | API key | **Yes** | Yes |
| `EB_RERANKER_MODEL` | No | `"Qwen/Qwen3-Reranker-4B"` | string | Python runtime | `Qwen/Qwen3-Reranker-4B` | No | Yes |

### 6. Compaction LLM (separate from primary LLM)

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_COMPACTION_LLM_MODEL` | No | `"gemini/gemini-2.5-flash-lite"` | string | Python runtime | `gemini/gemini-2.5-flash-lite` | No | Yes |
| `EB_COMPACTION_LLM_ENDPOINT` | No | Falls back to `EB_LLM_ENDPOINT` | string | Python runtime | `http://litellm:8811/v1` | No | Yes |
| `EB_COMPACTION_LLM_API_KEY` | No | Falls back to `EB_LLM_API_KEY` | string | Python runtime | API key | **Yes** | Yes |

### 7. Observability (OTEL, Tracing, ClickHouse)

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_OTEL_ENDPOINT` | No | `None` (disabled) | string | Python runtime | `http://localhost:4317` | No | Yes |
| `EB_LOG_LEVEL` | No | `"INFO"` | string | Python runtime | `DEBUG`, `VERBOSE`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (case-insensitive at parse, normalized upper-case; runtime registers `VERBOSE`=15 — see B10 note in §7) | No | Yes |
| `EB_METRICS_TTL_SECONDS` | No | `3600` | int (seconds) | Python runtime | `3600` | No | Yes |
| `EB_TRACE_OTEL_LOGS_ENABLED` | No | `false` | bool | Python runtime | `true`, `false` | No | Yes |
| `EB_TRACE_MEMORY_MAX_EVENTS` | No | `10000` | int | Python runtime | `10000`, `50000` | No | Yes |
| `EB_ENABLE_TRACE_LEDGER` | No | `true` | bool | Python runtime | `true`, `false` | No | Yes |
| `EB_CLICKHOUSE_ENABLED` | No | `false` | bool | Python runtime | `true`, `false` | No | Yes |
| `EB_CLICKHOUSE_HOST` | No | `"localhost"` | string | Python runtime | `clickhouse`, `10.10.0.10` | No | Yes |
| `EB_CLICKHOUSE_PORT` | No | `8123` | int | Python runtime | `8123` | No | Yes |
| `EB_CLICKHOUSE_DATABASE` | No | `"otel"` | string | Python runtime | `otel` | No | Yes |

### 8. Guards & HITL

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_GUARDS_ENABLED` | No | `true` | bool | Python runtime | `true`, `false` | No | Yes (→ `guards.enabled`) |
| `EB_HITL_CALLBACK_SECRET` | No (recommended for production) | `""` | string | Python runtime, HITL middleware | `<openssl rand -hex 32>` | **Yes** | Yes |

### 9. Consolidation (Phase 9 "sleep" pipeline)

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_DEV_CONSOLIDATION_AUTO_TRIGGER` | No | `"0"` (disabled) | string | Python runtime | `"0"`, `"5m"`, `"1h"`, `"1d"` | No | Yes |
| `EB_CONSOLIDATION_BATCH_SIZE` | No | `500` | int | Python runtime | `500`, `1000` | No | Yes |
| `EB_CONSOLIDATION_MIN_RETENTION_SECONDS` | No | `172800` (48h) | int (seconds) | Python runtime | `172800`, `86400` | No | Yes |

### 10. Successful-Use Feedback (Phase 9, LLM-based, off by default)

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_SUCCESSFUL_USE_ENABLED` | No | `false` | bool | Python runtime | `true`, `false` | No | Yes |
| `EB_SUCCESSFUL_USE_ENDPOINT` | No | `"http://localhost:8811/v1"` | string | Python runtime | LiteLLM URL | No | Yes |
| `EB_SUCCESSFUL_USE_API_KEY` | No | Falls back to `EB_LLM_API_KEY` | string | Python runtime | API key | **Yes** | Yes |
| `EB_SUCCESSFUL_USE_MODEL` | No | `"gemini/gemini-2.5-flash-lite"` | string | Python runtime | `gemini/gemini-2.5-flash-lite` | No | Yes |
| `EB_SUCCESSFUL_USE_BATCH_SIZE` | No | `5` | int | Python runtime | `5`, `10` | No | Yes |

### 12. Ingest Pipeline Tuning

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_INGEST_BATCH_SIZE` | No | `6` | int | Python runtime | `6`, `10` | No | Yes |
| `EB_INGEST_BATCH_TIMEOUT` | No | `60.0` | float (seconds) | Python runtime | `60.0`, `120.0` | No | Yes |
| `EB_INGEST_BUFFER_TTL` | No | `300` | int (seconds) | Python runtime | `300`, `600` | No | Yes |
| `EB_EXTRACTION_CONTEXT_FACTS` | No | `20` | int | Python runtime | `20`, `50` | No | Yes |
| `EB_EXTRACTION_CONTEXT_TTL` | No | `3600` | int (seconds) | Python runtime | `3600` | No | Yes |

### 13. Scoring & Working Set

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_SCORING_SNAPSHOT_TTL` | No | `300` | int (seconds) | Python runtime | `300`, `600` | No | Yes |
| `EB_SESSION_GOALS_TTL` | No | `86400` | int (seconds) | Python runtime | `86400` | No | Yes |
| `EB_MAX_CONCURRENT_SESSIONS` | No | `100` | int | Python runtime | `100`, `200` | No | Yes |

### 14. TS Plugins (OpenClaw-side)

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `EB_GATEWAY_ID` | **Yes** | None (fail if missing) | string | TS Memory + Context plugins | `gw-prod`, `gw-prod-assistant` | No | N/A (TS-only) |
| `EB_GATEWAY_SHORT_NAME` | No | First 8 chars of `EB_GATEWAY_ID` | string | TS Memory + Context plugins | `prod` | No | N/A (TS-only) |
| `EB_RUNTIME_URL` | No | `"http://localhost:8420"` | string | TS plugins, `ebrun` CLI, HITL middleware | `http://10.10.0.10:8420` | No | N/A (TS-only) |
| `EB_PROFILE` | No | `"coding"` | string | TS Memory + Context plugins | `coding`, `research` | No | N/A (TS-only) |

### 15. HITL Middleware (separate service)

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `HITL_HOST` | No | `"0.0.0.0"` | string | HITL middleware | `0.0.0.0`, `127.0.0.1` | No | N/A |
| `HITL_PORT` | No | `8421` | int | HITL middleware | `8421` | No | N/A |
| `HITL_LOG_LEVEL` | No | `"INFO"` | string | HITL middleware | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` — **NOT `VERBOSE`** (HITL uses stock Python `logging` and never registers the runtime's level-15 `VERBOSE`; setting `HITL_LOG_LEVEL=VERBOSE` raises `ValueError: Unknown level: 'VERBOSE'` at HITL startup) | No | N/A |
| `EB_HITL_CALLBACK_SECRET` | No (recommended) | `""` | string | HITL middleware (must match runtime) | HMAC-SHA256 hex string | **Yes** | N/A |
| `EB_RUNTIME_URL` | No | `"http://localhost:8420"` | string | HITL middleware | `http://localhost:8420` | No | N/A |

### 16. Cognee Internal (set automatically, not user-facing)

| Variable | Required | Default | Type | Read by | Example values | Secret? | Env override |
|----------|----------|---------|------|---------|----------------|---------|--------------|
| `COGNEE_DISABLE_TELEMETRY` | No | `"true"` (set by `elephantbroker/__init__.py` at import time) | string | Cognee SDK | `true` | No | N/A |
| `ENABLE_BACKEND_ACCESS_CONTROL` | No | `"false"` (set by `configure_cognee()`) | string | Cognee SDK | `false` | No | N/A |
| `COGNEE_SKIP_CONNECTION_TEST` | No | Not set (only needed in test env) | string | Cognee SDK (test only) | `true` | No | N/A |

---

### Summary: Secrets (must go in secrets manager in production)

| Variable | Purpose |
|----------|---------|
| `EB_NEO4J_PASSWORD` | Neo4j database password |
| `EB_LLM_API_KEY` | Primary LLM endpoint API key |
| `EB_EMBEDDING_API_KEY` | Embedding endpoint API key (if different from LLM) |
| `EB_RERANKER_API_KEY` | Reranker endpoint API key |
| `EB_COMPACTION_LLM_API_KEY` | Compaction LLM API key (falls back to `EB_LLM_API_KEY`) |
| `EB_SUCCESSFUL_USE_API_KEY` | Successful-use LLM API key (falls back to `EB_LLM_API_KEY`) |
| `EB_HITL_CALLBACK_SECRET` | HMAC-SHA256 secret for HITL approval callbacks |

### Summary: env var override registry

After the F2/F3 unification there is **no curated subset** — every entry in `ENV_OVERRIDE_BINDINGS` (`elephantbroker/schemas/config.py`) is honored on every load. The registry currently contains **67 bindings** spanning identity, Cognee, LLM, compaction LLM, reranker, infra, trace, ClickHouse, embedding cache, scoring, HITL, successful-use, consolidation, and the top-level toggles.

Read `ENV_OVERRIDE_BINDINGS` directly when you need the authoritative list — the inverse contract test `tests/test_env_var_registry_completeness.py::TestEnvVarRegistryCompleteness` enforces that the registry, the schema fields, and `default.yaml` stay in sync, so the registry is the canonical source of truth and cannot drift from the docs without breaking CI.

### Summary: Minimum production env file

```bash
## Identity (required)
EB_GATEWAY_ID=gw-prod

## Database connections
EB_NEO4J_URI=bolt://localhost:7687
EB_QDRANT_URL=http://localhost:6333
EB_REDIS_URL=redis://localhost:6379

## LLM (required for extraction/classification)
EB_LLM_API_KEY=your-key
EB_EMBEDDING_API_KEY=your-key

## Organization binding
EB_ORG_ID=your-org-uuid
EB_TEAM_ID=your-team-uuid

## Security
EB_HITL_CALLBACK_SECRET=<openssl rand -hex 32>

## Telemetry (recommended)
COGNEE_DISABLE_TELEMETRY=true
```

---

**Total: 76 distinct environment variables** across all components (57 Python runtime `EB_*`, 5 TS-plugin-specific, 5 HITL middleware, 3 Cognee internal, plus 6 shared between components).

**Key files examined:**
- `elephantbroker/schemas/config.py` -- `ENV_OVERRIDE_BINDINGS` registry + `ElephantBrokerConfig.load()` (canonical source after F2/F3)
- `elephantbroker/__init__.py` -- `COGNEE_DISABLE_TELEMETRY` early-set
- `elephantbroker/runtime/adapters/cognee/config.py` -- `COGNEE_DISABLE_TELEMETRY` + `ENABLE_BACKEND_ACCESS_CONTROL`
- `elephantbroker/cli.py` -- `EB_ACTOR_ID`, `EB_RUNTIME_URL` for `ebrun` CLI
- `hitl-middleware/hitl_middleware/config.py` -- HITL middleware env vars
- `openclaw-plugins/elephantbroker-memory/index.ts` and `client.ts` -- TS plugin env vars
- `openclaw-plugins/elephantbroker-context/index.ts` and `client.ts` -- TS context plugin env vars
- `docs/DEPLOYMENT.md` and `docs/OPENCLAW-SETUP.md` -- deployment documentation


---


## YAML Configuration

ElephantBroker is loaded via `ElephantBrokerConfig.load(path)` (or `load(None)` for the packaged default — both go through the same internal `from_yaml()` reader). The YAML structure mirrors the Pydantic config schema hierarchy.

### Example `config.yaml`

```yaml
cognee:
  neo4j_uri: bolt://neo4j:7687
  neo4j_user: neo4j
  neo4j_password: elephant_dev
  qdrant_url: http://qdrant:6333

llm:
  model: openai/gemini/gemini-2.5-pro
  api_key: your-api-key-here
  endpoint: http://litellm:8811/v1

embedding:
  model: gemini/text-embedding-004

redis:
  url: redis://redis:6379

gateway:
  gateway_id: gw-prod-main

scoring:
  turn_relevance_weight: 0.20
  goal_relevance_weight: 0.18
  recency_weight: 0.12

profiles:
  cache_ttl: 300

trace:
  enabled: true
  export_interval_seconds: 30
```

### Environment Override Precedence

After `load()` reads the YAML, every binding in `ENV_OVERRIDE_BINDINGS` (`elephantbroker/schemas/config.py`) is applied on top — there is no curated subset. See the [Configuration Schemas](#configuration-schemas) section, or read `ENV_OVERRIDE_BINDINGS` directly, for the full list of env-var → YAML-field mappings.


---


## ElephantBroker Configuration Reference

### Overview

All configuration is defined in `elephantbroker/schemas/config.py` with the top-level model `ElephantBrokerConfig`. After the F2/F3 unification there is a single load entry point:

- **`load(path: str | None)`** -- if `path` is given, reads that YAML; if `None`, reads the packaged `elephantbroker/config/default.yaml`. Either way the YAML is parsed and validated first, then every binding in `ENV_OVERRIDE_BINDINGS` is applied on top, then `_apply_inheritance_fallbacks()` runs to populate empty derived secrets and endpoints.

The internal classmethod `from_yaml(path)` is the implementation backbone (and is what `load()` calls). The legacy `from_env()` classmethod has been removed — env-only callers now flow through `load(None)` and the packaged default YAML.

**Resolution order:** `EB_* env var (if set)` > `YAML value` > `model default`

`ConsolidationConfig` is now a regular field on `ElephantBrokerConfig` (F4/TODO-3-009 — it used to be a `@property` reading env vars directly, which raced the registry). It is loaded like every other config section.

---

### `EB_*` -- Full Environment Variable Mapping

Every parameter below shows the `EB_*` env var that maps to it via `ENV_OVERRIDE_BINDINGS`. Parameters without an env var listed are only settable via YAML or code.

#### `ENV_OVERRIDE_BINDINGS` -- Env Override Registry

The registry is the single canonical list of every env-var → YAML-field mapping. After F2/F3 every binding is honored on every load (no curated subset). Read `elephantbroker/schemas/config.py` directly for the authoritative list — currently 72 bindings — and rely on the inverse contract test (`tests/test_env_var_registry_completeness.py`) to keep it in sync with the schema and packaged YAML.

#### YAML Structure

The YAML file mirrors the Pydantic model hierarchy exactly:

```yaml
cognee:
  neo4j_uri: "bolt://10.10.0.10:7687"
  neo4j_user: neo4j
  neo4j_password: "prod_secure_pw"
  qdrant_url: "http://10.10.0.10:6333"
  default_dataset: elephantbroker
  embedding_provider: openai
  embedding_model: "gemini/text-embedding-004"
  embedding_endpoint: "http://10.10.0.10:8811/v1"
  embedding_api_key: ""
  embedding_dimensions: 768

llm:
  model: "openai/gemini/gemini-2.5-pro"
  endpoint: "http://10.10.0.10:8811/v1"
  api_key: ""
  max_tokens: 8192
  temperature: 0.1
  # ... all LLMConfig fields

gateway:
  gateway_id: "gw-prod-01"
  org_id: "org-acme"
  team_id: "team-eng"
  # ...

reranker:
  endpoint: "http://reranker:1235"
  # ...

infra:
  redis_url: "redis://redis:6379"
  log_level: INFO
  trace:
    memory_max_events: 10000
    otel_logs_enabled: false
  clickhouse:
    enabled: false

## ... all other sub-sections
default_profile: coding
enable_trace_ledger: true
max_concurrent_sessions: 100
```

---

### Top-Level Parameters (`ElephantBrokerConfig`)

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `default_profile` | `str` | `"coding"` | `EB_DEFAULT_PROFILE` | -- | Which profile (coding/research/managerial/worker/personal_assistant) governs scoring weights, retrieval policy, compaction policy, guard strictness, and autorecall when no profile is specified | Wrong profile name causes fallback or error at bootstrap; wrong profile type yields suboptimal scoring weights and retrieval behavior | `coding` / `coding` / `coding` (or per-deployment) |
| `enable_trace_ledger` | `bool` | `True` | `EB_ENABLE_TRACE_LEDGER` | -- | Enables/disables TraceLedger in-memory event recording and OTEL trace export | Disabling loses all trace/audit visibility; only disable for benchmarks | `true` / `true` / `true` |
| `max_concurrent_sessions` | `int` | `100` | `EB_MAX_CONCURRENT_SESSIONS` | `ge=1` | Limits concurrent sessions the runtime will accept | Too low causes session rejections under load; too high risks OOM from Redis/Neo4j connection pressure | `10` / `50` / `100`-`500` |
| `consolidation_min_retention_seconds` | `int` | `172800` (48h) | `EB_CONSOLIDATION_MIN_RETENTION_SECONDS` | `ge=3600` | Minimum age (seconds) a fact must have before consolidation pipeline can decay/archive it | Too low causes premature fact decay (data loss); too high means stale facts linger forever | `3600` / `86400` / `172800` |

---

### `CogneeConfig` -- Cognee Knowledge Plane

Path prefix: `cognee.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `cognee.neo4j_uri` | `str` | `"bolt://localhost:7687"` | `EB_NEO4J_URI` | -- | Neo4j Bolt connection URI for graph storage (all DataPoints, edges, Cypher queries) | Wrong URI = no graph storage or search; entire runtime nonfunctional | `bolt://localhost:7687` / `bolt://neo4j-staging:7687` / `bolt://neo4j-prod:7687` |
| `cognee.neo4j_user` | `str` | `"neo4j"` | `EB_NEO4J_USER` | -- | Neo4j authentication username | Wrong user = auth failure, runtime cannot start | `neo4j` / `neo4j` / `neo4j` |
| `cognee.neo4j_password` | `str` | `"elephant_dev"` | `EB_NEO4J_PASSWORD` | -- | Neo4j authentication password | Wrong password = auth failure, runtime cannot start | `elephant_dev` / `staging_pw` / `<vault-secret>` |
| `cognee.qdrant_url` | `str` | `"http://localhost:6333"` | `EB_QDRANT_URL` | -- | Qdrant vector store HTTP URL for embedding storage and similarity search | Wrong URL = no vector search; retrieval degrades to graph-only | `http://localhost:6333` / `http://qdrant-staging:6333` / `http://qdrant-prod:6333` |
| `cognee.default_dataset` | `str` | `"elephantbroker"` | `EB_DEFAULT_DATASET` | -- | Default Cognee dataset name (prefixed with `{gateway_id}__` at runtime) | Wrong name = data isolation issues between deployments | `elephantbroker` / `elephantbroker` / `elephantbroker` |
| `cognee.embedding_provider` | `str` | `"openai"` | `EB_EMBEDDING_PROVIDER` | -- | Embedding provider name passed to Cognee config; must be `"openai"` for LiteLLM-compatible endpoints | Wrong provider = embedding calls fail | `openai` / `openai` / `openai` |
| `cognee.embedding_model` | `str` | `"gemini/text-embedding-004"` | `EB_EMBEDDING_MODEL` | -- | Embedding model name routed by LiteLLM. Provider-prefixed (`gemini/`, `openai/`, etc.). Cognee uses the OpenAI client style regardless of backend. | Wrong model = dimension mismatch or API errors; must match what your LiteLLM proxy actually serves | `gemini/text-embedding-004` / same / `openai/text-embedding-3-large` |
| `cognee.embedding_endpoint` | `str` | `"http://localhost:8811/v1"` | `EB_EMBEDDING_ENDPOINT` | -- | OpenAI-compatible embedding API endpoint (typically LiteLLM proxy) | Wrong endpoint = all embedding operations fail, no vector search, no dedup | `http://localhost:8811/v1` / `http://litellm-staging:8811/v1` / `http://litellm-prod:8811/v1` |
| `cognee.embedding_api_key` | `str` | `""` | `EB_EMBEDDING_API_KEY` | -- | API key for embedding endpoint; also used as fallback for `llm.api_key` via `_apply_inheritance_fallbacks()` (post-F2/F3 unification) | Missing key = 401 from embedding endpoint (unless endpoint is unauthenticated) | `""` (local) / `sk-...` / `sk-...` |
| `cognee.embedding_dimensions` | `int` | `768` | `EB_EMBEDDING_DIMENSIONS` | `ge=1` | Embedding vector dimensionality; MUST match the configured `embedding_model`'s actual output dimensions | Mismatch = Qdrant collection creation failure or corrupted similarity scores. Changing this on existing data orphans the Qdrant collections — requires re-cognify | `768` (gemini/text-embedding-004) / same / `1024` (openai/text-embedding-3-large) |

> Note: prefixing the model name with `openai/` (e.g., `openai/text-embedding-3-large`) bypasses the `KNOWN_EMBEDDING_DIMS` startup cross-check in `elephantbroker/schemas/config.py`. This is an intentional escape hatch for deployments where LiteLLM truncates or re-routes — use only after probing the real dimension. See `docs/DEPLOYMENT.md § Embedding model prefix`.

---

### `LLMConfig` -- LLM for Extraction, Classification, Summarization

Path prefix: `llm.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `llm.model` | `str` | `"openai/gemini/gemini-2.5-pro"` | `EB_LLM_MODEL` | -- | Primary LLM model name for fact extraction, memory class classification, supersession detection, goal refinement. **MUST keep `openai/` prefix** — Cognee requires it for routing through its OpenAI-compatible client and strips it internally before sending to LiteLLM (LiteLLM sees `gemini/gemini-2.5-pro`). | Without `openai/` prefix, Cognee hangs at startup on the LLM connection test. Wrong model = API errors or poor extraction quality | `openai/gemini/gemini-2.5-pro` / same / same |
| `llm.endpoint` | `str` | `"http://localhost:8811/v1"` | `EB_LLM_ENDPOINT` | -- | OpenAI-compatible LLM API endpoint (LiteLLM proxy) | Wrong endpoint = all LLM-based pipelines fail (extraction, classification, summarization) | `http://localhost:8811/v1` / `http://litellm:8811/v1` / `http://litellm-prod:8811/v1` |
| `llm.api_key` | `str` | `""` | `EB_LLM_API_KEY` | -- | API key for LLM endpoint; falls back to `cognee.embedding_api_key` via `_apply_inheritance_fallbacks()` if empty after env overrides | Missing key = 401 from LLM endpoint | `""` (local) / `sk-...` / `sk-...` |
| `llm.max_tokens` | `int` | `8192` | `EB_LLM_MAX_TOKENS` | `ge=1` | Max output tokens for general LLM calls | Too low = truncated responses; too high = wasted tokens/cost | `8192` / `8192` / `8192` |
| `llm.temperature` | `float` | `0.1` | `EB_LLM_TEMPERATURE` | `ge=0.0, le=2.0` | LLM sampling temperature for extraction/classification tasks | Too high = nondeterministic/hallucinated extractions; too low = overly conservative | `0.1` / `0.1` / `0.1` |
| `llm.extraction_max_input_tokens` | `int` | `4000` | `EB_LLM_EXTRACTION_MAX_INPUT_TOKENS` | `ge=100` | Max input tokens per extraction batch (truncates conversation messages) | Too low = context loss, poor extraction; too high = slow/expensive extraction | `4000` / `4000` / `6000` |
| `llm.extraction_max_output_tokens` | `int` | `16384` | `EB_LLM_EXTRACTION_MAX_OUTPUT_TOKENS` | `ge=100` | Max output tokens for the fact extraction LLM call | Too low = truncated extraction JSON (parse errors); too high = wasted cost | `16384` / `16384` / `16384` |
| `llm.extraction_max_facts_per_batch` | `int` | `10` | `EB_LLM_EXTRACTION_MAX_FACTS` | `ge=1` | Max facts to extract per LLM call batch | Too low = misses facts; too high = slower extraction, higher cost | `10` / `10` / `15` |
| `llm.summarization_max_output_tokens` | `int` | `200` | `EB_LLM_SUMMARIZATION_MAX_OUTPUT_TOKENS` | `ge=10` | Max output tokens for artifact summarization LLM calls | Too low = truncated summaries; too high = verbose summaries wasting context budget | `200` / `200` / `300` |
| `llm.summarization_min_artifact_chars` | `int` | `500` | `EB_LLM_SUMMARIZATION_MIN_CHARS` | `ge=1` | Minimum artifact content length (chars) before triggering LLM summarization | Too low = summarizes trivial content (waste); too high = skips useful artifacts | `500` / `500` / `500` |
| `llm.ingest_batch_size` | `int` | `6` | `EB_INGEST_BATCH_SIZE` | `ge=1` | Number of messages batched before triggering ingest pipeline | Too low = excessive LLM calls per turn; too high = delayed fact extraction | `6` / `6` / `8` |
| `llm.ingest_batch_timeout_seconds` | `float` | `60.0` | `EB_INGEST_BATCH_TIMEOUT` | `ge=1.0` | Max seconds to wait for batch to fill before forcing ingest | Too low = many small batches (expensive); too high = stale messages sit unprocessed | `60.0` / `60.0` / `30.0` |
| `llm.ingest_buffer_ttl_seconds` | `int` | `300` | `EB_INGEST_BUFFER_TTL` | `ge=60` | TTL for Redis ingest buffer keys (messages awaiting batch processing) | Too low = messages dropped before processing; too high = stale buffers accumulate | `300` / `300` / `300` |
| `llm.extraction_context_facts` | `int` | `20` | `EB_EXTRACTION_CONTEXT_FACTS` | `ge=0` | Number of recent existing facts injected into extraction prompt for dedup/supersession awareness | Too low = redundant fact extraction; too high = expensive prompts, slower extraction | `20` / `20` / `30` |
| `llm.extraction_context_ttl_seconds` | `int` | `3600` | `EB_EXTRACTION_CONTEXT_TTL` | `ge=60` | TTL for cached extraction context facts in Redis | Too low = frequent context rebuilds (expensive); too high = stale context | `3600` / `3600` / `3600` |

---

### `RerankerConfig` -- Cross-Encoder Reranker (Phase 5+)

Path prefix: `reranker.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `reranker.endpoint` | `str` | `"http://localhost:1235"` | `EB_RERANKER_ENDPOINT` | -- | HTTP endpoint for Qwen3-Reranker-4B cross-encoder inference | Wrong endpoint = reranking fails; falls back to scoring-only if `fallback_on_error=true` | `http://localhost:1235` / `http://reranker:1235` / `http://reranker-prod:1235` |
| `reranker.api_key` | `str` | `""` | `EB_RERANKER_API_KEY` | -- | API key for reranker endpoint | Missing key with auth-required endpoint = 401 | `""` / `sk-...` / `sk-...` |
| `reranker.model` | `str` | `"Qwen/Qwen3-Reranker-4B"` | `EB_RERANKER_MODEL` | -- | Model name sent to the reranker endpoint | Wrong model name = model not found error | `Qwen/Qwen3-Reranker-4B` / same / same |
| `reranker.enabled` | `bool` | `True` | -- | -- | Master switch for cross-encoder reranking stage in working set build | Disabling skips reranking; scoring-only results may be less precise | `true` / `true` / `true` |
| `reranker.timeout_seconds` | `float` | `10.0` | -- | `ge=1.0` | HTTP timeout for reranker requests | Too low = timeouts on large batches; too high = blocks working set build on slow reranker | `10.0` / `10.0` / `15.0` |
| `reranker.batch_size` | `int` | `32` | -- | `ge=1` | Number of documents per reranker API call | Too low = many HTTP roundtrips; too high = reranker OOM on large payloads | `32` / `32` / `64` |
| `reranker.max_documents` | `int` | `100` | -- | `ge=1` | Max total documents sent to reranker (candidates are truncated to this) | Too low = discards potentially relevant candidates; too high = slow reranking | `100` / `100` / `200` |
| `reranker.fallback_on_error` | `bool` | `True` | -- | -- | If true, reranker errors fall back to score-only ranking; if false, errors propagate | Setting false makes working set build fragile to reranker outages | `true` / `true` / `true` |
| `reranker.top_n` | `int \| None` | `None` | -- | -- | If set, limits reranker output to top N results; None means return all | Setting too low discards good candidates after reranking | `None` / `None` / `50` |

---

### `InfraConfig` -- Infrastructure

Path prefix: `infra.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `infra.redis_url` | `str` | `"redis://localhost:6379"` | `EB_REDIS_URL` | -- | Redis connection URL for caching (embedding cache, session state, working set snapshots, ingest buffers, guard history, HITL queue) | Wrong URL = runtime cannot start; all session state, caching, and real-time features break | `redis://localhost:6379` / `redis://redis:6379` / `redis://redis-prod:6379` |
| `infra.otel_endpoint` | `str \| None` | `None` | `EB_OTEL_ENDPOINT` | -- | OTEL collector gRPC endpoint for distributed tracing export | None = no trace export (traces only in-memory); wrong URL = traces silently dropped | `None` / `http://otel-collector:4317` / `http://otel-prod:4317` |
| `infra.log_level` | `str` | `"INFO"` | `EB_LOG_LEVEL` | -- | Python logging level. Accepts `DEBUG`, `VERBOSE`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (case-insensitive at parse, normalized upper-case). The runtime registers a custom `VERBOSE`=15 level via `register_verbose_level()` in `runtime/observability.py` (between `DEBUG`=10 and `INFO`=20). **Note:** the HITL middleware uses stock Python `logging` and never registers `VERBOSE` — `HITL_LOG_LEVEL=VERBOSE` raises `ValueError: Unknown level: 'VERBOSE'`. See B10 note in §7. | Too verbose (DEBUG) in prod = log volume explosion; too quiet = missed diagnostics | `DEBUG` / `INFO` / `WARNING` |
| `infra.metrics_ttl_seconds` | `int` | `3600` | `EB_METRICS_TTL_SECONDS` | `ge=60` | TTL for Prometheus metrics data retained in memory | Too low = metrics disappear before scrape; too high = memory usage grows | `3600` / `3600` / `3600` |

#### `TraceConfig` (nested in `infra.trace.*`)

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `infra.trace.memory_max_events` | `int` | `10000` | `EB_TRACE_MEMORY_MAX_EVENTS` | `ge=100` | Maximum trace events held in TraceLedger in-memory ring buffer | Too low = recent events evicted before inspection; too high = memory pressure | `10000` / `10000` / `5000` |
| `infra.trace.memory_ttl_seconds` | `int` | `3600` | -- | `ge=60` | TTL for in-memory trace events before eviction | Too low = events lost before analysis; too high = stale events accumulate | `3600` / `3600` / `1800` |
| `infra.trace.otel_logs_enabled` | `bool` | `False` | `EB_TRACE_OTEL_LOGS_ENABLED` | -- | If true, TraceLedger exports events as OTEL log records (in addition to in-memory) | Enabling without OTEL endpoint configured = silent failures | `false` / `true` / `true` |

#### `ClickHouseConfig` (nested in `infra.clickhouse.*`)

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `infra.clickhouse.enabled` | `bool` | `False` | `EB_CLICKHOUSE_ENABLED` | -- | Enables ClickHouse cross-session analytics queries (consolidation Stage 7) | Enabling without ClickHouse deployed = connection errors in consolidation | `false` / `false` / `true` |
| `infra.clickhouse.host` | `str` | `"localhost"` | `EB_CLICKHOUSE_HOST` | -- | ClickHouse server hostname | Wrong host = connection failure | `localhost` / `clickhouse` / `clickhouse-prod` |
| `infra.clickhouse.port` | `int` | `8123` | `EB_CLICKHOUSE_PORT` | -- | ClickHouse HTTP port | Wrong port = connection failure | `8123` / `8123` / `8123` |
| `infra.clickhouse.database` | `str` | `"otel"` | `EB_CLICKHOUSE_DATABASE` | -- | ClickHouse database name for OTEL log queries | Wrong database = query failures in consolidation analytics | `otel` / `otel` / `otel` |
| `infra.clickhouse.logs_table` | `str` | `"otel_logs"` | -- | -- | ClickHouse table name for OTEL logs | Wrong table = consolidation analytics queries fail | `otel_logs` / `otel_logs` / `otel_logs` |

---

### `GatewayConfig` -- Gateway Identity

Path prefix: `gateway.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `gateway.gateway_id` | `str` | `"local"` | `EB_GATEWAY_ID` | -- | Primary gateway identifier; prefixes all Redis keys (`eb:{gateway_id}:*`), scopes all Cypher queries (`WHERE gateway_id = $gateway_id`), scopes Cognee datasets (`{gateway_id}__*`), labels all Prometheus metrics | Wrong/missing = data isolation failure between gateways; queries return wrong data; Redis key collisions | `local` / `gw-staging-01` / `gw-prod-main` |
| `gateway.gateway_short_name` | `str` | `""` | `EB_GATEWAY_SHORT_NAME` | -- | Short display name for logging (falls back to first 8 chars of gateway_id) | Cosmetic only; wrong value = confusing logs | `""` / `staging` / `admin` |
| `gateway.register_agent_identity` | `bool` | `True` | -- | -- | If true, registers agent identity in Neo4j on session start | Disabling = agent ActorRef not created; per-message attribution fails | `true` / `true` / `true` |
| `gateway.register_agent_actor` | `bool` | `True` | -- | -- | If true, registers agent as ActorRef DataPoint via `add_data_points()` | Disabling = no agent actor node in graph; OWNS_GOAL edges cannot link to agent | `true` / `true` / `true` |
| `gateway.org_id` | `str \| None` | `None` | `EB_ORG_ID` | -- | Organization ID binding the gateway to an org for authority/scope filtering | None = no org-level scope filtering; wrong value = data leaks across orgs | `None` / `"org-staging"` / `"org-acme"` |
| `gateway.team_id` | `str \| None` | `None` | `EB_TEAM_ID` | -- | Team ID binding the gateway to a team within an org | None = no team-level scope filtering; wrong value = data leaks across teams | `None` / `"team-eng"` / `"team-eng"` |
| `gateway.agent_authority_level` | `int` | `0` | `EB_AGENT_AUTHORITY_LEVEL` | `ge=0` | Default authority level for the agent actor (used by guard autonomy classification) | Too high = agent bypasses safety guards; too low = excessive HITL approvals | `0` / `0` / `1` |

Computed property: `gateway.effective_short_name` returns `gateway_short_name` if nonempty, else `gateway_id[:8]`.

---

### `EmbeddingCacheConfig` -- Redis Embedding Cache (Phase 5)

Path prefix: `embedding_cache.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `embedding_cache.enabled` | `bool` | `True` | `EB_EMBEDDING_CACHE_ENABLED` | -- | Enables Redis-backed embedding cache (CachedEmbeddingService) to avoid redundant embedding API calls | Disabling = every scoring cycle re-embeds all queries (expensive, slow) | `true` / `true` / `true` |
| `embedding_cache.ttl_seconds` | `int` | `3600` | `EB_EMBEDDING_CACHE_TTL` | `ge=60` | TTL for cached embeddings in Redis | Too low = cache churn, frequent re-embedding; too high = stale embeddings if model changes | `3600` / `3600` / `7200` |
| `embedding_cache.key_prefix` | `str` | `"eb:emb_cache"` | -- | -- | Redis key prefix for embedding cache entries | Changing breaks existing cache; collisions with other Redis keys | `eb:emb_cache` / same / same |

---

### `ScoringConfig` -- Working Set Scoring Pipeline (Phase 5)

Path prefix: `scoring.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `scoring.neutral_use_prior` | `float` | `0.5` | -- | `ge=0.0, le=1.0` | Default use-prior score for facts with no usage history (D10 successful_use_prior dimension) | Too high = overvalues unproven facts; too low = new facts never surface | `0.5` / `0.5` / `0.5` |
| `scoring.cheap_prune_max_candidates` | `int` | `80` | -- | `ge=1` | Max candidates after cheap prune stage in RerankOrchestrator (before expensive semantic/cross-encoder stages) | Too low = discards good candidates early; too high = expensive reranking stages | `80` / `80` / `120` |
| `scoring.semantic_blend_weight` | `float` | `0.6` | -- | `ge=0.0, le=1.0` | Weight of semantic similarity vs. score in the semantic reranking blend (0 = score only, 1 = similarity only) | Too high = ignores scoring signals; too low = ignores semantic relevance | `0.6` / `0.6` / `0.6` |
| `scoring.merge_similarity_threshold` | `float` | `0.95` | -- | `ge=0.0, le=1.0` | Cosine similarity threshold above which candidates are merged as duplicates in reranking | Too low = merges distinct facts; too high = lets near-duplicates through | `0.95` / `0.95` / `0.95` |
| `scoring.snapshot_ttl_seconds` | `int` | `300` | `EB_SCORING_SNAPSHOT_TTL` | `ge=30` | TTL for working set scoring snapshots cached in Redis | Too low = frequent re-scoring (expensive); too high = stale working sets | `300` / `300` / `300` |
| `scoring.session_goals_ttl_seconds` | `int` | `86400` | `EB_SESSION_GOALS_TTL` | `ge=60` | TTL for session goal data stored in Redis (two-bucket model) | Too low = goals lost mid-session; too high = stale goals from abandoned sessions persist | `86400` / `86400` / `86400` |
| `scoring.working_set_build_global_goals_filter_by_actors` | `bool` | `True` | -- | -- | If true, filters global goals by actor OWNS_GOAL edges during working set build | Disabling = all global goals injected regardless of actor ownership (noisy) | `true` / `true` / `true` |

---

### `VerificationMultipliers` -- Claim Verification Impact on Confidence

Path prefix: `verification_multipliers.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `verification_multipliers.supervisor_verified` | `float` | `1.0` | -- | `ge=0.0, le=2.0` | Confidence multiplier for claims verified by supervisor (D4 verification dimension) | >1.0 inflates verified claim scores; <1.0 penalizes even verified claims | `1.0` / `1.0` / `1.0` |
| `verification_multipliers.tool_supported` | `float` | `0.9` | -- | `ge=0.0, le=2.0` | Confidence multiplier for claims with tool evidence | Lower values penalize tool-backed claims more aggressively | `0.9` / `0.9` / `0.9` |
| `verification_multipliers.self_supported` | `float` | `0.7` | -- | `ge=0.0, le=2.0` | Confidence multiplier for self-reported claims (agent's own assertion) | Higher values trust self-assertions more; risky if agent hallucinates | `0.7` / `0.7` / `0.7` |
| `verification_multipliers.unverified` | `float` | `0.5` | -- | `ge=0.0, le=2.0` | Confidence multiplier for unverified claims | Higher = unverified claims rank higher; lower = they get buried | `0.5` / `0.5` / `0.5` |
| `verification_multipliers.no_claim` | `float` | `0.8` | -- | `ge=0.0, le=2.0` | Confidence multiplier for facts without any claim record | Higher = unclaimed facts treated as relatively trustworthy | `0.8` / `0.8` / `0.8` |

---

### `ConflictDetectionConfig` -- Contradiction Detection

Path prefix: `conflict_detection.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `conflict_detection.supersession_penalty` | `float` | `1.0` | -- | `ge=0.0` | Penalty applied to facts superseded via SUPERSEDES edges (D7 obsolescence dimension) | <1.0 = superseded facts still partially surface; >1.0 = amplified penalty | `1.0` / `1.0` / `1.0` |
| `conflict_detection.contradiction_edge_penalty` | `float` | `0.9` | -- | `ge=0.0` | Penalty for facts with CONTRADICTS edges | Lower = stronger contradiction penalty; 0 = contradicted facts fully suppressed | `0.9` / `0.9` / `0.9` |
| `conflict_detection.layer2_penalty` | `float` | `0.7` | -- | `ge=0.0` | Penalty from Layer 2 detection (semantic similarity-based contradiction) | Lower = stronger semantic contradiction penalty | `0.7` / `0.7` / `0.7` |
| `conflict_detection.similarity_threshold` | `float` | `0.9` | -- | `ge=0.0, le=1.0` | Cosine similarity threshold for Layer 2 contradiction detection (global default, overridable per-profile) | Too low = false positive contradictions; too high = misses actual contradictions | `0.9` / `0.9` / `0.9` |
| `conflict_detection.confidence_gap_threshold` | `float` | `0.3` | -- | `ge=0.0, le=1.0` | Minimum confidence gap between two similar facts to trigger contradiction detection | Too low = noisy contradiction signals; too high = misses real conflicts | `0.3` / `0.3` / `0.3` |
| `conflict_detection.redundancy_similarity_threshold` | `float` | `0.85` | -- | `ge=0.0, le=1.0` | Threshold for D9 redundancy penalty (similar facts in same working set get penalized) | Too low = penalizes loosely related facts; too high = lets near-duplicates through | `0.85` / `0.85` / `0.85` |

---

### `SuccessfulUseConfig` -- LLM-Based Use Feedback (Phase 9)

Path prefix: `successful_use.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `successful_use.enabled` | `bool` | `False` | `EB_SUCCESSFUL_USE_ENABLED` | -- | Enables LLM-based batch evaluation of which injected facts actually contributed to agent actions | Enabling is expensive (LLM calls per turn); disabling loses D10 scoring accuracy | `false` / `false` / `true` |
| `successful_use.endpoint` | `str` | `"http://localhost:8811/v1"` | `EB_SUCCESSFUL_USE_ENDPOINT` | -- | LLM endpoint for use evaluation (typically a cheaper/faster model) | Wrong endpoint = evaluation fails silently (async) | `http://localhost:8811/v1` / `http://litellm:8811/v1` / `http://litellm-prod:8811/v1` |
| `successful_use.api_key` | `str` | `""` | `EB_SUCCESSFUL_USE_API_KEY` | -- | API key (falls back to `llm.api_key` via `_apply_inheritance_fallbacks()` if empty) | Missing = auth failure on evaluation calls | `""` / `sk-...` / `sk-...` |
| `successful_use.model` | `str` | `"gemini/gemini-2.5-flash-lite"` | `EB_SUCCESSFUL_USE_MODEL` | -- | LLM model for use evaluation (flash-class model for cost efficiency; the unsuffixed `gemini/gemini-2.5-flash` alias on the staging LiteLLM proxy resolves to a deleted preview and returns HTTP 404) | Wrong model = evaluation errors | `gemini/gemini-2.5-flash-lite` / same / same |
| `successful_use.batch_size` | `int` | `5` | `EB_SUCCESSFUL_USE_BATCH_SIZE` | `ge=1` | Number of facts evaluated per LLM call | Too low = many LLM calls (expensive); too high = context window overflow | `5` / `5` / `10` |
| `successful_use.batch_timeout_seconds` | `float` | `120.0` | -- | `ge=10.0` | Timeout for each use evaluation LLM call | Too low = frequent timeouts; too high = blocks turn processing | `120.0` / `120.0` / `60.0` |
| `successful_use.feed_last_facts` | `int` | `20` | -- | `ge=1` | Number of most recently injected facts to evaluate per turn | Too low = misses used facts; too high = expensive evaluation | `20` / `20` / `20` |
| `successful_use.min_confidence` | `float` | `0.7` | -- | `ge=0.0, le=1.0` | Minimum LLM confidence to mark a fact as "successfully used" | Too low = noisy positive signals; too high = underreports usage | `0.7` / `0.7` / `0.7` |
| `successful_use.run_async` | `bool` | `True` | -- | -- | If true, evaluation runs as background async task (non-blocking) | Setting false blocks turn processing on evaluation completion | `true` / `true` / `true` |

---

### `GoalInjectionConfig` -- Goal Context in Extraction Prompts

Path prefix: `goal_injection.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `goal_injection.enabled` | `bool` | `True` | -- | -- | If true, injects active goals into fact extraction prompts for goal-aware extraction | Disabling = extraction misses goal-relevant facts, no goal_ids on extracted facts | `true` / `true` / `true` |
| `goal_injection.max_session_goals` | `int` | `5` | -- | `ge=0` | Max session goals injected into extraction prompt | Too low = misses relevant goals; too high = prompt bloat, extraction confusion | `5` / `5` / `5` |
| `goal_injection.max_persistent_goals` | `int` | `3` | -- | `ge=0` | Max persistent goals injected into extraction prompt | Too low = misses long-term goals; too high = prompt bloat | `3` / `3` / `5` |
| `goal_injection.include_persistent_goals` | `bool` | `True` | -- | -- | Whether to include persistent (Cognee-stored) goals alongside session goals | Disabling = extraction only aware of ephemeral session goals | `true` / `true` / `true` |

---

### `GoalRefinementConfig` -- Goal Refinement Pipeline

Path prefix: `goal_refinement.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `goal_refinement.hints_enabled` | `bool` | `True` | -- | -- | Enables Tier 1 goal hints (zero-LLM-cost direct Redis updates from message patterns) | Disabling = no cheap goal signal processing | `true` / `true` / `true` |
| `goal_refinement.refinement_task_enabled` | `bool` | `True` | -- | -- | Enables Tier 2 async LLM-based goal refinement (subgoal creation, progress updates) | Disabling = goals never refined or decomposed; only manual goal management | `true` / `true` / `true` |
| `goal_refinement.model` | `str` | `"gemini/gemini-2.5-flash-lite"` | -- | -- | LLM model for goal refinement tasks (the unsuffixed `gemini/gemini-2.5-flash` alias on the staging LiteLLM proxy resolves to a deleted preview and returns HTTP 404; flash-lite is the working flash-class replacement) | Wrong model = refinement errors | `gemini/gemini-2.5-flash-lite` / same / same |
| `goal_refinement.max_subgoals_per_session` | `int` | `10` | -- | `ge=1` | Maximum subgoals a session can create through refinement | Too low = incomplete goal decomposition; too high = goal sprawl | `10` / `10` / `15` |
| `goal_refinement.feed_recent_messages` | `int` | `6` | -- | `ge=1` | Number of recent messages fed to refinement LLM for context | Too low = refinement lacks context; too high = expensive prompts | `6` / `6` / `10` |
| `goal_refinement.run_refinement_async` | `bool` | `True` | -- | -- | If true, Tier 2 refinement runs as background async task | Setting false blocks turn on refinement completion | `true` / `true` / `true` |
| `goal_refinement.progress_confidence_delta` | `float` | `0.1` | -- | `ge=0.0, le=1.0` | Minimum confidence change to count as meaningful goal progress | Too low = noisy progress updates; too high = progress rarely reported | `0.1` / `0.1` / `0.1` |
| `goal_refinement.subgoal_dedup_threshold` | `float` | `0.6` | -- | `ge=0.0, le=1.0` | Embedding similarity threshold for deduplicating proposed subgoals | Too low = creates duplicate subgoals; too high = rejects valid distinct subgoals | `0.6` / `0.6` / `0.6` |

---

### `ProcedureCandidateConfig` -- Procedure Surfacing in Working Set

Path prefix: `procedure_candidates.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `procedure_candidates.enabled` | `bool` | `True` | -- | -- | Enables procedure injection into working set candidate pool | Disabling = procedures never surface in context (manual search only) | `true` / `true` / `true` |
| `procedure_candidates.filter_by_relevance` | `bool` | `True` | -- | -- | If true, filters procedures by semantic relevance to current turn before injection | Disabling = all procedures injected regardless of relevance (noisy) | `true` / `true` / `true` |
| `procedure_candidates.relevance_threshold` | `float` | `0.3` | -- | `ge=0.0, le=1.0` | Minimum relevance score for procedure to be injected | Too low = irrelevant procedures injected; too high = useful procedures filtered out | `0.3` / `0.3` / `0.3` |
| `procedure_candidates.top_k` | `int` | `3` | -- | `ge=1` | Maximum number of procedures injected into working set per build | Too low = misses relevant procedures; too high = procedures crowd out facts | `3` / `3` / `5` |
| `procedure_candidates.always_include_proof_required` | `bool` | `True` | -- | -- | If true, procedures requiring proof (active, with unfulfilled evidence) are always injected regardless of relevance score | Disabling = proof-required procedures may not surface (compliance risk) | `true` / `true` / `true` |

---

### `AuditConfig` -- SQLite Audit Trail

Path prefix: `audit.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `audit.procedure_audit_enabled` | `bool` | `True` | -- | -- | Enables SQLite audit logging for procedure lifecycle events | Disabling = no audit trail for procedure operations (compliance gap) | `true` / `true` / `true` |
| `audit.procedure_audit_db_path` | `str` | `"data/procedure_audit.db"` | -- | -- | File path for procedure audit SQLite database | Wrong path = write failures; relative path resolves from CWD | `data/procedure_audit.db` / same / `/data/procedure_audit.db` |
| `audit.session_goal_audit_enabled` | `bool` | `True` | -- | -- | Enables SQLite audit logging for session goal lifecycle events | Disabling = no audit trail for goal operations | `true` / `true` / `true` |
| `audit.session_goal_audit_db_path` | `str` | `"data/session_goals_audit.db"` | -- | -- | File path for session goal audit SQLite database | Wrong path = write failures | `data/session_goals_audit.db` / same / `/data/session_goals_audit.db` |
| `audit.org_overrides_db_path` | `str` | `"data/org_overrides.db"` | -- | -- | File path for org-level profile override SQLite database | Wrong path = org overrides unavailable | `data/org_overrides.db` / same / `/data/org_overrides.db` |
| `audit.authority_rules_db_path` | `str` | `"data/authority_rules.db"` | -- | -- | File path for authority rules SQLite database (guard autonomy rules) | Wrong path = authority rules unavailable | `data/authority_rules.db` / same / `/data/authority_rules.db` |
| `audit.consolidation_reports_db_path` | `str` | `"data/consolidation_reports.db"` | -- | -- | File path for consolidation run reports (Phase 9) | Wrong path = consolidation reports not persisted | `data/consolidation_reports.db` / same / `/data/consolidation_reports.db` |
| `audit.tuning_deltas_db_path` | `str` | `"data/tuning_deltas.db"` | -- | -- | File path for scoring weight tuning deltas (Phase 9 consolidation Stage 9) | Wrong path = weight adjustments not persisted between runs | `data/tuning_deltas.db` / same / `/data/tuning_deltas.db` |
| `audit.scoring_ledger_db_path` | `str` | `"data/scoring_ledger.db"` | -- | -- | File path for scoring ledger SQLite database (tracks per-fact scoring history for consolidation) | Wrong path = scoring history unavailable for Stage 9 weight tuning | `data/scoring_ledger.db` / same / `/data/scoring_ledger.db` |
| `audit.retention_days` | `int` | `90` | -- | `ge=7` | Days to retain audit records before cleanup | Too low = audit history truncated (compliance risk); too high = DB grows unbounded | `90` / `90` / `365` |

---

### `GuardConfig` -- Guard Engine (Phase 7)

Path prefix: `guards.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `guards.enabled` | `bool` | `True` | `EB_GUARDS_ENABLED` | -- | Master switch for the 6-layer guard pipeline (autonomy classification, static rules, BM25/semantic, structural validators, reinjection, LLM escalation). When `false`, `RedLineGuardEngine.preflight_check()` short-circuits to PASS without invoking any layer. | Disabling removes all safety guardrails; only for testing or controlled benchmarks | `true` / `true` / `true` |
| `guards.builtin_rules_enabled` | `bool` | `True` | -- | -- | Enables the 16 built-in static guard rules (Layer 2) | Disabling = no static rule matching, only semantic/LLM guards remain | `true` / `true` / `true` |
| `guards.history_ttl_seconds` | `int` | `86400` | -- | `ge=60` | TTL for guard evaluation history stored in Redis | Too low = guard history lost mid-session; too high = stale history accumulates | `86400` / `86400` / `86400` |
| `guards.max_history_events` | `int` | `50` | -- | `ge=1` | Maximum guard evaluation events retained per session | Too low = insufficient history for pattern analysis; too high = memory pressure | `50` / `50` / `100` |
| `guards.input_summary_max_chars` | `int` | `500` | -- | `ge=50` | Max characters of input context passed to guard evaluators | Too low = guards lack context for accurate evaluation; too high = slow evaluation | `500` / `500` / `500` |
| `guards.llm_escalation_max_tokens` | `int` | `500` | -- | `ge=50` | Max output tokens for LLM escalation calls (Layer 6) | Too low = truncated LLM guard responses; too high = wasted tokens | `500` / `500` / `500` |
| `guards.llm_escalation_timeout_seconds` | `float` | `10.0` | -- | `ge=1.0` | Timeout for LLM escalation HTTP calls | Too low = frequent timeouts; too high = blocks guard pipeline on slow LLM | `10.0` / `10.0` / `15.0` |
| `guards.max_pattern_length` | `int` | `500` | -- | `ge=10` | Max characters for pattern strings in static rule matching | Too low = truncated patterns miss matches; too high = slow regex evaluation | `500` / `500` / `500` |

#### `StrictnessPreset` (nested in `guards.strictness_presets.*`)

Three built-in presets: `loose`, `medium`, `strict`.

| Name | Type | Default (medium) | Constraints | Controls |
|------|------|-------------------|-------------|----------|
| `bm25_threshold_multiplier` | `float` | `1.0` | `ge=0.1, le=3.0` | Multiplier on BM25 similarity threshold (higher = more lenient) |
| `semantic_threshold_override` | `float \| None` | `None` | -- | If set, overrides the default semantic similarity threshold |
| `warn_outcome_upgrade` | `str \| None` | `None` | -- | If set, upgrades "warn" outcomes to this level (e.g., `"require_approval"` in strict) |
| `structural_validators_enabled` | `bool` | `True` | -- | Enables structural validator checks (Layer 4) |
| `reinjection_on` | `str` | `"elevated_risk"` | -- | When to trigger forced reinjection: `"block_only"`, `"elevated_risk"`, `"any_non_pass"` |
| `llm_escalation_on` | `str` | `"ambiguous"` | -- | When to trigger LLM escalation: `"disabled"`, `"ambiguous"`, `"any_non_pass"` |

**Preset defaults:**

| Preset | `bm25_threshold_multiplier` | `semantic_threshold_override` | `warn_outcome_upgrade` | `structural_validators_enabled` | `reinjection_on` | `llm_escalation_on` |
|--------|----------------------------|-------------------------------|----------------------|-------------------------------|------------------|---------------------|
| `loose` | `1.5` | `0.90` | `None` | `False` | `"block_only"` | `"disabled"` |
| `medium` | `1.0` | `None` | `None` | `True` | `"elevated_risk"` | `"ambiguous"` |
| `strict` | `0.7` | `0.70` | `"require_approval"` | `True` | `"any_non_pass"` | `"any_non_pass"` |

---

### `HitlConfig` -- Human-in-the-Loop Middleware (Phase 7)

Path prefix: `hitl.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `hitl.enabled` | `bool` | `False` | -- | -- | Enables HITL approval queue middleware | Enabling without middleware service running = approval requests fail | `false` / `false` / `true` |
| `hitl.default_url` | `str` | `"http://localhost:8421"` | -- | -- | URL of the HITL middleware service | Wrong URL = approval requests fail; guards that require approval block | `http://localhost:8421` / `http://hitl:8421` / `http://hitl-prod:8421` |
| `hitl.timeout_seconds` | `float` | `10.0` | -- | `ge=1.0` | HTTP timeout for HITL middleware requests | Too low = frequent timeouts; too high = blocks guard pipeline | `10.0` / `10.0` / `15.0` |
| `hitl.approval_default_timeout_seconds` | `int` | `300` | -- | `ge=30` | Default timeout for pending approval requests before auto-resolution | Too low = approvals auto-expire before human reviews; too high = requests hang | `300` / `300` / `600` |
| `hitl.callback_hmac_secret` | `str` | `""` | `EB_HITL_CALLBACK_SECRET` | -- | HMAC secret for validating HITL callback signatures | Empty = HMAC validation disabled (insecure); wrong = callback validation fails | `""` / `test-secret` / `<vault-secret>` |
| `hitl.gateway_overrides` | `dict[str, str]` | `{}` | -- | -- | Per-gateway HITL URL overrides (`{gateway_id: url}`) | Wrong URL per gateway = that gateway's approvals fail | `{}` / `{}` / `{"gw-special": "http://hitl-special:8421"}` |

---

### `CompactionLLMConfig` -- Compaction Summarization LLM (Phase 6)

Path prefix: `compaction_llm.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `compaction_llm.model` | `str` | `"gemini/gemini-2.5-flash-lite"` | `EB_COMPACTION_LLM_MODEL` | -- | LLM model for compaction summarization (uses cheaper model than extraction; the unsuffixed `gemini/gemini-2.5-flash` alias on the staging LiteLLM proxy resolves to a deleted preview and returns HTTP 404) | Wrong model = compaction summarization errors | `gemini/gemini-2.5-flash-lite` / same / same |
| `compaction_llm.endpoint` | `str` | `"http://localhost:8811/v1"` | `EB_COMPACTION_LLM_ENDPOINT` | -- | LLM endpoint for compaction (falls back to `llm.endpoint` via `_apply_inheritance_fallbacks()` Tier 3 / F7) | Wrong endpoint = compaction summaries fail | `http://localhost:8811/v1` / `http://litellm:8811/v1` / `http://litellm-prod:8811/v1` |
| `compaction_llm.api_key` | `str` | `""` | `EB_COMPACTION_LLM_API_KEY` | -- | API key (falls back to `llm.api_key` via `_apply_inheritance_fallbacks()` if empty) | Missing = auth failure on compaction LLM calls | `""` / `sk-...` / `sk-...` |
| `compaction_llm.max_tokens` | `int` | `2000` | -- | `ge=100` | Max output tokens for compaction summaries | Too low = truncated summaries; too high = verbose summaries wasting context | `2000` / `2000` / `2000` |
| `compaction_llm.temperature` | `float` | `0.2` | -- | `ge=0.0, le=2.0` | Temperature for compaction summarization | Too high = non-deterministic summaries; too low = overly rigid | `0.2` / `0.2` / `0.2` |

---

### `ContextAssemblyConfig` -- 4-Block Context Assembly (Phase 6)

Path prefix: `context_assembly.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `context_assembly.max_context_window_fraction` | `float` | `0.15` | -- | `ge=0.01, le=0.5` | Fraction of agent's context window allocated to EB's context blocks | Too low = EB content truncated, facts/goals omitted; too high = crowds out agent's own context | `0.15` / `0.15` / `0.20` |
| `context_assembly.fallback_context_window` | `int` | `128000` | -- | `ge=1000` | Assumed context window size when agent doesn't report its actual window | Wrong = budget calculation off; too low = undersized context; too high = overly generous budget | `128000` / `128000` / `128000` |
| `context_assembly.enable_dynamic_budget` | `bool` | `True` | -- | -- | If true, adjusts context budget dynamically based on content availability | Disabling = fixed budget allocation regardless of content volume | `true` / `true` / `true` |
| `context_assembly.system_overlay_budget_fraction` | `float` | `0.25` | -- | `ge=0.05, le=0.5` | Fraction of EB's context budget allocated to Block 1 (systemPromptAddition) | Too low = critical system instructions truncated; too high = crowds out Block 2/3/4 | `0.25` / `0.25` / `0.25` |
| `context_assembly.goal_block_budget_fraction` | `float` | `0.10` | -- | `ge=0.0, le=0.3` | Fraction of EB's context budget allocated to goal status block | Too low = goal context truncated; too high = goals dominate context | `0.10` / `0.10` / `0.10` |
| `context_assembly.evidence_budget_max_tokens` | `int` | `500` | -- | `ge=0` | Max tokens for evidence/verification status in context assembly | Too low = evidence context omitted; too high = evidence crowds out facts | `500` / `500` / `500` |
| `context_assembly.compaction_trigger_multiplier` | `float` | `2.0` | -- | `ge=1.5, le=5.0` | Triggers compaction when context exceeds budget by this factor | Too low = compaction fires too often (expensive); too high = context blows past budget | `2.0` / `2.0` / `2.0` |
| `context_assembly.compaction_summary_max_tokens` | `int` | `1000` | -- | `ge=100` | Max tokens for compaction summary output | Too low = lossy compaction; too high = compaction provides diminishing returns | `1000` / `1000` / `1000` |

---

### `ArtifactCaptureConfig` -- Automatic Tool Artifact Capture (Phase 6)

Path prefix: `artifact_capture.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `artifact_capture.enabled` | `bool` | `True` | -- | -- | Enables automatic capture of tool outputs as artifacts | Disabling = tool outputs not stored, not searchable, no placeholders in context | `true` / `true` / `true` |
| `artifact_capture.min_content_chars` | `int` | `200` | -- | `ge=0` | Minimum tool output length (chars) to trigger artifact capture | Too low = captures trivial outputs (noise); too high = misses useful artifacts | `200` / `200` / `200` |
| `artifact_capture.max_content_chars` | `int` | `50000` | -- | `ge=1000` | Maximum tool output length stored; longer outputs are truncated | Too low = truncates useful artifacts; too high = storage bloat | `50000` / `50000` / `100000` |
| `artifact_capture.skip_tools` | `list[str]` | `[]` | -- | -- | Tool names to exclude from artifact capture | Adding critical tools = their outputs never captured | `[]` / `[]` / `["internal_debug"]` |

---

### `ArtifactAssemblyConfig` -- Artifact Placeholder Rendering (Phase 6)

Path prefix: `artifact_assembly.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `artifact_assembly.placeholder_enabled` | `bool` | `True` | -- | -- | If true, large artifacts are replaced with summary placeholders in assembled context | Disabling = full artifact text injected (token-expensive) or omitted entirely | `true` / `true` / `true` |
| `artifact_assembly.placeholder_min_tokens` | `int` | `100` | -- | `ge=0` | Minimum artifact token count to trigger placeholder replacement (smaller artifacts embedded inline) | Too low = even small artifacts get placeholders; too high = large artifacts bloat context | `100` / `100` / `100` |
| `artifact_assembly.placeholder_template` | `str` | `'[Tool output: {tool_name} -- {summary}\n -> Call artifact_search("{artifact_id}") for full output]'` | -- | -- | Template string for artifact placeholders (supports `{tool_name}`, `{summary}`, `{artifact_id}` variables) | Malformed template = broken placeholders in context | (use default) / same / same |

---

### `AsyncAnalysisConfig` -- Async Injection Analysis (Phase 6)

Path prefix: `async_analysis.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `async_analysis.enabled` | `bool` | `False` | -- | -- | Enables background embedding analysis for injection quality (AD-24) | Enabling adds background compute per turn; disabling loses injection quality metrics | `false` / `false` / `true` |
| `async_analysis.topic_continuation_threshold` | `float` | `0.6` | -- | `ge=0.3, le=0.9` | Cosine similarity threshold for detecting topic continuation between turns | Too low = everything is a "continuation" (false positives); too high = misses real continuations | `0.6` / `0.6` / `0.6` |
| `async_analysis.batch_size` | `int` | `20` | -- | `ge=1` | Number of injected items analyzed per batch | Too low = slow analysis; too high = memory pressure | `20` / `20` / `20` |

---

### `ProfileCacheConfig` -- Profile Resolution Cache (Phase 8)

Path prefix: `profile_cache.*`

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `profile_cache.ttl_seconds` | `int` | `300` | -- | `ge=10` | TTL for resolved profile cache entries | Too low = frequent profile re-resolution (DB hits); too high = profile changes take too long to propagate | `300` / `300` / `300` |

---

### `ConsolidationConfig` -- Consolidation ("Sleep") Pipeline (Phase 9)

Path prefix: `consolidation.*`. After F4 (TODO-3-009), `consolidation` is a regular `ElephantBrokerConfig` field — it used to be a `@property` that read env vars directly inside the getter, which raced `ENV_OVERRIDE_BINDINGS`, hid the vars from the inverse contract test, and silently ignored env vars set after the first access. Routing it through the standard registry kills both bugs.

The two consolidation env vars now flow through `ENV_OVERRIDE_BINDINGS` like every other binding:
- `EB_DEV_CONSOLIDATION_AUTO_TRIGGER` → `consolidation.dev_auto_trigger_interval`
- `EB_CONSOLIDATION_BATCH_SIZE` → `consolidation.batch_size`

#### Fact Loading

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.batch_size` | `int` | `500` | `EB_CONSOLIDATION_BATCH_SIZE` | `ge=50, le=5000` | Number of facts loaded per batch from Neo4j during consolidation | Too low = many DB roundtrips; too high = memory pressure during processing | `500` / `500` / `2000` |
| `consolidation.active_session_protection_hours` | `float` | `1.0` | -- | `ge=0.0` | Facts from sessions active within this window are excluded from consolidation | Too low = consolidation modifies facts in active sessions (data corruption); too high = stale facts immune too long | `1.0` / `1.0` / `2.0` |

#### Stage 1: Cluster Near-Duplicates

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.cluster_similarity_threshold` | `float` | `0.92` | -- | `ge=0.5, le=1.0` | Cosine similarity threshold for clustering near-duplicate facts | Too low = clusters dissimilar facts (data loss); too high = misses near-duplicates | `0.92` / `0.92` / `0.92` |

#### Stage 2: Canonicalize

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.canonicalize_divergence_threshold` | `float` | `0.7` | -- | `ge=0.0, le=1.0` | Text similarity below which LLM merge review is triggered (instead of deterministic merge) | Too low = LLM used too often (expensive); too high = deterministic merge of semantically different facts | `0.7` / `0.7` / `0.7` |

#### Stage 3: Strengthen

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.strengthen_success_ratio_threshold` | `float` | `0.5` | -- | `ge=0.0, le=1.0` | Minimum successful_use/use ratio to qualify for confidence boost | Too low = weakly used facts get boosted; too high = only heavily used facts strengthened | `0.5` / `0.5` / `0.5` |
| `consolidation.strengthen_min_use_count` | `int` | `3` | -- | `ge=1` | Minimum total uses before a fact can be strengthened | Too low = facts strengthened on insufficient data; too high = rarely-used-but-good facts never strengthened | `3` / `3` / `5` |
| `consolidation.strengthen_boost_factor` | `float` | `0.1` | -- | `ge=0.01, le=0.5` | Confidence boost amount: `new = min(1.0, old + boost_factor * success_ratio)` | Too low = negligible strengthening; too high = aggressive confidence inflation | `0.1` / `0.1` / `0.1` |

#### Stage 4: Decay

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.decay_recalled_unused_factor` | `float` | `0.85` | -- | `ge=0.1, le=1.0` | Per-cycle decay multiplier for facts recalled but never used: `confidence *= factor` | Too low = aggressive decay (data loss); too high = useless facts persist | `0.85` / `0.85` / `0.85` |
| `consolidation.decay_never_recalled_factor` | `float` | `0.95` | -- | `ge=0.1, le=1.0` | Per-cycle decay multiplier for facts never recalled (gentler time-based decay) | Too low = aggressive decay of untested facts; too high = orphan facts never decay | `0.95` / `0.95` / `0.95` |
| `consolidation.decay_archival_threshold` | `float` | `0.05` | -- | `ge=0.0, le=0.5` | Confidence below which facts are marked for archival | Too low = nearly-zero-confidence facts linger; too high = premature archival | `0.05` / `0.05` / `0.05` |
| `consolidation.decay_scope_multipliers` | `dict[str, float]` | `{"session": 1.5, "actor": 1.0, "team": 0.8, "organization": 0.7, "global": 0.5}` | -- | -- | Scope-specific multipliers on decay rate (higher = faster decay) | Wrong multipliers = scope-inappropriate decay rates; session facts should decay fastest, global slowest | Use defaults |

#### Stage 5: Prune Bad Autorecall

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.autorecall_blacklist_min_recalls` | `int` | `5` | -- | `ge=2` | Minimum recall count before considering blacklisting | Too low = premature blacklisting on insufficient data; too high = bad autorecalls persist too long | `5` / `5` / `10` |
| `consolidation.autorecall_blacklist_max_success_ratio` | `float` | `0.0` | -- | `ge=0.0, le=1.0` | Maximum success ratio to trigger blacklisting (0.0 = only blacklist if NEVER successfully used) | Higher = more aggressive blacklisting; 0.0 is very conservative | `0.0` / `0.0` / `0.1` |

#### Stage 6: Promote

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.promote_session_threshold` | `int` | `3` | -- | `ge=2` | Number of distinct sessions a session-scoped fact must appear in to be promoted to broader scope | Too low = premature promotion; too high = useful cross-session facts stay session-scoped | `3` / `3` / `5` |
| `consolidation.promote_artifact_injected_threshold` | `int` | `3` | -- | `ge=1` | Minimum artifact injection count for promotion consideration | Too low = noisy promotion; too high = useful artifacts never promoted | `3` / `3` / `3` |

#### Stage 7: Refine Procedures

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.pattern_recurrence_threshold` | `int` | `3` | -- | `ge=2` | Minimum recurrences of an action pattern to suggest a procedure | Too low = noisy procedure suggestions; too high = misses useful patterns | `3` / `3` / `5` |
| `consolidation.pattern_min_steps` | `int` | `3` | -- | `ge=2` | Minimum steps in a detected pattern to suggest a procedure | Too low = trivial procedures suggested; too high = misses short useful procedures | `3` / `3` / `3` |
| `consolidation.max_patterns_per_run` | `int` | `10` | -- | `ge=1` | Maximum procedure suggestions per consolidation run | Too low = misses patterns; too high = overwhelming number of suggestions | `10` / `10` / `20` |

#### Stage 9: Recompute Salience (Weight Tuning)

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.ema_alpha` | `float` | `0.3` | -- | `ge=0.01, le=1.0` | EMA smoothing factor for weight adjustment correlation tracking (higher = more responsive to recent data) | Too low = sluggish adaptation; too high = volatile weight adjustments | `0.3` / `0.3` / `0.3` |
| `consolidation.max_weight_adjustment_pct` | `float` | `0.05` | -- | `ge=0.01, le=0.20` | Maximum accumulated delta as fraction of BASE weight (caps weight drift) | Too low = weight tuning ineffective; too high = weights drift dangerously from profile baseline | `0.05` / `0.05` / `0.03` |
| `consolidation.min_correlation_samples` | `int` | `20` | -- | `ge=5` | Minimum scored facts needed before attempting weight adjustments | Too low = adjustments on noisy data; too high = never adjusts for low-volume gateways | `20` / `20` / `50` |

#### LLM and Dev

| Name | Type | Default | Env Var | Constraints | Controls | Impact of Wrong Values | Example (dev / staging / prod) |
|------|------|---------|---------|-------------|----------|----------------------|-------------------------------|
| `consolidation.llm_calls_per_run_cap` | `int` | `50` | -- | `ge=0` | Maximum LLM calls across all stages in one consolidation run (cost guard) | Too low = consolidation skips LLM-dependent stages; too high = runaway costs | `50` / `50` / `100` |
| `consolidation.dev_auto_trigger_interval` | `str` | `"0"` | `EB_DEV_CONSOLIDATION_AUTO_TRIGGER` | -- | Auto-trigger interval for dev/test (`"0"` = disabled, supports `"1m"`, `"5m"`, `"1h"`, `"1d"`) | Enabling in prod = consolidation runs too frequently, overwhelming LLM costs | `"1m"` / `"0"` / `"0"` |

---

### CLI-Only Environment Variables

These are not in `ElephantBrokerConfig` but are read by the `ebrun` CLI (`elephantbroker/cli.py`):

| Env Var | Default | Controls |
|---------|---------|----------|
| `EB_ACTOR_ID` | (none) | Pre-populates the `--actor-id` flag for `ebrun` CLI commands |
| `EB_RUNTIME_URL` | (none) | Pre-populates the `--url` flag for `ebrun` CLI commands (the EB runtime HTTP endpoint) |

---

### Complete Environment Variable Index

All 70+ `EB_*` environment variables in one alphabetical table:

| Env Var | Config Path | Type | Default |
|---------|------------|------|---------|
| `EB_ACTOR_ID` | CLI only | `str` | -- |
| `EB_AGENT_AUTHORITY_LEVEL` | `gateway.agent_authority_level` | `int` | `0` |
| `EB_CLICKHOUSE_DATABASE` | `infra.clickhouse.database` | `str` | `otel` |
| `EB_CLICKHOUSE_ENABLED` | `infra.clickhouse.enabled` | `bool` | `false` |
| `EB_CLICKHOUSE_HOST` | `infra.clickhouse.host` | `str` | `localhost` |
| `EB_CLICKHOUSE_PORT` | `infra.clickhouse.port` | `int` | `8123` |
| `EB_COMPACTION_LLM_API_KEY` | `compaction_llm.api_key` | `str` | `""` (fallback: `EB_LLM_API_KEY`) |
| `EB_COMPACTION_LLM_ENDPOINT` | `compaction_llm.endpoint` | `str` | (fallback: `llm.endpoint`) |
| `EB_COMPACTION_LLM_MODEL` | `compaction_llm.model` | `str` | `gemini/gemini-2.5-flash-lite` |
| `EB_CONSOLIDATION_BATCH_SIZE` | `consolidation.batch_size` | `int` | `500` |
| `EB_CONSOLIDATION_MIN_RETENTION_SECONDS` | `consolidation_min_retention_seconds` | `int` | `172800` |
| `EB_DEFAULT_DATASET` | `cognee.default_dataset` | `str` | `elephantbroker` |
| `EB_DEFAULT_PROFILE` | `default_profile` | `str` | `coding` |
| `EB_DEV_CONSOLIDATION_AUTO_TRIGGER` | `consolidation.dev_auto_trigger_interval` | `str` | `0` |
| `EB_EMBEDDING_API_KEY` | `cognee.embedding_api_key` | `str` | `""` |
| `EB_EMBEDDING_CACHE_ENABLED` | `embedding_cache.enabled` | `bool` | `true` |
| `EB_EMBEDDING_CACHE_TTL` | `embedding_cache.ttl_seconds` | `int` | `3600` |
| `EB_EMBEDDING_DIMENSIONS` | `cognee.embedding_dimensions` | `int` | `768` |
| `EB_EMBEDDING_ENDPOINT` | `cognee.embedding_endpoint` | `str` | `http://localhost:8811/v1` |
| `EB_EMBEDDING_MODEL` | `cognee.embedding_model` | `str` | `gemini/text-embedding-004` |
| `EB_EMBEDDING_PROVIDER` | `cognee.embedding_provider` | `str` | `openai` |
| `EB_GUARDS_ENABLED` | `guards.enabled` | `bool` | `true` |
| `EB_ENABLE_TRACE_LEDGER` | `enable_trace_ledger` | `bool` | `true` |
| `EB_EXTRACTION_CONTEXT_FACTS` | `llm.extraction_context_facts` | `int` | `20` |
| `EB_EXTRACTION_CONTEXT_TTL` | `llm.extraction_context_ttl_seconds` | `int` | `3600` |
| `EB_GATEWAY_ID` | `gateway.gateway_id` | `str` | `local` |
| `EB_GATEWAY_SHORT_NAME` | `gateway.gateway_short_name` | `str` | `""` |
| `EB_HITL_CALLBACK_SECRET` | `hitl.callback_hmac_secret` | `str` | `""` |
| `EB_INGEST_BATCH_SIZE` | `llm.ingest_batch_size` | `int` | `6` |
| `EB_INGEST_BATCH_TIMEOUT` | `llm.ingest_batch_timeout_seconds` | `float` | `60.0` |
| `EB_INGEST_BUFFER_TTL` | `llm.ingest_buffer_ttl_seconds` | `int` | `300` |
| `EB_LLM_API_KEY` | `llm.api_key` | `str` | `""` (fallback: `EB_EMBEDDING_API_KEY`) |
| `EB_LLM_ENDPOINT` | `llm.endpoint` | `str` | `http://localhost:8811/v1` |
| `EB_LLM_EXTRACTION_MAX_FACTS` | `llm.extraction_max_facts_per_batch` | `int` | `10` |
| `EB_LLM_EXTRACTION_MAX_INPUT_TOKENS` | `llm.extraction_max_input_tokens` | `int` | `4000` |
| `EB_LLM_EXTRACTION_MAX_OUTPUT_TOKENS` | `llm.extraction_max_output_tokens` | `int` | `16384` |
| `EB_LLM_MAX_TOKENS` | `llm.max_tokens` | `int` | `8192` |
| `EB_LLM_MODEL` | `llm.model` | `str` | `openai/gemini/gemini-2.5-pro` |
| `EB_LLM_SUMMARIZATION_MAX_OUTPUT_TOKENS` | `llm.summarization_max_output_tokens` | `int` | `200` |
| `EB_LLM_SUMMARIZATION_MIN_CHARS` | `llm.summarization_min_artifact_chars` | `int` | `500` |
| `EB_LLM_TEMPERATURE` | `llm.temperature` | `float` | `0.1` |
| `EB_LOG_LEVEL` | `infra.log_level` | `str` | `INFO` |
| `EB_MAX_CONCURRENT_SESSIONS` | `max_concurrent_sessions` | `int` | `100` |
| `EB_METRICS_TTL_SECONDS` | `infra.metrics_ttl_seconds` | `int` | `3600` |
| `EB_NEO4J_PASSWORD` | `cognee.neo4j_password` | `str` | `elephant_dev` |
| `EB_NEO4J_URI` | `cognee.neo4j_uri` | `str` | `bolt://localhost:7687` |
| `EB_NEO4J_USER` | `cognee.neo4j_user` | `str` | `neo4j` |
| `EB_OTEL_ENDPOINT` | `infra.otel_endpoint` | `str` | `None` |
| `EB_ORG_ID` | `gateway.org_id` | `str` | `None` |
| `EB_QDRANT_URL` | `cognee.qdrant_url` | `str` | `http://localhost:6333` |
| `EB_REDIS_URL` | `infra.redis_url` | `str` | `redis://localhost:6379` |
| `EB_RERANKER_API_KEY` | `reranker.api_key` | `str` | `""` |
| `EB_RERANKER_ENDPOINT` | `reranker.endpoint` | `str` | `http://localhost:1235` |
| `EB_RERANKER_MODEL` | `reranker.model` | `str` | `Qwen/Qwen3-Reranker-4B` |
| `EB_RUNTIME_URL` | CLI only | `str` | -- |
| `EB_SCORING_SNAPSHOT_TTL` | `scoring.snapshot_ttl_seconds` | `int` | `300` |
| `EB_SESSION_GOALS_TTL` | `scoring.session_goals_ttl_seconds` | `int` | `86400` |
| `EB_SUCCESSFUL_USE_API_KEY` | `successful_use.api_key` | `str` | `""` (fallback: `EB_LLM_API_KEY`) |
| `EB_SUCCESSFUL_USE_BATCH_SIZE` | `successful_use.batch_size` | `int` | `5` |
| `EB_SUCCESSFUL_USE_ENABLED` | `successful_use.enabled` | `bool` | `false` |
| `EB_SUCCESSFUL_USE_ENDPOINT` | `successful_use.endpoint` | `str` | `http://localhost:8811/v1` |
| `EB_SUCCESSFUL_USE_MODEL` | `successful_use.model` | `str` | `gemini/gemini-2.5-flash-lite` |
| `EB_TEAM_ID` | `gateway.team_id` | `str` | `None` |
| `EB_TRACE_MEMORY_MAX_EVENTS` | `infra.trace.memory_max_events` | `int` | `10000` |
| `EB_TRACE_OTEL_LOGS_ENABLED` | `infra.trace.otel_logs_enabled` | `bool` | `false` |

---

### API Key Fallback Chain

After F2/F3, `_apply_inheritance_fallbacks()` (renamed from `_apply_api_key_fallbacks` in F7 once endpoint inheritance was added) runs after env overrides on every `load()`. The chain runs in tiers and only fires when the target field is empty after env override application — explicit YAML or env values are always respected.

**Tier 1:** `llm.api_key` ← `cognee.embedding_api_key` (if `llm.api_key` is empty)
**Tier 2:** `compaction_llm.api_key`, `successful_use.api_key` ← `llm.api_key` (each only if its own value is empty)
**Tier 3 (F7):** `compaction_llm.endpoint` ← `llm.endpoint` (if `compaction_llm.endpoint` is empty)

In practice, setting `EB_EMBEDDING_API_KEY` alone covers all LLM/embedding calls when using a single LiteLLM proxy. Because there is now exactly one load path (the F2/F3 unification removed the asymmetry between env-only and YAML+env modes), the chain works identically whether you use `--config` or rely on the packaged default.

---

### Parameters Only Settable via YAML (not in `ENV_OVERRIDE_BINDINGS`)

These parameters have no `EB_*` env var mapping in `ENV_OVERRIDE_BINDINGS` and can only be configured via YAML or code:

- `reranker.enabled`, `reranker.timeout_seconds`, `reranker.batch_size`, `reranker.max_documents`, `reranker.fallback_on_error`, `reranker.top_n`
- `infra.trace.memory_ttl_seconds`
- `infra.clickhouse.logs_table`
- `embedding_cache.key_prefix`
- All `scoring.*` except `snapshot_ttl_seconds` and `session_goals_ttl_seconds`
- All `verification_multipliers.*`
- All `conflict_detection.*`
- `successful_use.batch_timeout_seconds`, `successful_use.feed_last_facts`, `successful_use.min_confidence`, `successful_use.run_async`
- All `goal_injection.*`
- All `goal_refinement.*`
- All `procedure_candidates.*`
- All `audit.*`
- All `guards.*` and `guards.strictness_presets.*`
- `hitl.enabled`, `hitl.default_url`, `hitl.timeout_seconds`, `hitl.approval_default_timeout_seconds`, `hitl.gateway_overrides`
- `compaction_llm.max_tokens`, `compaction_llm.temperature`
- All `context_assembly.*`
- All `artifact_capture.*`
- All `artifact_assembly.*`
- All `async_analysis.*`
- `profile_cache.ttl_seconds`
- `gateway.register_agent_identity`, `gateway.register_agent_actor`
- All `consolidation.*` except `batch_size` and `dev_auto_trigger_interval`


---


## ElephantBroker Profile Configuration Reference

### 1. ProfilePolicy -- Top-Level Structure

Every profile is a `ProfilePolicy` object containing 13 top-level fields and 8 nested sub-policy objects. The profile controls all runtime behavior: scoring weights, retrieval strategy, guard strictness, compaction aggressiveness, budgets, autorecall, verification, and context assembly.

**Source:** `elephantbroker/schemas/profile.py`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | (required) | Unique profile identifier (e.g. "coding", "research") |
| `name` | `str` | (required) | Human-readable display name |
| `extends` | `str \| None` | `None` | Parent profile for inheritance chain (e.g. "base") |
| `graph_mode` | `GraphMode` | `"hybrid"` | Graph traversal mode: `local`, `hybrid`, `global`. Controls how far the knowledge graph is walked during retrieval and scoring. |
| `session_data_ttl_seconds` | `int` | `86400` | How long Redis session data (goals, working set, artifacts) persists. Minimum 3600. |
| `budgets` | `Budgets` | (see below) | Token and item count limits |
| `scoring_weights` | `ScoringWeights` | (see below) | 11-dimension weight vector for working-set competition |
| `compaction` | `CompactionPolicy` | (see below) | Compaction behavior |
| `autorecall` | `AutorecallPolicy` | (see below) | Auto-injection at turn start |
| `retrieval` | `RetrievalPolicy` | (see below) | 5-source retrieval pipeline configuration |
| `verification` | `VerificationPolicy` | (see below) | Claim verification strictness |
| `guards` | `GuardPolicy` | (see below) | Red-line guard pipeline configuration |
| `assembly_placement` | `AssemblyPlacementPolicy` | (see below) | 4-block context assembly placement controls |

---

### 2. Sub-Policy Schemas -- Every Field

#### 2.1 ScoringWeights (11 dimensions + 5 tuning knobs)

**Source:** `elephantbroker/schemas/working_set.py`

Controls the weighted sum used in working-set budget competition. Each candidate fact is scored on 11 dimensions; the weight vector determines which dimensions matter most. The `weighted_sum()` method computes the final score.

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `turn_relevance` | `float` | `1.0` | Weight for how relevant a fact is to the current turn's text (embedding cosine similarity). Higher = favor facts related to what the user just said. |
| `session_goal_relevance` | `float` | `1.0` | Weight for alignment with active session goals. Higher = goal-focused memory. |
| `global_goal_relevance` | `float` | `0.5` | Weight for alignment with persistent (org/team/actor-scoped) goals. |
| `recency` | `float` | `0.8` | Weight for time decay score. Controlled by exponential decay with `recency_half_life_hours`. |
| `successful_use_prior` | `float` | `0.6` | Weight for prior successful usage. Facts used successfully in past turns get boosted. Uses `successful_use_count / (use_count + 1)` ratio; neutral prior is 0.5 (configurable). |
| `confidence` | `float` | `0.4` | Weight for fact confidence level. Multiplied by verification status multipliers (`VerificationMultipliers`). |
| `evidence_strength` | `float` | `0.3` | Weight for number of supporting evidence refs. Score = `min(evidence_ref_count, evidence_refs_for_max_score) / evidence_refs_for_max_score`. |
| `novelty` | `float` | `0.5` | Weight for novelty (1.0 if not in compact_state, else 0.0). Rewards facts not yet seen in compacted context. |
| `redundancy_penalty` | `float` | `-0.7` | **Negative weight.** Penalizes facts that are near-duplicates of higher-scoring candidates already selected. Computed pairwise during greedy selection (context-dependent dimension). |
| `contradiction_penalty` | `float` | `-1.0` | **Negative weight.** Penalizes facts that contradict higher-confidence facts. Three-layer detection: SUPERSEDES edges, CONTRADICTS edges, embedding similarity + confidence gap. |
| `cost_penalty` | `float` | `-0.3` | **Negative weight.** Penalizes high-token-cost facts. Score = `token_size / token_budget`. |
| `recency_half_life_hours` | `float` | `69.0` | Exponential decay half-life. After this many hours, recency score drops to 0.5. Short = aggressive forgetting; long = persistent memory. Min 1.0. |
| `evidence_refs_for_max_score` | `int` | `3` | Number of evidence refs needed for max evidence_strength score (1.0). Min 1. |
| `redundancy_similarity_threshold` | `float` | `0.85` | Cosine similarity threshold above which two facts are considered redundant. Lower = more aggressive dedup. |
| `contradiction_similarity_threshold` | `float` | `0.9` | Cosine similarity threshold for Layer 2 contradiction detection. Two similar facts with large confidence gap are flagged contradictory. |
| `contradiction_confidence_gap` | `float` | `0.3` | Minimum confidence difference between two similar facts for contradiction detection. |

#### 2.2 Budgets

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `mem0_fetch_k` | `int` | `20` | Max items fetched from primary memory store per retrieval. |
| `graph_fetch_k` | `int` | `15` | Max items fetched via graph expansion. |
| `artifact_fetch_k` | `int` | `10` | Max artifacts fetched per retrieval. |
| `final_prompt_k` | `int` | `30` | Max items in the final working set after budget competition. |
| `root_top_k` | `int` | `40` | Max candidates entering the scoring pipeline before budget cut. |
| `max_prompt_tokens` | `int` | `8000` | Token budget for the working set block in context assembly. The budget selector greedily fills this. |
| `max_system_overlay_tokens` | `int` | `1500` | Token budget for systemPromptAddition (Block 1: constraints, procedures, guards). |
| `subagent_packet_tokens` | `int` | `3000` | Token budget for the inherited context packet sent to subagents via SUBAGENT_INHERIT. |

#### 2.3 CompactionPolicy

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `cadence` | `str` | `"balanced"` | Compaction aggressiveness: `"aggressive"` (compact often, smaller context), `"balanced"` (normal), `"minimal"` (rarely compact, larger context). |
| `target_tokens` | `int` | `4000` | Target token count after compaction. Compaction engine summarizes until context fits. Min 100. |
| `preserve_goal_state` | `bool` | `True` | If true, goal-relevant facts are protected from compaction summarization. |
| `preserve_open_questions` | `bool` | `True` | If true, unanswered questions are kept verbatim during compaction. |
| `preserve_evidence_refs` | `bool` | `True` | If true, facts with evidence references are protected from compaction. |

#### 2.4 RetrievalPolicy

Controls the 5-source retrieval pipeline (structural Cypher, keyword/BM25, semantic/vector, graph expansion, artifacts).

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `isolation_level` | `IsolationLevel` | `"loose"` | Memory partition strictness: `"none"` (no filtering), `"loose"` (prefer same scope, allow cross-scope), `"strict"` (hard filter by scope). |
| `isolation_scope` | `IsolationScope` | `"session_key"` | Dimension for isolation: `"global"`, `"session_key"`, `"actor"`, `"subagent_inherit"`. |
| `structural_enabled` | `bool` | `True` | Enable/disable structural (Cypher) retrieval source. |
| `structural_fetch_k` | `int` | `20` | Max items from structural retrieval. |
| `structural_weight` | `float` | `0.4` | Weight for structural source in merge/dedup. |
| `keyword_enabled` | `bool` | `True` | Enable/disable keyword (CHUNKS_LEXICAL via cognify) retrieval. |
| `keyword_fetch_k` | `int` | `15` | Max items from keyword search. |
| `keyword_weight` | `float` | `0.3` | Weight for keyword source in merge/dedup. |
| `vector_enabled` | `bool` | `True` | Enable/disable semantic (CHUNKS embedding) retrieval. |
| `vector_fetch_k` | `int` | `20` | Max items from vector search. |
| `vector_weight` | `float` | `0.5` | Weight for vector source in merge/dedup. |
| `graph_expansion_enabled` | `bool` | `True` | Enable/disable graph expansion (GRAPH_COMPLETION) retrieval. |
| `graph_mode` | `GraphMode` | `"hybrid"` | Graph traversal mode: `local` (1-hop), `hybrid` (configurable), `global` (full graph). |
| `graph_max_depth` | `int` | `2` | Max graph traversal depth. Range 1-5. |
| `graph_expansion_weight` | `float` | `0.2` | Weight for graph expansion source in merge/dedup. |
| `artifact_enabled` | `bool` | `True` | Enable/disable artifact retrieval source. |
| `artifact_fetch_k` | `int` | `10` | Max artifacts fetched. |
| `root_top_k` | `int` | `40` | Max merged candidates after all sources are combined. |

#### 2.5 AutorecallPolicy

Controls the `before_agent_start` hook -- automatic fact injection at the beginning of each turn.

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `enabled` | `bool` | `True` | Master switch for autorecall. |
| `require_successful_use_prior` | `bool` | `False` | If true, only recall facts that have been successfully used before. Prevents injection of untested facts. |
| `require_not_in_compact_state` | `bool` | `True` | If true, skip facts already in the compacted context state. Prevents redundant injection. |
| `retrieval` | `RetrievalPolicy` | (nested) | Separate retrieval policy for autorecall queries. Can differ from the main retrieval policy (e.g., disable vector search for speed). |
| `auto_recall_injection_top_k` | `int` | `10` | Max facts injected per turn via autorecall. |
| `min_similarity` | `float` | `0.3` | Minimum cosine similarity to the turn text for a fact to be auto-recalled. |
| `extraction_max_facts_per_batch_before_dedup` | `int` | `5` | Max facts extracted per LLM batch before deduplication. |
| `dedup_similarity` | `float` | `0.95` | Cosine similarity threshold for dedup during extraction. Facts above this threshold are considered duplicates. |
| `extraction_focus` | `list[str]` | `[]` | LLM extraction focus categories (e.g., `["code decisions", "architecture choices"]`). Guides what types of facts the extraction pipeline looks for. |
| `custom_categories` | `list[str]` | `[]` | Custom fact categories added to the extraction taxonomy (e.g., `["code_decision", "architecture"]`). |
| `superseded_confidence_factor` | `float` | `0.3` | Confidence multiplier applied to facts that have been superseded by newer facts. Lower = more aggressive supersession penalty. |

#### 2.6 VerificationPolicy

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `proof_required_for_completion` | `bool` | `False` | If true, procedures cannot be marked complete without evidence for all steps. |
| `supervisor_sampling_rate` | `float` | `0.0` | Fraction of claims routed to human supervisor for verification (0.0 = none, 1.0 = all). |

#### 2.7 GuardPolicy

Controls the 6-layer red-line guard pipeline.

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `force_system_constraint_injection` | `bool` | `True` | Always inject active constraints into Block 1 systemPromptAddition, regardless of guard outcome. |
| `preflight_check_strictness` | `str` | `"medium"` | Strictness preset name (`"loose"`, `"medium"`, `"strict"`). Controls BM25 threshold multiplier, semantic threshold, structural validators, reinjection triggers, and LLM escalation triggers. See Section 5 below. |
| `static_rules` | `list[StaticRule]` | `[]` | Custom static rules (keyword/phrase/regex/tool_target patterns). Added on top of 16 built-in rules. |
| `redline_exemplars` | `list[str]` | `[]` | Seed exemplar texts for the BM25+semantic similarity index. |
| `structural_validators` | `list[StructuralValidatorSpec]` | `[]` | Custom structural validators for Layer 3 (required fields, action_target patterns). |
| `bm25_block_threshold` | `float` | `0.85` | BM25 score threshold for BLOCK outcome in semantic similarity layer. |
| `bm25_warn_threshold` | `float` | `0.60` | BM25 score threshold for WARN outcome. |
| `semantic_similarity_threshold` | `float` | `0.80` | Embedding cosine similarity threshold for semantic match in guard layer. |
| `llm_escalation_enabled` | `bool` | `False` | Enable Layer 6 LLM escalation for ambiguous guard outcomes. Expensive -- off by default. |
| `autonomy` | `AutonomyPolicy` | (see below) | Per-domain autonomy levels. |
| `approval_routing` | `ApprovalRouting` | (see below) | Approval timeout, fallback action, notification channels. |
| `near_miss_escalation_threshold` | `int` | `3` | Number of near-miss guard events within the window that triggers escalation. |
| `near_miss_window_turns` | `int` | `5` | Turn window for near-miss counting. |
| `load_procedure_redline_bindings` | `bool` | `True` | If true, load procedure-specific red-line rules into the guard index at session start. |

**AutonomyPolicy sub-fields:**

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `domain_levels` | `dict[str, AutonomyLevel]` | `{}` | Map of decision domain to autonomy level. 9 built-in domains (see Section 5). |
| `default_level` | `AutonomyLevel` | `"inform"` | Fallback level for domains not explicitly mapped. |
| `custom_domains` | `list[CustomDomain]` | `[]` | User-defined domains with keyword and tool_pattern matchers. |

**ApprovalRouting sub-fields:**

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `timeout_seconds` | `int` | `300` | Seconds before an unanswered approval request times out. Min 30. |
| `timeout_action` | `AutonomyLevel` | `"hard_stop"` | What happens on approval timeout: `"hard_stop"` (block), `"inform"`, etc. |
| `notify_channels` | `list[str]` | `[]` | Notification channels for approval requests. |

#### 2.8 AssemblyPlacementPolicy

Controls how context is placed in the 4-block assembly (Block 1: systemPromptAddition, Block 2: runtimeContext, Block 3: tool output replacement, Block 4: conversation dedup).

| Field | Type | Default | Runtime Impact |
|-------|------|---------|----------------|
| `system_prompt_constraints` | `bool` | `True` | Include active constraints in Block 1 systemPromptAddition. |
| `system_prompt_procedures` | `bool` | `True` | Include active procedure steps in Block 1. |
| `system_prompt_guards` | `bool` | `True` | Include guard-reinjected constraints in Block 1. |
| `system_context_goals` | `bool` | `True` | Include goal state in Block 2 runtimeContext. |
| `system_context_blockers` | `bool` | `True` | Include blockers in Block 2 runtimeContext. |
| `context_working_set` | `bool` | `True` | Include working set facts in Block 2 runtimeContext. |
| `evidence_refs` | `bool` | `True` | Include evidence references alongside facts. |
| `replace_tool_outputs` | `bool` | `True` | Replace tool outputs with compact placeholders in conversation history. |
| `replace_tool_output_min_tokens` | `int` | `100` | Minimum token count for a tool output to be replaced. Outputs below this threshold are kept verbatim. |
| `keep_last_n_tool_outputs` | `int` | `1` | Number of most recent tool outputs to keep verbatim (not replaced). |
| `conversation_dedup_enabled` | `bool` | `True` | Enable deduplication of repeated content in conversation history. |
| `conversation_dedup_threshold` | `float` | `0.7` | Similarity threshold for conversation dedup. Range 0.3-1.0. |
| `goal_injection_cadence` | `Literal["always", "smart"]` | `"smart"` | `"always"` = inject goals every turn. `"smart"` = inject only every N turns (controlled by `goal_reminder_interval`). |
| `goal_reminder_interval` | `int` | `5` | When cadence is `"smart"`, inject goals every N turns. |

---

### 3. Per-Profile Preset Values

**Source:** `elephantbroker/runtime/profiles/presets.py`

All 5 named profiles extend `"base"`. The base profile uses Pydantic defaults (shown above).

#### 3.1 ScoringWeights by Profile

| Dimension | Base | Coding | Research | Managerial | Worker | Personal Assistant |
|-----------|------|--------|----------|------------|--------|--------------------|
| `turn_relevance` | 1.0 | **1.5** | 0.8 | 0.7 | **1.3** | 1.0 |
| `session_goal_relevance` | 1.0 | 1.2 | 1.0 | **1.5** | **1.4** | 0.8 |
| `global_goal_relevance` | 0.5 | 0.3 | **0.8** | **1.0** | 0.3 | 0.4 |
| `recency` | 0.8 | **1.2** | 0.5 | 0.6 | **1.3** | 0.9 |
| `successful_use_prior` | 0.6 | 0.8 | 0.6 | 0.5 | 0.7 | **0.9** |
| `confidence` | 0.4 | 0.3 | **0.8** | 0.5 | 0.4 | 0.3 |
| `evidence_strength` | 0.3 | 0.2 | **0.9** | **0.7** | 0.3 | 0.2 |
| `novelty` | 0.5 | 0.6 | **0.7** | 0.4 | 0.5 | 0.5 |
| `redundancy_penalty` | -0.7 | **-0.8** | -0.5 | **-0.9** | -0.7 | -0.6 |
| `contradiction_penalty` | -1.0 | -1.0 | -1.0 | -1.0 | -1.0 | -1.0 |
| `cost_penalty` | -0.3 | -0.4 | -0.2 | **-0.5** | -0.4 | -0.3 |
| `recency_half_life_hours` | 69.0 | **24.0** | **168.0** | 72.0 | **12.0** | **720.0** |
| `evidence_refs_for_max_score` | 3 | 2 | **5** | 3 | 2 | 3 |
| `redundancy_similarity_threshold` | 0.85 | 0.85 | **0.80** | **0.90** | 0.85 | 0.85 |
| `contradiction_similarity_threshold` | 0.9 | 0.9 | **0.85** | 0.9 | 0.9 | 0.9 |
| `contradiction_confidence_gap` | 0.3 | 0.3 | 0.25 | 0.3 | 0.3 | **0.35** |

**Design rationale:** Coding and Worker emphasize turn relevance and recency (fast-moving task context). Research and Managerial emphasize evidence, confidence, and goals (accountability). Personal Assistant has the longest recency half-life (720h = 30 days) and highest successful_use_prior weight -- it remembers habits and preferences long-term.

#### 3.2 Budgets by Profile

| Field | Base | Coding | Research | Managerial | Worker | Personal Assistant |
|-------|------|--------|----------|------------|--------|--------------------|
| `max_prompt_tokens` | 8000 | 8000 | **12000** | 8000 | **6000** | 8000 |
| `max_system_overlay_tokens` | 1500 | 1500 | 1500 | 1500 | 1500 | 1500 |
| `subagent_packet_tokens` | 3000 | 3000 | 3000 | 3000 | 3000 | 3000 |
| `mem0_fetch_k` | 20 | 20 | 20 | 20 | 20 | 20 |
| `graph_fetch_k` | 15 | 15 | 15 | 15 | 15 | 15 |
| `artifact_fetch_k` | 10 | 10 | 10 | 10 | 10 | 10 |
| `final_prompt_k` | 30 | 30 | 30 | 30 | 30 | 30 |
| `root_top_k` | 40 | 40 | 40 | 40 | 40 | 40 |

Only `max_prompt_tokens` varies. Research gets 12000 (more context for literature), Worker gets 6000 (focused tasks, less context noise).

#### 3.3 CompactionPolicy by Profile

| Field | Base | Coding | Research | Managerial | Worker | Personal Assistant |
|-------|------|--------|----------|------------|--------|--------------------|
| `cadence` | balanced | **aggressive** | **minimal** | **aggressive** | balanced | balanced |
| `target_tokens` | 4000 | 4000 | 4000 | 4000 | 4000 | 4000 |
| `preserve_goal_state` | true | true | true | true | true | true |
| `preserve_open_questions` | true | true | true | true | true | true |
| `preserve_evidence_refs` | true | true | true | true | true | true |

Coding and Managerial compact aggressively (stay focused). Research compacts minimally (preserve full context for cross-reference).

#### 3.4 RetrievalPolicy by Profile

| Field | Base | Coding | Research | Managerial | Worker | Personal Assistant |
|-------|------|--------|----------|------------|--------|--------------------|
| `isolation_level` | loose | loose | **none** | loose | loose | **strict** |
| `isolation_scope` | session_key | session_key | **global** | session_key | session_key | session_key |
| `structural_weight` | 0.4 | **0.5** | 0.3 | 0.4 | **0.5** | 0.3 |
| `keyword_weight` | 0.3 | **0.4** | 0.2 | 0.2 | **0.4** | 0.3 |
| `vector_weight` | 0.5 | 0.3 | **0.5** | 0.3 | 0.3 | **0.5** |
| `graph_expansion_weight` | 0.2 | 0.1 | **0.3** | **0.4** | 0.1 | 0.2 |
| `graph_mode` | hybrid | **local** | **global** | hybrid | **local** | hybrid |
| `graph_max_depth` | 2 | **1** | **3** | 2 | **1** | 2 |
| `structural_fetch_k` | 20 | 20 | 15 | 20 | 20 | 15 |
| `keyword_fetch_k` | 15 | 15 | 15 | 15 | 15 | 15 |
| `vector_fetch_k` | 20 | 20 | **25** | 20 | 20 | 20 |
| `artifact_fetch_k` | 10 | 10 | **15** | 10 | 10 | 10 |
| `root_top_k` | 40 | 40 | 40 | 40 | 40 | 40 |
| `structural_enabled` | true | true | true | true | true | true |
| `keyword_enabled` | true | true | true | true | true | true |
| `vector_enabled` | true | true | true | true | true | true |
| `graph_expansion_enabled` | true | true | true | true | true | true |
| `artifact_enabled` | true | true | true | true | true | true |

Research uses no isolation (global scope, full graph traversal depth 3). Personal Assistant uses strict isolation (privacy). Coding and Worker use local graph (1-hop, fast).

#### 3.5 AutorecallPolicy by Profile

| Field | Base | Coding | Research | Managerial | Worker | Personal Assistant |
|-------|------|--------|----------|------------|--------|--------------------|
| `enabled` | true | true | true | true | true | true |
| `extraction_focus` | [] | code decisions, architecture, tech prefs, tool configs, error patterns | findings, hypotheses, methodology, data sources, citations | decisions, delegations, deadlines, blockers, team dynamics | task instructions, tool outputs, progress, blockers | preferences, habits, schedules, relationships, reminders |
| `custom_categories` | [] | code_decision, architecture, debugging, tooling | hypothesis, finding, methodology, citation | delegation, deadline, blocker, team_dynamic | (none) | (none) |
| `superseded_confidence_factor` | 0.3 | **0.1** | **0.5** | 0.3 | **0.2** | **0.4** |
| **Autorecall retrieval** | | | | | | |
| `structural_weight` | 0.4 | **0.7** | 0.3 | **0.5** | **0.7** | 0.4 |
| `keyword_weight` | 0.3 | 0.3 | 0.2 | (disabled) | 0.3 | (disabled) |
| `vector_enabled` | true | **false** | true | true | **false** | true |
| `vector_weight` | 0.5 | -- | **0.5** | 0.5 | -- | **0.6** |
| `graph_expansion_enabled` | true | **false** | true | true | **false** | **false** |
| `keyword_enabled` | true | true | true | **false** | true | **false** |
| `root_top_k` | 40 | **15** | 15 | **20** | **15** | **15** |

Coding and Worker use fast structural-only autorecall (no vector, no graph). Research mirrors its main retrieval policy (full breadth). Personal Assistant uses vector+structural but no keyword/graph (preferences are better found by semantic similarity).

#### 3.6 Session TTL by Profile

| Profile | `session_data_ttl_seconds` | Human-readable |
|---------|---------------------------|----------------|
| Base | 86400 | 1 day |
| Coding | 86400 | 1 day |
| Research | **259200** | **3 days** |
| Managerial | **172800** | **2 days** |
| Worker | 86400 | 1 day |
| Personal Assistant | **604800** | **7 days** |

Personal Assistant sessions persist for a full week. Research gets 3 days for literature review continuity.

#### 3.7 AssemblyPlacementPolicy by Profile

| Field | Base | Coding | Research | Managerial | Worker | Personal Assistant |
|-------|------|--------|----------|------------|--------|--------------------|
| `goal_injection_cadence` | smart | smart | smart | **always** | smart | smart |
| `goal_reminder_interval` | 5 | 5 | **10** | -- (always) | **3** | **8** |
| `keep_last_n_tool_outputs` | 1 | **2** | **0** | 1 | 1 | 1 |
| `replace_tool_outputs` | true | true | **false** | true | true | true |
| `system_context_blockers` | true | true | true | true | true | **false** |

Managerial injects goals every turn (always aware of objectives). Research keeps all tool outputs verbatim (never replaces). Worker gets frequent goal reminders (every 3 turns). Personal Assistant hides blockers from context (not task-oriented).

---

### 4. Inheritance Chain and Org Override Mechanism

**Source files:**
- `elephantbroker/runtime/profiles/inheritance.py`
- `elephantbroker/runtime/profiles/org_override_store.py`
- `elephantbroker/runtime/profiles/registry.py`

#### 4.1 Inheritance Chain

Resolution order (4 layers):

```
Layer 1: Base Profile (Pydantic defaults)
    |
Layer 2: Named Profile (e.g. "coding" extends "base")
    |
Layer 3: Org Override (sparse dict from SQLite, applied on top)
    |
Layer 4: Tuning Delta (Phase 9 ScoringTuner, per-gateway adaptive adjustments)
```

**How it works:**

1. `ProfileInheritanceEngine.flatten()` walks the `extends` chain from leaf to root, validating ancestry and detecting circular references.
2. Since all builtin presets are complete `ProfilePolicy` objects (not sparse overrides), the leaf profile is already fully resolved. The chain walk is primarily for validation.
3. Org overrides are applied via `_apply_org_overrides()` -- a sparse dict merge where only specified keys change.
4. Tuning deltas (Phase 9) are applied by the `ScoringTuner` after resolution. These are per-gateway scoring weight adjustments learned from the consolidation pipeline (capped at +/-5% of base weight per consolidation cycle, EMA-smoothed).

#### 4.2 Org Override Mechanism

**Storage:** SQLite database (`data/org_overrides.db`), table `org_profile_overrides` with composite primary key `(org_id, profile_id)`.

**Schema:**
```sql
org_profile_overrides (
    org_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    overrides_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by_actor_id TEXT,
    PRIMARY KEY (org_id, profile_id)
)
```

**What can be overridden:** Any field on `ProfilePolicy` and any field on any nested sub-policy. The override is a sparse JSON dict. Example:

```json
{
    "scoring_weights": {"evidence_strength": 0.9, "turn_relevance": 1.2},
    "budgets": {"max_prompt_tokens": 10000},
    "guards": {"preflight_check_strictness": "strict"},
    "session_data_ttl_seconds": 172800
}
```

**Merge semantics:**
- Top-level scalar fields: direct replacement.
- Nested Pydantic models (e.g., `scoring_weights`, `budgets`, `retrieval`): field-by-field merge. Only specified nested keys change; unspecified keys retain the resolved profile value.
- Unknown top-level keys: logged as warning, ignored.
- Unknown nested keys: logged as warning, ignored.

**Validation:** `OrgOverrideStore.set_override()` performs strict validation before persisting:
- Rejects unknown top-level keys (must be `ProfilePolicy` fields).
- Rejects unknown nested keys (e.g., `scoring_weights.nonexistent_field`).
- Rejects invalid types (attempts partial model construction with `model_validate()`).
- Raises `ValueError` on any validation failure.

**Cache:** `ProfileRegistry` caches resolved profiles in-memory by `(profile_id, org_id)` tuple. Default TTL is 300 seconds (5 minutes), configurable via `ProfileCacheConfig.ttl_seconds`. Cache is explicitly invalidated when overrides are registered or deleted. Returns deep copies to ensure immutability for session lifetime.

#### 4.3 API for Org Overrides

Registered via admin API endpoints (`/admin/profile/override`). Requires authority level checks per `AuthorityRuleStore` -- default `min_authority_level: 70` with `require_matching_org: True` and `matching_exempt_level: 90`.

---

### 5. Guard Policy Per Profile -- Autonomy Levels

**Source:** Guard presets are defined inline in each profile in `presets.py`.

#### 5.1 Autonomy Level Definitions

| Level | GuardOutcome | Behavior |
|-------|-------------|----------|
| `AUTONOMOUS` | `pass` | Agent proceeds without any notification. |
| `INFORM` | `inform` | Agent proceeds but logs the action and informs the human. |
| `APPROVE_FIRST` | `require_approval` | Agent must wait for human approval before proceeding. Routed via HITL middleware. |
| `HARD_STOP` | `block` | Action is blocked entirely. Cannot proceed even with approval. |

#### 5.2 Decision Domain x Profile Matrix (9 domains x 5 profiles = 45 cells)

| Domain | Coding | Research | Managerial | Worker | Personal Assistant |
|--------|--------|----------|------------|--------|--------------------|
| `financial` | HARD_STOP | APPROVE_FIRST | APPROVE_FIRST | HARD_STOP | HARD_STOP |
| `data_access` | APPROVE_FIRST | APPROVE_FIRST | APPROVE_FIRST | APPROVE_FIRST | HARD_STOP |
| `communication` | INFORM | INFORM | APPROVE_FIRST | INFORM | APPROVE_FIRST |
| `code_change` | **AUTONOMOUS** | INFORM | **HARD_STOP** | **AUTONOMOUS** | APPROVE_FIRST |
| `scope_change` | INFORM | INFORM | **AUTONOMOUS** | APPROVE_FIRST | INFORM |
| `resource` | **AUTONOMOUS** | **AUTONOMOUS** | INFORM | **AUTONOMOUS** | INFORM |
| `info_share` | INFORM | INFORM | APPROVE_FIRST | INFORM | APPROVE_FIRST |
| `delegation` | **AUTONOMOUS** | INFORM | **AUTONOMOUS** | INFORM | INFORM |
| `record_mutation` | **AUTONOMOUS** | INFORM | INFORM | **AUTONOMOUS** | INFORM |
| **Default level** | INFORM | INFORM | APPROVE_FIRST | INFORM | INFORM |
| **Strictness preset** | medium | loose | strict | medium | strict |

**Key patterns:**
- **Coding:** Free to change code, use resources, delegate, and mutate records autonomously. Financial actions are hard-stopped.
- **Research:** Relaxed everywhere (loose strictness). Only financial and data_access require approval. No domain is hard-stopped.
- **Managerial:** Strictest overall (strict preset, APPROVE_FIRST default). Can change scope and delegate autonomously but code changes are hard-stopped. Most domains require approval.
- **Worker:** Similar to Coding (task-oriented autonomy on code/resource/records) but scope changes require approval and delegation requires inform.
- **Personal Assistant:** Strict on financial and data_access (HARD_STOP). Communication and info_share require approval (privacy protection). Code changes require approval.

#### 5.3 Strictness Presets

**Source:** `elephantbroker/schemas/config.py` (`GuardConfig.strictness_presets`)

| Setting | Loose | Medium | Strict |
|---------|-------|--------|--------|
| `bm25_threshold_multiplier` | 1.5 (fewer triggers) | 1.0 (baseline) | 0.7 (more triggers) |
| `semantic_threshold_override` | 0.90 (high bar) | None (use policy default 0.80) | 0.70 (low bar) |
| `structural_validators_enabled` | false | true (implicit) | true (implicit) |
| `warn_outcome_upgrade` | None | None | `"require_approval"` |
| `reinjection_on` | `"block_only"` | `"elevated_risk"` | `"any_non_pass"` |
| `llm_escalation_on` | `"disabled"` | `"ambiguous"` | `"any_non_pass"` |

**Profiles using each preset:**
- **Loose:** Research
- **Medium:** Coding, Worker
- **Strict:** Managerial, Personal Assistant

---

### 6. What Each Parameter Controls -- Runtime Behavior Summary

#### Scoring Pipeline
The 11 scoring weights directly control which facts win the budget competition for inclusion in the working set. A fact's final score = `sum(weight_i * raw_score_i)` across all 11 dimensions. The `BudgetSelector` greedily selects facts by descending final score until `max_prompt_tokens` is reached. The two context-dependent dimensions (`redundancy_penalty`, `contradiction_penalty`) are computed during greedy selection -- after each fact is selected, remaining candidates are re-scored against the selected set.

#### Retrieval Pipeline
The retrieval weights and fetch_k values control how many candidates each of the 5 sources produces and how they are weighted during merge. The `RetrievalOrchestrator` runs all enabled sources in parallel, then merges results with weighted dedup. `isolation_level` and `isolation_scope` control whether facts from other sessions/actors/orgs are visible.

#### Guard Pipeline
The 6-layer pipeline runs in order: (1) Autonomy classification maps the action to a decision domain and looks up the per-profile autonomy level. (2) Static rules match patterns. (3) BM25+semantic similarity checks against the red-line exemplar index. (4) Structural validators check required fields. (5) Forced reinjection adds constraints to context. (6) Optional LLM escalation. The `preflight_check_strictness` preset modifies thresholds across layers 3-6. Guards are advisory -- they inject constraints into `systemPromptAddition` rather than physically blocking tool calls.

#### Compaction Pipeline
`cadence` determines how often compaction triggers (aggressive = lower threshold). `target_tokens` is the post-compaction size target. The three `preserve_*` flags protect specific fact categories from summarization.

#### Autorecall Pipeline
Runs at the start of each turn. Uses its own `RetrievalPolicy` (which can be simpler/faster than the main retrieval policy). `extraction_focus` guides what the LLM extracts from messages. `superseded_confidence_factor` controls how harshly superseded facts are penalized.

#### Context Assembly
Controls the 4-block layout: Block 1 (systemPromptAddition) gets constraints, procedures, guards. Block 2 (runtimeContext) gets goals, blockers, working set. `goal_injection_cadence` and `goal_reminder_interval` control goal injection frequency. `replace_tool_outputs` and `keep_last_n_tool_outputs` control how verbose tool outputs are in conversation history.


---


## ElephantBroker Infrastructure & Deployment Configuration

### 1. Docker Compose Services

#### 1.1 Production Infrastructure (`infrastructure/docker-compose.yml`)

All core infrastructure runs via Docker Compose. The ElephantBroker runtime itself runs as a native Python venv process, NOT inside Docker.

##### Neo4j (Graph Store)

```yaml
neo4j:
  image: neo4j:5-community
  ports:
    - "17474:7474"   # HTTP browser (remapped from default 7474)
    - "17687:7687"   # Bolt protocol (remapped from default 7687)
  environment:
    NEO4J_AUTH: neo4j/elephant_dev
    NEO4J_PLUGINS: '["apoc"]'
  volumes:
    - neo4j_data:/data   # persistent named volume
```

- **Image:** `neo4j:5-community` (Neo4j 5.x Community Edition)
- **APOC plugin** enabled for advanced Cypher procedures
- **Auth:** user `neo4j`, password `elephant_dev` (dev default)
- **Storage:** persistent Docker named volume `neo4j_data`

##### Qdrant (Vector Store)

```yaml
qdrant:
  image: qdrant/qdrant:v1.17.0
  ports:
    - "16333:6333"   # REST API (remapped from default 6333)
    - "16334:6334"   # gRPC (remapped from default 6334)
  volumes:
    - qdrant_data:/qdrant/storage
```

- **Image:** `qdrant/qdrant:v1.17.0` -- pinned to v1.17.0 (must match `qdrant-client>=1.7` in pyproject.toml)
- **Storage:** persistent Docker named volume `qdrant_data`

##### Redis (Cache / Session State)

```yaml
redis:
  image: redis:7-alpine
  ports:
    - "16379:6379"   # remapped from default 6379
  volumes:
    - redis_data:/data
```

- **Image:** `redis:7-alpine` (lightweight Alpine variant)
- **Storage:** persistent Docker named volume `redis_data`

##### OTEL Collector (Observability Profile)

```yaml
otel-collector:
  image: otel/opentelemetry-collector-contrib:latest
  ports:
    - "4317:4317"     # OTLP gRPC receiver
  volumes:
    - ./otel-collector-config.yaml:/etc/otelcol-contrib/config.yaml
  depends_on:
    - clickhouse
    - jaeger
  profiles:
    - observability   # only starts with --profile observability
```

##### ClickHouse (Analytics)

```yaml
clickhouse:
  image: clickhouse/clickhouse-server:latest
  ports:
    - "8123:8123"     # HTTP interface
    - "9000:9000"     # Native TCP
  environment:
    CLICKHOUSE_DB: otel
    CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    CLICKHOUSE_PASSWORD: ""     # empty password, dev/staging only
  volumes:
    - clickhouse_data:/var/lib/clickhouse
  profiles:
    - observability
```

##### Jaeger (Distributed Tracing UI)

```yaml
jaeger:
  image: jaegertracing/all-in-one:latest
  ports:
    - "16686:16686"   # Jaeger UI
    - "14250:14250"   # gRPC collector
  environment:
    SPAN_STORAGE_TYPE: memory
    COLLECTOR_OTLP_ENABLED: "true"
  profiles:
    - observability
```

- **Storage:** in-memory only (not persistent across restarts)

##### Grafana (Dashboards)

```yaml
grafana:
  image: grafana/grafana:latest
  ports:
    - "13000:3000"    # Web UI (remapped from default 3000)
  profiles:
    - observability
```

#### 1.2 Named Volumes

```yaml
volumes:
  neo4j_data:
  qdrant_data:
  redis_data:
  clickhouse_data:
```

All four are Docker-managed named volumes. Destroyed with `docker compose down -v`.

---

### 2. Production vs Test Config Differences

#### Test Infrastructure (`infrastructure/docker-compose.test.yml`)

| Aspect | Production | Test |
|--------|-----------|------|
| **Neo4j auth** | `neo4j/elephant_dev` | `neo4j/testpassword` |
| **Neo4j auth lockout** | Default (3 attempts) | Disabled (`auth_max_failed_attempts: 0`, `auth_lock_time: 0`) |
| **Storage** | Persistent named volumes | `tmpfs` (RAM-backed, ephemeral) |
| **Redis command** | Default | Explicit `redis-server` |
| **Observability stack** | Optional profile | Not included |
| **Port mappings** | Same (`17474`, `17687`, `16333`, `16334`, `16379`) | Same (no conflicts if test and prod don't run simultaneously) |
| **Images** | Same versions | Same versions (Qdrant pinned to v1.17.0 in both) |

Test compose uses `tmpfs` for all storage, ensuring each test run starts clean with no state carryover:

```yaml
## Test: ephemeral storage
neo4j:
  tmpfs:
    - /data
  environment:
    NEO4J_dbms_security_auth__max__failed__attempts: "0"
    NEO4J_dbms_security_auth__lock__time: "0"

qdrant:
  tmpfs:
    - /qdrant/storage

redis:
  command: redis-server
  tmpfs:
    - /data
```

---

### 3. OTEL Pipeline

#### Collector Configuration (`infrastructure/otel-collector-config.yaml`)

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  clickhouse:
    endpoint: tcp://clickhouse:9000?dial_timeout=10s
    database: otel
    username: default
    password: ""
    ttl: 72h
    logs_table_name: otel_logs
    create_schema: true
  otlp/jaeger:
    endpoint: jaeger:4317
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/jaeger]
    logs:
      receivers: [otlp]
      exporters: [clickhouse]
```

**Pipeline topology:**
- **Traces:** OTLP gRPC (port 4317) --> Jaeger (for visual trace exploration at port 16686)
- **Logs:** OTLP gRPC (port 4317) --> ClickHouse `otel_logs` table (for cross-session analytics, 72h TTL)
- **No processors** defined (passthrough)
- ClickHouse auth uses `default` user with empty password (dev/staging only)
- Jaeger connection uses `insecure: true` (no TLS between collector and Jaeger)

#### Python OTEL Integration

OTEL tracing is initialized in `RuntimeContainer.from_config()` via `setup_tracing()`:

```python
## elephantbroker/runtime/observability.py
def setup_tracing(config: InfraConfig, gateway_id: str = "local") -> TracerProvider:
    resource = Resource.create({
        "service.name": "elephantbroker",
        "gateway.id": gateway_id,
    })
    provider = TracerProvider(resource=resource)
    if config.otel_endpoint:
        exporter = OTLPSpanExporter(endpoint=config.otel_endpoint)
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
```

- Requires `opentelemetry-exporter-otlp-proto-grpc` pip package (optional install)
- `EB_OTEL_ENDPOINT` env var controls whether export is active (no endpoint = no-op spans)
- FastAPI auto-instrumented via `FastAPIInstrumentor.instrument_app(app)` in `create_app()`
- OTEL log export (TraceLedger events to ClickHouse) requires both `EB_OTEL_ENDPOINT` and `EB_TRACE_OTEL_LOGS_ENABLED=true`

---

### 4. Server Startup

#### CLI Entry Points (`pyproject.toml`)

```toml
[project.scripts]
elephantbroker = "elephantbroker.server:main"    # API server
ebrun = "elephantbroker.cli:main"                # Admin CLI
```

#### Server Command (`elephantbroker serve`)

```python
## elephantbroker/server.py
@cli.command()
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8420, type=int)
@click.option("--log-level", default="info")
@click.option("--config", type=click.Path(exists=True), default=None)
def serve(host, port, log_level, config):
    # Loads ElephantBrokerConfig from YAML or env
    # Builds RuntimeContainer (async, initializes all adapters)
    # Creates FastAPI app via create_app(container)
    # Runs uvicorn.Server with mapped log level
```

**Uvicorn configuration:**
- **Host:** `0.0.0.0` (default, configurable via `--host`)
- **Port:** `8420` (default, configurable via `--port`)
- **Workers:** 1 (single-process, no `--workers` flag -- uvicorn.Server, not uvicorn.run)
- **Log level:** maps `verbose` to `info` for uvicorn (uvicorn does not support custom levels)
- **Config source:** `--config /path/to/default.yaml` or env vars if no config flag

#### Health Check Command

```python
@cli.command("health-check")
@click.option("--host", default="localhost")
@click.option("--port", default=8420, type=int)
def health_check(host, port):
    # GET http://{host}:{port}/health/ready with 5s timeout
```

#### HITL Middleware Startup

```python
## hitl-middleware/hitl_middleware/__main__.py
def main():
    config = HitlMiddlewareConfig.from_env()
    app = create_app(config)
    uvicorn.run(app, host=config.host, port=config.port, log_level=config.log_level.lower())
```

- **Host:** `0.0.0.0` (from `HITL_HOST`, default)
- **Port:** `8421` (from `HITL_PORT`)
- **Log level:** from `HITL_LOG_LEVEL` (does NOT support `verbose` -- use `info` or `debug`)

---

### 5. Dockerfile

#### Build Stages

```dockerfile
## Stage 1: Builder — copies the uv binary from Astral's official image
FROM python:3.11-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY elephantbroker/ elephantbroker/

# uv sync --frozen installs EXACTLY what uv.lock specifies
RUN uv sync --frozen --no-dev

## Stage 2: Runtime
FROM python:3.11-slim AS runtime
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/elephantbroker /app/elephantbroker
COPY --from=builder /app/pyproject.toml /app/pyproject.toml
COPY elephantbroker/config/default.yaml /etc/elephantbroker/default.yaml

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8420
ENTRYPOINT ["elephantbroker", "serve", "--config", "/etc/elephantbroker/default.yaml"]
```

**Key details:**
- **Base image:** `python:3.11-slim` (both stages)
- **Package manager:** uv (Astral's `ghcr.io/astral-sh/uv:0.11.3` image), not pip — same as the native install path. Bit-for-bit identical environments between Docker and bare-metal deployments.
- **Lockfile-driven:** `uv sync --frozen` installs exactly what `uv.lock` specifies. No transitive drift.
- **No mistralai workaround needed:** uv's holistic resolver picks `mistralai==1.12.4` (a working modern version) automatically. The pip-era `--force-reinstall --no-deps 'mistralai>=1.0'` hack has been removed.
- **Baked-in config:** `elephantbroker/config/default.yaml` copied to `/etc/elephantbroker/default.yaml`
- **Required env var at runtime:** `EB_GATEWAY_ID` (must be set when running the container)
- **Optional env vars:** `EB_ORG_ID`, `EB_TEAM_ID`, `EB_ACTOR_ID`, `EB_NEO4J_URI`, `EB_QDRANT_URL`, `EB_REDIS_URL`
- **NOTE:** Per `CLAUDE.md`, the Dockerfile is for dev/CI only — production deployments use `deploy/install.sh` on a real host.

---

### 6. YAML Config

#### Default Configuration (`elephantbroker/config/default.yaml`)

```yaml
gateway:
  gateway_id: "local"
  org_id: ""
  team_id: ""
  agent_authority_level: 0

cognee:
  neo4j_uri: "bolt://localhost:7687"
  neo4j_user: "neo4j"
  neo4j_password: "elephant_dev"
  qdrant_url: "http://localhost:6333"
  embedding_endpoint: "http://localhost:8811/v1"

llm:
  model: "openai/gemini/gemini-2.5-pro"   # Cognee strips "openai/", sends "gemini/gemini-2.5-pro"
  endpoint: "http://localhost:8811/v1"

infra:
  redis_url: "redis://localhost:6379"
  log_level: "info"                        # debug | verbose | info | warning | error | critical

profile_cache:
  ttl_seconds: 300

audit:
  org_overrides_db_path: "data/org_overrides.db"
  authority_rules_db_path: "data/authority_rules.db"
```

#### Full Config Schema (`ElephantBrokerConfig`)

The `ElephantBrokerConfig` Pydantic model contains 25+ config sections. Top-level sections:

| Section | Purpose | Key Defaults |
|---------|---------|-------------|
| `gateway` | Gateway/agent/org identity | `gateway_id: "local"`, `agent_authority_level: 0` |
| `cognee` | Neo4j + Qdrant + Embedding config | `embedding_model: "gemini/text-embedding-004"`, `embedding_dimensions: 768` |
| `llm` | LLM for extraction/classification | `model: "openai/gemini/gemini-2.5-pro"`, `temperature: 0.1`, `max_tokens: 8192` |
| `reranker` | Cross-encoder reranking | `model: "Qwen/Qwen3-Reranker-4B"`, `endpoint: "http://localhost:1235"`, `timeout: 10s` |
| `infra` | Redis + OTEL + ClickHouse | `redis_url: "redis://localhost:6379"` |
| `scoring` | Working set scoring pipeline | `neutral_use_prior: 0.5`, `cheap_prune_max_candidates: 80` |
| `embedding_cache` | Redis-backed embedding cache | `ttl_seconds: 3600`, `enabled: true` |
| `context_assembly` | 4-block context assembly | `max_context_window_fraction: 0.15`, `fallback_context_window: 128000` |
| `guards` | Red-line guard engine | `enabled: true`, 3 strictness presets (loose/medium/strict) |
| `hitl` | Human-in-the-loop | `enabled: false`, `default_url: "http://localhost:8421"`, `timeout: 10s` |
| `audit` | SQLite audit trail paths | 7 DB paths under `data/`, `retention_days: 90` |
| `successful_use` | LLM-based use reasoning | `enabled: false`, `batch_size: 5` |
| `compaction_llm` | Separate LLM for compaction | `model: "gemini/gemini-2.5-flash-lite"`, `temperature: 0.2` |
| `goal_refinement` | Goal hint/refinement pipeline | `hints_enabled: true`, `max_subgoals_per_session: 10` |

#### Config Resolution Order

```
Environment variable (if set) > YAML value > Pydantic model default
```

After F2/F3, `ElephantBrokerConfig.load()` (or its internal `from_yaml()` reader, used either directly or via `load(None)` for the packaged default YAML) parses the YAML and then applies every binding in `ENV_OVERRIDE_BINDINGS` on top — currently 67 entries spanning identity, Cognee, LLM, compaction LLM, reranker, infra, trace, ClickHouse, embedding cache, scoring, HITL, successful-use, consolidation, and the top-level toggles.

There is no curated subset and no separate "env-only" path: the env-only callers go through `load(None)` and use the packaged `default.yaml` as their starting point. Read `ENV_OVERRIDE_BINDINGS` in `elephantbroker/schemas/config.py` for the canonical, contract-test-enforced list — every entry has a matching `env: EB_*` tag in `default.yaml` (verified by `tests/test_env_var_registry_completeness.py`).

---

### 7. Security Defaults

#### Authentication

| Component | Mechanism | Default |
|-----------|-----------|---------|
| **Runtime API** | `AuthMiddleware` | **Stub -- always passes** (no auth enforced) |
| **Gateway identity** | `GatewayIdentityMiddleware` | 5 HTTP headers (`X-EB-Gateway-ID`, `X-EB-Agent-Key`, `X-EB-Agent-ID`, `X-EB-Session-Key`, `X-EB-Actor-Id`) with fallback to `"local"` |
| **HITL callbacks** | HMAC validation | `EB_HITL_CALLBACK_SECRET` (shared secret, must match between runtime and HITL middleware) |
| **Neo4j** | Username/password | `neo4j/elephant_dev` (dev default) |
| **ClickHouse** | Password | Empty password (`""`) -- dev/staging only |
| **Redis** | None | No auth configured |
| **Qdrant** | None | No auth configured |
| **OTEL Collector to Jaeger** | TLS | `insecure: true` (plaintext) |

#### Credential Environment Variables

```bash
EB_LLM_API_KEY          # LLM provider API key
EB_EMBEDDING_API_KEY    # Embedding provider API key (falls back to LLM key if empty)
EB_HITL_CALLBACK_SECRET # HMAC secret for HITL callback validation
EB_RERANKER_API_KEY     # Reranker endpoint API key
```

#### TLS

No TLS configuration exists in any component. All inter-service communication is plaintext HTTP/Bolt/gRPC. The OTEL Collector to Jaeger link explicitly sets `insecure: true`.

#### Firewall Recommendations (from DEPLOYMENT.md)

| Port | Service | Expose to |
|------|---------|-----------|
| 8420 | Runtime API | OpenClaw VM only |
| 8421 | HITL middleware | OpenClaw VM only |
| 7474, 7687 | Neo4j | Internal only |
| 6333, 6334 | Qdrant | Internal only |
| 6379 | Redis | Internal only |

---

### 8. Resource Limits & Timeouts

#### Connection Configuration

| Resource | Setting | Default |
|----------|---------|---------|
| **Redis** | `decode_responses` | `True` (all values as strings) |
| **Redis** | Connection pool | Default `redis.asyncio` pool (no explicit max) |
| **Max concurrent sessions** | `max_concurrent_sessions` | `100` |
| **Embedding cache TTL** | `embedding_cache.ttl_seconds` | `3600` (1 hour) |
| **Profile cache TTL** | `profile_cache.ttl_seconds` | `300` (5 minutes) |
| **Metrics TTL** | `infra.metrics_ttl_seconds` | `3600` (1 hour) |
| **Session goals TTL** | `scoring.session_goals_ttl_seconds` | `86400` (24 hours) |
| **Guard history TTL** | `guards.history_ttl_seconds` | `86400` (24 hours) |
| **Consolidation min retention** | `consolidation_min_retention_seconds` | `172800` (48 hours) |
| **Audit retention** | `audit.retention_days` | `90` days |
| **ClickHouse log TTL** | OTEL Collector `ttl` | `72h` |

#### Timeouts

| Component | Setting | Default |
|-----------|---------|---------|
| **Reranker HTTP** | `reranker.timeout_seconds` | `10.0s` |
| **HITL HTTP** | `hitl.timeout_seconds` | `10.0s` |
| **HITL approval** | `hitl.approval_default_timeout_seconds` | `300s` (5 min) |
| **Guard LLM escalation** | `guards.llm_escalation_timeout_seconds` | `10.0s` |
| **Health check** | httpx timeout | `5.0s` |
| **Ingest batch** | `llm.ingest_batch_timeout_seconds` | `60.0s` |
| **Ingest buffer TTL** | `llm.ingest_buffer_ttl_seconds` | `300s` (5 min) |
| **Extraction context TTL** | `llm.extraction_context_ttl_seconds` | `3600s` (1 hour) |
| **Successful-use batch** | `successful_use.batch_timeout_seconds` | `120.0s` |
| **Webhook retry** | HITL `retry_delay_seconds` | `1.0s` (3 retries) |
| **Scoring snapshot TTL** | `scoring.snapshot_ttl_seconds` | `300s` (5 min) |
| **ClickHouse dial** | OTEL Collector `dial_timeout` | `10s` |

#### LLM Token Limits

| Parameter | Default |
|-----------|---------|
| `llm.max_tokens` | `8192` |
| `llm.extraction_max_input_tokens` | `4000` |
| `llm.extraction_max_output_tokens` | `16384` |
| `llm.extraction_max_facts_per_batch` | `10` |
| `llm.summarization_max_output_tokens` | `200` |
| `llm.ingest_batch_size` | `6` messages per batch |
| `compaction_llm.max_tokens` | `2000` |
| `guards.input_summary_max_chars` | `500` |
| `guards.llm_escalation_max_tokens` | `500` |
| `guards.max_pattern_length` | `500` |
| `guards.max_history_events` | `50` |

#### Scoring & Retrieval Limits

| Parameter | Default |
|-----------|---------|
| `scoring.cheap_prune_max_candidates` | `80` |
| `reranker.batch_size` | `32` |
| `reranker.max_documents` | `100` |
| `context_assembly.max_context_window_fraction` | `0.15` (15% of context window) |
| `context_assembly.fallback_context_window` | `128000` tokens |
| `context_assembly.compaction_trigger_multiplier` | `2.0x` |
| `procedure_candidates.top_k` | `3` |
| `goal_refinement.max_subgoals_per_session` | `10` |
| `artifact_capture.max_content_chars` | `50000` |

#### TraceLedger In-Memory Limits

| Parameter | Default |
|-----------|---------|
| `infra.trace.memory_max_events` | `10000` |
| `infra.trace.memory_ttl_seconds` | `3600` (1 hour) |

#### SQLite Audit Store Paths

All relative to working directory (typically `/var/lib/elephantbroker`):

```
data/procedure_audit.db
data/session_goals_audit.db
data/org_overrides.db
data/authority_rules.db
data/consolidation_reports.db
data/tuning_deltas.db
data/scoring_ledger.db
```

#### Redis Key Namespacing

All Redis keys are gateway-scoped via `RedisKeyBuilder(gateway_id)`:

```
eb:{gateway_id}:ingest_buffer:{session_key}
eb:{gateway_id}:recent_facts:{session_key}
eb:{gateway_id}:session_goals:{session_key}:{session_id}
eb:{gateway_id}:ws_snapshot:{session_key}:{session_id}
eb:{gateway_id}:compact_state:{session_key}:{session_id}
eb:{gateway_id}:session_context:{session_key}:{session_id}
eb:{gateway_id}:session_messages:{session_key}:{session_id}
eb:{gateway_id}:session_artifacts:{session_key}:{session_id}
eb:{gateway_id}:procedure_exec:{session_key}:{session_id}
eb:{gateway_id}:guard_history:{session_key}:{session_id}
eb:{gateway_id}:fact_domains:{session_key}:{session_id}
eb:{gateway_id}:session_parent:{session_key}
eb:{gateway_id}:session_children:{parent_session_key}
eb:{gateway_id}:{agent_id}:approval:{request_id}
eb:{gateway_id}:consolidation_lock
eb:{gateway_id}:consolidation_status
eb:emb_cache:{text_hash}                         # global, not gateway-scoped
```

Session key TTLs are refreshed collectively via `touch_session_keys()` on every turn (10 keys per pipeline call).

#### Port Summary

| Port | Service | Compose Mapping |
|------|---------|----------------|
| 8420 | ElephantBroker runtime (native) | N/A (not in compose) |
| 8421 | HITL middleware (native) | N/A (not in compose) |
| 17474 | Neo4j HTTP | 17474:7474 |
| 17687 | Neo4j Bolt | 17687:7687 |
| 16333 | Qdrant REST | 16333:6333 |
| 16334 | Qdrant gRPC | 16334:6334 |
| 16379 | Redis | 16379:6379 |
| 4317 | OTEL Collector gRPC | 4317:4317 |
| 8123 | ClickHouse HTTP | 8123:8123 |
| 9000 | ClickHouse Native | 9000:9000 |
| 16686 | Jaeger UI | 16686:16686 |
| 14250 | Jaeger gRPC | 14250:14250 |
| 13000 | Grafana | 13000:3000 |
| 8811 | LiteLLM proxy (external) | N/A (not in compose) |
| 1235 | Reranker endpoint (external) | N/A (not in compose) |

All core infra ports are remapped to 1xxxx range to avoid conflicts with other services on the host.

#### systemd Unit Configuration

Two systemd units for production operation. Versioned in `deploy/systemd/`
and installed by `deploy/install.sh`. Both run under the dedicated
`elephantbroker` system user (created by the installer) with strict
systemd hardening directives — see `deploy/systemd/elephantbroker.service`
for the full list (`ProtectSystem=strict`, `NoNewPrivileges=true`,
`PrivateTmp=true`, `PrivateDevices=true`, etc.).

**elephantbroker.service:**
- `Type=simple`, `User=elephantbroker`, `Group=elephantbroker`
- `WorkingDirectory=/var/lib/elephantbroker`
- `EnvironmentFile=/etc/elephantbroker/env` (mode 640 root:elephantbroker)
- `ExecStart=/opt/elephantbroker/.venv/bin/elephantbroker serve --config /etc/elephantbroker/default.yaml --host 0.0.0.0 --port 8420`
- `ReadWritePaths=/var/lib/elephantbroker /opt/elephantbroker`
- `Restart=on-failure`, `RestartSec=5`

**elephantbroker-hitl.service:**
- `Type=simple`, `User=elephantbroker`, `Group=elephantbroker`
- `After=elephantbroker.service`
- `EnvironmentFile=/etc/elephantbroker/hitl.env` (mode 640 root:elephantbroker)
- `ExecStart=/opt/elephantbroker/.venv/bin/python -m hitl_middleware`
- Same hardening directives as the main runtime
- `Restart=on-failure`, `RestartSec=5`

#### Dependencies (`pyproject.toml`)

All direct dependencies are pinned to exact versions for reproducible builds.
Full transitive lock lives in `uv.lock` (committed alongside `pyproject.toml`).
See `deploy/UPDATING-DEPS.md` for the upgrade workflow.

```
pydantic==2.12.5
cognee[neo4j]==0.5.3
cognee-community-vector-adapter-qdrant==0.2.2
httpx==0.28.1
qdrant-client==1.17.1
redis==7.4.0
fastapi==0.135.3
uvicorn[standard]==0.44.0
opentelemetry-api==1.40.0
opentelemetry-sdk==1.40.0
opentelemetry-instrumentation-fastapi==0.61b0
click==8.3.2
prometheus_client==0.24.1
pyyaml==6.0.3
clickhouse-connect==0.15.1
```

Dev dependencies (also pinned): `pytest==9.0.2`, `pytest-asyncio==1.3.0`,
`ruff==0.15.9`, `tiktoken==0.12.0`, `websockets==15.0.1`, `PyNaCl==1.6.2`.

Python target: `>=3.11,<3.13` (3.11 and 3.12 supported). Ruff line length: 120 characters.

Package manager: [`uv`](https://docs.astral.sh/uv/) (Astral). Install/update
scripts use `uv sync --frozen` to install exactly what `uv.lock` specifies.
The previous `mistralai>=1.0` direct dependency was removed — uv's holistic
resolver picks `mistralai==1.12.4` automatically as a transitive of cognee.


---


## ElephantBroker TypeScript Plugin Configuration Reference

### 1. Plugin Manifests

#### Memory Plugin (`openclaw.plugin.json`)

**File:** `/openclaw-plugins/elephantbroker-memory/openclaw.plugin.json`

| Field | Value | Notes |
|-------|-------|-------|
| `id` | `elephantbroker-memory` | Must match directory name |
| `kind` | `memory` | Registers in OpenClaw's memory slot |
| `name` | `ElephantBroker Memory` | Display name |
| `version` | `0.1.0` | |
| `entry` | `dist/index.js` | Bundled via esbuild — requires `npm run build` on the gateway |
| `configSchema` | JSON Schema object | 4 optional properties (see Section 3) |

#### ContextEngine Plugin (`openclaw.plugin.json`)

**File:** `/openclaw-plugins/elephantbroker-context/openclaw.plugin.json`

| Field | Value | Notes |
|-------|-------|-------|
| `id` | `elephantbroker-context` | Must match directory name |
| `kind` | `context-engine` | Registers in OpenClaw's contextEngine slot |
| `name` | `ElephantBroker ContextEngine` | Display name |
| `version` | `0.1.0` | |
| `entry` | `dist/index.js` | Bundled via esbuild — requires `npm run build` on the gateway |
| `configSchema` | JSON Schema object | Same 4 properties as memory plugin |

#### Package Metadata (`package.json`)

Both plugins share identical `package.json` structure (except `name`):

```json
{
  "name": "elephantbroker-memory",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "dist/index.js",
  "openclaw": {
    "extensions": ["./dist/index.js"]
  },
  "dependencies": {
    "@opentelemetry/api": "^1.9.0",
    "@opentelemetry/sdk-trace-base": "^1.25.0"
  },
  "devDependencies": {
    "@types/node": "^20.0.0",
    "typescript": "^5.4",
    "vitest": "^1.6"
  }
}
```

Critical fields:
- `"openclaw": { "extensions": ["./dist/index.js"] }` -- required for OpenClaw extension discovery; the bundle is produced by `npm run build` (esbuild)
- `"type": "module"` -- ESM imports with `.js` suffixes in source
- Root `index.ts` is a thin re-export from `src/`; the source of truth lives under `src/`

---

### 2. Environment Variables

#### Required

| Variable | Used By | Default | Notes |
|----------|---------|---------|-------|
| `EB_GATEWAY_ID` | Both plugins | **None -- throws Error if missing** | Unique gateway instance identifier (e.g., `gw-prod-us-east-1`). Client constructors in both `ElephantBrokerClient` and `ContextEngineClient` throw `new Error(...)` if empty. |

#### Optional

| Variable | Used By | Default | Notes |
|----------|---------|---------|-------|
| `EB_RUNTIME_URL` | Both plugins | `http://localhost:8420` | ElephantBroker Python runtime base URL |
| `EB_GATEWAY_SHORT_NAME` | Both plugins | `EB_GATEWAY_ID.substring(0, 8)` | Human-friendly label for logs/traces |
| `EB_PROFILE` | Both plugins | `coding` | Profile preset name |

#### Config Resolution Order

Both plugins use the same 3-level cascade:

```
api.pluginConfig (from openclaw.json "config" block)
  > process.env
    > hardcoded default
```

Specifically, in both `register()` functions:
```typescript
const baseUrl = cfg.baseUrl || process.env.EB_RUNTIME_URL || "http://localhost:8420";
const profileName = cfg.profileName || process.env.EB_PROFILE || "coding";
const gatewayId = cfg.gatewayId || process.env.EB_GATEWAY_ID;
const gatewayShortName = cfg.gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME;
```

---

### 3. Plugin Config Options (`configSchema`)

Both plugins share the same 4 configSchema properties. These are set in `openclaw.json` under `plugins.entries.<id>.config`:

| Property | Type | Description | Env Fallback | Default |
|----------|------|-------------|-------------|---------|
| `baseUrl` | `string` | ElephantBroker runtime URL | `EB_RUNTIME_URL` | `http://localhost:8420` |
| `gatewayId` | `string` | Gateway instance ID | `EB_GATEWAY_ID` | None (required) |
| `gatewayShortName` | `string` | Short display name | `EB_GATEWAY_SHORT_NAME` | First 8 chars of `gatewayId` |
| `profileName` | `string` | Profile preset | `EB_PROFILE` | `coding` |

Valid profile presets: `coding`, `research`, `managerial`, `worker`, `personal_assistant`.

Example `openclaw.json` config block:

```json
{
  "plugins": {
    "entries": {
      "elephantbroker-memory": {
        "enabled": true,
        "config": {
          "baseUrl": "${EB_RUNTIME_URL}",
          "gatewayId": "${EB_GATEWAY_ID}",
          "gatewayShortName": "${EB_GATEWAY_SHORT_NAME}",
          "profileName": "coding"
        }
      },
      "elephantbroker-context": {
        "enabled": true,
        "config": {
          "baseUrl": "${EB_RUNTIME_URL}",
          "gatewayId": "${EB_GATEWAY_ID}",
          "gatewayShortName": "${EB_GATEWAY_SHORT_NAME}",
          "profileName": "coding"
        }
      }
    }
  }
}
```

`${VAR}` values are interpolated from `env.vars` at config load time.

---

### 4. Hardcoded Values

#### Memory Plugin (`index.ts`)

| Value | Location | Purpose |
|-------|----------|---------|
| `"agent:main:main"` | `src/index.ts` | Default `currentSessionKey` before `session_start` hook fires |
| `crypto.randomUUID()` | `src/index.ts` | Initial `currentSessionId` (overwritten by `session_start`) |
| `10` | `src/index.ts` | `max_results` for auto-recall search in `before_agent_start` hook |
| `4` | `src/index.ts` (`messages.slice(-4)`) | Last N messages sent for ingest in `agent_end` hook |

#### Memory Plugin (`client.ts`)

| Value | Location | Purpose |
|-------|----------|---------|
| `"http://localhost:8420"` | `src/client.ts` | Constructor default for `baseUrl` |
| `.substring(0, 8)` | `src/client.ts` | Default `gatewayShortName` derived from `gatewayId` |
| `"session"` | `src/client.ts` | Default `scope` for procedure creation |
| `5` | `src/client.ts` | Default `maxResults` for `searchArtifacts()` |
| `5` | `src/client.ts` | Default `max_results` for `searchSessionArtifacts()` |
| `"manual"` | `src/client.ts` | Default `tool_name` for `createArtifact()` |
| `"session"` | `src/client.ts` | Default `scope` for `createArtifact()` |

#### Memory Plugin (tools)

| Value | File | Purpose |
|-------|------|---------|
| `0.7` | `memory_forget.ts:30` | Minimum score threshold for search-then-delete |
| `.slice(0, 80)` | `memory_forget.ts:33` | Truncated text in delete confirmation response |
| `0.7` | `memory_update.ts:24` | Minimum score threshold for search-then-update |
| `"general"` | `memory_store.ts:24` | Default category for stored facts |
| `"session"` | `goal_create.ts:58` | Default scope for `goal_create` tool |
| `5` | `artifact_search.ts:44` | Default `max_results` for artifact search |
| `50` | `artifact_search.ts:44` | Maximum cap on `max_results` (`Math.min(n, 50)`) |
| UUID regex | `artifact_search.ts:3` | `/^[0-9a-f]{8}-[0-9a-f]{4}-...$/i` for direct artifact lookup detection |

#### ContextEngine Plugin (`engine.ts`)

| Value | Location | Purpose |
|-------|----------|---------|
| `6` | `engine.ts:63` | Default `batchSize` for degraded ingest buffering (AD-29) |
| `"coding"` | `engine.ts:64` | Default `profileName` |
| `"agent:main:main"` | `engine.ts:47` | Default `currentSessionKey` |
| `128000` | `engine.ts:220` | Default `context_window_tokens` if LLM provider does not report it |

#### ContextEngine Plugin (`client.ts`)

| Value | Location | Purpose |
|-------|----------|---------|
| `"http://localhost:8420"` | `client.ts:36` | Constructor default for `baseUrl` |
| `.substring(0, 8)` | `client.ts:43` | Default `gatewayShortName` |
| `6` | `client.ts:208` | Fallback `ingest_batch_size` if `/context/config` request fails |
| `60000` | `client.ts:208` | Fallback `ingest_batch_timeout_ms` (60 seconds) |

#### Auto-Recall Format (`format.ts`)

| Value | Purpose |
|-------|---------|
| `<relevant-memories source="elephantbroker">` | XML wrapper tag for injected auto-recall context |
| `confidence.toFixed(2)` | Confidence displayed to 2 decimal places |

---

### 5. OpenClaw Workspace Configuration

#### Tools Profile Requirement

ElephantBroker registers 24 tools. The default `coding` tools profile only exposes `memory_search` and `memory_get`, blocking the other 22 tools.

**Required action:**
```bash
openclaw config unset tools.profile
## OR
openclaw config set tools.profile full
```

| Profile | EB Compatible | Exposed EB Tools |
|---------|--------------|-----------------|
| `full` | Yes | All 24 |
| `coding` | No | 2 only (`memory_search`, `memory_get`) |
| `messaging` | No | 0 |
| `minimal` | No | 0 |

#### Workspace Files

Both files live in `openclaw-plugins/elephantbroker-memory/workspace/` as templates for surgical edits into `~/.openclaw/workspace/`:

**`AGENTS.md`** -- Agent behavior instructions covering:
- Session startup (read SOUL.md, USER.md)
- Memory tool usage rules (when to store, search, forget)
- Goal management behavior (agent is planner, system is scorekeeper)
- Procedure lifecycle behavior
- Artifact behavior
- Guard-aware behavior (blocked actions, approval waiting)
- Red lines (no exfiltration, no destructive commands without asking)
- Group chat participation rules
- Heartbeat proactive behavior

**`TOOLS.md`** -- Tool documentation covering all 24 tools with descriptions and parameters.

#### Required Workspace Edits for Deployment

1. Replace file-based memory sections in `AGENTS.md` with EB-only memory
2. Remove "MEMORY.md - Your Long-Term Memory" subsection
3. Remove "Write It Down - No Mental Notes" subsection
4. Add EB tool documentation to `TOOLS.md`
5. Disable OpenClaw's built-in session-memory hook: `openclaw hooks disable session-memory`

---

### 6. Hook Registration

#### Memory Plugin Hooks (4 hooks)

| Hook | Trigger | Behavior | Async | Return |
|------|---------|----------|-------|--------|
| `before_agent_start` | Before each agent turn | Learns `agentId` from context, derives `agentKey` as `{gatewayId}:{agentId}`. Runs auto-recall search (max 10 results) and returns `{ prependContext }` with XML-formatted memories. | `async` | `{ prependContext: string }` or `{}` |
| `agent_end` | After each agent turn | Sends last 4 messages for turn ingest (fact extraction). Fire-and-forget (not awaited). | sync start, async fire-and-forget | `void` |
| `session_start` | New session begins | Sets `currentSessionId` and `currentSessionKey`. Calls `POST /sessions/start` with gateway identity. | `async` | `void` |
| `session_end` | Session ends | Calls `POST /sessions/end` to flush buffers and persist session state. | `async` | `void` |

#### ContextEngine Plugin Hooks (3 hooks)

| Hook | Trigger | Behavior | Async | Return |
|------|---------|----------|-------|--------|
| `before_prompt_build` | Before system prompt assembly | Calls `POST /context/build-overlay` to get system prompt overlay (Surface B). Returns `prepend_system_context`, `append_system_context`, `prepend_context`. | `async` | `SystemPromptOverlay` or `{}` |
| `onLlmInput` | Before LLM call | Reports context window size to runtime (first call only, `modelReported` flag). Fire-and-forget. | sync | `void` |
| `onLlmOutput` | After LLM call | Reports token usage (`input_tokens`, `output_tokens`, `total_tokens`) to runtime. Fire-and-forget, every call. | sync | `void` |

#### ContextEngine Lifecycle Methods (via `registerContextEngine`)

These are NOT hooks -- OpenClaw calls them directly on the engine instance:

| Method | OpenClaw Params Type | Maps To API | Notes |
|--------|---------------------|-------------|-------|
| `bootstrap()` | `OCBootstrapParams` | `POST /context/bootstrap` | Sets session identity, derives agent key |
| `ingest()` | `OCIngestParams` | Buffered, flushes at `batchSize` | Degraded mode (AD-29): buffers single messages |
| `ingestBatch()` | `OCIngestBatchParams` | `POST /context/ingest-batch` | Primary path, no buffering |
| `assemble()` | `OCAssembleParams` | `POST /context/assemble` | Flushes buffer first. Returns camelCase: `{ messages, estimatedTokens, systemPromptAddition }` |
| `compact()` | `OCCompactParams` | `POST /context/compact` | `ownsCompaction: true` -- OpenClaw delegates compaction |
| `afterTurn()` | `OCAfterTurnParams` | `POST /context/after-turn` | Flushes buffer first |
| `prepareSubagentSpawn()` | `OCSubagentSpawnParams` | `POST /context/subagent/spawn` | |
| `onSubagentEnded()` | `OCSubagentEndedParams` | `POST /context/subagent/ended` | |
| `dispose()` | None | `POST /context/dispose` | Resets all state, flushes buffer |

---

### 7. Identity Propagation

#### HTTP Headers

Both plugins send identity headers on every request via `getHeaders()`:

| Header | Source | Always Sent | Notes |
|--------|--------|-------------|-------|
| `X-EB-Gateway-ID` | `this.gatewayId` | Yes (required) | From config/env, validated at construction |
| `X-EB-Agent-Key` | `this.agentKey` | Conditional (if set) | Format: `{gateway_id}:{agent_id}` |
| `X-EB-Agent-ID` | `this.agentId` | Conditional (if set) | Raw agent ID from OpenClaw hook |
| `X-EB-Session-Key` | `this.currentSessionKey` | Conditional (if set) | Memory plugin only |
| `X-EB-Actor-Id` | `this.actorId` | Conditional (if set) | Memory plugin only (Phase 8 admin API) |
| `Content-Type` | hardcoded | Yes | `application/json` |
| `traceparent` / `tracestate` | W3C propagation | Yes | Injected via `propagation.inject(context.active(), headers)` |

#### Agent Identity Derivation

In the memory plugin `before_agent_start` hook:
```typescript
currentAgentId = hookContext.agentId;                    // e.g., "main"
currentAgentKey = `${gatewayId}:${hookContext.agentId}`; // e.g., "gw-prod:main"
```

In the context engine `bootstrap()`:
```typescript
this.setAgentIdentity("main", `${this.gatewayId}:main`);
```

#### Gateway Short Name Derivation

Both clients compute the short name identically:
```typescript
this.gatewayShortName = gatewayShortName || process.env.EB_GATEWAY_SHORT_NAME || this.gatewayId.substring(0, 8);
```

Example: `EB_GATEWAY_ID=gw-prod-assistant` produces short name `gw-prod-` (first 8 chars).

#### Session Key Pattern

Default: `agent:main:main` (format: `agent:{agentType}:{agentName}`)

Updated from:
- `session_start` hook: `hookContext.sessionKey`
- `before_agent_start` hook: `hookContext.sessionKey`
- `bootstrap()` lifecycle method: `params.sessionKey`

---

### 8. Tool Inventory (24 tools -- all registered by Memory Plugin)

The Context Engine plugin registers **0 tools** -- all agent-facing tools are on the Memory plugin.

#### Memory Tools (5)

| Tool ID | Required Params | Optional Params |
|---------|----------------|-----------------|
| `memory_search` | `query` | `max_results`, `scope`, `memory_class` |
| `memory_get` | `fact_id` | -- |
| `memory_store` | `text` | `category` (default `"general"`), `scope`, `confidence` |
| `memory_forget` | -- | `fact_id`, `query` (one required, score threshold `0.7`) |
| `memory_update` | -- | `fact_id`, `query` (one required, score threshold `0.7`), `new_text`, `updates` |

#### Session Goal Tools (5)

| Tool ID | Required Params | Optional Params |
|---------|----------------|-----------------|
| `session_goals_list` | -- | -- |
| `goal_create` | `title` | `description`, `scope` (default `"session"`), `org_id`, `team_id`, `parent_goal_id`, `success_criteria[]`, `owner_actor_ids[]` |
| `session_goals_update_status` | `goal_id`, `status` | `evidence` |
| `session_goals_add_blocker` | `goal_id`, `blocker` | -- |
| `session_goals_progress` | `goal_id`, `evidence` | -- |

#### Procedure Tools (4)

| Tool ID | Required Params | Optional Params |
|---------|----------------|-----------------|
| `procedure_create` | `name`, `steps[]` | `description`, `scope` |
| `procedure_activate` | `procedure_id` | -- (uses `actor_id` internally) |
| `procedure_complete_step` | `execution_id`, `step_id` | `evidence`, `proof_type` |
| `procedure_session_status` | -- | -- |

#### Artifact Tools (2)

| Tool ID | Required Params | Optional Params |
|---------|----------------|-----------------|
| `artifact_search` | `query` | `tool_name`, `scope` (session/persistent/all, default `"all"`), `max_results` (default `5`, max `50`) |
| `create_artifact` | `content` | `tool_name` (default `"manual"`), `scope` (default `"session"`), `tags[]`, `goal_id`, `summary` |

#### Guard Tools (2)

| Tool ID | Required Params | Optional Params |
|---------|----------------|-----------------|
| `guards_list` | -- | -- |
| `guard_status` | `guard_event_id` | -- |

#### Admin Tools (6 -- authority-gated, server returns 403 if insufficient)

| Tool ID | Required Params | Optional Params |
|---------|----------------|-----------------|
| `admin_create_org` | `name` | `display_label` |
| `admin_create_team` | `name`, `org_id` | `display_label` |
| `admin_register_actor` | `display_name`, `type` | `authority_level`, `org_id`, `team_ids[]`, `handles[]` |
| `admin_add_member` | `team_id`, `actor_id` | -- |
| `admin_remove_member` | `team_id`, `actor_id` | -- |
| `admin_merge_actors` | `canonical_id`, `duplicate_id` | -- |

---

### 9. API Endpoints Called

#### Memory Plugin Client Endpoints

| Method | Endpoint | Used By |
|--------|----------|---------|
| `POST` | `/memory/search` | `memory_search` tool, auto-recall hook |
| `POST` | `/memory/store` | `memory_store` tool |
| `GET` | `/memory/{factId}` | `memory_get` tool |
| `DELETE` | `/memory/{factId}` | `memory_forget` tool |
| `PATCH` | `/memory/{factId}` | `memory_update` tool |
| `POST` | `/memory/ingest-messages` | `agent_end` hook |
| `POST` | `/sessions/start` | `session_start` hook |
| `POST` | `/sessions/end` | `session_end` hook |
| `GET` | `/goals/session` | `session_goals_list` tool |
| `POST` | `/goals/session` | `goal_create` (session scope) |
| `PATCH` | `/goals/session/{goalId}` | `session_goals_update_status` |
| `POST` | `/goals/session/{goalId}/blocker` | `session_goals_add_blocker` |
| `POST` | `/goals/session/{goalId}/progress` | `session_goals_progress` |
| `POST` | `/procedures/` | `procedure_create` |
| `POST` | `/procedures/{id}/activate` | `procedure_activate` |
| `POST` | `/procedures/{id}/step/{stepId}/complete` | `procedure_complete_step` |
| `GET` | `/procedures/session/status` | `procedure_session_status` |
| `GET` | `/guards/active/{sessionId}` | `guards_list` |
| `GET` | `/guards/events/detail/{eventId}` | `guard_status` |
| `POST` | `/artifacts/search` | `artifact_search` (persistent) |
| `POST` | `/artifacts/session/search` | `artifact_search` (session) |
| `GET` | `/artifacts/session/{id}` | `artifact_search` (UUID direct lookup) |
| `POST` | `/artifacts/create` | `create_artifact` |
| `POST` | `/admin/goals` | `goal_create` (persistent scope) |
| `POST` | `/admin/organizations` | `admin_create_org` |
| `POST` | `/admin/teams` | `admin_create_team` |
| `POST` | `/admin/actors` | `admin_register_actor` |
| `POST` | `/admin/teams/{id}/members` | `admin_add_member` |
| `DELETE` | `/admin/teams/{id}/members/{actorId}` | `admin_remove_member` |
| `POST` | `/admin/actors/{id}/merge` | `admin_merge_actors` |

#### ContextEngine Plugin Client Endpoints

| Method | Endpoint | Used By |
|--------|----------|---------|
| `POST` | `/context/bootstrap` | `bootstrap()` lifecycle |
| `POST` | `/context/ingest-batch` | `ingestBatch()` / buffer flush |
| `POST` | `/context/assemble` | `assemble()` lifecycle |
| `POST` | `/context/build-overlay` | `before_prompt_build` hook |
| `POST` | `/context/compact` | `compact()` lifecycle |
| `POST` | `/context/after-turn` | `afterTurn()` lifecycle |
| `POST` | `/context/subagent/spawn` | `prepareSubagentSpawn()` |
| `POST` | `/context/subagent/ended` | `onSubagentEnded()` |
| `POST` | `/context/dispose` | `dispose()` lifecycle |
| `GET` | `/context/config` | Startup config fetch (batch size) |
| `POST` | `/sessions/context-window` | `onLlmInput` hook |
| `POST` | `/sessions/token-usage` | `onLlmOutput` hook |

---

### 10. OTEL Tracing

Both plugins create named tracers and wrap every HTTP call in OTEL spans:

| Plugin | Tracer Name | Span Kind |
|--------|------------|-----------|
| Memory | `elephantbroker.memory-plugin` | `CLIENT` |
| Context | `elephantbroker.context-engine-plugin` | `CLIENT` |

W3C trace context (`traceparent`, `tracestate`) is injected into HTTP headers on every request via `propagation.inject(context.active(), headers)`.

---

### 11. Minimal Deployment Checklist

```bash
## 1. Set required env var
export EB_GATEWAY_ID="gw-prod-us-east-1"
export EB_RUNTIME_URL="http://10.10.0.10:8420"
export EB_GATEWAY_SHORT_NAME="prod"

## 2. Symlink plugins
ln -s /opt/elephantbroker/openclaw-plugins/elephantbroker-memory \
      ~/.openclaw/extensions/elephantbroker-memory
ln -s /opt/elephantbroker/openclaw-plugins/elephantbroker-context \
      ~/.openclaw/extensions/elephantbroker-context

## 3. Install deps + build the bundle (npm ci + npm run build; NOT npm install)
##    Requires Node 24+ — pinned via engines.node in each plugin's package.json
##    `npm run build` produces dist/index.js via esbuild; OpenClaw loads that
##    bundle (see openclaw.plugin.json entry + package.json openclaw.extensions).
cd ~/.openclaw/extensions/elephantbroker-memory && npm ci && npm run build
cd ~/.openclaw/extensions/elephantbroker-context && npm ci && npm run build

## 4. Configure openclaw.json (see Section 3 for full example)

## 5. Set tools profile to full (CRITICAL)
openclaw config unset tools.profile

## 6. Disable built-in session-memory hook
openclaw hooks disable session-memory

## 7. Register plugin slots
openclaw config set plugins.slots.memory elephantbroker-memory
openclaw config set plugins.slots.contextEngine elephantbroker-context

## 8. Restart
openclaw gateway restart

## 9. Verify
openclaw plugins list
```


---


## ElephantBroker Redis Configuration Reference

### 1. Connection Configuration

| Setting | Source | Default | Notes |
|---------|--------|---------|-------|
| **URL** | `EB_REDIS_URL` env / `infra.redis_url` config | `redis://localhost:6379` | Standard Redis URL; supports `rediss://` for TLS |
| **Client library** | `redis.asyncio` (`redis-py`) | -- | Uses `aioredis.from_url()` |
| **decode_responses** | hardcoded in `container.py` | `True` | All values stored as JSON strings via `json.dumps()`; all reads return `str` |
| **Connection pool** | `redis-py` default pool | default | No explicit `max_connections` or `socket_timeout` set; relies on library defaults |
| **Graceful shutdown** | `RuntimeContainer.close()` | -- | Calls `redis.aclose()` |

**File:** `/elephantbroker/runtime/container.py` lines 159-168

### 2. Key Prefix Convention

All gateway-scoped keys use the format `eb:{gateway_id}:...`, built by `RedisKeyBuilder(gateway_id)`.

One exception: the **embedding cache** is globally scoped (`eb:emb_cache:{hash}`) because identical text always produces identical embeddings regardless of gateway.

**File:** `/elephantbroker/runtime/redis_keys.py`

### 3. Redis Key Inventory

#### 3.1 Session-Scoped Keys (refreshed every turn via `touch_session_keys()`)

| # | Key Pattern | Data Type | TTL | Written By | Read By | Size Estimate | Eviction Impact |
|---|-------------|-----------|-----|------------|---------|---------------|-----------------|
| 1 | `eb:{gw}:session_context:{sk}:{sid}` | STRING (JSON) | `max(profile.session_data_ttl_seconds, consolidation_min_retention_seconds)` default 172800s (48h) | `SessionContextStore.save()` | `SessionContextStore.get()`, `SessionContextStore.get_context_window()` | 2-8 KB (SessionContext with profile, goals metadata, turn count) | Session state lost; next request triggers fresh bootstrap. Context assembly uses defaults. |
| 2 | `eb:{gw}:session_messages:{sk}:{sid}` | LIST (JSON per element) | Same effective TTL as session_context (refreshed by `touch_session_keys`) | `ContextLifecycle.ingest_batch()` via `rpush` | `ContextLifecycle.build_overlay()`, `ContextLifecycle.after_turn()`, `CompactionEngine` (indirectly) | 5-50 KB per session (grows with conversation; each message ~0.5-2 KB) | Compaction loses message history for classification; summarization degrades to empty. |
| 3 | `eb:{gw}:session_goals:{sk}:{sid}` | STRING (JSON array) | `scoring.session_goals_ttl_seconds` default 86400s (24h); refreshed by `touch_session_keys` | `SessionGoalStore.set_goals()` via `setex` | `SessionGoalStore.get_goals()`, `CandidateGenerator._collect_goal_items()`, `WorkingSetManager._get_session_goals_redis()`, `ApprovalQueue.resolve_approval_goal()` | 1-10 KB (JSON array of GoalState objects; typically 1-10 goals per session) | Session goals lost mid-session; agent loses goal tracking. Goals not flushed to Cognee (flush happens on session end). Working set loses goal-relevance scoring signal. |
| 4 | `eb:{gw}:ws_snapshot:{sk}:{sid}` | STRING (JSON) | `scoring.snapshot_ttl_seconds` default 300s (5 min); refreshed by `touch_session_keys` | `WorkingSetManager.build_working_set()` via `setex`, `ContextLifecycle.assemble()` via `setex` | `WorkingSetManager.get_working_set()` (scan_iter fallback), `ContextLifecycle.build_overlay()`, `ContextLifecycle.after_turn()` | 5-30 KB (full WorkingSetSnapshot with scored items, embeddings excluded) | Working set must be rebuilt on next assemble(); one extra retrieval+scoring cycle. No data loss. |
| 5 | `eb:{gw}:compact_state:{sk}:{sid}` | SET (string members) | `consolidation_min_retention_seconds` default 172800s (48h); refreshed by `touch_session_keys` | `CompactionEngine._persist_compact_state()` via `sadd`, `SessionContextStore.add_compact_ids()` | `SessionContextStore.get_compact_ids()` via `smembers`, `ScoringEngine.compute_novelty()` | 0.5-5 KB (set of compacted fact/message ID strings; typically 10-100 members) | Novelty dimension in scoring loses compaction awareness; already-summarized facts may be re-injected with lower novelty penalty. |
| 6 | `eb:{gw}:compact_state_obj:{sk}:{sid}` | STRING (JSON) | `consolidation_min_retention_seconds` default 172800s (48h); refreshed by `touch_session_keys` | `CompactionEngine._persist_compact_state()` via `setex`, `SessionContextStore.save_compact_state()` | `CompactionEngine.get_session_compact_state()`, `SessionContextStore.get_compact_state()` | 2-10 KB (SessionCompactState with goal_summary, decisions_made, compressed_digest, etc.) | Compact state lost; compaction restarts from scratch on next trigger. May produce slightly redundant summaries. |
| 7 | `eb:{gw}:session_artifacts:{sk}:{sid}` | HASH (field=artifact_id, value=JSON) | `max(profile.session_data_ttl_seconds, consolidation_min_retention_seconds)` default 172800s (48h); refreshed by `touch_session_keys` | `SessionArtifactStore.store()` via `hset`, `.increment_injected()`, `.increment_searched()` | `SessionArtifactStore.get()`, `.get_by_hash()`, `.search()`, `.list_all()`, `.promote_to_persistent()` | 5-200 KB per session (each artifact is 0.5-5 KB summary JSON; typically 1-50 artifacts) | Session artifacts lost; tool outputs must be re-generated. Artifact search returns empty. Persistent promotion from session data is lost. |
| 8 | `eb:{gw}:procedure_exec:{sk}:{sid}` | STRING (JSON dict of execution_id -> ProcedureExecution) | `ttl_seconds` from `ProcedureEngine` constructor, default `consolidation_min_retention_seconds` (172800s / 48h); refreshed by `touch_session_keys` | `ProcedureEngine._persist_execution()` via `setex` | `ProcedureEngine._restore_execution()`, `.restore_executions()` | 1-5 KB (JSON dict of active procedure executions; typically 0-3 per session) | Active procedure state lost; step completions and evidence tracking reset. Agent must re-activate procedures. |
| 9 | `eb:{gw}:guard_history:{sk}:{sid}` | LIST (JSON per element, capped at `max_history_events`) | `guards.history_ttl_seconds` default 86400s (24h); refreshed by `touch_session_keys` | `RedLineGuardEngine._record_guard_event()` via `lpush` + `ltrim` | `RedLineGuardEngine.get_guard_history()`, `DomainDiscoveryTask.run()` (scan) | 2-20 KB (JSON GuardEvent objects; max 50 per list) | Guard history lost; no impact on safety (guards still evaluate in real-time). Domain discovery (Tier 3) loses training data. |
| 10 | `eb:{gw}:fact_domains:{sk}:{sid}` | LIST (string elements, capped at 20) | 86400s (24h) hardcoded; refreshed by `touch_session_keys` | `TurnIngestPipeline` (step 9b) via `lpush` + `ltrim` | `RedLineGuardEngine._layer0_autonomy()` | 0.1-0.5 KB (max 20 domain strings like "code_change", "data_access") | Guard Tier 2 autonomy classification loses recent-domain signal; falls back to Tier 1 tool-name matching. No safety regression, slightly less precise domain routing. |

#### 3.2 Subagent Tracking Keys

| # | Key Pattern | Data Type | TTL | Written By | Read By | Size Estimate | Eviction Impact |
|---|-------------|-----------|-----|------------|---------|---------------|-----------------|
| 11 | `eb:{gw}:session_parent:{child_sk}` | STRING (parent session_key) | `consolidation_min_retention_seconds` default 172800s (48h), or `params.ttl_ms // 1000` | `ContextLifecycle.prepare_subagent_spawn()` via `setex`, `sessions.session_start()` route via `setex` | `ContextLifecycle.bootstrap()` (subagent auto-detection), `touch_session_keys()` (conditional) | 20-60 bytes (session_key string like `gw-prod-assistant:main:main`) | Subagent loses parent relationship; cannot inherit parent context. Falls back to independent session behavior. |
| 12 | `eb:{gw}:session_children:{parent_sk}` | SET (child session_key strings) | Same as session_parent TTL; refreshed by `touch_session_keys()` (via parent lookup) | `ContextLifecycle.prepare_subagent_spawn()` via `sadd` | `touch_session_keys()` (for TTL refresh of parent's children set) | 0.1-1 KB (set of child session_key strings; typically 0-5 children) | Parent loses track of children; no functional impact beyond TTL refresh. |

#### 3.3 HITL Approval Keys

| # | Key Pattern | Data Type | TTL | Written By | Read By | Size Estimate | Eviction Impact |
|---|-------------|-----------|-----|------------|---------|---------------|-----------------|
| 13 | `eb:{gw}:{agent_id}:approval:{request_id}` | STRING (JSON ApprovalRequest) | `hitl.approval_default_timeout_seconds + 60` default 360s (6 min); on timeout resolution: 300s | `ApprovalQueue.create()`, `.approve()`, `.reject()`, `.cancel()`, `.check_timeout()` | `ApprovalQueue.get()` | 0.5-2 KB (ApprovalRequest with action_summary, matched_rules, status, timestamps) | Pending approval silently lost; agent cannot resolve the approval. Auto-goal remains active until session ends. |
| 14 | `eb:{gw}:{agent_id}:approvals_by_session:{sid}` | SET (request_id strings) | Same as approval key TTL (360s default) | `ApprovalQueue.create()` via `sadd` | `ApprovalQueue.get_for_session()` via `smembers` | 0.1-0.5 KB (set of UUID strings; typically 0-5 per session) | Session-level approval lookup fails; individual approvals still accessible by ID. Dedup check (`find_matching`) may create duplicate approval requests. |

#### 3.4 Async Analysis Keys

| # | Key Pattern | Data Type | TTL | Written By | Read By | Size Estimate | Eviction Impact |
|---|-------------|-----------|-----|------------|---------|---------------|-----------------|
| 15 | `eb:{gw}:fact_async_use:{source_id}` | STRING (float counter via INCRBYFLOAT) | 86400s (24h) hardcoded | `AsyncInjectionAnalyzer.analyze()` via `incrbyfloat` | Phase 9 `ScoringTuner` (reads for weight adjustment) | 8-16 bytes (single float value) | Successful-use signal for one fact lost; ScoringTuner weight adjustment slightly less accurate. Non-critical. |

#### 3.5 Ingest Buffer Keys

| # | Key Pattern | Data Type | TTL | Written By | Read By | Size Estimate | Eviction Impact |
|---|-------------|-----------|-----|------------|---------|---------------|-----------------|
| 16 | `eb:{gw}:ingest_buffer:{sk}` | LIST (JSON per element) | `llm.ingest_buffer_ttl_seconds` default 300s (5 min) | `IngestBuffer.add_messages()` via `rpush` | `IngestBuffer.flush()` / `.force_flush()` via `lrange` + `delete` | 1-10 KB (buffered messages awaiting batch extraction; max `ingest_batch_size` items, default 6) | Buffered messages lost before extraction; those messages' facts never get extracted. The messages themselves are still in the conversation (host agent has them). |
| 17 | `eb:{gw}:recent_facts:{sk}` | STRING (JSON array) | `llm.extraction_context_ttl_seconds` default 3600s (1h) | `IngestBuffer.update_recent_facts()` via `set(..., ex=...)` | `IngestBuffer.load_recent_facts()` | 2-10 KB (JSON array of up to `extraction_context_facts` (default 20) recent fact dicts) | Extraction context lost; next batch extraction lacks dedup context, may produce slightly more redundant facts. Self-healing on next extraction. |

#### 3.6 Consolidation Keys

| # | Key Pattern | Data Type | TTL | Written By | Read By | Size Estimate | Eviction Impact |
|---|-------------|-----------|-----|------------|---------|---------------|-----------------|
| 18 | `eb:{gw}:consolidation_lock` | STRING (Redis lock via `redis.lock()`) | 3600s (1h) lock timeout | `ConsolidationEngine.run_consolidation()` via `redis.lock()` | Same method (acquire check) | ~100 bytes (Redis lock internal structure) | Distributed lock lost; concurrent consolidation runs may overlap (but each is idempotent per stage, so no data corruption -- just wasted compute). |
| 19 | `eb:{gw}:consolidation_status` | STRING (JSON) | No explicit TTL (persists until overwritten) | `ConsolidationEngine.run_consolidation()` via `set()` (running/completed/failed) | `consolidation.get_status()` API route | 100-200 bytes (JSON with `running`, `started_at`, `current_stage` or `last_run_at`) | Status API returns `{"running": false}`. No functional impact. |

#### 3.7 Global (Non-Gateway-Scoped) Keys

| # | Key Pattern | Data Type | TTL | Written By | Read By | Size Estimate | Eviction Impact |
|---|-------------|-----------|-----|------------|---------|---------------|-----------------|
| 20 | `eb:emb_cache:{sha256_hash_32}` | STRING (JSON float array) | `embedding_cache.ttl_seconds` default 3600s (1h) | `CachedEmbeddingService.embed_text()` and `.embed_batch()` via `setex` / pipeline `setex` | `CachedEmbeddingService.embed_text()` / `.embed_batch()` via `get` / `mget` | ~3-6 KB per entry at 768 dims (gemini/text-embedding-004 default); ~4-8 KB at 1024 dims (openai/text-embedding-3-large) | Cache miss; triggers a call to the embedding API. Increases latency by ~50-200ms per miss. Fully self-healing. |

### 4. TTL Summary

| TTL Parameter | Config Path | Default | Env Var | Keys Using It |
|---------------|-------------|---------|---------|---------------|
| `consolidation_min_retention_seconds` | `ElephantBrokerConfig.consolidation_min_retention_seconds` | 172800 (48h) | `EB_CONSOLIDATION_MIN_RETENTION_SECONDS` | session_context, compact_state, compact_state_obj, session_artifacts, procedure_exec, session_parent |
| `session_goals_ttl_seconds` | `ScoringConfig.session_goals_ttl_seconds` | 86400 (24h) | `EB_SESSION_GOALS_TTL` | session_goals |
| `snapshot_ttl_seconds` | `ScoringConfig.snapshot_ttl_seconds` | 300 (5 min) | `EB_SCORING_SNAPSHOT_TTL` | ws_snapshot |
| `history_ttl_seconds` | `GuardConfig.history_ttl_seconds` | 86400 (24h) | -- (YAML only) | guard_history, fact_domains (refresh) |
| `ingest_buffer_ttl_seconds` | `LLMConfig.ingest_buffer_ttl_seconds` | 300 (5 min) | `EB_INGEST_BUFFER_TTL` | ingest_buffer |
| `extraction_context_ttl_seconds` | `LLMConfig.extraction_context_ttl_seconds` | 3600 (1h) | `EB_EXTRACTION_CONTEXT_TTL` | recent_facts |
| `embedding_cache.ttl_seconds` | `EmbeddingCacheConfig.ttl_seconds` | 3600 (1h) | `EB_EMBEDDING_CACHE_TTL` | emb_cache |
| `approval_default_timeout_seconds` | `HitlConfig.approval_default_timeout_seconds` | 300 (5 min) | -- (YAML only) | approval, approvals_by_session (TTL = timeout + 60s) |
| hardcoded 86400 | -- | 86400 (24h) | -- | fact_async_use, fact_domains |
| lock timeout 3600 | hardcoded | 3600 (1h) | -- | consolidation_lock |

### 5. TTL Refresh Mechanism (`touch_session_keys`)

On every `ingest_batch()` call (i.e., every new turn), `touch_session_keys()` refreshes TTL on 10-11 keys via a single Redis pipeline:

- `session_context`, `session_messages`, `session_goals`, `session_artifacts`
- `ws_snapshot`, `compact_state`, `compact_state_obj`, `procedure_exec`
- `guard_history`, `fact_domains`
- `session_parent` (only for subagent sessions)
- Plus parent's `session_children` set (separate operation, not pipelined)

This ensures active sessions never expire due to TTL while the user is still interacting.

**File:** `/elephantbroker/runtime/redis_keys.py` lines 98-140

### 6. Features That Degrade Without Redis

| Feature | Degradation Mode |
|---------|-----------------|
| **Session goals** | Completely non-functional. Goals cannot be stored, retrieved, or scored. |
| **Working set caching** | Falls back to in-memory `_snapshots` dict. Lost on process restart. |
| **Ingest buffering** | `IngestBuffer` is `None`; messages cannot be batched. Turn ingest pipeline may skip or process one-at-a-time. |
| **Compaction state** | Cannot persist compact state. Compaction metadata lost between turns. |
| **Context lifecycle** | Messages not stored in Redis; compaction and after_turn lose message history. |
| **Procedure execution persistence** | Falls back to in-memory dict only. Lost on restart. |
| **Guard history** | Not recorded; `get_guard_history()` returns empty. Domain discovery returns empty. |
| **Subagent tracking** | Cannot store parent/children mappings. Subagent isolation breaks. |
| **HITL approvals** | `ApprovalQueue` is `None` (conditional creation). Guard checks skip approval flow. |
| **Embedding cache** | Falls through to raw `EmbeddingService`; every call hits the embedding API. |
| **Async injection analysis** | `AsyncInjectionAnalyzer` is not created (conditional on `config.enabled` AND `redis`). |
| **Consolidation locking** | No distributed lock; concurrent runs not prevented (but stages are idempotent). |
| **Consolidation status** | API returns `{"running": false}` always. |

**Core retrieval, storage, and Cognee operations all work without Redis.** Redis failure degrades session-level state management and caching but does not prevent fact storage/search.

### 7. Memory Usage Projections

#### Per-Session Estimate (Active)

| Key Group | Typical Size | Notes |
|-----------|-------------|-------|
| session_context | 5 KB | SessionContext JSON |
| session_messages | 20 KB | ~20 messages at ~1 KB each |
| session_goals | 3 KB | ~5 goals at ~0.6 KB each |
| ws_snapshot | 15 KB | Scored items without embeddings |
| compact_state (SET) | 1 KB | ~50 item IDs |
| compact_state_obj | 5 KB | Compressed digest + metadata |
| session_artifacts | 10 KB | ~5 artifacts at ~2 KB each |
| procedure_exec | 2 KB | 1-2 active procedures |
| guard_history | 10 KB | ~20 events at ~0.5 KB each |
| fact_domains | 0.3 KB | 20 domain strings |
| **Session total** | **~71 KB** | Conservative estimate for an active coding session |

#### Per-Gateway Estimate

| Component | Count x Size | Total |
|-----------|-------------|-------|
| Active sessions (10) | 10 x 71 KB | 710 KB |
| Ingest buffers (10) | 10 x 5 KB | 50 KB |
| Recent facts (10) | 10 x 5 KB | 50 KB |
| Embedding cache (shared, ~500 unique texts) | 500 x 6 KB | 3 MB |
| Consolidation status | 1 x 0.2 KB | 0.2 KB |
| HITL approvals (5 pending) | 5 x 1.5 KB | 7.5 KB |
| Subagent mappings (3 active) | 6 x 0.1 KB | 0.6 KB |
| **Gateway total** | | **~3.8 MB** |

#### Multi-Gateway Deployment

| Scale | Sessions | Estimated Redis Memory |
|-------|----------|----------------------|
| Single developer gateway | 1-3 active sessions | ~1-2 MB |
| Small team (5 gateways, 3 sessions each) | 15 sessions | ~15-20 MB |
| Medium deployment (20 gateways, 10 sessions each) | 200 sessions | ~80-120 MB |
| Large deployment (100 gateways, 20 sessions each) | 2000 sessions | ~500 MB - 1 GB |

**Embedding cache dominates.** The `eb:emb_cache:*` namespace is global (shared across gateways) and each entry is 4-8 KB. With 10,000 unique text embeddings cached, the cache alone consumes ~50-80 MB. The 1-hour TTL provides natural eviction.

#### Redis `maxmemory` Recommendation

- **Development:** 64 MB (handles 1-3 sessions easily)
- **Production (single gateway):** 256 MB
- **Production (multi-gateway, 10-50 gateways):** 1 GB
- **Eviction policy:** `allkeys-lru` recommended. All keys have explicit TTLs, but LRU eviction provides a safety net. The `volatile-lru` policy also works since all keys have TTLs.

### 8. Redis Data Structures Used

| Structure | Count | Purpose |
|-----------|-------|---------|
| STRING (JSON) | 13 keys | Primary state storage (session context, goals, snapshots, compact state, approvals, consolidation status, embedding cache, ingest recent_facts, procedure_exec) |
| LIST | 5 keys | Ordered collections (session messages, guard history, fact domains, ingest buffer) |
| SET | 3 keys | Unique member collections (compact_state IDs, approvals_by_session, session_children) |
| HASH | 1 key | Session artifacts (field=artifact_id, value=JSON) |
| Lock | 1 key | Redis distributed lock (consolidation_lock) |
| INCRBYFLOAT counter | 1 key per fact | Atomic float counter (fact_async_use) |

---

**Key files referenced:**
- `/elephantbroker/runtime/redis_keys.py` -- `RedisKeyBuilder` class and `touch_session_keys()` function
- `/elephantbroker/runtime/container.py` -- Redis client creation and wiring
- `/elephantbroker/schemas/config.py` -- All TTL configuration parameters
- `/elephantbroker/runtime/working_set/session_goals.py` -- `SessionGoalStore`
- `/elephantbroker/runtime/working_set/manager.py` -- `WorkingSetManager`
- `/elephantbroker/runtime/compaction/engine.py` -- `CompactionEngine`
- `/elephantbroker/runtime/context/session_store.py` -- `SessionContextStore`
- `/elephantbroker/runtime/context/session_artifact_store.py` -- `SessionArtifactStore`
- `/elephantbroker/runtime/context/lifecycle.py` -- `ContextLifecycle`
- `/elephantbroker/runtime/context/async_analyzer.py` -- `AsyncInjectionAnalyzer`
- `/elephantbroker/runtime/guards/engine.py` -- `RedLineGuardEngine`
- `/elephantbroker/runtime/guards/approval_queue.py` -- `ApprovalQueue`
- `/elephantbroker/runtime/procedures/engine.py` -- `ProcedureEngine`
- `/elephantbroker/runtime/consolidation/engine.py` -- `ConsolidationEngine`
- `/elephantbroker/runtime/consolidation/stages/domain_discovery.py` -- `DomainDiscoveryTask`
- `/elephantbroker/runtime/adapters/cognee/cached_embeddings.py` -- `CachedEmbeddingService`
- `/elephantbroker/pipelines/turn_ingest/buffer.py` -- `IngestBuffer`
- `/elephantbroker/pipelines/turn_ingest/pipeline.py` -- fact_domains write
- `/elephantbroker/api/routes/sessions.py` -- session_parent write
- `/elephantbroker/api/routes/consolidation.py` -- consolidation_status read
- `/elephantbroker/config/default.yaml` -- default config


---


## ElephantBroker -- Graph & Vector Store Configuration Reference

### 1. Neo4j Connection

#### Connection Parameters

| Parameter | Default | Env Var | Source |
|-----------|---------|---------|--------|
| URI | `bolt://localhost:7687` | `EB_NEO4J_URI` | `CogneeConfig.neo4j_uri` |
| Username | `neo4j` | `EB_NEO4J_USER` | `CogneeConfig.neo4j_user` |
| Password | `elephant_dev` | `EB_NEO4J_PASSWORD` | `CogneeConfig.neo4j_password` |

#### Driver Configuration

The `GraphAdapter` (`elephantbroker/runtime/adapters/cognee/graph.py`) creates a Neo4j async driver on demand:

```python
self._driver = AsyncGraphDatabase.driver(self._uri, auth=self._auth)
```

- Uses `neo4j.AsyncDriver` (official Python async driver)
- No explicit pool settings -- uses Neo4j driver defaults (100 max connections)
- No explicit timeouts -- uses driver defaults
- Connection is lazily initialized on first use
- Single driver instance shared per `GraphAdapter` lifetime (one per `RuntimeContainer`)

#### Cognee SDK Configuration

In `configure_cognee()` (`elephantbroker/runtime/adapters/cognee/config.py`), Neo4j is registered as:

```python
cognee.config.set_graph_database_provider("neo4j")
cognee.config.set_graph_db_config({
    "graph_database_url": config.neo4j_uri,
    "graph_database_username": config.neo4j_user,
    "graph_database_password": config.neo4j_password,
})
```

#### Docker Infrastructure

From `infrastructure/docker-compose.yml`:

```yaml
neo4j:
  image: neo4j:5-community
  ports:
    - "17474:7474"   # Browser
    - "17687:7687"   # Bolt
  environment:
    NEO4J_AUTH: neo4j/elephant_dev
    NEO4J_PLUGINS: '["apoc"]'
  volumes:
    - neo4j_data:/data
```

APOC plugin is installed for advanced Cypher operations. The Community edition is used (no enterprise clustering).

---

### 2. Neo4j Node Types

All node types are defined as Cognee `DataPoint` subclasses in `elephantbroker/runtime/adapters/cognee/datapoints.py`. Storage is exclusively via `add_data_points()` which performs `MERGE` by Cognee ID and auto-embeds `index_fields` in Qdrant.

#### FactDataPoint

The primary knowledge unit. Stores extracted facts from conversations.

| Field | Type | Indexed (Qdrant) | Notes |
|-------|------|-------------------|-------|
| `text` | str | **Yes** (`FactDataPoint_text`) | The fact content; primary search field |
| `category` | str | No | FactCategory enum value |
| `scope` | str | No | Scope enum: session, actor, team, organization, global, etc. |
| `confidence` | float | No | 0.0-1.0, subject to decay |
| `memory_class` | str | No | episodic, semantic, procedural |
| `session_key` | str | No | Stable routing key (e.g., `agent:main:main`) |
| `session_id` | str | No | Ephemeral UUID |
| `source_actor_id` | str | No | Who produced this fact |
| `target_actor_ids` | list[str] | No | Who the fact is about |
| `goal_ids` | list[str] | No | Related goals |
| `eb_created_at` | int | No | Epoch ms |
| `eb_updated_at` | int | No | Epoch ms |
| `use_count` | int | No | How many times retrieved |
| `successful_use_count` | int | No | How many times usefully consumed |
| `provenance_refs` | list[str] | No | Evidence trail |
| `embedding_ref` | str | No | `FactDataPoint_text:{id}` |
| `token_size` | int | No | Token count of text |
| `eb_id` | str | No | ElephantBroker UUID (match key for structural queries) |
| `gateway_id` | str | No | Gateway isolation key |
| `decision_domain` | str | No | Autonomy domain taxonomy |
| `archived` | bool | No | Consolidation archive flag |
| `autorecall_blacklisted` | bool | No | Pruned from auto-recall |

#### ActorDataPoint

Represents a user, agent, or service.

| Field | Type | Indexed (Qdrant) | Notes |
|-------|------|-------------------|-------|
| `display_name` | str | **Yes** (`ActorDataPoint_display_name`) | Human-readable name |
| `actor_type` | str | No | ActorType enum value |
| `authority_level` | int | No | 0-100 |
| `handles` | list[str] | No | Platform-qualified handles (`platform:identifier`) |
| `org_id` | str | No | Organization UUID |
| `team_ids` | list[str] | No | Team UUIDs |
| `trust_level` | float | No | 0.0-1.0 |
| `tags` | list[str] | No | Arbitrary tags |
| `eb_id` | str | No | ElephantBroker UUID |
| `gateway_id` | str | No | Gateway isolation |

#### GoalDataPoint

Persistent goals (global/org/team/actor scope).

| Field | Type | Indexed (Qdrant) | Notes |
|-------|------|-------------------|-------|
| `title` | str | **Yes** (`GoalDataPoint_title`) | Goal title |
| `description` | str | **Yes** (`GoalDataPoint_description`) | Goal details |
| `status` | str | No | active, completed, abandoned |
| `scope` | str | No | global, organization, team, actor, session |
| `parent_goal_id` | str | No | Hierarchy parent |
| `owner_actor_ids` | list[str] | No | Goal owners |
| `success_criteria` | list[str] | No | Completion conditions |
| `blockers` | list[str] | No | Current blockers |
| `confidence` | float | No | 0.0-1.0 |
| `org_id` | str | No | Organization scoping |
| `team_id` | str | No | Team scoping |
| `goal_meta` | dict | No | Auto-goal tracking metadata |
| `eb_id` | str | No | ElephantBroker UUID |
| `gateway_id` | str | No | Gateway isolation |

#### ProcedureDataPoint

Learned workflows with step sequences.

| Field | Type | Indexed (Qdrant) | Notes |
|-------|------|-------------------|-------|
| `name` | str | **Yes** (`ProcedureDataPoint_name`) | Procedure name |
| `description` | str | **Yes** (`ProcedureDataPoint_description`) | Procedure description |
| `scope` | str | No | Scope enum value |
| `dp_version` | int | No | Version counter, incremented on update |
| `source_actor_id` | str | No | Who created it |
| `decision_domain` | str | No | Autonomy domain |
| `steps_json` | str | No | JSON-serialized list of ProcedureStep |
| `red_line_bindings_json` | str | No | JSON-serialized red-line bindings |
| `approval_requirements_json` | str | No | JSON-serialized approval requirements |
| `eb_id` | str | No | ElephantBroker UUID |
| `gateway_id` | str | No | Gateway isolation |

#### ClaimDataPoint

Verification claims attached to facts.

| Field | Type | Indexed (Qdrant) | Notes |
|-------|------|-------------------|-------|
| `claim_text` | str | **Yes** (`ClaimDataPoint_claim_text`) | Claim content |
| `claim_type` | str | No | Claim category |
| `status` | str | No | unverified, self_supported, tool_supported, supervisor_verified |
| `procedure_id` | str | No | Related procedure |
| `goal_id` | str | No | Related goal |
| `actor_id` | str | No | Claiming actor |
| `eb_id` | str | No | ElephantBroker UUID |
| `gateway_id` | str | No | Gateway isolation |

#### EvidenceDataPoint

Evidence supporting claims.

| Field | Type | Indexed (Qdrant) | Notes |
|-------|------|-------------------|-------|
| `evidence_type` | str | No | Type of evidence |
| `ref_value` | str | **Yes** (`EvidenceDataPoint_ref_value`) | Evidence reference value |
| `content_hash` | str | No | SHA256 of content |
| `created_by_actor_id` | str | No | Who submitted |
| `eb_id` | str | No | ElephantBroker UUID |
| `gateway_id` | str | No | Gateway isolation |

#### ArtifactDataPoint

Tool output artifacts (code blocks, search results, etc.).

| Field | Type | Indexed (Qdrant) | Notes |
|-------|------|-------------------|-------|
| `tool_name` | str | No | Source tool name |
| `summary` | str | **Yes** (`ArtifactDataPoint_summary`) | LLM-generated summary |
| `content` | str | No | Full tool output |
| `session_id` | str | No | Session UUID |
| `actor_id` | str | No | Creating actor |
| `goal_id` | str | No | Related goal |
| `token_estimate` | int | No | Content token count |
| `tags` | list[str] | No | Arbitrary tags |
| `eb_id` | str | No | ElephantBroker UUID |
| `gateway_id` | str | No | Gateway isolation |

#### OrganizationDataPoint

Business-level org entity. No `gateway_id` -- spans gateways.

| Field | Type | Indexed (Qdrant) |
|-------|------|-------------------|
| `name` | str | **Yes** (`OrganizationDataPoint_name`) |
| `display_label` | str | No |
| `eb_id` | str | No |

#### TeamDataPoint

Team within an organization. No `gateway_id` -- spans gateways.

| Field | Type | Indexed (Qdrant) |
|-------|------|-------------------|
| `name` | str | **Yes** (`TeamDataPoint_name`) |
| `display_label` | str | No |
| `org_id` | str | No |
| `eb_id` | str | No |

#### AgentIdentity (non-DataPoint node)

Created directly via Cypher `MERGE` in `sessions.py`, not via `add_data_points()`:

| Field | Type | Notes |
|-------|------|-------|
| `agent_key` | str | Primary key, e.g., `gw-prod:main` |
| `agent_id` | str | Agent identifier within gateway |
| `gateway_id` | str | Gateway identifier |
| `short_name` | str | Display label |
| `gateway_short_name` | str | Gateway display label |
| `registered_at` | datetime | ON CREATE timestamp |
| `last_seen_at` | datetime | ON MATCH timestamp |

---

### 3. Neo4j Edge Types

All edges are created via `GraphAdapter.add_relation()` using `MERGE` semantics (idempotent).

#### CREATED_BY

- **Direction:** DataPoint --> ActorDataPoint
- **Created by:** `MemoryStoreFacade.store()` (fact -> source actor), `SessionArtifactStore` (artifact -> agent actor)
- **Used in queries:** Not directly queried; traversed via `get_neighbors()` / `get_relationships()`

#### ABOUT_ACTOR

- **Direction:** FactDataPoint --> ActorDataPoint
- **Created by:** `MemoryStoreFacade.store()` (fact -> target actors)
- **Used in queries:** Traversed via neighbor discovery

#### SERVES_GOAL

- **Direction:** FactDataPoint/ArtifactDataPoint --> GoalDataPoint
- **Created by:** `MemoryStoreFacade.store()` (fact -> goals), `SessionArtifactStore` (artifact -> goal)
- **Used in queries:** Traversed via neighbor discovery

#### CHILD_OF

- **Direction:** GoalDataPoint --> GoalDataPoint (child -> parent)
- **Created by:** `GoalManager.set_goal()`, `SessionGoalStore._flush_to_graph()`
- **Used in queries:**
```cypher
MATCH (child:GoalDataPoint)-[:CHILD_OF]->(parent {eb_id: $root_id})
WHERE child.gateway_id = $gateway_id
RETURN properties(child) AS props
```

#### OWNS_GOAL

- **Direction:** ActorDataPoint --> GoalDataPoint
- **Created by:** `GoalManager.set_goal()`, `SessionGoalStore._flush_to_graph()`
- **Used in queries (Phase 8 scope-aware):**
```cypher
MATCH (g:GoalDataPoint)
WHERE g.status = 'active' AND g.gateway_id = $gateway_id
AND (
  g.scope = 'global'
  OR (g.scope = 'organization' AND g.org_id = $org_id)
  OR (g.scope = 'team' AND g.team_id IN $team_ids)
  OR (g.scope = 'actor' AND EXISTS {
    MATCH (g)<-[:OWNS_GOAL]-(a:ActorDataPoint)
    WHERE a.eb_id IN $actor_ids
  })
)
RETURN properties(g) AS props
```

#### SUPERSEDES

- **Direction:** FactDataPoint/ProcedureDataPoint --> FactDataPoint/ProcedureDataPoint (new -> old)
- **Created by:** `TurnIngestPipeline` (fact supersession), `ProcedureIngestPipeline` (procedure version update)
- **Used in queries (conflict detection):**
```cypher
MATCH (f1:FactDataPoint)-[r:SUPERSEDES|CONTRADICTS]->(f2:FactDataPoint)
WHERE f1.gateway_id = $gateway_id
RETURN f1.eb_id AS src, f2.eb_id AS tgt, type(r) AS rel
```

#### CONTRADICTS

- **Direction:** FactDataPoint --> FactDataPoint (new -> old)
- **Created by:** `TurnIngestPipeline` (detected contradictions during ingest)
- **Used in queries:** Same conflict detection query as SUPERSEDES (above)

#### SUPPORTS

- **Direction:** EvidenceDataPoint --> ClaimDataPoint
- **Created by:** `EvidenceAndVerificationEngine.attach_evidence()`
- **Used in queries (evidence counting):**
```cypher
MATCH (e:EvidenceDataPoint)-[:SUPPORTS]->(c:ClaimDataPoint)-[:SUPPORTS]->(f:FactDataPoint)
WHERE f.gateway_id = $gateway_id
RETURN f.eb_id AS fid, count(e) AS cnt
```

#### MEMBER_OF

- **Direction:** ActorDataPoint --> TeamDataPoint
- **Created by:** `ActorRegistry.register_actor()`, admin API `add_team_member`
- **Used in queries:**
```cypher
MATCH (a:ActorDataPoint)-[:MEMBER_OF]->(t:TeamDataPoint {eb_id: $team_id})
RETURN properties(a) AS props
```

#### BELONGS_TO

- **Direction:** TeamDataPoint --> OrganizationDataPoint
- **Created by:** Admin API `create_team`
- **Used in queries:**
```cypher
MATCH (t:TeamDataPoint)-[:BELONGS_TO]->(o:OrganizationDataPoint {eb_id: $org_id})
RETURN properties(t) AS props
```

#### REPORTS_TO / SUPERVISES

- **Direction:** ActorDataPoint --> ActorDataPoint
- **Created by:** External setup (admin operations)
- **Used in queries (authority chain traversal):**
```cypher
MATCH path = (start {eb_id: $actor_id})-[:REPORTS_TO|SUPERVISES*1..10]->(supervisor)
WHERE start.gateway_id = $gateway_id
RETURN properties(supervisor) AS props
ORDER BY length(path)
```

#### SUPERSEDED_BY

- **Direction:** FactDataPoint --> FactDataPoint (old -> new canonical)
- **Created by:** Consolidation `CanonicalizeStage` (archived members -> canonical fact)
- **Used in queries:** Traversed via neighbor discovery

#### HAS_TRIGGER

- **Direction:** ProcedureDataPoint --> trigger word (string node)
- **Created by:** `ProcedureIngestPipeline`
- **Used in queries:** Traversed via neighbor discovery

#### OWNED_BY

- **Direction:** ArtifactDataPoint --> ActorDataPoint
- **Created by:** `SessionArtifactStore` (visibility filtering)
- **Used in queries:** Traversed via neighbor discovery

---

### 4. Qdrant Collections

Collections are auto-created by Cognee's `add_data_points()` from the `metadata.index_fields` on each DataPoint subclass. The naming convention is `{ClassName}_{field_name}`.

#### Collection Inventory

| Collection | DataPoint Class | Field | Dimension | Distance | Usage |
|------------|----------------|-------|-----------|----------|-------|
| `FactDataPoint_text` | FactDataPoint | text | 768 | Cosine (Cognee default) | Primary fact search, dedup, direct vector fallback |
| `ActorDataPoint_display_name` | ActorDataPoint | display_name | 768 | Cosine | Actor discovery |
| `GoalDataPoint_title` | GoalDataPoint | title | 768 | Cosine | Goal search |
| `GoalDataPoint_description` | GoalDataPoint | description | 768 | Cosine | Goal search |
| `ProcedureDataPoint_name` | ProcedureDataPoint | name | 768 | Cosine | Procedure discovery |
| `ProcedureDataPoint_description` | ProcedureDataPoint | description | 768 | Cosine | Procedure discovery |
| `ClaimDataPoint_claim_text` | ClaimDataPoint | claim_text | 768 | Cosine | Claim search |
| `EvidenceDataPoint_ref_value` | EvidenceDataPoint | ref_value | 768 | Cosine | Evidence search |
| `ArtifactDataPoint_summary` | ArtifactDataPoint | summary | 768 | Cosine | Artifact search |
| `OrganizationDataPoint_name` | OrganizationDataPoint | name | 768 | Cosine | Org discovery |
| `TeamDataPoint_name` | TeamDataPoint | name | 768 | Cosine | Team discovery |

Dimension is set by `CogneeConfig.embedding_dimensions` (default 768, matching `gemini/text-embedding-004`). If you switch to `openai/text-embedding-3-large`, set dimensions to 1024 (or 3072 for full output). Distance metric is Cognee's default (cosine similarity).

#### Directly Referenced Collections

Only two collections are referenced by name in runtime code:

- **`FactDataPoint_text`** -- used in `MemoryStoreFacade` for dedup checks, in `RetrievalOrchestrator` for direct vector fallback, in `ConsolidationEngine`/`CanonicalizeStage` for GDPR vector cleanup
- **`ArtifactDataPoint_summary`** -- used in `ToolArtifactStore` (defined as constant but actual search goes through `cognee.search()`)

---

### 5. Qdrant Connection

#### Connection Parameters

| Parameter | Default | Env Var |
|-----------|---------|---------|
| URL | `http://localhost:6333` | `EB_QDRANT_URL` |

#### Client Configuration

The `VectorAdapter` (`elephantbroker/runtime/adapters/cognee/vector.py`) creates an async client:

```python
self._client = AsyncQdrantClient(url=self._url)
```

- Uses `qdrant-client` Python SDK (async variant)
- No API key configured (local/internal deployment)
- No explicit timeout settings
- Client lazily initialized on first use

#### Cognee SDK Configuration

```python
cognee.config.set_vector_db_provider("qdrant")
cognee.config.set_vector_db_config({
    "vector_db_url": config.qdrant_url,
})
```

Requires the community adapter: `cognee-community-vector-adapter-qdrant >= 0.2.2` (registered via `from cognee_community_vector_adapter_qdrant import register`).

#### Named Vectors

The `VectorAdapter.search_similar()` method accepts a `using` parameter (default `"text"`), which maps to Qdrant named vectors within each collection. Cognee creates these named vectors during `add_data_points()`.

#### Docker Infrastructure

```yaml
qdrant:
  image: qdrant/qdrant:v1.17.0
  ports:
    - "16333:6333"   # REST API
    - "16334:6334"   # gRPC
  volumes:
    - qdrant_data:/qdrant/storage
```

Pinned to Qdrant v1.17.0 for compatibility with `qdrant-client` and Cognee's community adapter.

---

### 6. Cognee Configuration

#### `configure_cognee()` Parameters

Called at runtime boot (`elephantbroker/runtime/adapters/cognee/config.py`):

```python
async def configure_cognee(config: CogneeConfig, llm_config: LLMConfig | None = None)
```

**Environment overrides set before Cognee init:**

| Env Var | Value | Purpose |
|---------|-------|---------|
| `ENABLE_BACKEND_ACCESS_CONTROL` | `false` | Disable Cognee multi-user access control |
| `COGNEE_DISABLE_TELEMETRY` | `true` | Disable Cognee usage telemetry |

**LLM configuration (for `cognee.cognify()` entity/relationship extraction):**

```python
cognee.config.set_llm_config({
    "llm_provider": "openai",
    "llm_model": llm_config.model,       # default: "openai/gemini/gemini-2.5-pro"
    "llm_endpoint": llm_config.endpoint,  # default: "http://localhost:8811/v1"
    "llm_api_key": llm_config.api_key,
})
```

**Embedding configuration (for chunk/triplet embedding during `cognify()`):**

```python
embedding_cfg.embedding_provider = config.embedding_provider    # "openai" (API client style, not vendor)
embedding_cfg.embedding_model = config.embedding_model          # "gemini/text-embedding-004"
embedding_cfg.embedding_dimensions = config.embedding_dimensions # 768
embedding_cfg.embedding_endpoint = config.embedding_endpoint     # "http://localhost:8811/v1"
embedding_cfg.embedding_api_key = config.embedding_api_key
```

#### Dataset Naming Convention

All Cognee datasets are gateway-scoped using the pattern: `{gateway_id}__{base_dataset}`

From `RuntimeContainer.from_config()`:
```python
dataset_name = f"{gw_id}__{config.cognee.default_dataset}"
## Example: "gw-prod-assistant__elephantbroker"
```

Special dataset patterns:
- **Turn ingest:** `{gateway_id}__{session_key}` (per-session dataset)
- **Artifact retrieval:** `{dataset_name}__artifacts` (artifact-specific sub-dataset)
- **Organization datasets:** `{org_id}__{dataset_name}` (via `DatasetManager`)

#### Cognee Pipeline Integration

`PipelineRunner` (`elephantbroker/runtime/adapters/cognee/pipeline_runner.py`) wraps Cognee's `Task` + `run_tasks`:

```python
pipeline = run_tasks(
    tasks=tasks,            # list[Task] — Cognee Task wrappers
    dataset_id=ds_id,       # UUID
    data=input_data,        # Input data
    pipeline_name=pipeline_name,
)
```

#### Cognee Search Types Used

| SearchType | Source Stage | Where Used |
|------------|-------------|------------|
| `GRAPH_COMPLETION` | Stage 3 (graph neighbors) | `RetrievalOrchestrator`, `MemoryStoreFacade.search()`, `ToolArtifactStore.search_artifacts()` |
| `CHUNKS` | Stage 2 (semantic chunks) | `RetrievalOrchestrator.get_semantic_hits_cognee()`, artifact hits |
| `CHUNKS_LEXICAL` | Stage 1 (keyword/BM25) | `RetrievalOrchestrator.get_keyword_hits()` |

#### EmbeddingService (Direct HTTP)

The `EmbeddingService` (`elephantbroker/runtime/adapters/cognee/embeddings.py`) bypasses Cognee for on-demand embeddings:

```python
POST {endpoint}/embeddings
{"model": "gemini/text-embedding-004", "input": texts}  # whatever EB_EMBEDDING_MODEL is set to
```

- Timeout: 30 seconds
- Wrapped by `CachedEmbeddingService` with Redis caching (key: `eb:emb_cache:{sha256(text)[:32]}`, TTL: 3600s)

---

### 7. Query Patterns

#### Gateway ID Filtering

**CRITICAL RULE:** All Cypher queries MUST include `WHERE gateway_id = $gateway_id` for data isolation between gateways. This is enforced at the code level -- every module passes its `self._gateway_id` into query parameters.

**Standard pattern (FactDataPoint queries):**
```cypher
MATCH (f:FactDataPoint)
WHERE f.gateway_id = $gateway_id
  AND f.scope = $scope
  AND f.memory_class = $memory_class
  AND f.session_key = $session_key
OPTIONAL MATCH (f)-[r]->(target)
RETURN properties(f) AS props,
       collect({type: type(r), target: properties(target)}) AS relations
LIMIT $limit
```

**Exceptions to gateway filtering:**
- `OrganizationDataPoint` and `TeamDataPoint` -- these are business entities that span gateways
- `AgentIdentity` nodes -- identified by `agent_key` which already contains `gateway_id`
- `GraphAdapter.add_relation()` -- matches by `eb_id` only (both nodes already gateway-scoped via their properties)

#### Structural Query Patterns

**Fact retrieval with relations (used by MemoryStoreFacade and RetrievalOrchestrator):**
```cypher
MATCH (f:FactDataPoint)
WHERE f.gateway_id = $gateway_id
  AND (f.archived IS NULL OR f.archived = false)
  AND (f.autorecall_blacklisted IS NULL OR f.autorecall_blacklisted = false)
OPTIONAL MATCH (f)-[r]->(target)
RETURN properties(f) AS props,
       collect({type: type(r), target: properties(target)}) AS relations
LIMIT $limit
```

**Actor handle lookup:**
```cypher
MATCH (a:ActorDataPoint)
WHERE $handle IN a.handles AND a.gateway_id = $gateway_id
RETURN properties(a) AS props LIMIT 1
```

**Authority chain traversal (variable-length path):**
```cypher
MATCH path = (start {eb_id: $actor_id})-[:REPORTS_TO|SUPERVISES*1..10]->(supervisor)
WHERE start.gateway_id = $gateway_id
RETURN properties(supervisor) AS props
ORDER BY length(path)
```

**Actor relationships (bidirectional UNION):**
```cypher
MATCH (a {eb_id: $actor_id})-[r]->(b)
WHERE a.gateway_id = $gateway_id
RETURN a.eb_id AS source, b.eb_id AS target, type(r) AS rel_type, properties(r) AS props
UNION
MATCH (b)-[r]->(a {eb_id: $actor_id})
WHERE a.gateway_id = $gateway_id
RETURN b.eb_id AS source, a.eb_id AS target, type(r) AS rel_type, properties(r) AS props
```

**Phase 8 scope-aware goal visibility (4-clause):**
```cypher
MATCH (g:GoalDataPoint)
WHERE g.status = 'active' AND g.gateway_id = $gateway_id
AND (
  g.scope = 'global'
  OR (g.scope = 'organization' AND g.org_id = $org_id)
  OR (g.scope = 'team' AND g.team_id IN $team_ids)
  OR (g.scope = 'actor' AND EXISTS {
    MATCH (g)<-[:OWNS_GOAL]-(a:ActorDataPoint)
    WHERE a.eb_id IN $actor_ids
  })
)
RETURN properties(g) AS props
```

**Evidence index (3-hop traversal):**
```cypher
MATCH (e:EvidenceDataPoint)-[:SUPPORTS]->(c:ClaimDataPoint)-[:SUPPORTS]->(f:FactDataPoint)
WHERE f.gateway_id = $gateway_id
RETURN f.eb_id AS fid, count(e) AS cnt
```

**Consolidation batch loading (paginated):**
```cypher
MATCH (f:FactDataPoint)
WHERE f.gateway_id = $gw
  AND (f.archived IS NULL OR f.archived = false)
  AND (f.eb_last_used_at IS NULL OR f.eb_last_used_at < $cutoff)
RETURN properties(f) AS props
ORDER BY f.eb_created_at
SKIP $offset LIMIT $batch_size
```

**AgentIdentity registration (MERGE with ON CREATE/ON MATCH):**
```cypher
MERGE (n:AgentIdentity {agent_key: $agent_key})
ON CREATE SET n.registered_at = datetime()
ON MATCH SET n.last_seen_at = datetime()
SET n.agent_id = $agent_id, n.gateway_id = $gw_id,
    n.short_name = $short_name, n.gateway_short_name = $gw_short
```

**Org/team queries (no gateway filtering):**
```cypher
MATCH (o:OrganizationDataPoint) RETURN properties(o) AS props
MATCH (t:TeamDataPoint)-[:BELONGS_TO]->(o:OrganizationDataPoint {eb_id: $org_id})
RETURN properties(t) AS props
```

#### Performance Implications

1. **No explicit Neo4j indexes** -- The codebase does not create Neo4j property indexes via `CREATE INDEX`. All queries rely on label scans filtered by property matching. For production, the following indexes would improve performance:
   - `CREATE INDEX FOR (f:FactDataPoint) ON (f.eb_id)`
   - `CREATE INDEX FOR (f:FactDataPoint) ON (f.gateway_id)`
   - `CREATE INDEX FOR (f:FactDataPoint) ON (f.session_key)`
   - `CREATE INDEX FOR (a:ActorDataPoint) ON (a.eb_id)`
   - `CREATE INDEX FOR (a:ActorDataPoint) ON (a.gateway_id)`
   - `CREATE INDEX FOR (g:GoalDataPoint) ON (g.eb_id)`
   - `CREATE INDEX FOR (g:GoalDataPoint) ON (g.gateway_id)`
   - `CREATE INDEX FOR (n:AgentIdentity) ON (n.agent_key)`

2. **Cognee internal indexes** -- `add_data_points()` uses `MERGE` by Cognee's internal `id` field (UUID), which Cognee may index internally. The `eb_id` property used by ElephantBroker's structural queries does NOT benefit from this.

3. **Variable-length path queries** -- Authority chain traversal (`*1..10`) could be expensive in deep hierarchies. Capped at 10 hops.

4. **Consolidation pagination** -- Uses `SKIP $offset LIMIT $batch_size` with configurable batch size (default 500). This is a known Neo4j anti-pattern for large offsets but acceptable for consolidation's background processing.

5. **Lazy driver init** -- Both `GraphAdapter` and `VectorAdapter` create connections on first use, avoiding startup overhead if the stores are not needed (e.g., in test/dev modes).

6. **EXISTS subquery** -- The Phase 8 scope-aware goal query uses `EXISTS { MATCH ... }` which requires Neo4j 5.0+ (compatible with `neo4j:5-community` image).


---


## ElephantBroker CLI & Configuration Reference

### 1. Entry Points

Installed via `pyproject.toml` as console scripts:

| Command | Entry point | Purpose |
|---------|------------|---------|
| `elephantbroker` | `elephantbroker.server:main` | Server management (serve, health-check, migrate) |
| `ebrun` | `elephantbroker.cli:main` | Admin CLI for org/team/actor/profile/goal management |

Both are Click-based CLI tools installed into the virtualenv when the package is installed (`pip install -e .`).

---

### 2. `elephantbroker` Server CLI

#### `elephantbroker serve`

Start the FastAPI/uvicorn API server.

```
elephantbroker serve [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Bind host address |
| `--port` | `8420` | Bind port |
| `--log-level` | `info` | Log level (`debug`, `verbose`, `info`, `warning`, `error`, `critical`). `verbose` is a custom level mapped to uvicorn `info`. |
| `--config` | None | Path to YAML config file. If omitted, config is built entirely from environment variables. |

**Examples:**

```bash
## Start with defaults (env vars only)
elephantbroker serve

## Start with custom YAML config
elephantbroker serve --config /etc/elephantbroker/config.yaml

## Start on a specific port with verbose logging
elephantbroker serve --port 8421 --log-level verbose

## Production bind
elephantbroker serve --host 0.0.0.0 --port 8420 --config ./config/default.yaml --log-level warning
```

**Startup sequence:** `ElephantBrokerConfig.load(path)` is called — when `--config` is omitted it falls through to `load(None)` and reads the packaged `elephantbroker/config/default.yaml`. Either way the YAML is parsed first, then every binding in `ENV_OVERRIDE_BINDINGS` is applied on top, then `_apply_inheritance_fallbacks()` populates derived secrets and endpoints. The merged config is passed to `RuntimeContainer.from_config()` which initializes Cognee, adapters, Redis, OTEL tracing, and all runtime modules.

#### `elephantbroker health-check`

Probe a running server's readiness endpoint.

```
elephantbroker health-check [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `localhost` | Target host to check |
| `--port` | `8420` | Target port to check |

**Examples:**

```bash
## Check local server
elephantbroker health-check

## Check remote server
elephantbroker health-check --host 10.10.0.10 --port 8420
```

Exit codes: `0` = healthy (`GET /health/ready` returns 200), `1` = unhealthy or unreachable.

#### `elephantbroker migrate`

Run database migrations. Currently a placeholder (prints "No migrations needed.").

```bash
elephantbroker migrate
```

---

### 3. `ebrun` Admin CLI

All commands call the runtime API over HTTP. The runtime must be running.

#### Global Options

```
ebrun [OPTIONS] COMMAND [ARGS]...
```

| Flag | Env Var | Description |
|------|---------|-------------|
| `--actor-id` | `EB_ACTOR_ID` | Actor UUID for authorization (sent as `X-EB-Actor-Id` header) |
| `--runtime-url` | `EB_RUNTIME_URL` | Runtime API base URL |

#### `ebrun config` -- CLI Configuration

##### `ebrun config set KEY VALUE`

Set a persistent config value in `~/.elephantbroker/config.json`.

Valid keys:
- `actor-id` -- stored as `actor_id` in JSON
- `runtime-url` -- stored as `runtime_url` in JSON

```bash
ebrun config set actor-id "550e8400-e29b-41d4-a716-446655440000"
ebrun config set runtime-url "http://10.10.0.10:8420"
```

##### `ebrun config show`

Display the current contents of `~/.elephantbroker/config.json`.

```bash
ebrun config show
## Output: { "actor_id": "...", "runtime_url": "..." }
```

#### `ebrun bootstrap`

Bootstrap the system: create the first organization, team, and admin actor. Only works when no actors exist yet (bootstrap mode is active).

```
ebrun bootstrap [OPTIONS]
```

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--org-name` | Yes | -- | Organization name |
| `--org-label` | No | first 20 chars of org-name | Short display label |
| `--team-name` | Yes | -- | Team name |
| `--team-label` | No | first 20 chars of team-name | Short display label |
| `--admin-name` | Yes | -- | Admin actor display name |
| `--admin-authority` | No | `90` | Admin authority level (0-100) |
| `--admin-handles` | No | (none) | Admin handles, repeatable (e.g. `email:admin@acme.com`) |

**Example:**

```bash
ebrun bootstrap \
  --org-name "Acme Corp" \
  --team-name "Backend" \
  --admin-name "Admin" \
  --admin-authority 90 \
  --admin-handles "email:admin@acme.com"
```

This creates the org, team, and admin actor sequentially via the admin API, then saves the resulting `actor_id` and `runtime_url` to `~/.elephantbroker/config.json`.

#### `ebrun org` -- Organization Management

##### `ebrun org create`

```bash
ebrun org create --name "Acme Corp" --label "ACME"
```

| Flag | Required | Default |
|------|----------|---------|
| `--name` | Yes | -- |
| `--label` | No | `""` |

##### `ebrun org list`

```bash
ebrun org list
```

#### `ebrun team` -- Team Management

##### `ebrun team create`

```bash
ebrun team create --name "Backend" --label "BE" --org-id <uuid>
```

| Flag | Required | Default |
|------|----------|---------|
| `--name` | Yes | -- |
| `--label` | No | `""` |
| `--org-id` | Yes | -- |

##### `ebrun team list`

```bash
ebrun team list
ebrun team list --org-id <uuid>
```

##### `ebrun team add-member TEAM_ID ACTOR_ID`

```bash
ebrun team add-member <team-uuid> <actor-uuid>
```

##### `ebrun team remove-member TEAM_ID ACTOR_ID`

```bash
ebrun team remove-member <team-uuid> <actor-uuid>
```

##### `ebrun team members TEAM_ID`

```bash
ebrun team members <team-uuid>
```

#### `ebrun actor` -- Actor Management

##### `ebrun actor create`

```bash
ebrun actor create \
  --display-name "Maria" \
  --type human_operator \
  --authority-level 50 \
  --org-id <uuid> \
  --team-ids <team-uuid> \
  --handles "email:maria@acme.com"
```

| Flag | Required | Default |
|------|----------|---------|
| `--display-name` | Yes | -- |
| `--type` | No | `human_operator` |
| `--authority-level` | No | `0` |
| `--org-id` | No | None |
| `--team-ids` | No | (none), repeatable |
| `--handles` | No | (none), repeatable |

##### `ebrun actor list`

```bash
ebrun actor list
ebrun actor list --org-id <uuid>
```

##### `ebrun actor merge CANONICAL_ID DUPLICATE_ID`

Merge a duplicate actor into the canonical actor.

```bash
ebrun actor merge <canonical-uuid> <duplicate-uuid>
```

#### `ebrun profile` -- Profile Management

##### `ebrun profile list`

Lists available profile names (hardcoded: `coding`, `research`, `managerial`, `worker`, `personal_assistant`).

```bash
ebrun profile list
```

##### `ebrun profile resolve PROFILE_ID`

Show the fully resolved profile weights (base + org overrides).

```bash
ebrun profile resolve coding
```

##### `ebrun profile override-set ORG_ID PROFILE_ID OVERRIDES_JSON`

Set per-org profile overrides. The overrides argument is a JSON string.

```bash
ebrun profile override-set <org-uuid> coding '{"scoring_weights": {"turn_relevance": 0.25}}'
```

##### `ebrun profile override-list ORG_ID`

List all profile overrides for an organization.

```bash
ebrun profile override-list <org-uuid>
```

#### `ebrun authority` -- Authority Rules

##### `ebrun authority list`

```bash
ebrun authority list
```

##### `ebrun authority set ACTION`

Update an authority rule for a specific action.

```bash
ebrun authority set create_org --min-level 90
ebrun authority set manage_team --min-level 50 --require-matching-org
ebrun authority set manage_actor --min-level 70 --require-matching-org --require-matching-team --matching-exempt-level 90
```

| Flag | Required | Default |
|------|----------|---------|
| `--min-level` | Yes | -- |
| `--require-matching-org` | No | `False` |
| `--require-matching-team` | No | `False` |
| `--matching-exempt-level` | No | None |

#### `ebrun goal` -- Persistent Goal Management

##### `ebrun goal create`

```bash
ebrun goal create --title "Q1 Roadmap" --scope organization --org-id <uuid>
ebrun goal create --title "Ship v2.0" --scope team --team-id <uuid>
ebrun goal create --title "Learn Rust" --scope actor
ebrun goal create --title "System uptime" --scope global
```

| Flag | Required | Default |
|------|----------|---------|
| `--title` | Yes | -- |
| `--scope` | No | `actor` (choices: `actor`, `team`, `organization`, `global`) |
| `--org-id` | No | None |
| `--team-id` | No | None |
| `--description` | No | `""` |

##### `ebrun goal list`

```bash
ebrun goal list
ebrun goal list --scope organization --org-id <uuid>
```

---

### 4. Config File: `~/.elephantbroker/config.json`

The CLI config file stores persistent admin identity for `ebrun` commands. Created automatically by `ebrun bootstrap` or manually via `ebrun config set`.

**Location:** `~/.elephantbroker/config.json`

**Format:**

```json
{
  "actor_id": "550e8400-e29b-41d4-a716-446655440000",
  "runtime_url": "http://localhost:8420"
}
```

| Key | Description | Set via |
|-----|-------------|---------|
| `actor_id` | UUID of the actor making admin requests | `ebrun config set actor-id <uuid>` |
| `runtime_url` | Base URL of the runtime API | `ebrun config set runtime-url <url>` |

---

### 5. Identity Resolution (Actor ID)

Resolution priority chain (first non-empty wins):

| Priority | Source | Example |
|----------|--------|---------|
| 1 (highest) | `--actor-id` CLI flag | `ebrun --actor-id <uuid> org list` |
| 2 | `EB_ACTOR_ID` environment variable | `export EB_ACTOR_ID=<uuid>` |
| 3 | `~/.elephantbroker/config.json` `actor_id` key | Set via `ebrun config set actor-id <uuid>` |
| 4 (lowest) | Empty string `""` | (no actor identity, bootstrap mode only) |

Implementation: `_resolve_actor_id()` in `elephantbroker/cli.py` (line 44).

---

### 6. Runtime URL Resolution

Resolution priority chain (first non-empty wins):

| Priority | Source | Example |
|----------|--------|---------|
| 1 (highest) | `--runtime-url` CLI flag | `ebrun --runtime-url http://10.10.0.10:8420 org list` |
| 2 | `EB_RUNTIME_URL` environment variable | `export EB_RUNTIME_URL=http://10.10.0.10:8420` |
| 3 | `~/.elephantbroker/config.json` `runtime_url` key | Set via `ebrun config set runtime-url <url>` |
| 4 (lowest) | Default `http://localhost:8420` | Hardcoded fallback |

Implementation: `_resolve_runtime_url()` in `elephantbroker/cli.py` (line 57).

---

### 7. YAML Config: Full Schema with Defaults

**File location:** Typically `elephantbroker/config/default.yaml` or any path passed via `--config`.

**Resolution order for server config:** env var (if set) > YAML value > model default. The YAML contains literal values; env vars override them when explicitly set.

#### Complete YAML Schema

```yaml
## --- Gateway identity ---
gateway:
  gateway_id: "local"               # EB_GATEWAY_ID — gateway identifier
  gateway_short_name: ""             # EB_GATEWAY_SHORT_NAME
  org_id: ""                         # EB_ORG_ID — bind gateway to org
  team_id: ""                        # EB_TEAM_ID — bind gateway to team
  agent_authority_level: 0           # EB_AGENT_AUTHORITY_LEVEL — 0-100

## --- Cognee knowledge plane ---
cognee:
  neo4j_uri: "bolt://localhost:7687"                # EB_NEO4J_URI
  neo4j_user: "neo4j"                               # EB_NEO4J_USER
  neo4j_password: "elephant_dev"                     # EB_NEO4J_PASSWORD
  qdrant_url: "http://localhost:6333"                # EB_QDRANT_URL
  default_dataset: "elephantbroker"                  # EB_DEFAULT_DATASET
  embedding_provider: "openai"                       # EB_EMBEDDING_PROVIDER (API client style, not vendor)
  embedding_model: "gemini/text-embedding-004"       # EB_EMBEDDING_MODEL
  embedding_endpoint: "http://localhost:8811/v1"      # EB_EMBEDDING_ENDPOINT
  embedding_api_key: ""                              # EB_EMBEDDING_API_KEY
  embedding_dimensions: 768                          # EB_EMBEDDING_DIMENSIONS — must match model output

## --- LLM (extraction, classification, summarization) ---
llm:
  model: "openai/gemini/gemini-2.5-pro"    # EB_LLM_MODEL — MUST keep openai/ prefix
  endpoint: "http://localhost:8811/v1"      # EB_LLM_ENDPOINT
  api_key: ""                              # EB_LLM_API_KEY
  max_tokens: 8192                         # EB_LLM_MAX_TOKENS
  temperature: 0.1                         # EB_LLM_TEMPERATURE
  extraction_max_input_tokens: 4000        # EB_LLM_EXTRACTION_MAX_INPUT_TOKENS
  extraction_max_output_tokens: 16384      # EB_LLM_EXTRACTION_MAX_OUTPUT_TOKENS
  extraction_max_facts_per_batch: 10       # EB_LLM_EXTRACTION_MAX_FACTS
  summarization_max_output_tokens: 200     # EB_LLM_SUMMARIZATION_MAX_OUTPUT_TOKENS
  summarization_min_artifact_chars: 500    # EB_LLM_SUMMARIZATION_MIN_CHARS
  ingest_batch_size: 6                     # EB_INGEST_BATCH_SIZE
  ingest_batch_timeout_seconds: 60.0       # EB_INGEST_BATCH_TIMEOUT
  ingest_buffer_ttl_seconds: 300           # EB_INGEST_BUFFER_TTL
  extraction_context_facts: 20             # EB_EXTRACTION_CONTEXT_FACTS
  extraction_context_ttl_seconds: 3600     # EB_EXTRACTION_CONTEXT_TTL

## --- Reranker (Phase 5+) ---
reranker:
  endpoint: "http://localhost:1235"          # EB_RERANKER_ENDPOINT
  api_key: ""                                # EB_RERANKER_API_KEY
  model: "Qwen/Qwen3-Reranker-4B"           # EB_RERANKER_MODEL
  enabled: true
  timeout_seconds: 10.0
  batch_size: 32
  max_documents: 100
  fallback_on_error: true
  top_n: null                                # null = use all

## --- Infrastructure ---
infra:
  redis_url: "redis://localhost:6379"        # EB_REDIS_URL
  otel_endpoint: null                        # EB_OTEL_ENDPOINT — null = disabled
  log_level: "info"                          # EB_LOG_LEVEL (debug/verbose/info/warning/error/critical)
  metrics_ttl_seconds: 3600                  # EB_METRICS_TTL_SECONDS
  trace:
    memory_max_events: 10000                 # EB_TRACE_MEMORY_MAX_EVENTS
    memory_ttl_seconds: 3600
    otel_logs_enabled: false                 # EB_TRACE_OTEL_LOGS_ENABLED
  clickhouse:
    enabled: false                           # EB_CLICKHOUSE_ENABLED
    host: "localhost"                        # EB_CLICKHOUSE_HOST
    port: 8123                               # EB_CLICKHOUSE_PORT
    database: "otel"                         # EB_CLICKHOUSE_DATABASE
    logs_table: "otel_logs"

## --- Top-level runtime settings ---
default_profile: "coding"                   # EB_DEFAULT_PROFILE
enable_trace_ledger: true                    # EB_ENABLE_TRACE_LEDGER
max_concurrent_sessions: 100                 # EB_MAX_CONCURRENT_SESSIONS
consolidation_min_retention_seconds: 172800  # EB_CONSOLIDATION_MIN_RETENTION_SECONDS (48h)
# (master guard switch lives at `guards.enabled` above — env: EB_GUARDS_ENABLED)

## --- Embedding cache (Phase 5) ---
embedding_cache:
  enabled: true                              # EB_EMBEDDING_CACHE_ENABLED
  ttl_seconds: 3600                          # EB_EMBEDDING_CACHE_TTL
  key_prefix: "eb:emb_cache"

## --- Scoring pipeline (Phase 5) ---
scoring:
  neutral_use_prior: 0.5
  cheap_prune_max_candidates: 80
  semantic_blend_weight: 0.6
  merge_similarity_threshold: 0.95
  snapshot_ttl_seconds: 300                  # EB_SCORING_SNAPSHOT_TTL
  session_goals_ttl_seconds: 86400           # EB_SESSION_GOALS_TTL
  working_set_build_global_goals_filter_by_actors: true

## --- Verification multipliers ---
verification_multipliers:
  supervisor_verified: 1.0
  tool_supported: 0.9
  self_supported: 0.7
  unverified: 0.5
  no_claim: 0.8

## --- Conflict detection ---
conflict_detection:
  supersession_penalty: 1.0
  contradiction_edge_penalty: 0.9
  layer2_penalty: 0.7
  similarity_threshold: 0.9
  confidence_gap_threshold: 0.3
  redundancy_similarity_threshold: 0.85

## --- Successful-use feedback (Phase 9, opt-in) ---
successful_use:
  enabled: false                             # EB_SUCCESSFUL_USE_ENABLED
  endpoint: "http://localhost:8811/v1"  # EB_SUCCESSFUL_USE_ENDPOINT
  api_key: ""                                # EB_SUCCESSFUL_USE_API_KEY (falls back to EB_LLM_API_KEY)
  model: "gemini/gemini-2.5-flash-lite"      # EB_SUCCESSFUL_USE_MODEL
  batch_size: 5                              # EB_SUCCESSFUL_USE_BATCH_SIZE
  batch_timeout_seconds: 120.0
  feed_last_facts: 20
  min_confidence: 0.7
  run_async: true

## --- Goal injection ---
goal_injection:
  enabled: true
  max_session_goals: 5
  max_persistent_goals: 3
  include_persistent_goals: true

## --- Goal refinement ---
goal_refinement:
  hints_enabled: true
  refinement_task_enabled: true
  model: "gemini/gemini-2.5-flash-lite"
  max_subgoals_per_session: 10
  feed_recent_messages: 6
  run_refinement_async: true
  progress_confidence_delta: 0.1
  subgoal_dedup_threshold: 0.6

## --- Procedure candidates ---
procedure_candidates:
  enabled: true
  filter_by_relevance: true
  relevance_threshold: 0.3
  top_k: 3
  always_include_proof_required: true

## --- Audit trail (SQLite) ---
audit:
  procedure_audit_enabled: true
  procedure_audit_db_path: "data/procedure_audit.db"
  session_goal_audit_enabled: true
  session_goal_audit_db_path: "data/session_goals_audit.db"
  org_overrides_db_path: "data/org_overrides.db"
  authority_rules_db_path: "data/authority_rules.db"
  consolidation_reports_db_path: "data/consolidation_reports.db"
  tuning_deltas_db_path: "data/tuning_deltas.db"
  scoring_ledger_db_path: "data/scoring_ledger.db"
  retention_days: 90

## --- Profile cache ---
profile_cache:
  ttl_seconds: 300

## --- Guards (Phase 7) ---
guards:
  enabled: true
  builtin_rules_enabled: true
  history_ttl_seconds: 86400
  max_history_events: 50
  input_summary_max_chars: 500
  llm_escalation_max_tokens: 500
  llm_escalation_timeout_seconds: 10.0
  max_pattern_length: 500
  strictness_presets:
    loose:
      bm25_threshold_multiplier: 1.5
      semantic_threshold_override: 0.90
      structural_validators_enabled: false
      reinjection_on: "block_only"
      llm_escalation_on: "disabled"
    medium:
      bm25_threshold_multiplier: 1.0
      reinjection_on: "elevated_risk"
      llm_escalation_on: "ambiguous"
    strict:
      bm25_threshold_multiplier: 0.7
      semantic_threshold_override: 0.70
      warn_outcome_upgrade: "require_approval"
      reinjection_on: "any_non_pass"
      llm_escalation_on: "any_non_pass"

## --- HITL middleware ---
hitl:
  enabled: false
  default_url: "http://localhost:8421"
  timeout_seconds: 10.0
  approval_default_timeout_seconds: 300
  callback_hmac_secret: ""               # EB_HITL_CALLBACK_SECRET
  gateway_overrides: {}

## --- Context assembly (Phase 6) ---
context_assembly:
  max_context_window_fraction: 0.15
  fallback_context_window: 128000
  enable_dynamic_budget: true
  system_overlay_budget_fraction: 0.25
  goal_block_budget_fraction: 0.10
  evidence_budget_max_tokens: 500
  compaction_trigger_multiplier: 2.0
  compaction_summary_max_tokens: 1000

## --- Artifact capture (Phase 6) ---
artifact_capture:
  enabled: true
  min_content_chars: 200
  max_content_chars: 50000
  skip_tools: []

## --- Artifact assembly ---
artifact_assembly:
  placeholder_enabled: true
  placeholder_min_tokens: 100
  placeholder_template: '[Tool output: {tool_name} — {summary}\n → Call artifact_search("{artifact_id}") for full output]'

## --- Async injection analysis ---
async_analysis:
  enabled: false
  topic_continuation_threshold: 0.6
  batch_size: 20

## --- Compaction LLM ---
compaction_llm:
  model: "gemini/gemini-2.5-flash-lite"  # EB_COMPACTION_LLM_MODEL
  endpoint: "http://localhost:8811/v1"    # EB_COMPACTION_LLM_ENDPOINT
  api_key: ""                            # EB_COMPACTION_LLM_API_KEY (falls back to EB_LLM_API_KEY)
  max_tokens: 2000
  temperature: 0.2

```

#### Consolidation Config (accessed via `config.consolidation` property)

Loaded lazily from env vars (not YAML), accessible on `ElephantBrokerConfig.consolidation`:

| Env Var | Field | Default |
|---------|-------|---------|
| `EB_DEV_CONSOLIDATION_AUTO_TRIGGER` | `dev_auto_trigger_interval` | `"0"` (disabled; supports `1m`, `5m`, `1h`, `1d`) |
| `EB_CONSOLIDATION_BATCH_SIZE` | `batch_size` | `500` |

Full consolidation config fields (all have model defaults, no env var mapping beyond the two above): `active_session_protection_hours`, `cluster_similarity_threshold`, `canonicalize_divergence_threshold`, `strengthen_success_ratio_threshold`, `strengthen_min_use_count`, `strengthen_boost_factor`, `decay_recalled_unused_factor`, `decay_never_recalled_factor`, `decay_archival_threshold`, `decay_scope_multipliers`, `autorecall_blacklist_min_recalls`, `autorecall_blacklist_max_success_ratio`, `promote_session_threshold`, `promote_artifact_injected_threshold`, `pattern_recurrence_threshold`, `pattern_min_steps`, `max_patterns_per_run`, `ema_alpha`, `max_weight_adjustment_pct`, `min_correlation_samples`, `llm_calls_per_run_cap`.

---

### 8. Environment Variable Reference

All env vars use the `EB_` prefix. Below is the complete set recognized by `ENV_OVERRIDE_BINDINGS` (the single registry that powers `ElephantBrokerConfig.load()` after the F2/F3 unification).

| Env Var | Config Path | Default |
|---------|------------|---------|
| `EB_GATEWAY_ID` | `gateway.gateway_id` | `"local"` |
| `EB_GATEWAY_SHORT_NAME` | `gateway.gateway_short_name` | `""` |
| `EB_ORG_ID` | `gateway.org_id` | `None` |
| `EB_TEAM_ID` | `gateway.team_id` | `None` |
| `EB_AGENT_AUTHORITY_LEVEL` | `gateway.agent_authority_level` | `0` |
| `EB_NEO4J_URI` | `cognee.neo4j_uri` | `"bolt://localhost:7687"` |
| `EB_NEO4J_USER` | `cognee.neo4j_user` | `"neo4j"` |
| `EB_NEO4J_PASSWORD` | `cognee.neo4j_password` | `"elephant_dev"` |
| `EB_QDRANT_URL` | `cognee.qdrant_url` | `"http://localhost:6333"` |
| `EB_DEFAULT_DATASET` | `cognee.default_dataset` | `"elephantbroker"` |
| `EB_EMBEDDING_PROVIDER` | `cognee.embedding_provider` | `"openai"` |
| `EB_EMBEDDING_MODEL` | `cognee.embedding_model` | `"gemini/text-embedding-004"` |
| `EB_EMBEDDING_ENDPOINT` | `cognee.embedding_endpoint` | `"http://localhost:8811/v1"` |
| `EB_EMBEDDING_API_KEY` | `cognee.embedding_api_key` | `""` |
| `EB_EMBEDDING_DIMENSIONS` | `cognee.embedding_dimensions` | `768` |
| `EB_LLM_MODEL` | `llm.model` | `"openai/gemini/gemini-2.5-pro"` |
| `EB_LLM_ENDPOINT` | `llm.endpoint` | `"http://localhost:8811/v1"` |
| `EB_LLM_API_KEY` | `llm.api_key` | `""` |
| `EB_LLM_MAX_TOKENS` | `llm.max_tokens` | `8192` |
| `EB_LLM_TEMPERATURE` | `llm.temperature` | `0.1` |
| `EB_LLM_EXTRACTION_MAX_INPUT_TOKENS` | `llm.extraction_max_input_tokens` | `4000` |
| `EB_LLM_EXTRACTION_MAX_OUTPUT_TOKENS` | `llm.extraction_max_output_tokens` | `16384` |
| `EB_LLM_EXTRACTION_MAX_FACTS` | `llm.extraction_max_facts_per_batch` | `10` |
| `EB_LLM_SUMMARIZATION_MAX_OUTPUT_TOKENS` | `llm.summarization_max_output_tokens` | `200` |
| `EB_LLM_SUMMARIZATION_MIN_CHARS` | `llm.summarization_min_artifact_chars` | `500` |
| `EB_INGEST_BATCH_SIZE` | `llm.ingest_batch_size` | `6` |
| `EB_INGEST_BATCH_TIMEOUT` | `llm.ingest_batch_timeout_seconds` | `60.0` |
| `EB_INGEST_BUFFER_TTL` | `llm.ingest_buffer_ttl_seconds` | `300` |
| `EB_EXTRACTION_CONTEXT_FACTS` | `llm.extraction_context_facts` | `20` |
| `EB_EXTRACTION_CONTEXT_TTL` | `llm.extraction_context_ttl_seconds` | `3600` |
| `EB_RERANKER_ENDPOINT` | `reranker.endpoint` | `"http://localhost:1235"` |
| `EB_RERANKER_API_KEY` | `reranker.api_key` | `""` |
| `EB_RERANKER_MODEL` | `reranker.model` | `"Qwen/Qwen3-Reranker-4B"` |
| `EB_REDIS_URL` | `infra.redis_url` | `"redis://localhost:6379"` |
| `EB_OTEL_ENDPOINT` | `infra.otel_endpoint` | `None` |
| `EB_LOG_LEVEL` | `infra.log_level` | `"INFO"` |
| `EB_METRICS_TTL_SECONDS` | `infra.metrics_ttl_seconds` | `3600` |
| `EB_TRACE_OTEL_LOGS_ENABLED` | `infra.trace.otel_logs_enabled` | `"false"` |
| `EB_TRACE_MEMORY_MAX_EVENTS` | `infra.trace.memory_max_events` | `10000` |
| `EB_CLICKHOUSE_ENABLED` | `infra.clickhouse.enabled` | `"false"` |
| `EB_CLICKHOUSE_HOST` | `infra.clickhouse.host` | `"localhost"` |
| `EB_CLICKHOUSE_PORT` | `infra.clickhouse.port` | `8123` |
| `EB_CLICKHOUSE_DATABASE` | `infra.clickhouse.database` | `"otel"` |
| `EB_DEFAULT_PROFILE` | `default_profile` | `"coding"` |
| `EB_ENABLE_TRACE_LEDGER` | `enable_trace_ledger` | `"true"` |
| `EB_GUARDS_ENABLED` | `guards.enabled` | `"true"` |
| `EB_MAX_CONCURRENT_SESSIONS` | `max_concurrent_sessions` | `100` |
| `EB_EMBEDDING_CACHE_ENABLED` | `embedding_cache.enabled` | `"true"` |
| `EB_EMBEDDING_CACHE_TTL` | `embedding_cache.ttl_seconds` | `3600` |
| `EB_SCORING_SNAPSHOT_TTL` | `scoring.snapshot_ttl_seconds` | `300` |
| `EB_SESSION_GOALS_TTL` | `scoring.session_goals_ttl_seconds` | `86400` |
| `EB_COMPACTION_LLM_MODEL` | `compaction_llm.model` | `"gemini/gemini-2.5-flash-lite"` |
| `EB_COMPACTION_LLM_ENDPOINT` | `compaction_llm.endpoint` | (same as `llm.endpoint`) |
| `EB_COMPACTION_LLM_API_KEY` | `compaction_llm.api_key` | (falls back to `EB_LLM_API_KEY`) |
| `EB_HITL_CALLBACK_SECRET` | `hitl.callback_hmac_secret` | `""` |
| `EB_CONSOLIDATION_MIN_RETENTION_SECONDS` | `consolidation_min_retention_seconds` | `172800` |
| `EB_SUCCESSFUL_USE_ENABLED` | `successful_use.enabled` | `"false"` |
| `EB_SUCCESSFUL_USE_ENDPOINT` | `successful_use.endpoint` | `"http://localhost:8811/v1"` |
| `EB_SUCCESSFUL_USE_API_KEY` | `successful_use.api_key` | (falls back to `EB_LLM_API_KEY`) |
| `EB_SUCCESSFUL_USE_MODEL` | `successful_use.model` | `"gemini/gemini-2.5-flash-lite"` |
| `EB_SUCCESSFUL_USE_BATCH_SIZE` | `successful_use.batch_size` | `5` |
| `EB_DEV_CONSOLIDATION_AUTO_TRIGGER` | `consolidation.dev_auto_trigger_interval` | `"0"` |
| `EB_CONSOLIDATION_BATCH_SIZE` | `consolidation.batch_size` | `500` |
| `EB_ACTOR_ID` | (CLI only, not runtime config) | `""` |
| `EB_RUNTIME_URL` | (CLI only, not runtime config) | `"http://localhost:8420"` |

---

### 9. Server Config Resolution

After the F2/F3 unification there is exactly one path. When `elephantbroker serve [--config path.yaml]` is used:

1. `ElephantBrokerConfig.load(path)` is called. If `--config` is omitted, `load(None)` falls back to the packaged `elephantbroker/config/default.yaml`.
2. The YAML file is parsed and validated through Pydantic.
3. Every binding in `ENV_OVERRIDE_BINDINGS` (currently 72 entries) is applied on top — any env var that is set in `os.environ` overrides the corresponding YAML field.
4. `_apply_inheritance_fallbacks()` populates empty derived secrets (`compaction_llm.api_key`, `successful_use.api_key` ← `llm.api_key`; `llm.api_key` ← `cognee.embedding_api_key`) and the F7 endpoint inheritance (`compaction_llm.endpoint` ← `llm.endpoint`).
5. The merged dict is re-validated through `cls.model_validate()` and passed to `RuntimeContainer.from_config()`.

There is no curated subset of "override-eligible" env vars — all 72 `ENV_OVERRIDE_BINDINGS` entries apply on every load. Read the registry in `elephantbroker/schemas/config.py` for the authoritative list, which the inverse contract test in `tests/test_env_var_registry_completeness.py` keeps in sync with the schema and packaged YAML.

---

### 10. Bootstrap Workflow

Step-by-step guide for initializing a new ElephantBroker deployment:

**Prerequisites:** Runtime server is running, infrastructure (Neo4j, Qdrant, Redis) is up, no actors exist in the graph yet.

```bash
## 1. Start infrastructure
cd infrastructure && docker-compose up -d

## 2. Start the runtime (in another terminal)
elephantbroker serve --config config/default.yaml

## 3. Bootstrap the system
ebrun --runtime-url http://localhost:8420 bootstrap \
  --org-name "Acme Corp" \
  --team-name "Engineering" \
  --admin-name "Admin" \
  --admin-authority 90 \
  --admin-handles "email:admin@acme.com"

## Output:
##   Organization created: org_id=<uuid>
##   Team created: team_id=<uuid>
##   Admin actor created: actor_id=<uuid> (authority_level=90)
##   Config saved: actor-id=<uuid>, runtime-url=http://localhost:8420

## 4. Verify config was saved
ebrun config show
## { "actor_id": "<uuid>", "runtime_url": "http://localhost:8420" }

## 5. Create additional resources (actor-id is now auto-resolved from config)
ebrun actor create --display-name "CI Bot" --type worker_agent --authority-level 10 --org-id <org-uuid>
ebrun team create --name "QA" --org-id <org-uuid>
ebrun goal create --title "Ship v2.0" --scope organization --org-id <org-uuid>
```

The bootstrap command checks `GET /admin/bootstrap-status` first and aborts if actors already exist. It then creates the org, team, and admin actor sequentially, and saves the admin's `actor_id` and the `runtime_url` to `~/.elephantbroker/config.json` so subsequent `ebrun` commands automatically authenticate.

---

### 11. Scenario Test Runner

Located at `tests/scenarios/runner.py`, run as a Python module (not installed as a console script).

```
python -m tests.scenarios.runner [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--scenario` | None | Run a single scenario by name |
| `--base-url` | `http://localhost:8420` | Base URL of the ElephantBroker API |
| `--max-phase` | auto-detected | Only run scenarios with `required_phase <= value` |
| `--detect-phase` | `false` | Print detected phase and exit |
| `--json` | `false` | Output results as JSON |
| `--live` | `false` | Run in live mode against a real gateway |
| `--gateway-url` | None | Gateway URL (required for live mode) |
| `--gateway-token` | None | Auth token (required for live mode) |
| `--agent-id` | `"main"` | Agent ID for live mode |

**Available scenarios:** `basic_memory`, `multi_turn_memory`, `goal_driven`, `context_lifecycle`, `procedure_execution`, `guard_check`, `subagent_lifecycle`.

**Examples:**

```bash
## Run all scenarios against local server
python -m tests.scenarios.runner

## Run a single scenario
python -m tests.scenarios.runner --scenario basic_memory

## Run with JSON output
python -m tests.scenarios.runner --json

## Live mode against a real gateway
python -m tests.scenarios.runner --live \
  --gateway-url "http://10.10.0.51:18789" \
  --gateway-token "<token>" \
  --base-url "http://10.10.0.10:8420"
```

---

**Source files:**
- `elephantbroker/cli.py` -- ebrun CLI
- `elephantbroker/server.py` -- elephantbroker server CLI
- `elephantbroker/schemas/config.py` -- all config models, `ENV_OVERRIDE_BINDINGS` registry, and `ElephantBrokerConfig.load()` / internal `from_yaml()` reader
- `elephantbroker/config/default.yaml` -- default YAML config
- `elephantbroker/schemas/consolidation.py` -- ConsolidationConfig model
- `tests/scenarios/runner.py` -- scenario test runner CLI


---


## ElephantBroker Observability Configuration

### 1. Prometheus Metrics

All metrics are defined in `elephantbroker/runtime/metrics.py`. Metrics are conditionally available -- if `prometheus_client` is not installed, all metrics become no-ops via `METRICS_AVAILABLE = False`. Every metric includes a `gateway_id` label for multi-gateway isolation. Access via `MetricsContext(gateway_id)` which auto-injects the label.

**Endpoint:** `GET /metrics` (served by `elephantbroker/api/routes/metrics.py`)

#### Core Memory & Storage (Phase 3-4)

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `eb_memory_store_total` | Counter | `gateway_id`, `operation`, `status` | Total memory operations |
| `eb_memory_store_duration_seconds` | Histogram | `gateway_id`, `operation` | Operation latency |
| `eb_facts_stored_total` | Counter | `gateway_id`, `memory_class`, `profile_name` | Facts stored |
| `eb_facts_superseded_total` | Counter | `gateway_id`, `profile_name` | Facts superseded |
| `eb_dedup_checks_total` | Counter | `gateway_id`, `result` | Dedup outcomes |
| `eb_retrieval_total` | Counter | `gateway_id`, `auto_recall`, `profile_name` | Retrieval ops |
| `eb_retrieval_duration_seconds` | Histogram | `gateway_id`, `auto_recall`, `profile_name` | Retrieval latency |
| `eb_pipeline_runs_total` | Counter | `gateway_id`, `pipeline`, `status` | Pipeline runs |
| `eb_pipeline_duration_seconds` | Histogram | `gateway_id`, `pipeline` | Pipeline latency |
| `eb_llm_calls_total` | Counter | `gateway_id`, `operation`, `status`, `model` | LLM calls |
| `eb_llm_duration_seconds` | Histogram | `gateway_id`, `operation`, `model` | LLM latency |
| `eb_llm_tokens_used` | Counter | `gateway_id`, `direction`, `model` | Token consumption |
| `eb_ingest_buffer_flushes_total` | Counter | `gateway_id`, `trigger` | Buffer flushes |
| `eb_ingest_gate_skips_total` | Counter | `gateway_id`, `reason` | Ingest gate skips (FULL mode — extraction via context engine) |
| `eb_session_active` | Gauge | `gateway_id`, `profile_name` | Active sessions |
| `eb_edges_created_total` | Counter | `gateway_id`, `edge_type` | Graph edges created |
| `eb_edges_failed_total` | Counter | `gateway_id`, `edge_type` | Failed edges |
| `eb_cognify_runs_total` | Counter | `gateway_id`, `status` | Cognify runs |
| `eb_cognify_duration_seconds` | Histogram | `gateway_id` | Cognify latency |
| `eb_gdpr_deletes_total` | Counter | `gateway_id` | GDPR deletions |
| `eb_backend_health` | Gauge | `gateway_id`, `component` | Backend health 1=ok 0=down |
| `eb_degraded_operations_total` | Counter | `gateway_id`, `component`, `operation` | Degraded ops |

#### Working Set & Scoring (Phase 5)

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `eb_working_set_builds_total` | Counter | `gateway_id`, `profile_name`, `status` | Working set builds |
| `eb_working_set_build_duration_seconds` | Histogram | `gateway_id`, `profile_name` | Build latency |
| `eb_working_set_candidates` | Histogram | `gateway_id`, `source_type` | Candidates generated |
| `eb_working_set_selected` | Histogram | `gateway_id` | Items selected |
| `eb_working_set_tokens_used` | Histogram | `gateway_id` | Tokens used per build |
| `eb_working_set_must_inject_total` | Counter | `gateway_id` | Must-inject items |
| `eb_rerank_calls_total` | Counter | `gateway_id`, `status` | Rerank calls |
| `eb_rerank_duration_seconds` | Histogram | `gateway_id`, `stage` | Rerank latency |
| `eb_rerank_fallbacks_total` | Counter | `gateway_id` | Rerank fallbacks |
| `eb_rerank_candidates_in` | Histogram | `gateway_id` | Candidates submitted to reranker |
| `eb_rerank_candidates_out` | Histogram | `gateway_id` | Candidates returned from reranker |
| `eb_embedding_cache_total` | Counter | `gateway_id`, `result` | Embedding cache hits/misses |
| `eb_embedding_cache_batch_size` | Histogram | `gateway_id` | Batch sizes |
| `eb_embedding_cache_latency_seconds` | Histogram | `gateway_id`, `operation` | Cache operation latency |
| `eb_goal_hints_total` | Counter | `gateway_id`, `hint_type` | Goal hints processed |
| `eb_goal_refinement_calls_total` | Counter | `gateway_id` | Goal refinement LLM calls |
| `eb_goal_refinement_duration_seconds` | Histogram | `gateway_id` | Refinement latency |
| `eb_session_goals_count` | Gauge | `gateway_id` | Active session goals |
| `eb_session_goals_flushed_total` | Counter | `gateway_id` | Goals flushed to Cognee |
| `eb_subgoals_created_total` | Counter | `gateway_id` | Sub-goals created |
| `eb_subgoals_dedup_skipped_total` | Counter | `gateway_id` | Sub-goals skipped (dedup) |
| `eb_session_goals_tool_calls_total` | Counter | `gateway_id`, `tool` | Session goals tool calls |
| `eb_procedure_qualified_total` | Counter | `gateway_id` | Procedures qualified for context |
| `eb_procedure_activated_total` | Counter | `gateway_id` | Procedures activated |
| `eb_procedure_step_completed_total` | Counter | `gateway_id` | Procedure steps completed |
| `eb_procedure_proof_submitted_total` | Counter | `gateway_id`, `proof_type` | Proofs submitted |
| `eb_procedure_completed_total` | Counter | `gateway_id` | Procedures completed |
| `eb_procedure_tool_calls_total` | Counter | `gateway_id`, `tool` | Procedure tool calls |

#### Context & Compaction (Phase 6)

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `eb_compaction_triggered_total` | Counter | `gateway_id`, `cadence`, `trigger` | Compaction triggers |
| `eb_compaction_tokens` | Histogram | `gateway_id`, `phase` | Compaction tokens |
| `eb_compaction_classification_total` | Counter | `gateway_id`, `classification` | Message classifications |
| `eb_compaction_llm_calls_total` | Counter | `gateway_id` | Compaction LLM calls |
| `eb_assembly_tokens_used` | Histogram | `gateway_id`, `profile_name` | Assembly tokens |
| `eb_assembly_block_tokens` | Histogram | `gateway_id`, `block` | Block tokens |
| `eb_budget_resolution_tokens` | Histogram | `gateway_id`, `source` | Budget resolution |
| `eb_tool_replacements_total` | Counter | `gateway_id`, `tool_name` | Tool replacements |
| `eb_tool_tokens_saved_total` | Counter | `gateway_id` | Tool tokens saved |
| `eb_injection_referenced_total` | Counter | `gateway_id`, `category`, `memory_class`, `source_type` | Items referenced |
| `eb_injection_ignored_total` | Counter | `gateway_id`, `category`, `memory_class`, `source_type` | Items ignored |
| `eb_subagent_spawns_total` | Counter | `gateway_id` | Subagent spawns |
| `eb_subagent_packet_tokens` | Histogram | `gateway_id` | Subagent packet tokens |
| `eb_lifecycle_calls_total` | Counter | `gateway_id`, `method`, `profile_name` | Lifecycle calls |
| `eb_lifecycle_duration_seconds` | Histogram | `gateway_id`, `method`, `profile_name` | Lifecycle latency |
| `eb_lifecycle_errors_total` | Counter | `gateway_id`, `method`, `error_type` | Lifecycle errors |
| `eb_successful_use_updates_total` | Counter | `gateway_id`, `method` | Successful use updates |
| `eb_successful_use_jaccard_score` | Histogram | `gateway_id` | Jaccard scores |
| `eb_context_window_reported_total` | Counter | `gateway_id`, `provider`, `model` | Context window reports |
| `eb_token_usage_input_tokens` | Histogram | `gateway_id` | Token usage input |
| `eb_token_usage_output_tokens` | Histogram | `gateway_id` | Token usage output |
| `eb_fact_attribution_total` | Counter | `gateway_id`, `role` | Fact attributions |

#### Async Injection Analysis (Amendment 6.2)

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `eb_async_analysis_calls_total` | Counter | `gateway_id` | Async analyzer invocations |
| `eb_async_analysis_matches_total` | Counter | `gateway_id` | Items exceeding similarity threshold |
| `eb_async_analysis_similarity_max` | Histogram | `gateway_id` | Max similarity per item |
| `eb_async_analysis_items_processed` | Histogram | `gateway_id` | Items processed per batch |

#### Session TTL (Amendment 6.1)

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `eb_session_ttl_touch_total` | Counter | `gateway_id` | Session TTL refresh operations |
| `eb_session_ttl_touch_keys` | Histogram | `gateway_id` | Keys refreshed per touch operation (buckets: 0-10) |

#### Guards & Verification (Phase 7)

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `eb_guard_checks_total` | Counter | `gateway_id`, `outcome` | Guard checks |
| `eb_guard_check_duration_seconds` | Histogram | `gateway_id` | Guard check latency |
| `eb_guard_layer_triggers_total` | Counter | `gateway_id`, `layer` | Guard layer triggers |
| `eb_guard_near_misses_total` | Counter | `gateway_id` | Near misses |
| `eb_guard_reinjections_total` | Counter | `gateway_id` | Constraint reinjections |
| `eb_guard_llm_escalations_total` | Counter | `gateway_id` | LLM escalations |
| `eb_autonomy_classifications_total` | Counter | `gateway_id`, `domain`, `level` | Domain classifications |
| `eb_autonomy_domain_tier_total` | Counter | `gateway_id`, `tier` | Classification tier used |
| `eb_autonomy_hard_stops_total` | Counter | `gateway_id`, `domain` | Hard stops |
| `eb_approval_requests_total` | Counter | `gateway_id`, `domain` | Approval requests |
| `eb_guard_bm25_score_max` | Histogram | `gateway_id` | Top BM25 scores |
| `eb_guard_semantic_score_max` | Histogram | `gateway_id` | Top semantic scores |
| `eb_guard_bm25_short_circuit_total` | Counter | `gateway_id` | BM25 definitive (skipped embedding) |
| `eb_verification_runs_total` | Counter | `gateway_id`, `result` | Verification pipeline runs |
| `eb_completion_checks_total` | Counter | `gateway_id`, `result` | Completion gate checks |

#### Profile, Authority & Admin (Phase 8)

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `eb_profile_resolve_total` | Counter | `gateway_id`, `profile_name`, `has_org_override` | Profile resolutions |
| `eb_profile_resolve_duration_seconds` | Histogram | `gateway_id` | Profile resolution latency |
| `eb_profile_cache_total` | Counter | `gateway_id`, `result` | Profile cache ops |
| `eb_authority_checks_total` | Counter | `gateway_id`, `action`, `result` | Authority checks |
| `eb_admin_ops_total` | Counter | `gateway_id`, `operation`, `status` | Admin API ops |
| `eb_org_team_edges_total` | Counter | `gateway_id`, `edge_type`, `operation` | Org/team edge ops |
| `eb_goal_scope_filter_total` | Counter | `gateway_id`, `scope` | Goal scope filter |
| `eb_goal_scope_filter_duration_seconds` | Histogram | `gateway_id` | Goal scope filter latency |
| `eb_handle_resolution_total` | Counter | `gateway_id`, `result` | Handle lookups |
| `eb_actor_merge_total` | Counter | `gateway_id`, `status` | Actor merges |
| `eb_bootstrap_mode_active` | Gauge | `gateway_id` | Bootstrap mode (1=active, 0=inactive) |

#### Consolidation (Phase 9)

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `eb_consolidation_runs_total` | Counter | `gateway_id`, `status` | Consolidation runs |
| `eb_consolidation_duration_seconds` | Histogram | `gateway_id` | Total consolidation duration |
| `eb_consolidation_stage_duration_seconds` | Histogram | `gateway_id`, `stage` | Per-stage duration |
| `eb_consolidation_facts_processed_total` | Counter | `gateway_id`, `stage` | Facts processed per stage |
| `eb_consolidation_facts_affected_total` | Counter | `gateway_id`, `stage`, `action` | Facts affected |
| `eb_consolidation_llm_calls_total` | Counter | `gateway_id`, `stage` | LLM calls in consolidation |
| `eb_consolidation_suggestions_total` | Counter | `gateway_id`, `status` | Procedure suggestions |
| `eb_scoring_tuner_adjustments_total` | Counter | `gateway_id`, `dimension` | Weight adjustments applied |
| `eb_scoring_tuner_adjustment_magnitude` | Histogram | `gateway_id`, `dimension` | Delta magnitude |

**Total: 99 registered Prometheus metrics.**

#### MetricsContext

`MetricsContext(gateway_id)` is a scoped wrapper class that auto-injects `gateway_id` on every metric call. Instantiated once per `RuntimeContainer` at boot. Modules receive it via DI and call methods like `self._metrics.inc_store("create", "ok")` without ever specifying the gateway ID.

---

### 2. OTEL Configuration

OTEL tracing is configured in `elephantbroker/runtime/observability.py`.

#### Enabling OTEL

Set the environment variable `EB_OTEL_ENDPOINT` to the OTEL Collector gRPC endpoint (e.g., `http://localhost:4317`). Without this variable, tracing is initialized but spans are not exported.

```bash
EB_OTEL_ENDPOINT=http://localhost:4317
```

#### TracerProvider setup (`setup_tracing()`)

Called from `RuntimeContainer.from_config()` during boot.

**Resource attributes:**
| Attribute | Value |
|---|---|
| `service.name` | `"elephantbroker"` |
| `gateway.id` | Value of `config.gateway.gateway_id` (default: `"local"`) |

**Span processor:** `SimpleSpanProcessor` with `OTLPSpanExporter(endpoint=config.otel_endpoint)`.

**Dependency:** `opentelemetry-exporter-otlp-proto-grpc` (optional -- logs a warning if not installed).

#### FastAPI auto-instrumentation

In `create_app()` (`elephantbroker/api/app.py`), `FastAPIInstrumentor.instrument_app(app)` is called if `opentelemetry.instrumentation.fastapi` is importable. This auto-creates spans for every HTTP request.

#### Span naming

Spans are named using the Python `qualname` of the decorated function: `{module}.{ClassName}.{method_name}` or `{module}.{function_name}`.

#### `@traced` decorator

Defined in `observability.py`. Wraps async functions in OTEL spans. Extracts the following kwargs into span attributes when present: `session_id`, `gateway_id`, `agent_key`, `agent_id`, `session_key`. Sets span status to `ERROR` on exception and records the exception.

#### `get_tracer(module_name)`

Returns a tracer scoped to `elephantbroker.{module_name}`.

---

### 3. Trace Event Types

Defined in `elephantbroker/schemas/trace.py` as `TraceEventType(StrEnum)`.
Descriptions in `elephantbroker/api/routes/trace_event_descriptions.py` as `TRACE_EVENT_DESCRIPTIONS` dict.
Accessible via `GET /trace/event-types`.

#### All 51 TraceEventType values

| # | Value | Description |
|---|---|---|
| 1 | `input_received` | User or agent message received for processing |
| 2 | `retrieval_performed` | Memory search executed (auto-recall or explicit) |
| 3 | `retrieval_source_result` | Individual retrieval source returned results (structural/keyword/semantic/graph/artifact) |
| 4 | `tool_invoked` | Agent invoked a tool (memory_search, memory_store, etc.) |
| 5 | `artifact_created` | Tool artifact stored (code, file, URL, etc.) |
| 6 | `claim_made` | Agent made a verifiable claim |
| 7 | `claim_verified` | Claim verification completed (accepted/rejected/pending) |
| 8 | `procedure_step_passed` | Procedure step completed successfully |
| 9 | `procedure_step_failed` | Procedure step failed validation |
| 10 | `guard_triggered` | Red-line guard triggered -- action blocked or requires approval |
| 11 | `compaction_action` | Context compaction performed (rule-classify or LLM-summarize) |
| 12 | `subagent_spawned` | Child agent spawned from parent session |
| 13 | `subagent_ended` | Child agent completed and results returned to parent |
| 14 | `context_assembled` | Context window assembled with token budget |
| 15 | `scoring_completed` | Working set scoring completed -- payload contains per-item dimensions |
| 16 | `fact_extracted` | Fact extracted from conversation by LLM pipeline |
| 17 | `fact_superseded` | Existing fact superseded by newer extraction |
| 18 | `memory_class_assigned` | Fact classified into memory class (EPISODIC/SEMANTIC/PROCEDURAL/POLICY) |
| 19 | `dedup_triggered` | Duplicate detection triggered during fact storage |
| 20 | `session_boundary` | Session ended -- buffer flushed, goals persisted |
| 21 | `ingest_buffer_flush` | Ingest buffer flushed to pipeline |
| 22 | `gdpr_delete` | GDPR deletion performed on fact/actor data |
| 23 | `cognee_cognify_completed` | Cognee cognify pipeline completed (chunking, entity extraction, triplet embedding) |
| 24 | `degraded_operation` | Operation ran in degraded mode due to backend failure -- check payload.error |
| 25 | `guard_passed` | Guard check passed -- no constraint violation detected |
| 26 | `guard_near_miss` | Guard check passed but was close to threshold -- logged for review |
| 27 | `constraint_reinjected` | Red-line constraint reinjected into system prompt |
| 28 | `procedure_completion_checked` | Procedure completion validation ran (all steps + proofs checked) |
| 29 | `bootstrap_completed` | Session bootstrap completed -- context initialized |
| 30 | `after_turn_completed` | After-turn processing completed (successful-use tracking, cleanup) |
| 31 | `token_usage_reported` | Token usage reported by agent (input/output tokens) |
| 32 | `context_window_reported` | Context window size reported by agent |
| 33 | `successful_use_tracked` | Fact successful-use tracking updated (S1 quote, S2 tool, S6 ignored) |
| 34 | `subagent_parent_mapped` | Parent-child session mapping created for subagent |
| 35 | `profile_resolved` | Profile resolved for session (base + named + org override) |
| 36 | `org_created` | Organization entity created in graph |
| 37 | `team_created` | Team entity created in graph |
| 38 | `member_added` | Actor added as member of organization or team |
| 39 | `member_removed` | Actor removed from organization or team membership |
| 40 | `actor_merged` | Two actor records merged into one |
| 41 | `authority_check_failed` | Authority rule check failed -- insufficient privileges |
| 42 | `handle_resolved` | Platform-qualified handle resolved to actor |
| 43 | `persistent_goal_created` | Persistent goal created with scope (GLOBAL/ORGANIZATION/TEAM/ACTOR) |
| 44 | `bootstrap_org_created` | Organization bootstrapped during first-run initialization |
| 45 | `session_goal_created` | Session goal created -- new goal tracked for the session |
| 46 | `session_goal_updated` | Session goal updated (status/priority/target change) |
| 47 | `session_goal_blocker_added` | Session goal blocker recorded -- reason why the goal is stalled |
| 48 | `session_goal_progress` | Session goal progress note -- incremental progress recorded |
| 49 | `consolidation_started` | Consolidation (sleep) pipeline started for gateway |
| 50 | `consolidation_stage_completed` | Single consolidation stage completed (1 of 9) |
| 51 | `consolidation_completed` | Full consolidation pipeline completed (all 9 stages) |

---

### 4. TraceLedger Configuration

Defined in `elephantbroker/runtime/trace/ledger.py`.

#### TraceConfig schema (`schemas/config.py`)

| Field | Type | Default | Env Var | Description |
|---|---|---|---|---|
| `memory_max_events` | int | 10,000 | `EB_TRACE_MEMORY_MAX_EVENTS` | Max in-memory events (circular buffer) |
| `memory_ttl_seconds` | int | 3600 | -- | TTL for in-memory events (seconds) |
| `otel_logs_enabled` | bool | false | `EB_TRACE_OTEL_LOGS_ENABLED` | Enable OTEL log export to ClickHouse |

#### Behavior

- **Circular buffer eviction:** When `memory_max_events` is exceeded, the oldest events are popped. Events older than `memory_ttl_seconds` are also evicted on each append (lazy GC).
- **Gateway auto-enrichment:** When constructed with `gateway_id`, any appended event without a `gateway_id` gets it set automatically.
- **OTEL log bridge:** When `otel_logs_enabled=true` and `EB_OTEL_ENDPOINT` is set, each appended event is also emitted as an OTEL LogRecord (JSON body with `event_type`, `session_id`, `session_key`, `gateway_id`, `agent_key` as attributes). Failures are silently swallowed to never block trace recording.

#### TraceLedger constructor

```python
TraceLedger(gateway_id="local", otel_logger=None, config=TraceConfig())
```

Created during `RuntimeContainer.from_config()` with `setup_otel_logging()` providing the OTEL logger.

---

### 5. ClickHouse Bridge

#### Data flow

```
TraceLedger.append_event()
  -> _emit_otel_log()  (LogRecord with JSON body + attributes)
  -> OTEL LoggerProvider (BatchLogRecordProcessor)
  -> OTLPLogExporter (gRPC to OTEL Collector on port 4317)
  -> OTEL Collector (otel-collector-config.yaml)
  -> ClickHouse exporter (tcp://clickhouse:9000)
  -> otel.otel_logs table (auto-created)
```

#### OTEL Collector config (`infrastructure/otel-collector-config.yaml`)

- **Receiver:** OTLP gRPC on `0.0.0.0:4317`
- **Trace pipeline:** OTLP receiver -> OTLP exporter to Jaeger (`jaeger:4317`)
- **Logs pipeline:** OTLP receiver -> ClickHouse exporter (`tcp://clickhouse:9000`, database `otel`, table `otel_logs`, TTL 72h, `create_schema: true`)

#### ClickHouseConfig schema

| Field | Type | Default | Env Var | Description |
|---|---|---|---|---|
| `enabled` | bool | false | `EB_CLICKHOUSE_ENABLED` | Enable ClickHouse client |
| `host` | str | `"localhost"` | `EB_CLICKHOUSE_HOST` | ClickHouse host |
| `port` | int | 8123 | `EB_CLICKHOUSE_PORT` | ClickHouse HTTP port |
| `database` | str | `"otel"` | `EB_CLICKHOUSE_DATABASE` | Database name |
| `logs_table` | str | `"otel_logs"` | -- | Table name for log records |

#### OtelTraceQueryClient

Defined in `elephantbroker/runtime/consolidation/otel_trace_query_client.py`. Used by consolidation Stage 7 (refine_procedures) to find repeated tool call sequences across sessions. Queries ClickHouse's `otel_logs` table filtering by `LogAttributes['event_type'] = 'tool_invoked'` and `LogAttributes['gateway_id']`. Uses `clickhouse-connect` Python client. Gracefully degrades: returns empty results if ClickHouse is not configured.

#### Docker infrastructure

In `infrastructure/docker-compose.yml`, the observability stack requires the `observability` Docker Compose profile:

```bash
docker compose --profile observability up -d
```

Services:
| Service | Image | Ports | Purpose |
|---|---|---|---|
| `otel-collector` | `otel/opentelemetry-collector-contrib:latest` | 4317 (gRPC) | Receives OTLP traces+logs, routes to Jaeger and ClickHouse |
| `clickhouse` | `clickhouse/clickhouse-server:latest` | 8123, 9000 | Durable log storage for cross-session analytics |
| `jaeger` | `jaegertracing/all-in-one:latest` | 16686 (UI), 14250 | Trace visualization |
| `grafana` | `grafana/grafana:latest` | 13000 | Dashboards |

---

### 6. Logging Levels

#### VERBOSE level (15)

Defined in `elephantbroker/runtime/observability.py`.

A custom logging level between DEBUG (10) and INFO (20). Registered via `register_verbose_level()` which is called at the start of `RuntimeContainer.from_config()`.

After registration, loggers can use `logger.verbose("message")`.

#### Configuration

| Env Var | Default | Effect |
|---|---|---|
| `EB_LOG_LEVEL` | `"INFO"` | Python log level. Accepts `DEBUG`, `VERBOSE`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |

In `RuntimeContainer.from_config()`:
```python
level_name = config.infra.log_level.upper()
log_level = 15 if level_name == "VERBOSE" else getattr(logging, level_name, logging.INFO)
logging.basicConfig(level=log_level)
```

#### Uvicorn mapping

Uvicorn does not know about the custom VERBOSE level. In the server entry point:
```python
uvicorn_level = "info" if log_level.lower() == "verbose" else log_level
```

So `--log-level verbose` maps to uvicorn's `info` level while setting the application root logger to 15 (VERBOSE).

---

### 7. GatewayLoggerAdapter

Defined in `elephantbroker/runtime/observability.py`.

```python
class GatewayLoggerAdapter(logging.LoggerAdapter):
    """Prepends [gateway_id][agent_key] to all log messages."""
```

#### Usage

Modules create their logger adapter with gateway context:

```python
from elephantbroker.runtime.observability import GatewayLoggerAdapter
self._log = GatewayLoggerAdapter(
    logging.getLogger("elephantbroker.runtime.some_module"),
    {"gateway_id": gateway_id, "agent_key": agent_key},
)
self._log.info("Operation completed")
## Output: [gw-prod-assistant][gw-prod-assistant:main:main] Operation completed
```

#### Behavior

- Prepends `[{gateway_id}]` if `gateway_id` is set in `extra`
- Appends `[{agent_key}]` if `agent_key` is set in `extra`
- If neither is set, no prefix is added
- Used across all Phase 6+ modules (ContextLifecycle, CompactionEngine, ContextAssembler, AsyncInjectionAnalyzer, etc.)

#### GatewayIdentityMiddleware

Defined in `elephantbroker/api/middleware/gateway.py`. Extracts 4+1 HTTP headers on every request:

| Header | `request.state` attribute | Fallback |
|---|---|---|
| `X-EB-Gateway-ID` | `gateway_id` | `default_gateway_id` (from config, typically `"local"`) |
| `X-EB-Agent-Key` | `agent_key` | `""` |
| `X-EB-Agent-ID` | `agent_id` | `""` |
| `X-EB-Session-Key` | `session_key` | `""` |
| `X-EB-Actor-Id` | `actor_id` | `""` |

API routes read these from `request.state` and stamp them onto schema objects before passing to runtime modules.


---


## ElephantBroker Hardcoded Constants Audit

### 1. schemas/config.py -- Configuration Defaults (env-overridable)

These are the canonical defaults set in the Pydantic model fields of `ElephantBrokerConfig` (and its nested config models). Every field is overridable either via the corresponding `EB_*` env var (if it has a binding in `ENV_OVERRIDE_BINDINGS`) or via a YAML field. They are listed here because their default values directly affect runtime behavior.

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| config.py:11 | `"bolt://localhost:7687"` | Neo4j connection URI | Dev default, `EB_NEO4J_URI` overrides | None -- connection fails if wrong |
| config.py:13 | `"elephant_dev"` | Neo4j password | Dev default, `EB_NEO4J_PASSWORD` overrides | Security risk if deployed with default |
| config.py:14 | `"http://localhost:6333"` | Qdrant URL | Dev default, `EB_QDRANT_URL` overrides | None -- connection fails |
| config.py:17 | `"gemini/text-embedding-004"` | Embedding model name | LiteLLM-routed; `openai/` provider prefix optional, depends on model | Wrong model = wrong dimensions |
| config.py:20 | `768` | Embedding vector dimensions | Must match the embedding model's output dim. `EB_EMBEDDING_DIMENSIONS` overrides | Mismatch = Qdrant errors |
| config.py:29 | `"openai/gemini/gemini-2.5-pro"` | LLM model for extraction | `EB_LLM_MODEL` overrides; `openai/` prefix is REQUIRED by Cognee | Without prefix, Cognee hangs at startup |
| config.py:28 | `8192` | LLM max output tokens | `EB_LLM_MAX_TOKENS` overrides | Too low = truncated responses |
| config.py:29 | `0.1` | LLM temperature | `EB_LLM_TEMPERATURE` overrides | >0.5 = non-deterministic extraction |
| config.py:30 | `4000` | Max input tokens for fact extraction prompt | `EB_LLM_EXTRACTION_MAX_INPUT_TOKENS` overrides | Too low = truncated context |
| config.py:31 | `16384` | Max output tokens for extraction response | `EB_LLM_EXTRACTION_MAX_OUTPUT_TOKENS` overrides | Too low = incomplete facts |
| config.py:32 | `10` | Max facts per extraction batch | `EB_LLM_EXTRACTION_MAX_FACTS` overrides | Too high = noisy facts; too low = missed facts |
| config.py:33 | `200` | Max tokens for artifact summarization LLM output | `EB_LLM_SUMMARIZATION_MAX_OUTPUT_TOKENS` overrides | Too low = unhelpful summaries |
| config.py:34 | `500` | Min artifact chars before LLM summarization triggers | `EB_LLM_SUMMARIZATION_MIN_CHARS` overrides | Too high = short artifacts never summarized |
| config.py:35 | `6` | Ingest batch size (messages before flush) | `EB_INGEST_BATCH_SIZE` overrides | Too high = delayed fact extraction |
| config.py:36 | `60.0` | Ingest batch timeout (seconds) | `EB_INGEST_BATCH_TIMEOUT` overrides | Too long = stale facts |
| config.py:37 | `300` | Ingest buffer TTL (seconds) | `EB_INGEST_BUFFER_TTL` overrides | Too short = lost buffered messages |
| config.py:38 | `20` | Recent facts kept for extraction context | `EB_EXTRACTION_CONTEXT_FACTS` overrides | Too few = poor supersession detection |
| config.py:39 | `3600` | Extraction context TTL (seconds) | `EB_EXTRACTION_CONTEXT_TTL` overrides | Too short = context loss between turns |
| config.py:44 | `"http://localhost:1235"` | Reranker endpoint | `EB_RERANKER_ENDPOINT` overrides | None -- reranker disabled on failure |
| config.py:46 | `"Qwen/Qwen3-Reranker-4B"` | Reranker model | `EB_RERANKER_MODEL` overrides | Wrong model = bad reranking |
| config.py:48 | `10.0` | Reranker timeout (seconds) | Configurable via `RerankerConfig` | Too low = timeouts; too high = slow responses |
| config.py:49 | `32` | Reranker batch size | Configurable via `RerankerConfig` | Too high = memory pressure on reranker |
| config.py:50 | `100` | Reranker max documents | Configurable via `RerankerConfig` | Too high = slow; too low = missed candidates |
| config.py:57 | `10_000` | Trace ledger max in-memory events | `EB_TRACE_MEMORY_MAX_EVENTS` overrides | Too high = memory bloat; too low = lost trace events |
| config.py:58 | `3600` | Trace ledger memory TTL (seconds) | Configurable via `TraceConfig` | Too short = recent events evicted |
| config.py:73 | `"redis://localhost:6379"` | Redis URL | `EB_REDIS_URL` overrides | None -- connection fails |
| config.py:76 | `3600` | Metrics TTL (seconds) | `EB_METRICS_TTL_SECONDS` overrides | Too short = stale gauges |
| config.py:87 | `3600` | Embedding cache TTL (seconds) | `EB_EMBEDDING_CACHE_TTL` overrides | Too short = cache thrashing; too long = stale embeddings |
| config.py:88 | `"eb:emb_cache"` | Embedding cache Redis key prefix | Configurable via `EmbeddingCacheConfig` | Collision if changed without migration |
| config.py:93 | `0.5` | Neutral use-prior (no usage history) | Configurable via `ScoringConfig` | 0.0 = penalizes new facts; 1.0 = over-promotes new facts |
| config.py:94 | `80` | Max candidates after cheap prune | Configurable via `ScoringConfig` | Too low = lost good candidates |
| config.py:95 | `0.6` | Semantic blend weight (reranking stage 2) | Configurable via `ScoringConfig` | 0.0 = no semantic; 1.0 = only semantic |
| config.py:96 | `0.95` | Merge similarity threshold (dedup) | Configurable via `ScoringConfig` | Too low = over-merging; too high = duplicates |
| config.py:97 | `300` | Scoring snapshot TTL (seconds) | `EB_SCORING_SNAPSHOT_TTL` overrides | Too short = frequent recompute |
| config.py:98 | `86400` | Session goals TTL (seconds, 24h) | `EB_SESSION_GOALS_TTL` overrides | Too short = goals expire mid-session |
| config.py:104 | `1.0` | Verification multiplier: supervisor_verified | Configurable via `VerificationMultipliers` | >1.0 = inflation; <0.5 = suppressed |
| config.py:105 | `0.9` | Verification multiplier: tool_supported | Configurable via `VerificationMultipliers` | Same |
| config.py:106 | `0.7` | Verification multiplier: self_supported | Configurable via `VerificationMultipliers` | Same |
| config.py:107 | `0.5` | Verification multiplier: unverified | Configurable via `VerificationMultipliers` | Same |
| config.py:108 | `0.8` | Verification multiplier: no_claim | Configurable via `VerificationMultipliers` | Same |
| config.py:113 | `1.0` | Supersession penalty | Configurable via `ConflictDetectionConfig` | 0.0 = no penalty; >1.0 = excessive |
| config.py:114 | `0.9` | Contradiction edge penalty | Configurable via `ConflictDetectionConfig` | Same |
| config.py:115 | `0.7` | Layer 2 (similarity-based) contradiction penalty | Configurable via `ConflictDetectionConfig` | Same |
| config.py:117 | `0.9` | Contradiction similarity threshold | Configurable via `ConflictDetectionConfig` | Too low = false contradictions |
| config.py:118 | `0.3` | Confidence gap threshold for contradictions | Configurable via `ConflictDetectionConfig` | Too low = false contradictions |
| config.py:119 | `0.85` | Redundancy similarity threshold | Configurable via `ConflictDetectionConfig` | Too low = over-penalizes related facts |
| config.py:133 | `5` | Successful-use batch size (turns) | `EB_SUCCESSFUL_USE_BATCH_SIZE` overrides | Too high = delayed feedback |
| config.py:134 | `120.0` | Successful-use batch timeout (seconds) | Configurable via `SuccessfulUseConfig` | Too short = frequent LLM calls |
| config.py:135 | `20` | Feed last N facts to successful-use evaluator | Configurable via `SuccessfulUseConfig` | Too high = expensive LLM calls |
| config.py:136 | `0.7` | Min confidence for successful-use marking | Configurable via `SuccessfulUseConfig` | Too low = false positives |
| config.py:143 | `5` | Max session goals injected into extraction | Configurable via `GoalInjectionConfig` | Too many = prompt bloat |
| config.py:144 | `3` | Max persistent goals injected into extraction | Configurable via `GoalInjectionConfig` | Same |
| config.py:153 | `10` | Max subgoals per session | Configurable via `GoalRefinementConfig` | Too many = goal explosion |
| config.py:154 | `6` | Recent messages fed to goal refinement LLM | Configurable via `GoalRefinementConfig` | Too few = poor refinement |
| config.py:156 | `0.1` | Goal progress confidence delta | Configurable via `GoalRefinementConfig` | Too high = under-reporting progress |
| config.py:157 | `0.6` | Subgoal dedup similarity threshold | Configurable via `GoalRefinementConfig` | Too low = over-dedup |
| config.py:164 | `0.3` | Procedure candidate relevance threshold | Configurable via `ProcedureCandidateConfig` | Too high = no procedures surfaced |
| config.py:165 | `3` | Procedure candidate top_k | Configurable via `ProcedureCandidateConfig` | Too few = missed procedures |
| config.py:181 | `90` | Audit retention days | Configurable via `AuditConfig` | Too short = audit gaps |
| config.py:186 | `300` | Profile cache TTL (seconds, 5min) | Configurable via `ProfileCacheConfig` | Too long = stale profiles |
| config.py:196 | `"local"` | Default gateway_id | `EB_GATEWAY_ID` overrides | Multi-gateway deployments MUST override |
| config.py:214 | `0.15` | Context window fraction for working set | Configurable via `ContextAssemblyConfig` | Too high = context overwhelms conversation |
| config.py:215 | `128000` | Fallback context window size (tokens) | Configurable via `ContextAssemblyConfig` | Must match actual model limit |
| config.py:217 | `0.25` | System overlay budget fraction | Configurable via `ContextAssemblyConfig` | Too high = steals from content |
| config.py:218 | `0.10` | Goal block budget fraction | Configurable via `ContextAssemblyConfig` | Too high = too many goal tokens |
| config.py:219 | `500` | Evidence budget max tokens | Configurable via `ContextAssemblyConfig` | Too low = truncated citations |
| config.py:220 | `2.0` | Compaction trigger multiplier | Configurable via `ContextAssemblyConfig` | <1.5 = premature compaction |
| config.py:221 | `1000` | Compaction summary max tokens | Configurable via `ContextAssemblyConfig` | Too low = poor summaries |
| config.py:227 | `200` | Min artifact content chars for capture | Configurable via `ArtifactCaptureConfig` | Too low = noise; too high = missed artifacts |
| config.py:228 | `50000` | Max artifact content chars | Configurable via `ArtifactCaptureConfig` | Too low = large outputs lost |
| config.py:242 | `0.6` | Async analysis topic continuation threshold | Configurable via `AsyncAnalysisConfig` | Too low = false positives |
| config.py:260 | `86400` | Guard history TTL (seconds, 24h) | Configurable via `GuardConfig` | Too short = lost history |
| config.py:261 | `50` | Max guard history events per session | Configurable via `GuardConfig` | Too low = truncated history |
| config.py:262 | `500` | Input summary max chars for guard event | Configurable via `GuardConfig` | Too low = truncated summaries |
| config.py:263 | `500` | LLM escalation max tokens | Configurable via `GuardConfig` | Too low = incomplete analysis |
| config.py:264 | `10.0` | LLM escalation timeout (seconds) | Configurable via `GuardConfig` | Too low = frequent timeouts |
| config.py:265 | `500` | Max pattern length for static rules | Configurable via `GuardConfig` | Too low = long patterns rejected |
| config.py:268-286 | Strictness presets (loose/medium/strict) | BM25 multipliers, semantic thresholds, reinjection triggers | Spec-mandated preset values | Wrong values = incorrect guard behavior |
| config.py:294 | `300` | Approval default timeout (seconds) | Configurable via `HitlConfig` | Too short = auto-reject; too long = blocked agent |
| config.py:304 | `2000` | Compaction LLM max tokens | Configurable via `CompactionLLMConfig` | Too low = poor summaries |
| config.py:305 | `0.2` | Compaction LLM temperature | Configurable via `CompactionLLMConfig` | Too high = inconsistent summaries |
| config.py:328 | `100` | Max concurrent sessions | `EB_MAX_CONCURRENT_SESSIONS` overrides | Too low = rejected sessions |
| config.py:348 | `172800` | Consolidation min retention (seconds, 48h) | `EB_CONSOLIDATION_MIN_RETENTION_SECONDS` overrides | Too short = active session facts mutated |

### 2. schemas/working_set.py -- Scoring Weight Defaults

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| working_set.py:18 | `1.0` | Default turn_relevance weight | Spec §10.2 base profile default | 0.0 = ignores current turn |
| working_set.py:19 | `1.0` | Default session_goal_relevance weight | Spec §10.2 base profile default | 0.0 = ignores goals |
| working_set.py:20 | `0.5` | Default global_goal_relevance weight | Spec §10.2 base profile default | Too high = global goals dominate |
| working_set.py:21 | `0.8` | Default recency weight | Spec §10.2 base profile default | 0.0 = recency ignored |
| working_set.py:22 | `0.6` | Default successful_use_prior weight | Spec §10.2 base profile default | 0.0 = usage history ignored |
| working_set.py:23 | `0.4` | Default confidence weight | Spec §10.2 base profile default | 0.0 = unverified facts weighted equally |
| working_set.py:24 | `0.3` | Default evidence_strength weight | Spec §10.2 base profile default | 0.0 = evidence ignored |
| working_set.py:25 | `0.5` | Default novelty weight | Spec §10.2 base profile default | 0.0 = stale facts reinjected |
| working_set.py:26 | `-0.7` | Default redundancy_penalty weight | Spec §10.2 base profile default (negative) | 0.0 = no dedup penalty |
| working_set.py:27 | `-1.0` | Default contradiction_penalty weight | Spec §10.2 base profile default (negative) | 0.0 = contradictions not penalized |
| working_set.py:28 | `-0.3` | Default cost_penalty weight | Spec §10.2 base profile default (negative) | 0.0 = token cost ignored |
| working_set.py:30 | `69.0` | Recency half-life (hours, ~2.88 days) | Spec §10.2 base profile default | Too short = rapid recency decay; too long = stale facts never deprioritized |
| working_set.py:31 | `3` | Evidence refs for max evidence_strength score | Spec §10.2 base profile default | Too high = only heavily cited facts get credit |
| working_set.py:33 | `0.85` | Redundancy similarity threshold | Per-profile override from ScoringWeights | Too low = false redundancy penalties |
| working_set.py:34 | `0.9` | Contradiction similarity threshold | Per-profile override from ScoringWeights | Too low = false contradictions |
| working_set.py:35 | `0.3` | Contradiction confidence gap threshold | Per-profile override from ScoringWeights | Too low = false contradictions |
| working_set.py:117 | `8000` | Default ScoringContext token_budget | Fallback when not set by caller. Debug override: pass `token_budget` in `build_working_set()` params to cap selection for testing | Too low = insufficient context |

### 3. runtime/compaction/engine.py -- Compaction Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| engine.py:38 | `1.5` | CADENCE_MULTIPLIER for "aggressive" | Spec §10.2 compaction cadence | <1.0 = compaction never triggers |
| engine.py:39 | `2.0` | CADENCE_MULTIPLIER for "balanced" | Spec §10.2 compaction cadence | Same |
| engine.py:40 | `3.0` | CADENCE_MULTIPLIER for "minimal" | Spec §10.2 compaction cadence | Too high = compaction rarely triggers |
| engine.py:69 | `4` | _CHARS_PER_TOKEN (1 token ~ 4 chars) | Rough estimate; no tokenizer dependency | Inaccurate for non-English; tokenizer would be more precise |
| engine.py:99 | `172800` | Default compaction state TTL (seconds, 48h) | Matches consolidation_min_retention | Too short = compact state lost before consolidation reads it |
| engine.py:241 | `[:20]` | Max decisions preserved in compact state | Prevents bloat in SessionCompactState | Too few = lost decisions |
| engine.py:242 | `[:10]` | Max open questions preserved | Same | Too few = lost questions |
| engine.py:245 | `[:10]` | Max evidence refs preserved | Same | Too few = lost evidence |
| engine.py:440 | `20` | Phatic message max chars (< 20 = candidate for drop) | Heuristic for short messages | Too high = useful short messages dropped |
| engine.py:486 | `5` | Min answer length to count as answer to question | Heuristic for open question detection | Too high = short answers not recognized |
| engine.py:457 | regex | `_PHATIC_RE`: phatic greeting patterns | Heuristic compaction classification | Incomplete pattern list = missed phatic messages |

### 4. runtime/working_set/scoring.py -- Scoring Engine Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| scoring.py:36 | `0.0/1.0` | Clamp range for all score dimensions | Normalized [0,1] output | N/A -- correct behavior |
| scoring.py:44 | `1.0` | "direct" goal relevance tag score | Goal tagging convention | N/A |
| scoring.py:46 | `0.7` | "indirect" goal relevance tag score | Goal tagging convention | Too high = indirect=direct; too low = ignored |
| scoring.py:71 | `0.7` | Parent goal credit multiplier (child_sim * 0.7) | Hierarchical goal discount | Too low = parent goals invisible |
| scoring.py:93 | `0.5` | Neutral recency when no timestamp | Fallback for facts without timestamps | 0.0 = new facts penalized; 1.0 = over-promoted |
| scoring.py:184 | `1e-10` | Cosine similarity norm floor | Prevents division by zero | N/A -- numerical safety |

### 5. runtime/memory/facade.py -- Memory Store Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| facade.py:30 | `"FactDataPoint_text"` | Qdrant collection name for fact embeddings | Must match Cognee's auto-generated collection | Wrong = vector search fails |
| facade.py:31 | `0.95` | `_DEFAULT_DEDUP_THRESHOLD` (cosine similarity) | Dedup sensitivity when no caller override | Too low = over-dedup; too high = duplicates stored |
| facade.py:175 | `-0.01` | Freshness decay coefficient (exp(-0.01 * hours)) | Smooth freshness decay (half-life ~69h) | Too large = aggressive decay; too small = stale facts appear fresh |

### 6. runtime/context/assembler.py -- Assembly Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| assembler.py:30-36 | `0-4` | CLASS_PRIORITY ordering (policy=0, working_memory=4) | Memory class injection order | Wrong order = low-priority items injected first |
| assembler.py:142 | `0.20` | Block 1 (system prompt) budget fraction | Budget split for constraints/procedures | Too high = starves Block 3 |
| assembler.py:145 | `0.10` | Block 2 (goal context) budget fraction | Budget split for goals | Too high = starves Block 3 |
| assembler.py:148 | `0.05` | Block 4 (evidence refs) budget fraction | Budget split for citations | Too high = starves Block 3 |
| assembler.py:237 | `0.40` | Overlay Block 2 (goals) budget fraction | Within overlay budget | Same |
| assembler.py:243 | `0.20` | Overlay Block 4 (evidence) budget fraction | Within overlay budget | Same |
| assembler.py:275 | `[:3]` | Fallback subagent items (must_inject + top 3) | Deterministic fallback when LLM unavailable | Too few = insufficient context for subagent |
| assembler.py:330 | `400` | `_ARTIFACT_PLACEHOLDER_THRESHOLD` (chars, ~100 tokens) | Items larger than this get placeholder | Too low = too many placeholders; too high = token waste |
| assembler.py:342 | `[:120]` | Artifact placeholder summary truncation | Display length in prompt | Too short = unhelpful summary |
| assembler.py:388 | `[:80]` | Evidence citation text truncation | Display length in prompt | Too short = unhelpful citation |
| assembler.py:415 | `4` | `_truncate_to_budget` chars-per-token ratio | Same rough estimate as compaction | Same risk |
| assembler.py:99 | `CLASS_PRIORITY.get(_, 99)` | Unknown class sort priority | Unknown classes sorted last | N/A -- safe default |

### 7. runtime/context/lifecycle.py -- Lifecycle Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| lifecycle.py:42-48 | `TOOL_ALIASES` dict | Maps tool names to canonical names for fact extraction | Heuristic domain mapping | Incomplete = some tools unmapped |
| lifecycle.py:50-54 | `PROGRESS_SIGNALS` dict | Regex patterns for goal progress detection | Heuristic keyword matching | Incomplete = missed progress signals |
| lifecycle.py:135 | `128` | Max fallback session ID cache size | Prevents unbounded memory growth | Too small = cache thrashing |
| lifecycle.py:795,803 | `0.3` | Injection effectiveness confidence threshold | Threshold for counting fact as "referenced" | Too low = false positives; too high = under-counting |
| lifecycle.py:1074 | `200` | Min tool output chars for artifact capture (fallback) | Fallback when no ArtifactCaptureConfig | Too low = noise; too high = missed artifacts |
| lifecycle.py:1194 | `0.3` | Running Jaccard threshold for successful use | Similarity threshold for use detection | Too low = false positives |
| lifecycle.py:1209 | `3` | Ignored turns threshold for S6 tracking | Turns before fact flagged as "ignored" | Too low = premature flagging |
| lifecycle.py:1218 | `0.3` | Use confidence threshold for fact update | Below this = fact marked as "not used" | Too high = under-tracking |
| lifecycle.py:820 | `[-3:]` | Guard input extraction: last 3 messages | Fallback message window | Too few = insufficient context |
| lifecycle.py:800,807,816,824 | `[:1000]` | Guard action content truncation | Prevents oversized guard inputs | Too short = truncated context |

### 8. runtime/guards/ -- Guard Pipeline Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| rules.py:17 | `500` | `MAX_PATTERN_LENGTH` for regex rules | Prevents ReDoS from pathological patterns | Too low = long patterns rejected |
| semantic_index.py:51 | `1.5` | BM25 k1 parameter (term frequency saturation) | Standard BM25 tuning parameter | Too low = TF ignored; too high = TF dominant |
| semantic_index.py:51 | `0.75` | BM25 b parameter (document length normalization) | Standard BM25 tuning parameter | 0.0 = no length normalization; 1.0 = full |
| semantic_index.py:95 | `0.80` | Default semantic similarity threshold | Fallback when no profile override | Too low = false positives; too high = missed matches |
| engine.py:202 | `49` | Guard history LRANGE limit (0-49 = 50 events) | Matches `max_history_events` config default | Must stay in sync with config |
| engine.py:375 | `19` | Autonomy classifier: recent fact domains LRANGE limit (0-19) | Last 20 domains for Tier 2 classification | Too few = poor domain context |
| engine.py:732 | `[:20]` | LLM escalation: max rules in prompt | Prevents prompt overflow | Too few = missed rules |
| engine.py:765 | `0.8` | LLM escalation confidence score | Fixed confidence for LLM layer results | Should perhaps be parsed from LLM response |
| engine.py:772 | `0.5` | Timeout/error confidence score | Low confidence for fail-closed results | N/A -- conservative default |
| engine.py:939-940 | `50` | Guard event history max (LTRIM 0 to max-1) | Matches `max_history_events` config default | Must stay in sync |
| autonomy.py:17-47 | 22 entries | `_DEFAULT_TOOL_DOMAINS`: tool-to-domain mapping | Default classification heuristic | Incomplete = some tools classified as "uncategorized" |
| autonomy.py:50-60 | 9 domains | `_KEYWORD_DOMAINS`: keyword-to-domain heuristic | Tier 1 classification keywords | Incomplete = missed keyword matches |
| approval_queue.py:48 | `+60` | Approval TTL = timeout + 60 seconds grace | Ensures Redis key outlives timeout | Too small = data loss before callback |
| approval_queue.py:258 | `[:16]` | Dedup hash truncation (sha256[:16]) | Collision-resistant dedup key | Too short = hash collisions |

### 9. runtime/rerank/orchestrator.py -- Reranking Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| orchestrator.py:96 | `0.5` / `0.5` | Cheap prune blend: overlap * 0.5 + score * 0.5 | Equal weight for token overlap and retrieval score | Should be configurable via ScoringConfig |
| orchestrator.py:269 | `1e-10` | Cosine similarity norm floor | Prevents division by zero | N/A |

### 10. runtime/retrieval/orchestrator.py -- Retrieval Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| orchestrator.py:24 | `"FactDataPoint_text"` | Qdrant collection name | Must match Cognee convention | Wrong = vector search fails |
| orchestrator.py:112 | `0.3` | Fallback retrieval weight (for unknown sources) | Default when source not in weight_map | Too low = source results suppressed |
| orchestrator.py:199 | `1.0` | Structural hit base score | All structural hits start at score 1.0 | N/A -- weighted by policy afterward |
| orchestrator.py:286 | `0.5` | Direct vector search fallback score | When score attribute missing | Neutral value |
| orchestrator.py:307 | `0.8` | Cognee hits-to-candidates default score | All Cognee results scored at 0.8 | Should be parsed from Cognee response |

### 11. schemas/profile.py -- Profile Schema Defaults

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| profile.py:39 | `4000` | Default compaction target_tokens | Trigger threshold for compaction | Too low = constant compaction |
| profile.py:50 | `20` | structural_fetch_k | Cypher result limit | Too low = missed structural hits |
| profile.py:53 | `15` | keyword_fetch_k | Cognee keyword search limit | Too low = missed keyword hits |
| profile.py:56 | `20` | vector_fetch_k | Vector search result limit | Too low = missed semantic hits |
| profile.py:60 | `2` | graph_max_depth (1-5) | Graph traversal depth | Too deep = noisy results; too shallow = missed connections |
| profile.py:63 | `10` | artifact_fetch_k | Artifact search limit | Too low = missed artifacts |
| profile.py:64 | `40` | root_top_k | Final candidate cap after merge | Too low = good candidates dropped |
| profile.py:73 | `10` | auto_recall_injection_top_k | Max facts auto-injected | Too high = prompt bloat |
| profile.py:74 | `0.3` | autorecall min_similarity | Min similarity for auto-injection | Too high = nothing injected; too low = noise |
| profile.py:76 | `0.95` | autorecall dedup_similarity | Dedup threshold for autorecall | Too low = over-dedup |
| profile.py:79 | `0.3` | superseded_confidence_factor | Decay factor for superseded facts | Too low = old facts vanish; too high = old facts persist |
| profile.py:99 | `0.85` | bm25_block_threshold | BM25 score threshold for BLOCK | Too low = false blocks |
| profile.py:100 | `0.60` | bm25_warn_threshold | BM25 score threshold for WARN | Too low = false warnings |
| profile.py:101 | `0.80` | semantic_similarity_threshold | Embedding similarity for guard match | Too low = false positives |
| profile.py:105 | `3` | near_miss_escalation_threshold | Near-misses before escalation | Too low = excessive escalation |
| profile.py:106 | `5` | near_miss_window_turns | Turn window for near-miss counting | Too narrow = count resets too fast |
| profile.py:151 | `20` | mem0_fetch_k | Legacy fetch limit | Same as structural_fetch_k |
| profile.py:152 | `15` | graph_fetch_k | Graph expansion limit | Too low = missed graph neighbors |
| profile.py:153 | `10` | artifact_fetch_k (Budgets) | Budget-level artifact limit | Same |
| profile.py:154 | `30` | final_prompt_k | Final prompt item count limit | Too low = too few items in prompt |
| profile.py:155 | `40` | root_top_k (Budgets) | Budget-level top-k cap | Same |
| profile.py:156 | `8000` | max_prompt_tokens | Total working set token budget | Too low = insufficient context |
| profile.py:157 | `1500` | max_system_overlay_tokens | System prompt overlay budget | Too low = truncated constraints |
| profile.py:158 | `3000` | subagent_packet_tokens | Context budget for subagent spawn | Too low = insufficient delegation context |
| profile.py:171 | `100` | replace_tool_output_min_tokens | Min tokens before tool output replaced | Too low = short outputs replaced |
| profile.py:174 | `0.7` | conversation_dedup_threshold | Overlap ratio for conversation dedup | Too low = aggressive dedup |
| profile.py:176 | `5` | goal_reminder_interval (turns) | Turns between goal reinjection | Too large = stale goal awareness |
| profile.py:193 | `86400` | session_data_ttl_seconds (24h) | Redis session key TTL | Too short = mid-session data loss |

### 12. schemas/consolidation.py -- Consolidation Pipeline Defaults

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| consolidation.py:18 | `500` | Fact loading batch_size | Cypher pagination size | Too large = OOM on large graphs |
| consolidation.py:19 | `1.0` | active_session_protection_hours | Skip facts created within this window | 0.0 = consolidation mutates active facts |
| consolidation.py:22 | `0.92` | cluster_similarity_threshold | Near-duplicate clustering threshold | Too low = unrelated facts merged |
| consolidation.py:25 | `0.7` | canonicalize_divergence_threshold | Text divergence triggering LLM review | Too low = LLM called on obvious dupes |
| consolidation.py:29 | `0.5` | strengthen_success_ratio_threshold | Min success ratio to boost | Too high = under-strengthening |
| consolidation.py:30 | `3` | strengthen_min_use_count | Min uses before strengthening eligible | Too high = slow to boost |
| consolidation.py:31 | `0.1` | strengthen_boost_factor | Confidence boost per cycle | Too high = rapid inflation |
| consolidation.py:35 | `0.85` | decay_recalled_unused_factor | Per-cycle decay for unused facts | Too low = aggressive decay |
| consolidation.py:37 | `0.95` | decay_never_recalled_factor | Per-cycle decay for never-recalled | Same |
| consolidation.py:39 | `0.05` | decay_archival_threshold | Below this = archived | 0.0 = never archive; too high = over-archival |
| consolidation.py:42-48 | 5 entries | decay_scope_multipliers | Scope-based decay speed factors | session=1.5 (50% faster), global=0.5 (50% slower) |
| consolidation.py:51 | `5` | autorecall_blacklist_min_recalls | Min recalls before blacklisting | Too low = premature blacklisting |
| consolidation.py:52 | `0.0` | autorecall_blacklist_max_success_ratio | Max success ratio for blacklist | >0.0 = blacklists partially useful facts |
| consolidation.py:55 | `3` | promote_session_threshold | Sessions needed for promotion | Too high = never promotes |
| consolidation.py:57 | `3` | promote_artifact_injected_threshold | Injection count for artifact promotion | Same |
| consolidation.py:60 | `3` | pattern_recurrence_threshold | Sessions for procedure pattern | Same |
| consolidation.py:61 | `3` | pattern_min_steps | Min steps in tool sequence pattern | Too high = short procedures ignored |
| consolidation.py:62 | `10` | max_patterns_per_run | Cap on procedure suggestions per run | Too low = missed patterns |
| consolidation.py:65 | `0.3` | ema_alpha | EMA smoothing for weight adjustment | Too high = volatile; too low = unresponsive |
| consolidation.py:67 | `0.05` | max_weight_adjustment_pct (5%) | Max delta as fraction of base weight | Too low = weight tuning too slow; too high = instability |
| consolidation.py:71 | `20` | min_correlation_samples | Min scored facts for Stage 9 | Too high = tuning never fires |
| consolidation.py:75 | `50` | llm_calls_per_run_cap | Max LLM calls across all stages | 0 = LLM-free consolidation |

### 13. runtime/consolidation/stages/ -- Stage-Level Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| domain_discovery.py:36 | `5` | `_MIN_OCCURRENCES` for domain discovery | Min action_target occurrences for analysis | Too low = noisy suggestions |
| domain_discovery.py:84 | `100` | Redis SCAN count per iteration | Scan batch size for guard history | Too low = many round trips |
| recompute_salience.py:75 | `0.1` | Spearman correlation significance threshold | Below 0.1 = skip dimension adjustment | Too low = noisy adjustments |
| recompute_salience.py:102 | `3` | Min samples for Spearman correlation | Statistical minimum | Too low = meaningless correlations |
| refine_procedures.py:110 | `0.3 + 0.1 * session_count` | Procedure suggestion confidence formula | Heuristic: confidence grows with sessions | Cap at 0.9 is appropriate |
| strengthen.py:57 | Formula | `min(1.0, old + boost * ratio)` | Confidence boost with ceiling | Ceiling of 1.0 is correct |

### 14. runtime/trace/ledger.py -- Trace Ledger Fallbacks

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| ledger.py:41 | `10_000` | Fallback max events (when no config) | In-memory buffer cap | Too high = memory bloat |
| ledger.py:42 | `3600` | Fallback TTL seconds (when no config) | In-memory retention | Too short = events evicted |

### 15. runtime/adapters/ -- Adapter Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| cached_embeddings.py:35 | `[:32]` | SHA-256 hash truncation for cache key | 32 hex chars = 128 bits = negligible collision risk | Shorter = collision risk |
| vector.py:48 | `10` | Default `top_k` for `search_similar()` | Fallback vector search limit | Too low = missed results |
| datapoints.py:34 | `1.0` | Default FactDataPoint confidence | New facts start at full confidence | 0.0 = new facts invisible |
| datapoints.py:121 | `0.5` | Default ActorDataPoint trust_level | Neutral trust for new actors | 0.0 = untrusted; 1.0 = fully trusted |
| datapoints.py:175 | `1.0` | Default GoalDataPoint confidence | New goals at full confidence | Same as facts |
| tasks/extract_facts.py:216 | `10` | Min message chars for extraction (`total_chars < 10`) | Skip near-empty messages | Too high = short messages skipped |
| tasks/extract_facts.py:133 | `[:8]` | Fact ID display truncation (UUID to 8 hex chars) | Logging readability | N/A -- display only |
| tasks/summarize_artifact.py:33,49 | `[:200]` | Fallback summary truncation (no LLM) | Maximum summary chars when LLM fails | Too short = unhelpful summary |

### 16. runtime/profiles/presets.py -- Profile-Specific Constants

These are the 5 named profile presets from arch spec §10.2. Each profile's constants are intentionally hardcoded as the spec's canonical values.

| Profile | Key Distinguishing Values | session_data_ttl |
|---------|--------------------------|------------------|
| coding | turn_relevance=1.5, recency=1.2, recency_half_life=24h, max_prompt=8000, cadence=aggressive | 86400 (1d) |
| research | evidence_strength=0.9, confidence=0.8, recency_half_life=168h (7d), max_prompt=12000, cadence=minimal | 259200 (3d) |
| managerial | session_goal=1.5, global_goal=1.0, redundancy=-0.9, max_prompt=8000, cadence=aggressive | 172800 (2d) |
| worker | turn_relevance=1.3, session_goal=1.4, recency=1.3, recency_half_life=12h, max_prompt=6000, cadence=balanced | 86400 (1d) |
| personal_assistant | successful_use=0.9, recency=0.9, recency_half_life=720h (30d), isolation=STRICT, max_prompt=8000 | 604800 (7d) |

### 17. pipelines/ -- Pipeline Constants

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| turn_ingest/buffer.py:68 | `20` | Default `max_count` for recent facts window | Max facts kept for extraction context | Too few = poor supersession detection |
| turn_ingest/pipeline.py:183 | `[:50]` | Superseded fact text truncation in trace payload | Logging readability | N/A -- trace only |

### 18. api/ -- API Route Defaults

| File:Line | Value | What it controls | Why hardcoded | Risk at extremes |
|-----------|-------|-----------------|---------------|-----------------|
| routes/memory.py:35 | `0.0` | Default min_score for search endpoint | No minimum score filter by default | N/A -- inclusive |
| routes/memory.py:151 | `100` | Default limit for get_by_scope endpoint | Pagination default | Too high = slow responses |
| routes/trace.py:16 | `100` | Default limit for trace list endpoint | Pagination default | Same |
| routes/consolidation.py:43 | `10` | Default limit for consolidation reports | Pagination default | N/A |
| routes/guards.py:62 | `[:10]` | Guard history display limit in API response | Max events returned | Too few = incomplete view |
| routes/sessions.py:37,58 | `[:8]` | Gateway ID truncation for short_name fallback | Display brevity | N/A -- display only |
| routes/admin.py:151,190,208,255 | `[:20]` | Display label truncation from name | Fallback label | N/A -- display only |
| routes/artifacts.py:85 | `[:200]` | Auto-summary from content | Fallback when no explicit summary | Too short = unhelpful |

### 19. runtime/redis_keys.py -- Redis Key Patterns

| File:Line | Pattern | Purpose |
|-----------|---------|---------|
| redis_keys.py:13 | `eb:{gateway_id}` | Root prefix for ALL gateway-scoped keys |
| redis_keys.py:22 | `...:ingest_buffer:{sk}` | Buffered messages before extraction |
| redis_keys.py:25 | `...:recent_facts:{sk}` | Recent facts for extraction context |
| redis_keys.py:29 | `...:session_goals:{sk}:{sid}` | Session goal state |
| redis_keys.py:34 | `...:ws_snapshot:{sk}:{sid}` | Working set snapshot |
| redis_keys.py:37 | `...:compact_state:{sk}:{sid}` | Compacted item ID set |
| redis_keys.py:43 | `...:session_parent:{sk}` | Subagent parent tracking |
| redis_keys.py:50 | `...:compact_state_obj:{sk}:{sid}` | Full compact state JSON |
| redis_keys.py:53 | `...:session_artifacts:{sk}:{sid}` | Session artifact HASH |
| redis_keys.py:56 | `...:procedure_exec:{sk}:{sid}` | Procedure execution state |
| redis_keys.py:59 | `...:session_messages:{sk}:{sid}` | Message history for assembly |
| redis_keys.py:62 | `...:fact_async_use:{source_id}` | Async injection use counters |
| redis_keys.py:65 | `...:guard_history:{sk}:{sid}` | Guard event history LIST |
| redis_keys.py:68 | `...:{agent_id}:approval:{request_id}` | HITL approval request |
| redis_keys.py:71 | `...:{agent_id}:approvals_by_session:{sid}` | Approval session index SET |
| redis_keys.py:74 | `...:fact_domains:{sk}:{sid}` | Recent fact domains for autonomy |
| redis_keys.py:82 | `...:consolidation_lock` | Distributed lock for consolidation |
| redis_keys.py:87 | `...:consolidation_status` | Consolidation run status |
| redis_keys.py:93 | `eb:emb_cache:{text_hash}` | Global (NOT gateway-scoped) embedding cache |

### 20. String Constants and Sentinel Values

| File:Line | Value | What it controls |
|-----------|-------|-----------------|
| config.py:325 | `"coding"` | Default profile name |
| config.py:430 | `"elephantbroker"` | Default Cognee dataset name |
| compaction/engine.py:44-56 | regex patterns | Phatic detection, decision detection, evidence detection |
| context/lifecycle.py:42-48 | `TOOL_ALIASES` | 22 tool name → canonical name mappings |
| guards/autonomy.py:17-47 | `_DEFAULT_TOOL_DOMAINS` | 22 tool → domain default mappings |
| guards/autonomy.py:50-60 | `_KEYWORD_DOMAINS` | 9 domain → keyword list heuristic mappings |
| guards/rules.py:119-171 | 16 entries | Builtin static guard rules (credential, SQL, shell, exfiltrate, payment, refactor-gate, prod-deploy) |
| consolidation/domain_discovery.py:23-34 | 10 entries | Decision domain names and descriptions for embedding comparison |

---

**Summary**: The codebase has approximately 250+ distinct hardcoded constants. The vast majority (~170) are properly externalized via `ElephantBrokerConfig` with `EB_*` environment variable overrides and YAML support. The remaining ~80 are embedded in runtime code as module-level constants, function defaults, or inline literals. The highest-risk non-configurable constants are:

1. **Cheap prune blend weights** (rerank/orchestrator.py:96) -- hardcoded 0.5/0.5 split, should be in `ScoringConfig`
2. **Cognee hit default score** (retrieval/orchestrator.py:307) -- hardcoded 0.8, not parsed from Cognee response
3. **Freshness decay coefficient** (memory/facade.py:175) -- hardcoded -0.01, should derive from `recency_half_life_hours`
4. **Parent goal discount** (scoring.py:71) -- hardcoded 0.7 multiplier, should be in `ScoringWeights`
5. **BM25 parameters k1=1.5, b=0.75** (semantic_index.py:51) -- standard defaults but not configurable per-profile
6. **Budget fraction splits** (assembler.py:142-148) -- hardcoded 20/10/5% split, should be in `ContextAssemblyConfig`


---

## 13. Second Pass: Configuration Gap Analysis

> Findings from second-pass source code review identifying parameters not covered in sections 1-12.

### 13.1 Runtime Module Gaps


##### HTTP Client Timeouts (hardcoded, not configurable, not documented)

| Location | Parameter | Value | What it controls | Configurable? | Should be? | Impact spectrum |
|----------|-----------|-------|-----------------|---------------|------------|-----------------|
| `runtime/adapters/cognee/embeddings.py:29` | httpx timeout | `30.0` seconds | Embedding API HTTP timeout for all embed_text/embed_batch calls | No (hardcoded in `__init__`) | Yes -- embedding latency varies by provider/model size | Too low = timeout on large batches; too high = hangs on unresponsive endpoint |
| `runtime/adapters/llm/client.py:31` | httpx timeout | `120.0` seconds | LLM chat completion HTTP timeout for all complete/complete_json calls | No (hardcoded in `__init__`) | Yes -- large extraction prompts take time | Too low = truncated completions; too high = blocked pipeline on dead endpoint |
| `runtime/consolidation/successful_use_task.py:63` | httpx timeout | `30.0` seconds | RT-1 successful-use LLM evaluation HTTP timeout | No (hardcoded in `__init__`) | Yes -- should use `successful_use.batch_timeout_seconds` | Too low = RT-1 always fails; too high = blocks after_turn |

##### LLM Parameters in Consolidation Stages (hardcoded per-call, not configurable)

| Location | Parameter | Value | What it controls | Configurable? | Should be? | Impact spectrum |
|----------|-----------|-------|-----------------|---------------|------------|-----------------|
| `runtime/consolidation/stages/canonicalize.py:126` | max_tokens | `500` | LLM output limit for cluster merge text in Stage 2 | No (hardcoded in LLM call) | Maybe -- complex clusters need more tokens | Too low = truncated canonical facts; too high = verbose output |
| `runtime/consolidation/stages/refine_procedures.py:97` | max_tokens | `500` | LLM output limit for procedure draft in Stage 7 | No (hardcoded in LLM call) | Maybe -- complex procedures need more tokens | Too low = incomplete procedure definitions; too high = wasted tokens |
| `runtime/consolidation/successful_use_task.py:110` | max_tokens | `500` | LLM output limit for successful-use evaluation response | No (hardcoded in LLM call) | Maybe | Too low = truncated evaluation |
| `runtime/consolidation/successful_use_task.py:111` | temperature | `0.1` | LLM temperature for successful-use evaluation | No (hardcoded in LLM call) | No -- deterministic evaluation is correct | N/A |
| `runtime/adapters/llm/client.py:95` | temperature | `0.0` | Temperature for `complete_json()` calls (guards LLM escalation, goal refinement) | No (hardcoded in method) | Probably not -- JSON extraction should be deterministic | N/A |
| `runtime/compaction/engine.py:590` | temperature | `0.2` | Temperature for compaction LLM summarization | No (hardcoded inline, ignores `compaction_llm.temperature` config) | BUG: should use `self._config.compaction_llm.temperature` | Config says 0.2, code says 0.2, but code ignores config |

##### Prompt Truncation Limits (hardcoded inline, not documented)

| Location | Parameter | Value | What it controls | Configurable? | Should be? | Impact spectrum |
|----------|-----------|-------|-----------------|---------------|------------|-----------------|
| `runtime/consolidation/successful_use_task.py:90` | content truncation | `[:500]` | Per-message content truncation in RT-1 evaluation prompt | No (hardcoded) | Maybe -- longer context helps LLM evaluate | Too short = insufficient context for evaluation |
| `runtime/consolidation/successful_use_task.py:97` | conversation truncation | `[:4000]` chars | Total conversation truncation in RT-1 evaluation prompt | No (hardcoded) | Yes -- should derive from RT-1 max_input | Too short = missed conversation context |
| `runtime/guards/engine.py:741` | metadata truncation | `[:200]` | Action metadata JSON truncation in LLM escalation prompt | No (hardcoded) | No -- metadata is auxiliary | N/A |
| `runtime/artifacts/store.py:50` | text_for_cognee | `[:500]` | Artifact text sent to `cognee.add()` for indexing | No (hardcoded) | Maybe -- long summaries lose context | Too short = poor artifact search recall |

##### ClickHouse Query Parameters (hardcoded, not configurable)

| Location | Parameter | Value | What it controls | Configurable? | Should be? | Impact spectrum |
|----------|-----------|-------|-----------------|---------------|------------|-----------------|
| `runtime/consolidation/otel_trace_query_client.py:46` | days | `7` | Default lookback window for ClickHouse tool sequence queries (Stage 7) | No (hardcoded default) | Yes -- different deployment cadences need different windows | Too short = missed recurring patterns; too long = slow queries |
| `runtime/consolidation/otel_trace_query_client.py:47` | min_sessions | `3` | Minimum sessions with tool sequences to include in results | No (hardcoded default) | Yes -- should align with `pattern_recurrence_threshold` | Too high = no results; too low = noisy patterns |
| `runtime/consolidation/otel_trace_query_client.py:71` | `HAVING length(tools) >= 3` | `3` | Minimum tools per session for sequence analysis | No (hardcoded in SQL) | Yes -- should align with `pattern_min_steps` | Too high = missed short patterns |

##### Redis Operational Parameters (hardcoded, not documented)

| Location | Parameter | Value | What it controls | Configurable? | Should be? | Impact spectrum |
|----------|-----------|-------|-----------------|---------------|------------|-----------------|
| `runtime/working_set/manager.py:232` | scan_iter count | `10` | Redis SCAN batch size when searching for working set snapshots | No (hardcoded) | No -- operational parameter | Too low = many round trips; too high = blocks Redis |
| `runtime/consolidation/stages/domain_discovery.py:84` (referenced in existing audit at line 5158) | SCAN count | `100` | Redis SCAN batch size for guard history scan | Already documented | -- | -- |
| `runtime/context/async_analyzer.py:109` | async_use TTL | `86400` | Redis TTL for fact_async_use counter (24h) | No (hardcoded) | Maybe -- should match consolidation cadence | Too short = lost async use signal before consolidation |

##### Subagent and Graph Traversal Depth (hardcoded defaults, partially documented)

| Location | Parameter | Value | What it controls | Configurable? | Should be? | Impact spectrum |
|----------|-----------|-------|-----------------|---------------|------------|-----------------|
| `runtime/retrieval/isolation.py:12` | max_depth | `5` | Maximum parent chain walk-up depth for SUBAGENT_INHERIT isolation | No (hardcoded default) | Maybe -- deep subagent trees are rare | Too low = orphaned subagent context; too high = performance waste |
| `runtime/adapters/cognee/graph.py:121` | max_depth | `2` | Default graph subgraph traversal depth for `query_subgraph()` | No (hardcoded default) | Yes via RetrievalPolicy.graph_max_depth (already documented) | -- |

##### Scoring Internals (hardcoded, partially undocumented)

| Location | Parameter | Value | What it controls | Configurable? | Should be? | Impact spectrum |
|----------|-----------|-------|-----------------|---------------|------------|-----------------|
| `runtime/rerank/orchestrator.py:84` (in existing audit) | cheap prune blend | `0.5 / 0.5` | Blend ratio: token overlap vs retrieval score in cheap prune | Already documented as gap | Yes -- should be in `ScoringConfig` | -- |
| `runtime/context/lifecycle.py:1256` | direct quote ratio threshold | `0.4` | Ratio of key phrases matched to count as direct quote (S1 signal) | No (hardcoded) | Maybe -- affects injection tracking | Too low = false positive quotes; too high = under-detection |
| `runtime/context/lifecycle.py:1284` | tool correlation threshold | `0.3` | Overlap ratio to count as tool correlation (S2 signal) | No (hardcoded) | Maybe -- affects injection tracking | Too low = false positives; too high = missed correlations |
| `runtime/compaction/engine.py:457` (referenced `_is_phatic`) | phatic content word min length | `4` (in `\w{4,}` regex) | Minimum word length for goal reference matching in compaction | No (hardcoded in regex) | No -- linguistic constant | N/A |
| `runtime/compaction/engine.py:486` (in existing audit) | min answer length | `5` | Minimum answer content length to count as answered | Already documented | -- | -- |

##### Authority Level Thresholds (hardcoded, not in Hardcoded Constants section)

| Location | Parameter | Value | What it controls | Configurable? | Should be? | Impact spectrum |
|----------|-----------|-------|-----------------|---------------|------------|-----------------|
| `runtime/profiles/authority_store.py:23` | create_global_goal min_authority | `90` | Authority level required to create global goals | Yes (via authority_rules SQLite overrides) | Already configurable | Too low = unauthorized global goal creation |
| `runtime/profiles/authority_store.py:24` | create_org_goal min_authority | `70` | Authority level for org-scoped goal creation | Yes (via authority_rules SQLite overrides) | Already configurable | Same |
| `runtime/profiles/authority_store.py:24` | matching_exempt_level | `90` | Authority level that bypasses org/team matching checks | Yes (via authority_rules SQLite overrides) | Already configurable | Too low = permission bypass; too high = admin can't cross-org |
| `runtime/profiles/authority_store.py:25` | create_team_goal min_authority | `50` | Authority level for team-scoped goal creation | Yes (via authority_rules SQLite overrides) | Already configurable | Same |
| `runtime/profiles/authority_store.py:31` | register_actor min_authority | `70` | Authority level to register new actors | Yes (via authority_rules SQLite overrides) | Already configurable | Too low = unauthorized actor creation |
| `runtime/profiles/authority_store.py:35` | merge_actors min_authority | `70` | Authority level to merge duplicate actors | Yes (via authority_rules SQLite overrides) | Already configurable | Too low = unauthorized data merge |

##### Miscellaneous Gaps

| Location | Parameter | Value | What it controls | Configurable? | Should be? | Impact spectrum |
|----------|-----------|-------|-----------------|---------------|------------|-----------------|
| `runtime/context/lifecycle.py:168` | default profile_name fallback | `"coding"` | Fallback profile when bootstrap params don't specify one | Yes (via `default_profile` in `ElephantBrokerConfig`) | Already configurable | N/A |
| `runtime/guards/engine.py:459,479,740,897` | action_summary truncation | `[:500]` | Action content truncation in approval requests and HITL notifications | No (hardcoded, matches `input_summary_max_chars` config) | Should use config value instead of hardcoded `500` | Config and code both say 500 but code ignores config |
| `runtime/context/assembler.py:342` | artifact summary truncation | `[:120]` | Summary text shown in artifact placeholder in prompt | No (hardcoded) | Maybe | Too short = unhelpful placeholder |
| `runtime/guards/approval_queue.py:62` | matched_rules in blocker | `[:3]` | Max matched rules shown in auto-created blocker text | No (hardcoded) | No -- display limit | N/A |
| `runtime/consolidation/stages/refine_procedures.py:110` | confidence formula cap | `min(0.9, ...)` | Hard cap on procedure suggestion confidence | No (hardcoded) | No -- 0.9 leaves room for human verification | N/A |

---

##### Summary of Findings

**Total NEW gaps found (not in existing audit):** 24 distinct parameters

**Highest-priority gaps (should be configurable):**

1. **Embedding service HTTP timeout** (`embeddings.py:29`, 30s) -- varies dramatically by embedding provider and batch size. Should be in `CogneeConfig`.
2. **LLM client HTTP timeout** (`client.py:31`, 120s) -- varies by model speed and prompt size. Should be in `LLMConfig`.
3. **RT-1 HTTP timeout** (`successful_use_task.py:63`, 30s) -- should use its config `batch_timeout_seconds` instead of hardcoded 30s.
4. **ClickHouse query lookback window** (`otel_trace_query_client.py:46`, 7 days) -- deployment-specific, should be in `ConsolidationConfig`.
5. **Compaction LLM temperature bypass** (`compaction/engine.py:590`) -- hardcodes `temperature=0.2` in the LLM call instead of reading from `compaction_llm.temperature`. This is a **config bypass bug**, not a documentation gap.
6. **Guard action_summary truncation** (`guards/engine.py:459,479,740,897`) -- hardcodes `[:500]` in 4 places instead of reading `self._config.input_summary_max_chars`. This is another **config bypass bug**.

**Medium-priority (document only, not urgent to make configurable):**

7. Consolidation stage LLM `max_tokens=500` (3 locations in canonicalize, refine_procedures, successful_use)
8. Prompt content truncation limits (`[:500]` per message, `[:4000]` total) in RT-1
9. Artifact `cognee.add()` text truncation (`[:500]`) in `artifacts/store.py:50`
10. Subagent parent chain `max_depth=5` in `retrieval/isolation.py:12`
11. Async analysis Redis TTL `86400` in `context/async_analyzer.py:109`
12. Direct quote ratio threshold `0.4` and tool correlation threshold `0.3` in lifecycle injection tracking

**Low-priority (correctly hardcoded, just not documented):**

13-24. LLM temperatures for JSON extraction, scan batch sizes, display truncation limits, regex word-length parameters


### 13.2 Pipeline & API Gaps


#### Methodology

Searched all files under `elephantbroker/pipelines/`, `elephantbroker/api/`, `elephantbroker/server.py`, and `elephantbroker/cli.py` for hardcoded parameters, prompt templates, default values, and behavioral constants. Cross-referenced each against the existing `CONFIGURATION.md` (5200+ lines covering env vars, config schemas, profiles, infrastructure, and a 250+ hardcoded constants audit).

#### Finding: CONFIGURATION.md is Comprehensive

The existing CONFIGURATION.md already documents the vast majority of parameters found in the scanned files. The "Hardcoded Constants Audit" (Section 12, entries 17 and 18) explicitly covers pipeline and API route defaults. The configuration schemas section covers all `LLMConfig`, `GoalInjectionConfig`, `GoalRefinementConfig`, and other pipeline-tuning parameters.

#### Undocumented or Under-documented Parameters

The following table lists parameters found in the scanned code that are either completely missing from CONFIGURATION.md, or whose behavioral details are not fully described.

##### Pipelines

| Location | Parameter | Value | Controls | In CONFIGURATION.md? | Should be configurable? |
|----------|-----------|-------|----------|----------------------|------------------------|
| `pipelines/turn_ingest/pipeline.py:327` | Fact domains Redis list max length | `ltrim(key, 0, 19)` = 20 entries | Max recent decision domains stored per session for guard Tier 2 classification | Partially (Redis key table mentions "capped at 20" but not as a named constant) | Yes -- guard precision depends on domain window size |
| `pipelines/turn_ingest/pipeline.py:328` | Fact domains Redis TTL | `86400` (24h) hardcoded inline | TTL for fact domain list used by guard autonomy classifier | Partially (Redis TTL table mentions "86400s hardcoded" but not as a configurable param) | Yes -- should align with `guards.history_ttl_seconds` |
| `pipelines/verification/pipeline.py:31` | `sampling_rate` constructor param | `0.0` (default) | Fraction of unverified claims queued for supervisor review during batch verification | Not documented as a pipeline config param (only `VerificationPolicy.supervisor_sampling_rate` in profile schema) | No -- correctly sourced from profile; the constructor default is a safe fallback |
| `pipelines/turn_ingest/pipeline.py:168` | `superseded_factor` fallback | `0.3` | Confidence decay factor when no autorecall policy loaded | Yes (documented as `AutorecallPolicy.superseded_confidence_factor` default) | No -- fallback matches config default |

##### LLM Prompt Templates (Cognee Tasks)

| Location | Parameter | Value | Controls | In CONFIGURATION.md? | Should be configurable? |
|----------|-----------|-------|----------|----------------------|------------------------|
| `tasks/extract_facts.py:15-51` | `_SYSTEM_PROMPT_TEMPLATE` | Full system prompt with `{profile_name}`, `{focus_section}`, `{goal_section}`, `{valid_categories}`, `{max_facts}` | Fact extraction LLM behavior -- instruction text, output schema, decision domain taxonomy | No -- the prompt text itself is not documented | No -- prompt engineering is internal; parameters controlling it (max_facts, categories, focus) ARE documented |
| `tasks/extract_facts.py:53-79` | `_TOOL_SYSTEM_PROMPT_TEMPLATE` | Alternate system prompt for tool-output-only batches | Fact extraction for tool outputs (focuses on key results, errors, config values) | No | No -- same reasoning as above |
| `tasks/extract_facts.py:81-128` | `_RESPONSE_SCHEMA` | JSON schema with `facts[]`, `goal_status_hints[]`, per-fact fields | Structure of LLM response; defines valid `goal_relevance.strength` enum (`direct`, `indirect`, `none`) and `goal_status_hints.hint` enum (`completed`, `abandoned`, `blocked`, `progressed`, `refined`, `new_subgoal`) | No -- the enums for strength and hint types are not documented | Partially -- the enum values are architectural constants, not tunable, but they should be documented for operators who inspect trace events |
| `tasks/extract_facts.py:215-216` | Minimum batch chars threshold | `total_chars < 10` | Batches with <10 total chars skip extraction entirely | Yes (Section 15 of hardcoded audit: "Min message chars for extraction") | No -- trivial threshold, not worth externalizing |
| `tasks/extract_facts.py:258-260` | Input truncation method | `user_prompt[: int(len(user_prompt) * ratio)]` | Character-based proportional truncation when prompt exceeds `extraction_max_input_tokens` | No -- the truncation method is not documented | No -- internal implementation detail |
| `tasks/classify_memory.py:13-25` | `_CATEGORY_MAP` | 11-entry mapping (category -> MemoryClass) | Rule-based memory class assignment (constraint/procedure_ref -> POLICY, preference/identity/trait/relationship/project/system -> SEMANTIC, event/decision/verification -> EPISODIC) | No -- the complete category-to-class mapping is not documented | No -- spec-mandated mapping, but should be documented for operator reference |
| `tasks/classify_memory.py:31-36` | LLM classification prompt | `"Classify the following fact into one of: episodic, semantic, policy."` with `max_tokens=50` | Fallback LLM classification for `general` or unknown categories | No -- the fallback prompt and its token limit are not documented | No -- max_tokens=50 is inherently safe for a single-field JSON response |
| `tasks/summarize_artifact.py:33-34` | Truncation fallback length | `content[:200]` | Summary text length when content is below `summarization_min_artifact_chars` or LLM unavailable | Yes (Section 15 hardcoded audit) | No -- reasonable default |
| `tasks/summarize_artifact.py:41-43` | Summarization system prompt | `"You are a concise summarizer. Summarize the following tool output in 1-3 sentences..."` with `temperature=0.0` | LLM summarization behavior for artifacts | No -- the prompt text and fixed temperature=0.0 are not documented | Partially -- the fixed temperature=0.0 overrides the global LLM temperature; this is intentional (deterministic summaries) but should be noted |
| `tasks/resolve_actors.py:13` | Handle pattern regex | `r"@(\w+)"` | Pattern used to detect @mentions in messages | No | No -- standard convention |

##### API Routes

| Location | Parameter | Value | Controls | In CONFIGURATION.md? | Should be configurable? |
|----------|-----------|-------|----------|----------------------|------------------------|
| `routes/memory.py:36` | `SearchRequest.max_results` default | `20` | Default max results for `/memory/search` | Yes (Section 18) | No -- caller override via request body |
| `routes/memory.py:37` | `SearchRequest.min_score` default | `0.0` | Default minimum score filter | Yes (Section 18) | No -- caller override |
| `routes/memory.py:41` | `SearchRequest.auto_recall` default | `False` | Whether to use autorecall retrieval policy | No -- not documented as API param | No -- caller override |
| `routes/memory.py:64` | `IngestMessagesRequest.profile_name` default | `"coding"` | Default profile for ingest-messages endpoint | No -- not documented as API default | No -- matches `EB_DEFAULT_PROFILE` default |
| `routes/memory.py:145` | Fallback search score | `r.freshness_score or 0.5` | Score assigned to results when no orchestrator (fallback path) | No | No -- internal fallback |
| `routes/memory.py:145` | Fallback search source | `"hybrid"` | Source label for fallback search results | No | No -- internal label |
| `routes/memory.py:151` | `read_memory` default scope | `"session"` | Default scope for GET `/memory/read` | Yes (Section 18 mentions `100` limit) | No -- caller override |
| `routes/trace.py:16` | `list_traces` default limit | `100` | Pagination default for GET `/trace/` | Yes (Section 18) | No -- caller override |
| `routes/trace.py:39,49` | Timeline/summary query limit | `10000` | Internal limit for loading all events in a session | No -- this is an internal constant, not a pagination default | Potentially -- for sessions with >10000 events this silently truncates |
| `routes/trace.py:91` | `list_sessions` max limit | `le=1000` | Maximum allowed `limit` query param for session listing | No | No -- validation constraint |
| `routes/consolidation.py:43` | Reports list default limit | `10` | Default pagination for consolidation reports | Yes (Section 18) | No -- caller override |
| `routes/guards.py:62` | Guard history display limit | `events[:10]` | Max recent guard events in `/guards/active/{session_id}` response | Yes (Section 18) | No -- display truncation |
| `routes/guards.py:183` | Refresh default profile | `"coding"` | Default profile_name when refreshing guard rules | No | No -- caller provides in body |
| `routes/artifacts.py:22` | Artifact search max_results default | `10` | Default max results for `/artifacts/search` | No | No -- caller override |
| `routes/artifacts.py:85-86` | Auto-summary fallback | `body.summary or body.content[:200]` | Summary when caller provides no explicit summary | Yes (Section 18) | No -- sensible default |
| `routes/artifacts.py:89` | Token estimate formula | `len(body.content) // 4` | Rough token count for session artifacts | No -- the chars/4 method is not documented for this location | No -- matches the global `_CHARS_PER_TOKEN = 4` convention |
| `routes/working_set.py:19` | Build request default profile | `"coding"` | Default profile for working set build | No | No -- caller override |
| `routes/goals.py:121` | Session goal progress increment | `min(1.0, g.confidence + 0.1)` | Fixed confidence boost per progress event | No -- this hardcoded `0.1` is not documented | Yes -- should use `goal_refinement.progress_confidence_delta` (which defaults to `0.1`) but does not |
| `routes/sessions.py:83` | Subagent parent TTL | `consolidation_min_retention_seconds` (fallback: `172800`) | TTL for Redis subagent parent mapping | No -- the TTL source for this key is not documented | No -- correctly derives from config |
| `routes/health.py:16` | Health version string | `"0.1.0"` | Version reported by `/health/` endpoint | No | No -- should be auto-derived from package |
| `routes/admin.py:144` | Bootstrap admin authority | `90` | Authority level of synthetic bootstrap admin | No -- not documented as a configurable default | No -- bootstrap is a one-time operation |

##### Middleware

| Location | Parameter | Value | Controls | In CONFIGURATION.md? | Should be configurable? |
|----------|-----------|-------|----------|----------------------|------------------------|
| `middleware/gateway.py:22` | Default gateway_id | `"local"` | Fallback when `X-EB-Gateway-ID` header absent | Yes (documented) | Yes (already configurable via `EB_GATEWAY_ID`) |
| `middleware/gateway.py:27-31` | 5 header names | `X-EB-Gateway-ID`, `X-EB-Agent-Key`, `X-EB-Agent-ID`, `X-EB-Session-Key`, `X-EB-Actor-Id` | HTTP headers extracted into request state | Yes (documented in Gateway Identity section) | No -- protocol constants |
| `middleware/auth.py` | Auth behavior | Always passes (stub) | No authentication enforced | Yes (documented in Security Defaults) | N/A -- placeholder |

##### Server & CLI

| Location | Parameter | Value | Controls | In CONFIGURATION.md? | Should be configurable? |
|----------|-----------|-------|----------|----------------------|------------------------|
| `server.py:22` | Default host | `0.0.0.0` | Bind address for uvicorn | Yes (documented) | Yes (via `--host` flag) |
| `server.py:23` | Default port | `8420` | Bind port for uvicorn | Yes (documented) | Yes (via `--port` flag) |
| `server.py:24` | Default log level | `"info"` | Uvicorn log level | Yes (documented) | Yes (via `--log-level` flag) |
| `server.py:62` | Health check timeout | `5.0` | httpx timeout for health-check CLI command | Yes (Section 8 Timeouts) | No -- reasonable default |
| `cli.py:76` | CLI HTTP timeout | `10.0` | httpx timeout for all `ebrun` API calls | No -- not documented | Potentially -- slow networks may need longer |
| `cli.py:65` | Default runtime URL | `"http://localhost:8420"` | Fallback when no `--runtime-url` or `EB_RUNTIME_URL` | Yes (documented) | Yes (via flag/env) |
| `cli.py:144` | Bootstrap admin authority default | `90` | Default `--admin-authority` for bootstrap command | No -- not documented as a CLI default | No -- appropriate for initial admin |
| `app.py:41` | FastAPI app version | `"0.4.0"` | OpenAPI spec version field | No | No -- cosmetic |
| `app.py:41` | FastAPI app title | `"ElephantBroker"` | OpenAPI spec title | No | No -- cosmetic |

##### Token Estimation

| Location | Parameter | Value | Controls | In CONFIGURATION.md? | Should be configurable? |
|----------|-----------|-------|----------|----------------------|------------------------|
| `runtime/utils/tokens.py:5` | Tokenizer encoding | `"cl100k_base"` | tiktoken encoding used for `count_tokens()` | No -- the specific encoding is not documented | Potentially -- different models use different tokenizers, but cl100k_base is broadly applicable |
| `runtime/utils/tokens.py:14` | Fallback ratio | `len(text) // 4` | Token estimation when tiktoken unavailable | Yes (documented in hardcoded audit as `_CHARS_PER_TOKEN = 4`) | No -- fallback heuristic |
| `runtime/compaction/engine.py:68` | `_CHARS_PER_TOKEN` | `4` | Token estimation constant | Yes (Section 3 hardcoded audit) | No -- well-documented |
| `runtime/context/assembler.py:39-41` | `_estimate_tokens` | `len(text) // 4` | Assembly token estimation | Yes (Section 6 hardcoded audit) | No -- matches global convention |

#### Highest-Priority Gaps (items that operators might need to know)

1. **`routes/goals.py:121` -- hardcoded `0.1` confidence increment**: The session goal progress endpoint uses `g.confidence + 0.1` but does not read `goal_refinement.progress_confidence_delta` from config. This is a functional mismatch -- the config parameter exists (default `0.1`) but the API route does not use it. Operators who change `progress_confidence_delta` in their profile will see the change take effect in Tier 2 LLM refinement but NOT in the manual progress endpoint.

2. **`routes/trace.py:39,49` -- hardcoded `limit=10000` for timeline/summary**: This caps the number of events loaded for a single session's timeline or summary. Long-running sessions could exceed this silently. Not documented.

3. **`tasks/classify_memory.py` category-to-class mapping**: The rule table mapping fact categories to memory classes (POLICY/SEMANTIC/EPISODIC) is a core behavioral constant that affects how facts are classified. While the mechanism is mentioned in local/IMPLEMENTED docs, the specific mapping table is not in CONFIGURATION.md.

4. **`tasks/summarize_artifact.py:43` fixed `temperature=0.0`**: The artifact summarization LLM call overrides the global temperature setting with a hardcoded 0.0. This is intentional but not documented. Operators who set `EB_LLM_TEMPERATURE=0.3` expecting it to apply everywhere would not know artifact summarization is exempt.

5. **`cli.py:76` -- `10.0s` HTTP timeout for ebrun**: All `ebrun` CLI API calls use a hardcoded 10-second timeout. Not documented and not overridable. Could cause issues on slow networks or during long-running operations like bootstrap.

6. **LLM prompt template enums**: The `goal_status_hints.hint` enum values (`completed`, `abandoned`, `blocked`, `progressed`, `refined`, `new_subgoal`) and `goal_relevance.strength` enum values (`direct`, `indirect`, `none`) appear in trace events and affect goal processing but are not documented in CONFIGURATION.md.

7. **Fact domain taxonomy in prompts**: The extraction prompt includes a hardcoded decision domain taxonomy (`financial, data_access, communication, code_change, scope_change, resource, info_share, delegation, record_mutation, uncategorized`). This is the same list as the `DecisionDomain` enum in `schemas/guards.py` but is duplicated as literal text in the prompt.


### 13.3 TypeScript Plugin Gaps


##### Summary

After reading every TypeScript file across both plugins (35 files total, with `src/` as canonical) and cross-referencing against CONFIGURATION.md Section 7 "TypeScript Plugin Configuration", I found the following hardcoded values. The CONFIGURATION.md Section 7.4 "Hardcoded Values" already documents many of these, but several are missing and several others documented there deserve deeper analysis about whether they should be in `configSchema`.

##### Hardcoded Values NOT in CONFIGURATION.md

| # | File:Line | Value | What it controls | In CONFIGURATION.md? | Should be in configSchema? | Impact if changed |
|---|-----------|-------|-----------------|----------------------|----------------------------|-------------------|
| 1 | `memory/src/client.ts:28` | `"agent:main:main"` | Client-internal default `currentSessionKey` (separate from index.ts:47 which IS documented) | No (only index.ts:47 documented) | No -- internal state, overwritten by hooks | Mismatch with engine default would cause split-brain session routing |
| 2 | `memory/src/client.ts:39` | `/.$/` (regex: `baseUrl.replace(/\/$/, "")`) | Strips trailing slash from baseUrl | No | No -- defensive normalization | Without it, double-slash URLs like `http://host:8420//memory/search` |
| 3 | `memory/src/client.ts:332` | `true` | Default `enabled` for created procedures (`request.enabled ?? true`) | No | No -- semantic default (procedures start enabled) | `false` would create disabled-by-default procedures |
| 4 | `memory/src/client.ts:335` | `false` | Default `is_optional` for procedure steps (`s.is_optional \|\| false`) | No | No -- semantic default | `true` would make all steps optional by default |
| 5 | `context/src/client.ts:37` | `/.$/` (regex: same trailing-slash strip) | Same baseUrl normalization | No | No -- defensive | Same as #2 |
| 6 | `context/src/client.ts:79` | `{ bootstrapped: false, reason: "Runtime error" }` | Fallback return on bootstrap failure | No | No -- error handling | If bootstrap silently fails, engine proceeds in degraded state with no error propagation |
| 7 | `context/src/client.ts:94` | `{ ingested_count: 0 }` | Fallback return on ingestBatch failure | No | No -- error handling | Messages silently dropped on network failure |
| 8 | `context/src/client.ts:109` | `{ messages: params.messages \|\| [], estimated_tokens: 0 }` | Fallback return on assemble failure | No | No -- error handling | Returns unmodified messages with 0 token estimate -- LLM gets raw unassembled context |
| 9 | `context/src/client.ts:140` | `{ ok: false, compacted: false, reason: "Runtime error" }` | Fallback return on compact failure | No | No -- error handling | Compaction silently fails, context grows unbounded |
| 10 | `context/src/client.ts:168` | `{ parent_session_key: ..., child_session_key: ..., rollback_key: "", parent_mapping_stored: false }` | Fallback return on subagentSpawn failure | No | No -- error handling | Subagent spawned without parent context mapping |
| 11 | `context/src/engine.ts:39-43` | `{ id: "elephantbroker-context", name: "ElephantBroker ContextEngine", ownsCompaction: true }` | Engine info object exposed to OpenClaw | No | No -- identity (but `ownsCompaction: true` is behavioral) | `ownsCompaction: false` would make OpenClaw handle compaction itself |
| 12 | `context/src/engine.ts:96` | `"main"` | Default agentId in bootstrap (`this.setAgentIdentity("main", ...)`) | No | No -- convention | Different value breaks agent key derivation pattern |
| 13 | `context/src/engine.ts:103` | `""` | Empty string for `gateway_id` in bootstrap payload (relies on middleware) | No | No -- architectural | Runtime middleware fills this from header; value here is ignored |
| 14 | `context/src/engine.ts:106` | `false` | Default `is_subagent` in bootstrap | No | No -- semantic default | `true` would treat primary session as subagent |
| 15 | `context/src/engine.ts:130` | `false` | Default `is_heartbeat` in ingestBatch | No | No -- semantic default | `true` would mark all ingested messages as heartbeats |
| 16 | `context/src/engine.ts:142` | `""` | Default `query` in assemble params | No | No -- empty query is valid | Non-empty would bias assembly toward a query |
| 17 | `context/src/engine.ts:177` | `[]` | Empty `messages` array in afterTurn | No | No -- structural placeholder | OpenClaw passes no messages here; server ignores |
| 18 | `context/src/engine.ts:178` | `0` | Default `pre_prompt_message_count` in afterTurn | No | No -- tracking disabled | Non-zero would skew successful-use tracking |
| 19 | `context/src/engine.ts:194` | `"completed"` | Default `reason` for onSubagentEnded | No | Yes -- behavioral | Other values: `"deleted"`, `"swept"`, `"released"` affect how parent session resumes context |
| 20 | `context/src/engine.ts:200` | `"agent:main:main"` | Reset value for sessionKey in dispose() | No (documented at engine.ts:47 for initial, not for reset) | No -- matches initial | Different reset value would leave stale session identity |
| 21 | `context/src/engine.ts:218` | `"unknown"` | Default provider string when LLM event has no provider | No | No -- reporting fallback | Shows "unknown" in metrics/traces |
| 22 | `context/src/engine.ts:219` | `"unknown"` | Default model string when LLM event has no model | No | No -- reporting fallback | Shows "unknown" in metrics/traces |
| 23 | `memory/src/tools/create_artifact.ts:62` | `"manual"` | Default tool_name in create_artifact tool execute (duplicates client.ts:518 which IS documented) | No (only client.ts documented) | Already in configSchema consideration | N/A |
| 24 | `memory/src/tools/create_artifact.ts:63` | `"session"` | Default scope in create_artifact tool execute (duplicates client.ts:519 which IS documented) | No (only client.ts documented) | Already in configSchema consideration | N/A |
| 25 | `memory/src/tools/create_artifact.ts:73` | `"unknown"` | Fallback artifact_id when response lacks it | No | No -- defensive | Shows "unknown" instead of actual ID if server response malformed |
| 26 | `memory/src/tools/create_artifact.ts:77-78` | `"Artifact saved permanently to knowledge graph"` / `"Artifact saved for this session (auto-expires)"` | User-facing success messages | No | No -- display strings | Changing affects what the LLM "sees" as tool output |
| 27 | `memory/src/tools/artifact_search.ts:99` | `0` | Default `token_estimate` for session artifacts in search results | No | No -- structural zero | Session artifacts don't have token estimates |
| 28 | `memory/src/tools/memory_search.ts:26-36` | Field selection: `id`, `text`, `category`, `memory_class`, `confidence`, `score`, `created_at` | Which FactAssertion fields are returned to LLM in search results | No | No -- but controls LLM-visible information | Adding/removing fields changes what the LLM can reference |
| 29 | `memory/src/tools/memory_forget.ts:29` | `1` | `max_results: 1` for search-then-delete query | No | No -- correct behavior (delete best match only) | Higher value would delete multiple facts |
| 30 | `memory/src/tools/memory_update.ts:23` | `1` | `max_results: 1` for search-then-update query | No | No -- correct behavior (update best match only) | Higher value not useful (only uses first) |

##### Hardcoded Values Already in CONFIGURATION.md but Worth Reviewing for configSchema

| # | File:Line | Value | Documented? | Should be in configSchema? | Reasoning |
|---|-----------|-------|-------------|----------------------------|-----------|
| 1 | `memory/src/index.ts:115` | `10` (auto-recall max_results) | Yes | **YES** | Deployments with large memory stores may want 5 (cheaper) or 20 (richer). Currently requires code change. |
| 2 | `memory/src/index.ts:133` | `4` (messages.slice(-4) for ingest) | Yes | **YES** | Controls how much conversation context is sent for fact extraction. Short conversations may want 2; long multi-tool turns may want 8. |
| 3 | `memory/src/tools/memory_forget.ts:30` | `0.7` (score threshold for delete) | Yes | **YES** | Too low = accidentally deletes wrong fact. Too high = "no match" when user clearly specified a query. Deployments with noisy embeddings may want 0.5. |
| 4 | `memory/src/tools/memory_update.ts:24` | `0.7` (score threshold for update) | Yes | **YES** | Same reasoning as forget threshold. |
| 5 | `memory/src/tools/memory_forget.ts:33` | `.slice(0, 80)` (truncation in response) | Yes | No | Display-only, low impact. |
| 6 | `memory/src/tools/memory_store.ts:24` | `"general"` (default category) | Yes | Borderline | Deployments might want a different default category, but "general" is a safe universal default. |
| 7 | `memory/src/tools/artifact_search.ts:44` | `5` / `50` (default/max results) | Yes | **YES** (at least max cap) | Max cap of 50 prevents abuse, but some deployments may want 100 for artifact-heavy workflows. |
| 8 | `memory/src/tools/goal_create.ts:58` | `"session"` (default scope) | Yes | No | Session is the only safe default scope. |
| 9 | `context/src/engine.ts:63` | `6` (batchSize) | Yes | Already partially -- fetched from `/context/config` at startup. | The fallback default when `/context/config` fails should match server-side config. |
| 10 | `context/src/engine.ts:220` | `128000` (fallback context_window_tokens) | Yes | **YES** | This is the most impactful hardcoded value. Wrong value = compaction triggers at wrong threshold. Models with 200k windows (Claude) or 32k (smaller models) get wrong budget. |
| 11 | `context/src/client.ts:208` | `6` / `60000` (fallback batch size/timeout) | Yes | No -- only used when server is unreachable, and server config takes precedence. | Changing without matching server = inconsistent behavior. |

##### Missing from CONFIGURATION.md: OTEL Tracer/Span Names

| # | File:Line | Value | Type | In CONFIGURATION.md? |
|---|-----------|-------|------|----------------------|
| 1 | `memory/src/client.ts:23` | `"elephantbroker.memory-plugin"` | Tracer name | Yes (Section 10) |
| 2 | `context/src/client.ts:25` | `"elephantbroker.context-engine-plugin"` | Tracer name | Yes (Section 10) |
| 3 | `memory/src/client.ts:69` | `"memory.search"` | Span name | No |
| 4 | `memory/src/client.ts:85` | `"memory.store"` | Span name | No |
| 5 | `memory/src/client.ts:101` | `"memory.getById"` | Span name | No |
| 6 | `memory/src/client.ts:116` | `"memory.forget"` | Span name | No |
| 7 | `memory/src/client.ts:131` | `"memory.update"` | Span name | No |
| 8 | `memory/src/client.ts:148` | `"memory.ingestMessages"` | Span name | No |
| 9 | `memory/src/client.ts:162` | `"memory.sessionStart"` | Span name | No |
| 10 | `memory/src/client.ts:176` | `"memory.sessionEnd"` | Span name | No |
| 11 | `memory/src/client.ts:193` | `"goals.listSession"` | Span name | No |
| 12 | `memory/src/client.ts:216` | `"goals.createSession"` | Span name | No |
| 13 | `memory/src/client.ts:244` | `"goals.updateSessionStatus"` | Span name | No |
| 14 | `memory/src/client.ts:271` | `"goals.addSessionBlocker"` | Span name | No |
| 15 | `memory/src/client.ts:295` | `"goals.recordSessionProgress"` | Span name | No |
| 16 | `memory/src/client.ts:324` | `"procedures.create"` | Span name | No |
| 17 | `memory/src/client.ts:353` | `"procedures.activate"` | Span name | No |
| 18 | `memory/src/client.ts:374` | `"procedures.completeStep"` | Span name | No |
| 19 | `memory/src/client.ts:397` | `"procedures.sessionStatus"` | Span name | No |
| 20 | `memory/src/client.ts:417` | `"guards.getActive"` | Span name | No |
| 21 | `memory/src/client.ts:432` | `"guards.getEventDetail"` | Span name | No |
| 22 | `memory/src/client.ts:453` | `"artifacts.search"` | Span name | No |
| 23 | `memory/src/client.ts:469` | `"artifacts.searchSession"` | Span name | No |
| 24 | `memory/src/client.ts:491` | `"artifacts.getSession"` | Span name | No |
| 25 | `memory/src/client.ts:511` | `"artifacts.create"` | Span name | No |
| 26 | `memory/src/client.ts:571` | `"admin.createPersistentGoal"` | Span name | No |
| 27 | `memory/src/client.ts:582` | `"admin.createOrg"` | Span name | No |
| 28 | `memory/src/client.ts:592` | `"admin.createTeam"` | Span name | No |
| 29 | `memory/src/client.ts:607` | `"admin.registerActor"` | Span name | No |
| 30 | `memory/src/client.ts:617` | `"admin.addMember"` | Span name | No |
| 31 | `memory/src/client.ts:629` | `"admin.removeMember"` | Span name | No |
| 32 | `memory/src/client.ts:639` | `"admin.mergeActors"` | Span name | No |
| 33 | `context/src/client.ts:70` | `"context.bootstrap"` | Span name | No |
| 34 | `context/src/client.ts:85` | `"context.ingestBatch"` | Span name | No |
| 35 | `context/src/client.ts:100` | `"context.assemble"` | Span name | No |
| 36 | `context/src/client.ts:115` | `"context.buildOverlay"` | Span name | No |
| 37 | `context/src/client.ts:131` | `"context.compact"` | Span name | No |
| 38 | `context/src/client.ts:146` | `"context.afterTurn"` | Span name | No |
| 39 | `context/src/client.ts:159` | `"context.subagentSpawn"` | Span name | No |
| 40 | `context/src/client.ts:174` | `"context.subagentEnded"` | Span name | No |
| 41 | `context/src/client.ts:187` | `"context.dispose"` | Span name | No |
| 42 | `context/src/client.ts:201` | `"context.getConfig"` | Span name | No |

##### Missing from CONFIGURATION.md: Log Prefixes

All log messages use `[EB]` prefix (memory plugin) or `[ElephantBroker]` prefix (context engine degraded mode warning at `engine.ts:113`). The inconsistency between `[EB]` and `[ElephantBroker]` is undocumented.

##### Missing from CONFIGURATION.md: HTTP Behavior

| # | Observation | File(s) | Impact |
|---|-------------|---------|--------|
| 1 | **No HTTP timeout configured** -- all `fetch()` calls use default timeout (browser: none; Node.js 18+: 5 min default) | All client.ts files | Hung backend = hung plugin. No way to configure request timeout. |
| 2 | **No retry logic** -- all HTTP calls are single-attempt. Network blip = lost request. | All client.ts files | Transient failures cause silent data loss (especially fire-and-forget calls like ingest, token usage reporting). |
| 3 | **No AbortSignal usage** -- tool execute methods receive `signal?: AbortSignal` but never pass it to `fetch()` | All tool files | User cancellation cannot abort in-flight HTTP requests. |
| 4 | **Error messages expose HTTP status codes** -- e.g., `throw new Error(\`Search failed: ${res.status}\`)` | All client.ts files | Status codes leak to LLM via tool error responses. Not a security issue per se, but reveals implementation detail to the model. |

##### Top Priority Items for configSchema Addition

Based on the analysis, these 5 values have the highest operational impact and are currently not configurable without code changes:

1. **`128000` (context window fallback)** -- `context/src/engine.ts:220` -- Wrong value means compaction triggers at wrong threshold for the actual model being used.
2. **`10` (auto-recall max_results)** -- `memory/src/index.ts:115` -- Controls how much memory context is injected per turn. Directly affects prompt size and retrieval quality.
3. **`4` (ingest message window)** -- `memory/src/index.ts:133` -- Controls how much conversation is sent for fact extraction. Too few = missed facts. Too many = noise.
4. **`0.7` (forget/update score threshold)** -- `memory/src/tools/memory_forget.ts:30`, `memory_update.ts:24` -- Safety threshold that determines whether a search-then-modify operation proceeds.
5. **HTTP timeout** (absent) -- All `fetch()` calls -- No configurable timeout means a hung backend hangs the plugin indefinitely.


### 13.4 Deployment & Security Gaps


##### 1. Missing Security Hardening

###### 1.1 API Authentication is a No-Op Stub

**File:** `elephantbroker/api/middleware/auth.py`

The `AuthMiddleware` is explicitly a stub that always passes every request:

```python
class AuthMiddleware(BaseHTTPMiddleware):
    """Stub: always passes. Real auth in a future phase."""
    async def dispatch(self, request: Request, call_next) -> Response:
        return await call_next(request)
```

**Gaps not documented in CONFIGURATION.md:**
- No API key validation, JWT/OAuth2 support, or bearer token scheme
- No request signing or mutual authentication between gateway and runtime
- No rate limiting or throttling on any endpoint
- No CORS middleware configured (no `CORSMiddleware` found in codebase)
- Gateway identity headers (`X-EB-Gateway-ID`, etc.) fall back to `"local"` with no verification -- any caller can impersonate any gateway

###### 1.2 TLS Absent Everywhere

CONFIGURATION.md section 7 correctly notes "No TLS configuration exists in any component" but does not prescribe what to do about it. Missing:
- No guidance on deploying a reverse proxy (nginx/Caddy/Traefik) in front of ports 8420/8421
- No TLS termination config for Neo4j Bolt (`bolt://` vs `bolt+s://`)
- No TLS config for Qdrant (`http://` vs `https://`)
- No TLS config for Redis (`redis://` vs `rediss://`)
- No OTEL collector TLS (explicitly `insecure: true` between collector and Jaeger)
- No certificate management or rotation guidance

###### 1.3 Database Authentication Weak or Missing

| Service | Current State | Gap |
|---------|--------------|-----|
| Neo4j | Hardcoded `neo4j/elephant_dev` in `default.yaml` and `docker-compose.yml` | No guidance for rotating credentials, no secret management integration |
| Redis | No auth configured at all | No `requirepass`, no ACL, no `--requirepass` in compose |
| Qdrant | No auth configured | No API key (`--api-key`) |
| ClickHouse | Empty password (`CLICKHOUSE_PASSWORD: ""`) | Labeled "dev/staging only" but no production alternative documented |
| HITL | HMAC shared secret via env var | No key rotation procedure |

###### 1.4 Docker Compose Network Isolation Missing

**File:** `infrastructure/docker-compose.yml`

- No custom Docker networks defined -- all services share the default bridge
- No `internal: true` network to restrict database containers from outbound access
- All database ports are published to the host (e.g., `17474:7474`, `16333:6333`, `16379:6379`) -- should be exposed only to the runtime, not to `0.0.0.0`
- No `expose:` (container-only) vs `ports:` (host-published) separation

###### 1.5 Container Runs as Root

**File:** `Dockerfile`

- No `USER` directive -- container runs as `root`
- No `adduser`/`useradd` to create a non-root service user
- No `--chown` on COPY directives
- No `HEALTHCHECK` instruction in Dockerfile
- No read-only root filesystem (`--read-only`)

###### 1.6 systemd Units Lack Hardening

**File referenced in:** `DEPLOYMENT.md` lines 122-157

The documented systemd units are minimal. Missing:
- `ProtectSystem=strict`, `ProtectHome=true`, `NoNewPrivileges=true`
- `ReadWritePaths=/var/lib/elephantbroker` (restrict write access)
- `PrivateTmp=true`, `PrivateDevices=true`
- `MemoryLimit`, `CPUQuota` resource constraints
- `LimitNOFILE` (file descriptor limits for Neo4j connections)
- `TimeoutStartSec`, `TimeoutStopSec` (graceful shutdown budget)
- `StandardOutput=journal`, `StandardError=journal` (explicit log routing)
- Watchdog (`WatchdogSec`) for process liveness

---

##### 2. Missing Production Tuning

###### 2.1 Connection Pools Not Configured

**File:** `elephantbroker/runtime/container.py` (line 161)

```python
c.redis = aioredis.from_url(config.infra.redis_url, decode_responses=True)
```

No `max_connections`, `socket_timeout`, `socket_connect_timeout`, or `retry_on_timeout` parameters. The `redis.asyncio` default pool is unbounded.

Missing across all adapters:
- **Neo4j**: No connection pool size, max connection lifetime, or acquisition timeout documented (managed by Cognee, but not configurable)
- **Qdrant**: No connection pool or timeout settings exposed
- **httpx**: No pool limits for LLM/reranker/HITL HTTP clients
- **Redis**: No explicit `ConnectionPool` max connections, health check interval, or socket keepalive

###### 2.2 Uvicorn Single-Worker / No Tuning

CONFIGURATION.md notes "Workers: 1 (single-process)" but does not discuss:
- Whether to use `--workers N` or an external process manager (gunicorn with uvicorn workers)
- `--limit-concurrency` and `--limit-max-requests` for memory leak protection
- `--backlog` for connection queue sizing
- `--timeout-keep-alive` tuning
- `--access-log` vs structured JSON logging for production

###### 2.3 Docker Compose Resource Limits

**File:** `infrastructure/docker-compose.yml`

No resource constraints on any service:
- No `deploy.resources.limits` (memory/CPU caps)
- No `deploy.resources.reservations` (guaranteed minimums)
- No `ulimits` (nofile, memlock for Neo4j/Qdrant)
- No `restart:` policy on any service (containers stay down after crash)
- No `healthcheck:` on any service (compose won't detect unhealthy backends)
- No `logging:` driver configuration (defaults to json-file with no rotation)
- No `shm_size:` for Neo4j (may need larger shared memory)

###### 2.4 Neo4j Tuning Parameters Missing

No Neo4j JVM or server configuration:
- `NEO4J_server_memory_heap_initial__size` / `NEO4J_server_memory_heap_max__size`
- `NEO4J_server_memory_pagecache_size`
- `NEO4J_server_threads_worker__count`
- `NEO4J_db_tx__timeout`
- `NEO4J_server_bolt_thread__pool__size`
- Index creation guidance for common Cypher query patterns (gateway_id, scope, session_key, actor_id)

###### 2.5 Redis Tuning Missing

- No `maxmemory` policy (e.g., `allkeys-lru`)
- No `maxmemory` limit
- No `appendonly yes` for AOF persistence (currently relies on RDB only via default `redis:7-alpine`)
- No `tcp-keepalive` configuration
- No `save` configuration documented (RDB snapshot frequency)

###### 2.6 Qdrant Tuning Missing

- No `--max-request-size` configuration
- No HNSW index parameters (m, ef_construct) guidance
- No WAL configuration
- No snapshot/backup configuration

###### 2.7 OTEL Span Processor

**File:** `elephantbroker/runtime/observability.py` (line 50)

Uses `SimpleSpanProcessor` which exports spans synchronously (blocks the request thread). Production should use `BatchSpanProcessor` with configurable `max_queue_size`, `schedule_delay_millis`, and `max_export_batch_size`.

---

##### 3. Missing Monitoring Integration

###### 3.1 No Grafana Dashboards

Grafana is included in docker-compose under the observability profile, but:
- No provisioned dashboards (`/var/lib/grafana/dashboards/`)
- No datasource provisioning (Jaeger, ClickHouse, Prometheus not auto-configured)
- No dashboard JSON definitions for the 100+ Prometheus metrics defined in `metrics.py`
- The Grafana container has no volume mount for persistent dashboard storage

###### 3.2 No Prometheus Scrape Configuration

Prometheus metrics are exposed at `/metrics` (via `elephantbroker/api/routes/metrics.py`), but:
- No Prometheus server in docker-compose to scrape them
- No scrape configuration or `prometheus.yml` provided
- No service discovery configuration
- Metrics are only available if something external scrapes port 8420

###### 3.3 No Alerting Rules

No alerting configuration anywhere:
- No Prometheus alerting rules (e.g., `eb_backend_health == 0`, high error rates, pipeline latency SLOs)
- No Grafana alert definitions
- No AlertManager configuration
- No PagerDuty/Slack/webhook notification channel setup
- No alert for: Neo4j/Qdrant/Redis down, guard hard-stop rate spike, LLM error rate, embedding cache miss rate, consolidation failures

###### 3.4 No Log Aggregation Beyond ClickHouse

- ClickHouse receives OTEL logs but there is no query/alerting layer on top
- No log rotation for Python runtime stdout/stderr (relies on systemd journal defaults)
- No structured JSON logging configuration for production (currently uses Python standard logging)
- No log level override mechanism at runtime (requires restart)

---

##### 4. Missing Backup and Recovery Procedures

###### 4.1 Neo4j Backup

No procedures documented for:
- Online backup (`neo4j-admin database dump`)
- Point-in-time recovery
- Backup scheduling (cron job example)
- Backup verification (restore test procedure)
- Transaction log retention for incremental backup
- Note: Community Edition has limited backup capabilities vs Enterprise

###### 4.2 Qdrant Backup

- No snapshot creation procedure (`POST /snapshots`)
- No snapshot export/restore workflow
- No collection-level backup strategy
- Qdrant v1.17.0 snapshot API usage not documented

###### 4.3 Redis Backup

- No RDB snapshot schedule documented
- No AOF configuration
- No `BGSAVE` procedure or cron job
- No backup export procedure (copy RDB file from Docker volume)
- Redis data loss impact analysis missing (which data is ephemeral cache vs critical state)

###### 4.4 SQLite Audit Database Backup

Seven SQLite databases under `data/`:
```
data/procedure_audit.db
data/session_goals_audit.db
data/org_overrides.db
data/authority_rules.db
data/consolidation_reports.db
data/tuning_deltas.db
data/scoring_ledger.db
```

- No backup procedure (simple file copy while runtime is stopped, or `.backup` SQL command)
- No retention management beyond `audit.retention_days: 90` (no pruning job documented)
- No recovery procedure from corrupt SQLite file

###### 4.5 ClickHouse Backup

- 72-hour TTL configured but no long-term log archive
- No backup procedure for analytics data
- No schema migration guidance

###### 4.6 Cognee Internal State

- Cognee creates `.cognee_system/databases` inside its package directory
- No backup guidance for Cognee's internal SQLite state
- Unknown recovery impact if this state is lost

###### 4.7 Disaster Recovery

- No documented RTO/RPO targets
- No full-system restore procedure (all services from scratch)
- No data integrity verification after restore
- No bootstrap re-run guidance (noted as "one-shot" -- fails if graph is not empty)

---

##### 5. Missing Version Compatibility Matrix

No explicit compatibility matrix exists. Versions are scattered across files:

| Component | Specified Where | Version/Constraint | Tested With |
|-----------|----------------|-------------------|-------------|
| Python | `pyproject.toml` | `>=3.11` | "3.12 tested" (DEPLOYMENT.md) |
| Neo4j | `docker-compose.yml` | `neo4j:5-community` (any 5.x) | Not pinned |
| Qdrant server | `docker-compose.yml` | `v1.17.0` (pinned) | v1.17.0 |
| qdrant-client | `pyproject.toml` | `>=1.7` (unbounded upper) | Not documented |
| Redis server | `docker-compose.yml` | `redis:7-alpine` (any 7.x) | Not documented |
| redis (Python) | `pyproject.toml` | `>=5.0` (unbounded upper) | Not documented |
| Cognee | `pyproject.toml` | `==0.5.3` (exact pin) | 0.5.3 |
| FastAPI | `pyproject.toml` | `>=0.110` | Not documented |
| Pydantic | `pyproject.toml` | `>=2.0,<3.0` | Not documented |
| OTEL Collector | `docker-compose.yml` | `latest` (unpinned) | Not documented |
| ClickHouse | `docker-compose.yml` | `latest` (unpinned) | Not documented |
| Jaeger | `docker-compose.yml` | `latest` (unpinned) | Not documented |
| Grafana | `docker-compose.yml` | `latest` (unpinned) | Not documented |
| Node.js | `DEPLOYMENT.md` | `18+` | Not documented |

**Gaps:**
- Three observability images use `latest` tag -- not reproducible
- No upper bound on `qdrant-client`, `redis`, `fastapi`, `uvicorn` -- breaking changes possible
- No documented compatibility between Neo4j 5.x minor versions and the Cognee neo4j driver
- No Python 3.13 compatibility status
- No LiteLLM version requirement documented (external dependency)
- Qdrant server/client pairing note exists in DEPLOYMENT.md "Known Gotchas" but not as a formal matrix

---

##### 6. Missing Resource Sizing Guide

No guidance on hardware requirements for any component:

| Component | Missing Guidance |
|-----------|-----------------|
| **Runtime (Python)** | RAM requirements (baseline + per-session overhead), CPU requirements (single-threaded by default), disk for SQLite audit DBs |
| **Neo4j** | Heap size, page cache size, disk IOPS, expected graph size growth rate |
| **Qdrant** | RAM per collection, disk usage per vector, HNSW memory overhead, expected collection sizes per gateway |
| **Redis** | Memory per session key set (~10 keys per session), maxmemory planning based on `max_concurrent_sessions: 100` |
| **ClickHouse** | Disk growth rate at given trace/log volume, RAM for merge operations |
| **LLM Proxy** | Concurrent request capacity, token throughput requirements |
| **Reranker** | GPU/VRAM requirements for Qwen3-Reranker-4B, batch sizing impact on memory |
| **Overall** | Small/medium/large deployment profiles, single-node vs distributed thresholds |

---

##### 7. Missing Upgrade and Migration Procedures

###### 7.1 Runtime Upgrade

DEPLOYMENT.md and `deploy/UPDATING-DEPS.md` document the `deploy/update.sh`
flow (`uv sync --frozen` for code-only updates, `--upgrade` to regenerate
the lockfile against pyproject.toml). Still missing:
- Schema migration procedure (the `elephantbroker migrate` CLI command exists per CONFIGURATION.md line 3666, but no documentation of what migrations exist or how to run them pre-upgrade)
- Rollback procedure (how to revert to previous version — git checkout the previous commit + `update.sh` works but is undocumented)
- Pre-upgrade checklist (backup state, drain sessions, verify health)
- Zero-downtime upgrade strategy (not possible with single-worker uvicorn)
- Changelog or breaking-change notification mechanism

###### 7.2 Neo4j Upgrade

- No procedure for Neo4j 5.x minor version upgrades
- No guidance on Neo4j 5.x to 6.x future migration
- No schema evolution strategy for graph node properties
- No index management (create/drop) during upgrades

###### 7.3 Qdrant Upgrade

- Qdrant is pinned to v1.17.0 but no upgrade procedure to newer versions
- No collection schema migration guidance
- No reindexing procedure if embedding dimensions change
- DEPLOYMENT.md gotcha #9 mentions server/client pairing but not how to actually perform the upgrade

###### 7.4 Redis Upgrade

- No Redis 7.x to 8.x migration guidance
- No key format versioning (all keys are `eb:{gateway_id}:...` with no version prefix)
- No data migration if key schemas change between ElephantBroker versions

###### 7.5 Cognee Upgrade

- Cognee is pinned to `==0.5.3` -- what breaks if upgraded?
- No documented breaking changes between Cognee 0.5.x versions
- No migration path for Cognee's internal state databases
- The mistralai workaround is no longer needed when using uv (uv resolves cleanly), but if Cognee changes its transitive dep tree, the install.sh belt-and-suspenders cleanup may need updating

###### 7.6 Multi-Gateway Migration

- No procedure for adding a second gateway to an existing deployment
- No data isolation verification between gateways sharing the same Neo4j/Qdrant/Redis
- No gateway decommissioning procedure (cleanup of gateway-scoped data)

---

##### 8. Additional Gaps

###### 8.1 HITL Middleware Deployment Gaps

- HITL Dockerfile exists (`hitl-middleware/Dockerfile`) but is not referenced in docker-compose.yml
- No health check endpoint documented for HITL (exists at `/health` per DEPLOYMENT.md line 179 but not in compose)
- Webhook configuration (`WebhookConfig` in `hitl-middleware/hitl_middleware/config.py`) cannot be set via environment variables -- the HITL middleware's own `HitlMiddlewareConfig.from_env()` (separate from the runtime's `load()` path) sets 5 basic fields, not webhook endpoints
- No guidance on securing webhook endpoints (TLS, auth headers)

###### 8.2 Jaeger Storage

Jaeger uses `SPAN_STORAGE_TYPE: memory` -- all traces are lost on container restart. No guidance on:
- Switching to persistent storage (Cassandra, Elasticsearch, ClickHouse)
- Trace retention policy
- Jaeger query performance at scale

###### 8.3 Docker Volume Management

- No volume backup procedure (generic `docker volume` commands)
- No volume driver configuration (local default, no guidance on NFS/EBS/etc. for shared storage)
- `docker compose down -v` destroys all data -- no safeguard documented against accidental volume deletion

###### 8.4 Environment Variable Documentation Gap

CONFIGURATION.md documents 72 env vars via `ENV_OVERRIDE_BINDINGS` but:
- No `.env.example` file in the repository
- No env var validation at startup (invalid values may cause silent failures)
- `EB_HITL_CALLBACK_SECRET` generation procedure (`openssl rand -hex 32`) is in DEPLOYMENT.md but not in CONFIGURATION.md


### 13.5 Consolidation Pipeline Gaps


#### Summary

All 22 `ConsolidationConfig` schema parameters are documented in CONFIGURATION.md. However, there are **28+ parameters/constants hardcoded in stage code, supporting modules, and LLM prompt templates** that are NOT in `ConsolidationConfig` and have varying degrees of documentation in CONFIGURATION.md.

---

#### 1. ConsolidationConfig Schema Parameters (All Documented)

Every field in `elephantbroker/schemas/consolidation.py:ConsolidationConfig` is fully documented in CONFIGURATION.md sections 862-941. No gaps here.

| Parameter | Default | In Config? | In CONFIGURATION.md? | Status |
|-----------|---------|------------|---------------------|--------|
| `batch_size` | 500 | Yes | Yes (line 874) | OK |
| `active_session_protection_hours` | 1.0 | Yes | Yes (line 875) | OK |
| `cluster_similarity_threshold` | 0.92 | Yes | Yes (line 881) | OK |
| `canonicalize_divergence_threshold` | 0.7 | Yes | Yes (line 887) | OK |
| `strengthen_success_ratio_threshold` | 0.5 | Yes | Yes (line 893) | OK |
| `strengthen_min_use_count` | 3 | Yes | Yes (line 894) | OK |
| `strengthen_boost_factor` | 0.1 | Yes | Yes (line 895) | OK |
| `decay_recalled_unused_factor` | 0.85 | Yes | Yes (line 901) | OK |
| `decay_never_recalled_factor` | 0.95 | Yes | Yes (line 902) | OK |
| `decay_archival_threshold` | 0.05 | Yes | Yes (line 903) | OK |
| `decay_scope_multipliers` | dict (5 entries) | Yes | Yes (line 904) | OK |
| `autorecall_blacklist_min_recalls` | 5 | Yes | Yes (line 910) | OK |
| `autorecall_blacklist_max_success_ratio` | 0.0 | Yes | Yes (line 911) | OK |
| `promote_session_threshold` | 3 | Yes | Yes (line 917) | OK |
| `promote_artifact_injected_threshold` | 3 | Yes | Yes (line 918) | OK |
| `pattern_recurrence_threshold` | 3 | Yes | Yes (line 924) | OK |
| `pattern_min_steps` | 3 | Yes | Yes (line 925) | OK |
| `max_patterns_per_run` | 10 | Yes | Yes (line 926) | OK |
| `ema_alpha` | 0.3 | Yes | Yes (line 932) | OK |
| `max_weight_adjustment_pct` | 0.05 | Yes | Yes (line 933) | OK |
| `min_correlation_samples` | 20 | Yes | Yes (line 934) | OK |
| `llm_calls_per_run_cap` | 50 | Yes | Yes (line 940) | OK |
| `dev_auto_trigger_interval` | "0" | Yes | Yes (line 941) | OK |

---

#### 2. Engine-Level Hardcoded Constants

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| Redis lock timeout | engine.py:229 | `3600` (1 hour) | No | Partially (line 2770, 2791 in Redis tables) | **PARTIAL** -- mentioned in Redis key inventory but NOT in ConsolidationConfig section |
| Cleanup retention_days fallback | engine.py:622 | `90` | No (uses `audit.retention_days`) | Yes (line 181 in AuditConfig) | OK -- correctly delegates to AuditConfig |
| Cleanup retention_seconds conversion | engine.py:626 | `retention_days * 86400` | No | No explicit doc | Minor |
| Fallback LLM cap | engine.py:264 | `50` (fallback when config absent) | Matches schema default | Not separately documented | Minor |
| Fallback batch_size | engine.py:536 | `500` (fallback when config absent) | Matches schema default | Not separately documented | Minor |
| Fallback protection_hours | engine.py:537 | `1.0` (fallback when config absent) | Matches schema default | Not separately documented | Minor |

---

#### 3. Stage 2 (Canonicalize) -- LLM Prompt and Constants

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| `_MERGE_PROMPT` template | canonicalize.py:30-36 | Full prompt string | No | **No** | **GAP** |
| LLM system_prompt | canonicalize.py:124 | `"You are a knowledge synthesizer."` | No | **No** | **GAP** |
| LLM max_tokens | canonicalize.py:126 | `500` | No | **No** | **GAP** |
| Qdrant collection name | canonicalize.py:216 | `"FactDataPoint_text"` | No | Mentioned at line 3306 in separate section | Partial |
| SUPERSEDED_BY edge type | canonicalize.py:223 | `"SUPERSEDED_BY"` | No | Line 3263 in separate section | OK |

---

#### 4. Stage 4 (Decay) -- Implicit Constants

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| `recency_half_life_hours` fallback | decay.py:51 | `69.0` | No (read from `profile.scoring_weights`) | Yes, in scoring weights section (line 4983) | OK -- but fallback 69.0 not in consolidation docs |
| Minimum half_life clamp | decay.py:72 | `0.01` | No | **No** | **GAP** -- prevents division by zero, not documented |

---

#### 5. Stage 7 (Refine Procedures) -- LLM Prompt and Constants

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| `_PROCEDURE_PROMPT` template | refine_procedures.py:27-38 | Full prompt string | No | **No** | **GAP** |
| LLM system_prompt | refine_procedures.py:91 | `"You are a procedure definition generator."` | No | **No** | **GAP** |
| LLM max_tokens | refine_procedures.py:97 | `500` | No | **No** | **GAP** |
| Suggestion confidence formula | refine_procedures.py:110 | `min(0.9, 0.3 + 0.1 * session_count)` | No | Yes (line 5161) | OK -- in hardcoded constants section |

---

#### 6. Stage 9 (Recompute Salience) -- Implicit Constants

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| Spearman correlation significance threshold | recompute_salience.py:75 | `0.1` | No | Yes (line 5159) | OK -- but only in hardcoded constants section, not in ConsolidationConfig |
| Min samples for Spearman (scipy fallback) | recompute_salience.py:102 | `3` | No | Yes (line 5160) | OK -- same |

---

#### 7. Domain Discovery (Tier 3) -- Hardcoded Constants

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| `_MIN_OCCURRENCES` | domain_discovery.py:36 | `5` | No | Yes (line 5157) | OK -- in hardcoded constants section |
| `_EXISTING_DOMAINS` dict (10 entries) | domain_discovery.py:23-34 | 10 domain name/description pairs | No | Yes (line 5251) | OK |
| Redis SCAN count | domain_discovery.py:84 | `100` | No | Yes (line 5158) | OK |

---

#### 8. OtelTraceQueryClient -- ClickHouse Query Parameters

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| `days` (lookback window) | otel_trace_query_client.py:46 | `7` | No | **No** | **GAP** |
| `min_sessions` (parameter, unused in query) | otel_trace_query_client.py:47 | `3` | No | **No** | **GAP** |
| ClickHouse HAVING clause `length(tools) >= 3` | otel_trace_query_client.py:71 | `3` | No | **No** | **GAP** |
| Query structure (GROUP BY session_key, ORDER BY length DESC) | otel_trace_query_client.py:62-73 | SQL template | No | Line 4772 gives overview | Partial |

---

#### 9. ScoringLedgerStore -- Retention and Query Parameters

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| `cutoff_hours` (query window for Stage 9) | scoring_ledger_store.py:75 | `48` | No | **No** | **GAP** |
| `cleanup_old` default retention_seconds | scoring_ledger_store.py:98 | `172800` (48h) | No (caller passes value from `audit.retention_days`) | Partially -- the default is 172800 but engine passes `retention_days * 86400` | **GAP** -- mismatch: default 48h vs engine passes 90d |
| SQLite index pattern | scoring_ledger_store.py:42-44 | `idx_scoring_ledger_gw (gateway_id, created_at)` | No | **No** | Minor |

---

#### 10. ConsolidationReportStore -- Defaults

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| `list_reports` default limit | report_store.py:91 | `10` | No | Yes (line 5210) | OK |
| `cleanup_old` default retention_days | report_store.py:162 | `90` | No (caller passes value) | Via `audit.retention_days` | OK |

---

#### 11. RT-1 (SuccessfulUseReasoningTask) -- Internal Constants

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| `_EVAL_PROMPT` template | successful_use_task.py:22-40 | Full prompt string | No | **No** | **GAP** |
| LLM system_prompt | successful_use_task.py:108 | `"You are a knowledge evaluation assistant."` | No | **No** | **GAP** |
| LLM max_tokens | successful_use_task.py:110 | `500` | No | **No** | **GAP** |
| LLM temperature | successful_use_task.py:111 | `0.1` | No | **No** | **GAP** |
| httpx client timeout | successful_use_task.py:63 | `30.0` seconds | No | **No** | **GAP** |
| Message content truncation | successful_use_task.py:90 | `[:500]` per message | No | **No** | **GAP** |
| Conversation total truncation | successful_use_task.py:97 | `[:4000]` chars | No | **No** | **GAP** |

All `SuccessfulUseConfig` schema fields (enabled, endpoint, api_key, model, batch_size, batch_timeout_seconds, feed_last_facts, min_confidence, run_async) ARE documented in CONFIGURATION.md lines 641-648.

---

#### 13. ScoringTuner -- Implicit Constants

| Parameter | File:Line | Hardcoded Value | In ConsolidationConfig? | In CONFIGURATION.md? | Gap? |
|-----------|-----------|-----------------|------------------------|---------------------|------|
| Hardcoded profile list | scoring_tuner.py:127 | `["coding", "research", "managerial", "worker", "personal_assistant"]` | No | Not as a tuner constant | Minor |

---

#### 14. TuningDeltaStore -- No Undocumented Parameters

All parameters are passed by callers. The store is purely structural. Documented in CONFIGURATION.md line 710.

---

#### 15. LLM Prompt Template Inventory

All LLM prompts used by consolidation stages are **hardcoded strings, not configurable, and not documented in CONFIGURATION.md**:

| Stage/Task | Prompt Variable | System Prompt | max_tokens | Temperature | Documented? |
|------------|----------------|---------------|------------|-------------|-------------|
| Stage 2 (Canonicalize) | `_MERGE_PROMPT` | "You are a knowledge synthesizer." | 500 | (not set, uses LLMClient default) | **No** |
| Stage 7 (Refine Procedures) | `_PROCEDURE_PROMPT` | "You are a procedure definition generator." | 500 | (not set, uses LLMClient default) | **No** |
| RT-1 (Successful Use) | `_EVAL_PROMPT` | "You are a knowledge evaluation assistant." | 500 | 0.1 | **No** |

---

#### 16. Classified Gap Summary

##### Critical Gaps (parameters that affect behavior and cost, not documented or configurable)

1. **Scoring ledger query window** (`cutoff_hours=48`) -- determines how much scoring history Stage 9 correlates against. Not in ConsolidationConfig, not documented.
2. **ClickHouse lookback days** (`days=7`) -- how far back Stage 7 looks for tool sequences. Not configurable.
3. **ClickHouse min tools threshold** (`HAVING length(tools) >= 3`) -- minimum tool calls per session to consider. Not configurable.
4. **All 3 LLM prompt templates** -- hardcoded; no way to customize merge/procedure/evaluation prompts.
5. **LLM max_tokens across all stages** (all `500`) -- not configurable per stage, not documented.
6. **RT-1 httpx timeout** (`30.0s`) -- not in SuccessfulUseConfig.
7. **RT-1 LLM temperature** (`0.1`) -- not in config schemas, not documented.

##### Moderate Gaps (documented in hardcoded constants inventory but not in ConsolidationConfig)

8. **Spearman correlation significance threshold** (`0.1`) -- in CONFIGURATION.md hardcoded constants table but not configurable.
9. **Spearman min samples fallback** (`3`) -- same.
10. **Domain discovery `_MIN_OCCURRENCES`** (`5`) -- same.
11. **Redis lock timeout** (`3600s`) -- mentioned in Redis key tables but not surfaced as tunable.

##### Low Gaps (content truncation, display limits)

12. **RT-1 message content truncation** (`[:500]` per message, `[:4000]` total conversation).
13. **Decay half_life minimum clamp** (`0.01`) -- numerical safety, not tunable.
14. **Domain discovery Redis SCAN batch size** (`100`).


### 13.6 Configuration Parameter Interactions

> This document describes how configuration parameters **interact** with each other:
> dependencies, conflicts, cascading effects, and dangerous combinations.
> It supplements CONFIGURATION.md which covers individual parameters.
>
> Generated 2026-03-28.

---

## Table of Contents

1. [API Key Fallback Chains](#1-api-key-fallback-chains)
2. [Parameter Dependencies](#2-parameter-dependencies)
3. [Parameter Conflicts](#3-parameter-conflicts)
4. [Cascading Effects: Profile Changes](#4-cascading-effects-profile-changes)
5. [Budget Resolution Chain](#5-budget-resolution-chain)
6. [TTL Hierarchy and Dependencies](#6-ttl-hierarchy-and-dependencies)
7. [Compaction Trigger Chain](#7-compaction-trigger-chain)
8. [Tier Capability Gating](#8-tier-capability-gating)
9. [Single Load Path After F2/F3 Unification](#9-single-load-path-after-f2f3-unification)
10. [Dangerous Combinations](#10-dangerous-combinations)
11. [Configuration Recipes](#11-configuration-recipes)

---

## 1. API Key Fallback Chains

After F2/F3, `_apply_inheritance_fallbacks()` runs after env overrides on every `load()` and applies a cascading fallback for empty API keys (and the F7 endpoint inheritance). Understanding this chain is critical to avoid silent auth failures.

### Fallback chain

```
cognee.embedding_api_key (Tier 1) ──> llm.api_key (if empty)
       │
       └── llm.api_key (Tier 2) ──> compaction_llm.api_key      (if empty)
                                 ──> successful_use.api_key     (if empty)

llm.endpoint (Tier 3 / F7) ──> compaction_llm.endpoint (if empty)
```

**Source:** `_apply_inheritance_fallbacks()` in `elephantbroker/schemas/config.py` (search for the function definition — it documents the tier rules inline).

**Implication:** Setting only `EB_EMBEDDING_API_KEY` (and not `EB_LLM_API_KEY`) means all LLM subsystems -- primary extraction, compaction, successful-use -- share the embedding key. This works when both services use the same LiteLLM proxy and auth, but breaks silently when they use different providers.

**One unified path:** Because `load()` always reads YAML first and then applies env overrides through `ENV_OVERRIDE_BINDINGS`, the inheritance chain runs identically whether you use `--config` or rely on the packaged default. There is no longer an asymmetry between env-only and YAML+env modes — the legacy `from_env()` method has been removed and the only public entry point is `load(path: str | None)`.

---

## 2. Parameter Dependencies

### Hard dependencies (X requires Y)

| Parameter X | Requires Y | What breaks without Y |
|---|---|---|
| `infra.trace.otel_logs_enabled: true` | `infra.otel_endpoint` set to a valid gRPC URL | TraceLedger attempts OTEL log export but the `LoggerProvider` creation silently fails. Events are still stored in-memory but never exported to ClickHouse. |
| `infra.trace.otel_logs_enabled: true` | `pip install opentelemetry-exporter-otlp-proto-grpc` | Import fails at setup; OTEL logging silently disabled. Runtime continues without traces. |
| `infra.otel_endpoint` (for span export) | `pip install opentelemetry-exporter-otlp-proto-grpc` | Span exporter import fails; `setup_tracing()` logs a warning but the runtime starts with a no-op tracer provider. All `@traced` decorators become passthrough. |
| `infra.clickhouse.enabled: true` | `infra.otel_endpoint` set + OTEL Collector configured to forward to ClickHouse | Trace events never reach ClickHouse. Stage 7 (refine procedures) of consolidation falls back to `_pattern_fallback()` instead of querying ClickHouse for tool-use patterns. |
| `hitl.enabled: true` | `hitl.callback_hmac_secret` (recommended) | HMAC validation is skipped -- approval callbacks from the HITL middleware are accepted without cryptographic verification. Not a crash, but a security gap. |
| `hitl.enabled: true` | HITL middleware service running at `hitl.default_url` | `HitlClient` HTTP calls fail; approval requests are dropped. Guard engine falls through to the next guard layer rather than blocking. |
| `async_analysis.enabled: true` | Redis available (`infra.redis_url` valid) AND `CachedEmbeddingService` initialized | Container skips `AsyncInjectionAnalyzer` creation if either is missing (line 450 of `container.py`). Feature is silently disabled. |
| `successful_use.enabled: true` | `memory_store` available (requires FULL or MEMORY_ONLY tier) | `SuccessfulUseReasoningTask` is skipped (line 581-583 of `container.py`). Feature silently disabled. |
| `reranker.enabled: true` | Reranker service at `reranker.endpoint` | Reranking step throws exception; if `reranker.fallback_on_error: true` (default), falls back to scoring-only. If `false`, working set build fails. |
| `gateway.org_id` | Organization registered via `ebrun org create` (writes `OrganizationDataPoint` to Neo4j) | Profile inheritance org overrides fail to load; bootstrap resolves org_label to empty string; scope-aware goal visibility silently misses org-scoped goals. |
| `gateway.team_id` | Team registered via `ebrun team create` with `MEMBER_OF` edge | Scope-aware goal Cypher fails to match team-scoped goals; team_label in session context is empty. |

### Soft dependencies (X works better with Y)

| Parameter X | Benefits from Y | Degradation without Y |
|---|---|---|
| `scoring.working_set_build_global_goals_filter_by_actors: true` | `ActorRegistry` + `OWNS_GOAL` edges in graph | Global goal loading falls back to unfiltered query; scoring sees all global goals regardless of actor ownership. |
| `goal_injection.include_persistent_goals: true` | Persistent goals stored via `GoalDataPoint` in Cognee graph | No persistent goals injected into extraction prompts; fact extraction misses goal-relevant detail. |
| `compaction_llm.*` | LLM endpoint reachable | Compaction `compact_with_context()` cannot generate summaries; compaction falls back to rule-classify-only mode (drops messages instead of summarizing). |

---

## 3. Parameter Conflicts

### Direct conflicts

| Parameter A | Parameter B | Conflict |
|---|---|---|
| Profile `scoring_weights` (11 fields) | Consolidation Stage 9 tuning deltas (from `ScoringTuner`) | Both set scoring weights. Tuning deltas are **additive**: `effective_weight = base_weight + tuning_delta`. If you change profile scoring weights via org override AND tuning deltas exist, they stack. Tuning delta caps (`max_weight_adjustment_pct`) are referenced against **base profile + org override**, NOT current tuned weight. This prevents a convergence trap but means the two sources can interact in non-obvious ways. |
| Profile `compaction.cadence: "aggressive"` | Profile `budgets.max_prompt_tokens: 12000` | Aggressive compaction triggers at `target_tokens * 1.5`. With the default `target_tokens: 4000`, the threshold is 6000 tokens. A 12000-token budget means the working set can inject facts that exceed the compaction threshold before they ever reach the message buffer. Compaction triggers on the **message buffer size**, not the working set size. These are independent dimensions. |
| `consolidation_min_retention_seconds: 172800` | Profile `session_data_ttl_seconds: 86400` | The session store enforces `effective_ttl = max(profile.session_data_ttl_seconds, config.consolidation_min_retention_seconds)`. If the profile TTL (86400s = 24h) is less than the retention floor (172800s = 48h), the retention floor wins. Setting a low profile TTL has **no effect** when the retention floor is higher. |

### Implicit conflicts

| Scenario | Explanation |
|---|---|
| Setting `reranker.enabled: false` + `scoring.cheap_prune_max_candidates` very low | Without reranking, the cheap-prune stage is the only pre-filter before scoring. Setting `cheap_prune_max_candidates: 10` discards most retrieval results and the scoring engine works with a small, possibly suboptimal pool. |
| Setting `embedding_cache.enabled: false` + `successful_use.enabled: true` | Successful-use LLM evaluation re-embeds fact texts each batch. Without embedding caching, every batch re-computes embeddings from scratch, greatly increasing latency and LLM costs. |
| Profile `retrieval.isolation_level: strict` + `retrieval.graph_mode: global` | Strict isolation restricts queries to the current actor's scope, but global graph mode attempts cross-scope graph traversal. The isolation level filter in Cypher (`WHERE scope = ...`) overrides the graph mode's intent to explore broadly. Result: global graph mode degrades to actor-scoped behavior. |

---

## 4. Cascading Effects: Profile Changes

Changing `default_profile` (or `EB_PROFILE` on the TS side) is the single highest-impact configuration change. A profile change cascades through **8+ sub-policies**:

```
default_profile change (e.g., "coding" → "research")
  │
  ├── scoring_weights (11 dimensions)
  │     turn_relevance: 1.5 → 0.8
  │     evidence_strength: 0.2 → 0.9
  │     recency_half_life_hours: 24 → 168
  │     ... (all 11 weights change)
  │
  ├── budgets
  │     max_prompt_tokens: 8000 → 12000
  │     (affects budget resolution chain → compaction threshold)
  │
  ├── compaction policy
  │     cadence: aggressive → minimal
  │     (threshold = target_tokens * multiplier: 6000 → 12000)
  │
  ├── retrieval policy
  │     isolation_level: loose → none
  │     isolation_scope: session_key → global
  │     graph_mode: local → global
  │     graph_max_depth: 1 → 3
  │     vector_weight: 0.3 → 0.5
  │     (changes which facts are even candidates for scoring)
  │
  ├── autorecall policy
  │     extraction_focus: ["code decisions",...] → ["findings","hypotheses",...]
  │     superseded_confidence_factor: 0.1 → 0.5
  │     (completely different memory characteristics)
  │
  ├── guard policy
  │     preflight_check_strictness: medium → loose
  │     autonomy domain levels: (9 domains remapped)
  │     (different safety behavior)
  │
  ├── session_data_ttl_seconds: 86400 → 259200
  │     (session data lives 3x longer)
  │
  └── assembly_placement
        goal_injection_cadence: smart → smart (same)
        goal_reminder_interval: 5 → 10
        replace_tool_outputs: true → false
        (different context assembly strategy)
```

### Cross-system cascade example

Switching from `coding` to `research`:

1. **Retrieval** changes from `isolation_level: loose` to `none` -- the system now queries the full graph instead of session-scoped data.
2. **Graph mode** changes from `local` (depth 1) to `global` (depth 3) -- graph expansion retrieves more distant neighbors.
3. **Scoring** weights shift from turn-relevance-heavy (1.5) to evidence-heavy (0.9) -- facts with evidence citations score much higher.
4. **Budget** jumps from 8000 to 12000 tokens -- 50% more context injected.
5. **Compaction** threshold jumps from 6000 to 12000 tokens (cadence "minimal" uses 3.0x multiplier) -- compaction fires much less often.
6. **TTL** increases from 24h to 72h -- session data persists 3x longer.
7. **Net effect**: The agent sees more diverse, evidence-grounded facts from across the graph with less aggressive pruning, but at higher token cost and slower compaction.

---

## 5. Budget Resolution Chain

Token budgets flow through a 4-stage resolution chain. Understanding this chain is critical because the budget governs how much context is injected.

### Stage 1: Source resolution

Three budget sources compete:

```python
sources = [
    (profile.budgets.max_prompt_tokens, "profile"),        # Always present
    (openclaw_budget, "openclaw"),                          # If OpenClaw passes token_budget
    (context_window * max_context_window_fraction, "window"), # If context_window is known
]
effective_budget = min(sources)  # Smallest wins
```

**Source:** `lifecycle.py` `_resolve_effective_budget()` lines 1044-1058.

**Interaction:** If `context_assembly.enable_dynamic_budget: true` and no `context_window_tokens` is provided, the system uses `fallback_context_window * max_context_window_fraction` (default: `128000 * 0.15 = 19200`). This fallback is **always added** as a source when dynamic budget is enabled. So even with `max_prompt_tokens: 8000`, the profile budget wins because it is smaller.

**Gotcha:** If `max_context_window_fraction` is set very low (e.g., 0.01) with a small fallback window (e.g., 10000), the window-derived budget (`10000 * 0.01 = 100`) will override the profile budget, causing near-empty context injection.

### Stage 2: Assembly budget split

The effective budget is split into 4 blocks:

```
Block 1 (system_prompt_addition): min(20% of effective, profile.budgets.max_system_overlay_tokens)
Block 2 (goal context):           10% of effective
Block 4 (evidence refs):          min(5% of effective, context_assembly.evidence_budget_max_tokens)
Block 3 (working-set items):      remainder (65-85%)
```

**Source:** `assembler.py` `assemble_from_snapshot()` lines 141-150.

**Interaction with profile:** `max_system_overlay_tokens` (default 1500) caps Block 1. With a small budget (e.g., 6000 from the worker profile), Block 1 = min(1200, 1500) = 1200, leaving only 3600 for working-set items after goals (600) and evidence (300).

### Stage 3: Working set scoring budget

The `BudgetSelector` uses the same `token_budget` from Stage 1 for greedy item selection. Items are selected in descending score order until the budget is exhausted.

### Stage 4: Compaction target

Compaction uses `profile.compaction.target_tokens` (default 4000) **independently** of the assembly budget. The compaction threshold is:

```
threshold = target_tokens * CADENCE_MULTIPLIERS[cadence]
  aggressive: target_tokens * 1.5
  balanced:   target_tokens * 2.0
  minimal:    target_tokens * 3.0
```

**Key interaction:** The compaction threshold operates on the **message buffer** (accumulated conversation messages in Redis), not on the injected context. The assembly budget operates on the **injected working set**. They are separate dimensions but both constrained by the profile:

- A profile with `budgets.max_prompt_tokens: 12000` and `compaction.target_tokens: 4000, cadence: minimal` means the context window can hold 12000 tokens of injected facts, but the message buffer compacts at 12000 tokens of conversation. These numbers coincidentally align for `research` but are independent.

---

## 6. TTL Hierarchy and Dependencies

Multiple TTL values interact, and some enforce minimum floors on others.

### TTL dependency tree

```
consolidation_min_retention_seconds (default: 172800s = 48h)
  │  FLOOR for:
  ├── profile.session_data_ttl_seconds
  │     Used by: SessionContextStore, SessionArtifactStore, message buffer
  │     Effective TTL = max(profile_ttl, consolidation_min_retention)
  │
  └── ProcedureEngine ttl_seconds
        Wired in container.py: ttl_seconds=config.consolidation_min_retention_seconds
        Used for: procedure execution state in Redis

scoring.snapshot_ttl_seconds (default: 300s = 5m)
  │  Independent — controls working set snapshot cache lifetime in Redis
  │  If too short: snapshot expires between assemble() and after_turn(),
  │  causing successful-use tracking to find no snapshot.
  │  If too long: stale snapshots are used for injection effectiveness metrics.

scoring.session_goals_ttl_seconds (default: 86400s = 24h)
  │  Controls: session goals stored in Redis (SessionGoalStore)
  │  Independent of session_data_ttl_seconds — a session's goals can
  │  outlive or undershoot the session context TTL.

embedding_cache.ttl_seconds (default: 3600s = 1h)
  │  Controls: Redis-cached embedding vectors
  │  Global (not gateway-scoped) — all gateways share embedding cache.

guards.history_ttl_seconds (default: 86400s = 24h)
  │  Controls: guard check history in Redis (per session)
  │  Independent — can be longer than session_data_ttl.

llm.ingest_buffer_ttl_seconds (default: 300s = 5m)
  │  Controls: messages awaiting batch processing in Redis ingest buffer.
  │  Must be >= ingest_batch_timeout_seconds, or messages expire before
  │  the batch timeout fires.

llm.extraction_context_ttl_seconds (default: 3600s = 1h)
  │  Controls: cached recent facts used as extraction context.

infra.trace.memory_ttl_seconds (default: 3600s = 1h)
  │  Controls: in-memory trace event retention.
  │  Independent of OTEL export — events can be exported before eviction.
```

### Cross-TTL interaction: the consolidation floor

The `consolidation_min_retention_seconds` parameter acts as a global floor because:

1. `SessionContextStore._effective_ttl()` enforces `max(profile_ttl, config.consolidation_min_retention_seconds)`.
2. `ProcedureEngine` receives `ttl_seconds=config.consolidation_min_retention_seconds` from the container.
3. `CompactionEngine` receives `ttl_seconds=config.consolidation_min_retention_seconds` from the container.
4. `CompactState` in Redis uses this TTL.

**Implication:** If you set `consolidation_min_retention_seconds: 604800` (7 days) but `session_data_ttl_seconds: 86400` (1 day), ALL session data lives for 7 days regardless of the profile setting. Redis memory usage scales with the retention floor, not the profile TTL.

### TTL mismatch hazard: snapshot vs session

If `scoring.snapshot_ttl_seconds` (300s) is much shorter than `session_data_ttl_seconds` (86400s), the working set snapshot expires long before the session. This means:

- `after_turn()` cannot find the snapshot for successful-use tracking (line 756-762 of `lifecycle.py`)
- `build_overlay()` cannot find the snapshot for system prompt overlay construction
- The session stays alive but the per-turn context tracking degrades to no-ops

---

## 7. Compaction Trigger Chain

Auto-compaction has a multi-parameter trigger chain:

```
after_turn() called
  │
  ├── Read message buffer from Redis (session_messages key)
  ├── Sum token estimates: total_tokens = sum(estimate_tokens(msg) for msg in msgs)
  │
  ├── Read profile.compaction.cadence → look up CADENCE_MULTIPLIERS
  │     "aggressive" → 1.5
  │     "balanced"   → 2.0
  │     "minimal"    → 3.0
  │
  ├── Compute threshold = profile.compaction.target_tokens * multiplier
  │     Example: coding = 4000 * 1.5 = 6000
  │     Example: research = 4000 * 3.0 = 12000
  │
  └── IF total_tokens > threshold → trigger compact()
        │
        ├── compact() reads CompactionContext
        │     token_budget = params.token_budget OR profile.compaction.target_tokens
        │
        └── CompactionEngine.compact_with_context()
              Uses compaction_llm for summarization
              Preserves goal-relevant messages (preserve_goal_state)
              Preserves open questions (preserve_open_questions)
              Preserves evidence refs (preserve_evidence_refs)
```

**Interaction with context_assembly.compaction_trigger_multiplier:** This config field (default 2.0, range 1.5-5.0) exists on `ContextAssemblyConfig` but is **NOT used by the auto-compaction code**. The auto-compaction code uses `CADENCE_MULTIPLIERS[cadence]` from the profile's compaction policy. The `compaction_trigger_multiplier` appears to be an orphaned field or reserved for explicit `compact()` calls. This is a potential source of confusion.

**Interaction with `compaction_llm.*`:** If `compaction_llm.endpoint` equals `llm.endpoint` AND `compaction_llm.api_key` equals `llm.api_key`, the container reuses the primary `LLMClient` instance (line 395-404 of `container.py`). Otherwise, a separate `LLMClient` is created. Setting different compaction LLM configs lets you route summarization to a cheaper/faster model.

---

## 8. Tier Capability Gating

> **Production default: FULL mode.** ElephantBroker always runs in FULL mode today — both Memory and Context Engine modules are active. MEMORY_ONLY and CONTEXT_ONLY exist in schema and container wiring but are not deployed or tested (TD-15). Install both plugins (`elephantbroker-memory` + `elephantbroker-context`) and set no tier flag — FULL mode is the default and covers ~90% of production use cases.

The `BusinessTier` determines which modules are instantiated. This creates implicit parameter dependencies:

### Module availability by tier

| Module | MEMORY_ONLY | CONTEXT_ONLY | FULL |
|---|---|---|---|
| `WorkingSetManager` | No | Yes | Yes |
| `ContextAssembler` | No | Yes | Yes |
| `CompactionEngine` | No | Yes | Yes |
| `RedLineGuardEngine` | No | Yes | Yes |
| `ConsolidationEngine` | No | No | Yes |
| `MemoryStoreFacade` | Yes | No | Yes |
| `RetrievalOrchestrator` | Yes | No | Yes |
| `RerankOrchestrator` | Yes | No | Yes |
| `ProcedureEngine` | Yes | No | Yes |
| `EvidenceEngine` | Yes | No | Yes |
| `ScoringTuner` | No | Yes | Yes |

### Configuration parameters that are silently ignored per tier

- **MEMORY_ONLY tier:** All `context_assembly.*`, `compaction_llm.*`, `guards.*`, `hitl.*`, `async_analysis.*`, and `scoring.*` (as working set scoring weights) parameters are **ignored** because the modules that consume them are not instantiated.
- **CONTEXT_ONLY tier:** All `retrieval.*` (no `RetrievalOrchestrator`), `reranker.*` (no `RerankOrchestrator`), `consolidation.*` (no `ConsolidationEngine`), and `successful_use.*` (requires `memory_store`) parameters are **ignored**.

### Container wiring dependency chain

Some modules depend on other modules existing:

```
WorkingSetManager ──requires──> RetrievalOrchestrator
ContextAssembler ──requires──> WorkingSetManager
CompactionEngine ──independent── (but needs llm_client for summarization)
RedLineGuardEngine ──needs──> CachedEmbeddingService OR EmbeddingService (fallback)
ConsolidationEngine ──needs──> many modules (all optional, degrades gracefully)
```

If `RetrievalOrchestrator` is None (CONTEXT_ONLY tier), `WorkingSetManager` is also None, and `ContextAssembler` is also None. Context assembly degrades to returning raw messages.

---

## 9. Single Load Path After F2/F3 Unification

> **Historical note.** Pre-F2/F3 this section documented the asymmetry between two
> separate code paths: `from_env()` (env-only, hardcoded defaults) and
> `from_yaml()` (YAML + a curated 14-var subset). The two paths drifted on every
> schema change, the curated subset was always smaller than the registry, and
> `from_env()` had API key fallback logic that `from_yaml()` did not. F2/F3
> deleted `from_env()` outright. The runtime, CLI, and tests now all converge on
> `ElephantBrokerConfig.load(path: str | None)` — there is exactly one path, and
> the asymmetry no longer exists. Re-read this section if you remember the
> old behavior or hit a stack trace mentioning `from_env`.

### How `load()` resolves a config

`ElephantBrokerConfig.load(path)` (`elephantbroker/schemas/config.py:735`) is the
only public entry point. The internal classmethod `from_yaml(path)` is the
implementation backbone (and the test harness still calls it directly for some
fixtures).

1. If `path` is `None`, `load()` resolves the packaged
   `elephantbroker/config/default.yaml` via `importlib.resources.as_file()` so
   the runtime can boot with zero on-disk config.
2. The YAML file is parsed with `yaml.safe_load()` and validated through
   `cls(**data)` — any malformed YAML or schema violation raises a
   `ValidationError` *before* env vars are touched, so error reports point at
   the YAML problem, not at a half-applied env override.
3. `_apply_env_overrides()` walks `ENV_OVERRIDE_BINDINGS` (currently 72 entries)
   and, for every binding whose env var is present in `os.environ`, coerces the
   raw value through `_coerce_env_value()` and writes it into the dotted path.
   The check is `if env_var not in os.environ` — empty string IS treated as
   set, which collapses the historical empty-string asymmetry.
4. `_apply_inheritance_fallbacks()` populates empty derived secrets (Tier 1:
   `llm.api_key` ← `cognee.embedding_api_key`; Tier 2: derived LLMs ←
   `llm.api_key`; Tier 3 / F7: `compaction_llm.endpoint` ← `llm.endpoint`).
5. The merged dict is re-validated through `cls.model_validate()` so any
   constraint violation introduced by an env override (e.g.
   `EB_EMBEDDING_DIMENSIONS=0` violating `ge=1`) raises at load time.

### Parameters with an env var binding

Every entry in `ENV_OVERRIDE_BINDINGS` is honored on every load — there is no
"curated subset". Read the registry directly in `elephantbroker/schemas/config.py`
for the canonical, contract-test-enforced list. The inverse contract test
(`tests/test_env_var_registry_completeness.py`) keeps the registry, the schema
fields, and `default.yaml` in sync, so the registry cannot drift from the docs
without breaking CI.

If you need to know whether a specific env var has a binding without opening
the file, the per-section tables in [Section: ElephantBroker Environment
Variable Reference](#elephantbroker-environment-variable-reference) show
`Env override = Yes/No` for every variable.

### Parameters only settable via YAML

These parameters have no `EB_*` env var mapping in `ENV_OVERRIDE_BINDINGS` and
can only be set via YAML or code:

- `reranker.timeout_seconds`, `reranker.batch_size`, `reranker.max_documents`, `reranker.fallback_on_error`, `reranker.top_n` (the `reranker.enabled` toggle does have a binding via F10 / `EB_RERANKER_ENABLED`)
- `infra.trace.memory_ttl_seconds`
- All `ScoringConfig` fields except `snapshot_ttl_seconds` and `session_goals_ttl_seconds`
- All `ConflictDetectionConfig` fields
- All `VerificationMultipliers` fields
- All `GoalInjectionConfig` fields
- All `GoalRefinementConfig` fields
- All `ProcedureCandidateConfig` fields
- All `AuditConfig` fields (db paths, retention_days)
- All `ContextAssemblyConfig` fields
- All `ArtifactCaptureConfig` fields
- All `ArtifactAssemblyConfig` fields
- All `AsyncAnalysisConfig` fields
- All `GuardConfig` fields except the master `enabled` toggle
- All `HitlConfig` fields except `enabled` (F10) and `callback_hmac_secret`
- All `StrictnessPreset` fields
- `profile_cache.ttl_seconds`

### Empty-string semantics (post-unification)

Pre-F2/F3, `from_env()` and `from_yaml()` differed on whether `EB_GATEWAY_ID=""`
counted as "set". `from_env()` used `os.environ.get(key, default)` and treated
empty string as a set value; `from_yaml()` used `if os.environ.get(key):` and
treated empty string as "not set" (falsy). The two paths produced different
configs from identical environments.

After unification, the only behavior is the `_apply_env_overrides()` rule:

```python
if env_var not in os.environ:   # only "not set at all" is skipped
    continue
raw = os.environ[env_var]
value = _coerce_env_value(raw, coercer)
```

Empty string IS an override. This matters most for the `str_or_none` coercer
(which maps `""` to `None`) and for required string fields (which will accept
`""` and propagate it). Setting `EB_GATEWAY_ID=""` will produce
`gateway.gateway_id = ""` and trip the startup safety guard — the fix is
`unset EB_GATEWAY_ID`, not setting it to empty string.

---

## 10. Dangerous Combinations

### Critical: breaks the system

| Combination | Symptom | Root cause |
|---|---|---|
| `cognee.embedding_dimensions: 1024` + existing Qdrant collections at 768 | Qdrant `400 Bad Request` on vector insert/search | Qdrant collection was created with 768-dim vectors; new embeddings are 1024-dim. Requires dropping and recreating collections. Same applies whenever `embedding_model` changes to one with different output dim. |
| `infra.redis_url` pointing to nonexistent Redis | Runtime starts but all session state, caching, goals, ingest buffering, working set snapshots, guard history, and HITL queues fail. Most operations log warnings but return degraded results. | Redis is a soft dependency at the container level (line 160-170 of `container.py`), but nearly every feature depends on it at runtime. |
| `llm.model: "gemini-2.5-pro"` (without `openai/` prefix via LiteLLM) | LLM calls fail silently or route to wrong provider | `LLMClient` strips `openai/` before passing to LiteLLM. If the model name doesn't have the prefix and isn't a valid LiteLLM model spec, calls fail. But Cognee requires `openai/` for its own routing. |
| `cognee.embedding_model` changed after data exists | New embeddings have different dimensionality or distribution; cosine similarity between old and new vectors is meaningless | Similarity searches return garbage results; dedup detection breaks; retrieval quality collapses. Requires re-indexing all data. |

### High: degraded behavior

| Combination | Symptom | Root cause |
|---|---|---|
| `scoring.snapshot_ttl_seconds: 30` (very low) + multi-turn conversation | `after_turn()` logs "No snapshot available" and skips successful-use tracking | Snapshot expires between `assemble()` (which creates it) and `after_turn()` (which reads it). The 30-second TTL is too short for turns that take longer than 30 seconds. |
| `consolidation_min_retention_seconds: 604800` (7 days) + many sessions | Redis memory grows indefinitely | All session keys are forced to the 7-day floor regardless of profile TTL. Each session stores context, messages, goals, snapshots, artifacts, guard history, and compact state. |
| `llm.ingest_buffer_ttl_seconds: 60` + `llm.ingest_batch_timeout_seconds: 120` | Messages expire from the buffer before the batch timeout fires | Buffer TTL (60s) is shorter than batch timeout (120s). Messages in the buffer are deleted by Redis before the batch window closes. |
| `goal_refinement.hints_enabled: true` + `goal_refinement.refinement_task_enabled: true` + `goal_refinement.run_refinement_async: false` | Ingest latency spikes | Synchronous goal refinement runs an LLM call on every ingest batch. The refinement task blocks the ingest pipeline. |
| `conflict_detection.similarity_threshold: 0.5` (very low) | Many false-positive contradiction detections | Facts with only 50% similarity are flagged as contradictions. This triggers `contradiction_penalty` scoring on facts that are merely topically adjacent, not contradictory. |
| `conflict_detection.redundancy_similarity_threshold: 0.5` (very low) | Many false-positive redundancy detections | Facts with only 50% similarity are penalized for redundancy. The working set ejects valuable non-redundant facts. |

### Medium: suboptimal behavior

| Combination | Symptom | Root cause |
|---|---|---|
| Profile `retrieval.graph_max_depth: 5` + large graph | Retrieval latency spikes to seconds | Deep graph traversal in Neo4j is exponential in the number of paths. Depth 5 on a well-connected graph can traverse thousands of nodes. |
| `reranker.batch_size: 1` | Reranking makes one HTTP call per candidate | N candidates = N HTTP roundtrips to the reranker. Latency scales linearly with candidate count instead of being amortized over batches. |
| `audit.retention_days: 7` (very low) | Consolidation Stage 9 (recompute salience) has insufficient historical data | Salience recomputation uses scored-fact ledger entries. If entries are pruned after 7 days, the Spearman correlation in Stage 9 has too few samples for reliable weight adjustment. The `min_correlation_samples` guard may prevent any adjustment at all. |
| `profile_cache.ttl_seconds: 10` (very low) | Profile resolution hits SQLite on nearly every request | The 5-minute default balances freshness vs performance. A 10-second cache means every second or third request triggers a full profile resolution (preset lookup, inheritance flattening, org override load, deep copy). |

---

## 11. Configuration Recipes

### Development (local single-developer)

```yaml
# Minimal config — use defaults where possible
gateway:
  gateway_id: "gw-dev-local"

cognee:
  neo4j_uri: "bolt://localhost:7687"
  neo4j_password: "elephant_dev"
  qdrant_url: "http://localhost:6333"

llm:
  model: "openai/gemini/gemini-2.5-pro"
  endpoint: "http://localhost:8811/v1"
  api_key: "your-litellm-key"

# Embedding uses same endpoint/key
# (_apply_inheritance_fallbacks: llm.api_key falls back to cognee.embedding_api_key,
#  see Section 9 / 14.6 — runs uniformly via load() in both YAML+env and env-only modes)

infra:
  redis_url: "redis://localhost:6379"
  log_level: "DEBUG"

# Disable expensive optional features
reranker:
  enabled: false

successful_use:
  enabled: false

hitl:
  enabled: false

context_assembly:
  enable_dynamic_budget: false

default_profile: "coding"

# Short retention for fast iteration
consolidation_min_retention_seconds: 3600
```

**Key interactions:** Reranker disabled means scoring-only ranking (faster but less precise). Debug logging produces verbose output. Short retention means Redis cleans up fast but consolidation Stage 4 decay may be aggressive.

### Staging (multi-user, pre-production)

```yaml
gateway:
  gateway_id: "gw-staging"
  org_id: "org-acme-staging"

cognee:
  neo4j_uri: "bolt://neo4j-staging:7687"
  neo4j_password: "staging-secure-pw"
  qdrant_url: "http://qdrant-staging:6333"
  embedding_model: "gemini/text-embedding-004"
  embedding_dimensions: 768

llm:
  model: "openai/gemini/gemini-2.5-pro"
  endpoint: "http://litellm-staging:8811/v1"

reranker:
  enabled: true
  endpoint: "http://reranker-staging:1235"
  fallback_on_error: true

infra:
  redis_url: "redis://redis-staging:6379"
  log_level: "INFO"
  otel_endpoint: "http://otel-collector:4317"
  trace:
    otel_logs_enabled: true

hitl:
  enabled: true
  default_url: "http://hitl-staging:8421"
  # callback_hmac_secret via EB_HITL_CALLBACK_SECRET env var

guards:
  enabled: true

scoring:
  snapshot_ttl_seconds: 300
  session_goals_ttl_seconds: 86400

consolidation_min_retention_seconds: 86400

default_profile: "coding"
max_concurrent_sessions: 50
```

**Key interactions:** OTEL enabled with logs means traces export to Collector and ClickHouse (if configured). HITL enabled requires the middleware service running. Reranker with fallback protects against reranker outages.

### Production

```yaml
gateway:
  gateway_id: "gw-prod-01"
  org_id: "org-acme"
  team_id: "team-engineering"
  agent_authority_level: 0

cognee:
  neo4j_uri: "bolt://neo4j-prod:7687"
  # neo4j_password via EB_NEO4J_PASSWORD env var (secret)
  qdrant_url: "http://qdrant-prod:6333"
  embedding_model: "gemini/text-embedding-004"
  embedding_dimensions: 768

llm:
  model: "openai/gemini/gemini-2.5-pro"
  endpoint: "http://litellm-prod:8811/v1"
  # api_key via EB_LLM_API_KEY env var (secret)
  extraction_max_facts_per_batch: 15
  ingest_batch_size: 8
  ingest_batch_timeout_seconds: 30

compaction_llm:
  model: "gemini/gemini-2.5-flash-lite"
  # Uses cheaper model for compaction summarization

reranker:
  enabled: true
  endpoint: "http://reranker-prod:1235"
  batch_size: 64
  max_documents: 200
  fallback_on_error: true
  timeout_seconds: 15

infra:
  redis_url: "redis://redis-prod:6379"
  log_level: "WARNING"
  otel_endpoint: "http://otel-collector:4317"
  trace:
    otel_logs_enabled: true
    memory_max_events: 5000
    memory_ttl_seconds: 1800
  clickhouse:
    enabled: true
    host: "clickhouse-prod"

hitl:
  enabled: true
  default_url: "http://hitl-prod:8421"
  approval_default_timeout_seconds: 600
  # callback_hmac_secret via EB_HITL_CALLBACK_SECRET env var

guards:
  enabled: true
  max_history_events: 100
  history_ttl_seconds: 172800

scoring:
  snapshot_ttl_seconds: 600
  session_goals_ttl_seconds: 172800

context_assembly:
  max_context_window_fraction: 0.15
  enable_dynamic_budget: true
  fallback_context_window: 128000

audit:
  retention_days: 90

consolidation_min_retention_seconds: 172800

profile_cache:
  ttl_seconds: 300

default_profile: "coding"
enable_trace_ledger: true
max_concurrent_sessions: 200
```

**Key interactions:** All API keys via env vars (never in YAML). ClickHouse enabled for Stage 7 procedure pattern detection. Larger reranker batch size for throughput. Warning-level logging to avoid log volume. Longer scoring snapshot TTL (600s) to survive slow turns. Longer guard history for pattern detection.

### High-Security Deployment

```yaml
# Builds on production recipe with guard hardening
guards:
  enabled: true
  builtin_rules_enabled: true
  history_ttl_seconds: 604800  # 7-day guard history
  max_history_events: 200
  strictness_presets:
    strict:
      bm25_threshold_multiplier: 0.5  # Very sensitive
      semantic_threshold_override: 0.60
      warn_outcome_upgrade: "require_approval"
      reinjection_on: "any_non_pass"
      llm_escalation_on: "any_non_pass"

hitl:
  enabled: true
  approval_default_timeout_seconds: 3600  # 1 hour to approve

# Use managerial profile as default (strict guards, approval-first autonomy)
default_profile: "managerial"

# Disable optional LLM features to minimize data exposure
successful_use:
  enabled: false

async_analysis:
  enabled: false
```

**Key interactions:** Managerial profile sets `preflight_check_strictness: strict`, which looks up the "strict" strictness preset from `GuardConfig.strictness_presets`. The preset controls BM25 threshold, semantic threshold, and escalation behavior. Combined with `approval_default_timeout_seconds: 3600`, agents must wait up to 1 hour for human approval on sensitive operations. Disabled optional LLM features reduce the surface area for data exposure.

### Research-Focused Deployment

```yaml
default_profile: "research"

# Research profile already sets:
#   - graph_mode: global, graph_max_depth: 3
#   - isolation_level: none (cross-session access)
#   - evidence_strength weight: 0.9 (highest)
#   - compaction cadence: minimal (preserve context)
#   - session_data_ttl: 259200 (3 days)

# Extend budget for deep research sessions
# (These would go in an org override via admin API, NOT in YAML,
#  since they are profile fields, not config fields)

# Infrastructure to support research workload
scoring:
  snapshot_ttl_seconds: 900  # Longer snapshot for slow research turns
  session_goals_ttl_seconds: 604800  # Week-long goal persistence

consolidation_min_retention_seconds: 604800  # 7-day retention

# Enable all feedback features for research quality
successful_use:
  enabled: true
  batch_size: 10
  min_confidence: 0.5

# ClickHouse for pattern analysis
infra:
  clickhouse:
    enabled: true
```

**Key interactions:** Research profile's `isolation_level: none` means retrieval queries are not scoped -- the agent sees facts from all sessions. The 7-day retention floor (`consolidation_min_retention_seconds`) ensures research data persists long enough for consolidation Stage 7 (procedure refinement from patterns). Successful-use feedback enabled at lower confidence threshold (0.5 vs default 0.7) to capture more usage signals for research workflows.

---

## Summary of Key Interaction Patterns

1. **Profile is king:** Changing the profile cascades through scoring, retrieval, compaction, guards, budgets, TTLs, and assembly. It is the single most impactful configuration parameter.

2. **Consolidation floor dominates TTLs:** `consolidation_min_retention_seconds` overrides any lower `session_data_ttl_seconds`. Set it intentionally.

3. **Budget resolution picks the minimum:** The effective context budget is the smallest of profile budget, OpenClaw budget, and window-fraction budget. A misconfigured `max_context_window_fraction` can starve context injection.

4. **API key fallbacks always run via `_apply_inheritance_fallbacks()`:** Tier 1 (`llm.api_key` ← `cognee.embedding_api_key`), Tier 2 (`compaction_llm`/`successful_use.api_key` ← `llm.api_key`), Tier 3 (`compaction_llm.endpoint` ← `llm.endpoint`). After F2/F3 the chain runs in YAML+env, env-only, and `--config` modes uniformly — there is no longer an asymmetry where YAML mode skipped fallbacks.

5. **Tier gating silently disables features:** Parameters for modules not in the active tier are accepted without error but have no effect.

6. **Compaction and assembly are independent budgets:** Compaction threshold is based on message buffer tokens; assembly budget is based on injected context tokens. They are not linked.

7. **TTL mismatches cause silent degradation:** Snapshot TTL < turn duration means successful-use tracking fails. Buffer TTL < batch timeout means messages are lost.



---

## 14. Third Pass: Configuration Error Behavior & Troubleshooting

This section documents what happens when configuration is wrong -- missing, invalid, or misconfigured. Use the Symptom-Cause-Fix tables to diagnose production issues.

---

### 14.1 Missing Required Environment Variables

#### EB_GATEWAY_ID

| Component | Behavior when missing |
|---|---|
| **TS plugins** (`ElephantBrokerClient`) | **Hard crash at construction.** Constructor throws `Error("EB_GATEWAY_ID is required. Set it via the gatewayId constructor option or EB_GATEWAY_ID env var.")`. The plugin never registers with OpenClaw. Agent has no memory tools. |
| **Python runtime** (`ElephantBrokerConfig.load()` → `_apply_env_overrides()` → `GatewayConfig`) | **Silent default to `"local"`.** Packaged `default.yaml` ships `gateway.gateway_id: "local"`, and `EB_GATEWAY_ID` only overrides it if the env var is set. The runtime starts fine with gateway_id `"local"`. All Redis keys are prefixed `eb:local:`, all Cypher queries filter by `gateway_id = "local"`, all Cognee datasets are named `local__elephantbroker`. |
| **GatewayIdentityMiddleware** | **Falls back to config default.** The middleware reads `X-EB-Gateway-ID` from the HTTP header. If the header is absent (standalone mode, no TS plugin), it falls back to `default_gateway_id` set from `config.gateway.gateway_id` during `create_app()`. |

| Symptom | Cause | Fix |
|---|---|---|
| TS plugin fails to load in OpenClaw; agent has no memory/context tools | `EB_GATEWAY_ID` not set in the plugin's environment | Set `EB_GATEWAY_ID` in the plugin's shell environment or pass `gatewayId` to the `ElephantBrokerClient` constructor |
| Python runtime works but all data is scoped to `"local"` gateway; migrating to a real gateway_id later orphans all existing data | `EB_GATEWAY_ID` never set; developer used defaults | Set `EB_GATEWAY_ID` before first production use. All Cypher queries use strict `WHERE gateway_id = $gateway_id` -- data stored under `"local"` is invisible to queries using a different gateway_id |

#### EB_LLM_API_KEY

| Symptom | Cause | Fix |
|---|---|---|
| Runtime starts normally, but first LLM call (fact extraction, compaction, goal refinement, guard escalation) returns HTTP 401 or 403 | `EB_LLM_API_KEY` not set; defaults to empty string `""`. `LLMClient` sends `Authorization: Bearer ` (empty) which most LLM proxies reject. | Set `EB_LLM_API_KEY` to a valid API key. After F2/F3, `_apply_inheritance_fallbacks()` (Tier 1) automatically copies `cognee.embedding_api_key` into `llm.api_key` when the latter is empty — so setting `EB_EMBEDDING_API_KEY` alone unblocks LLM calls too. The chain runs in both YAML+env and env-only modes via `load()`. |
| `LLMClient` is constructed without error but every `complete()` / `complete_json()` call raises `httpx.HTTPStatusError` | Empty API key passes through constructor validation (it is not validated at construction time) | Set the key in env or YAML. The LLMClient only discovers the problem when it makes an HTTP request. |
| `AutorecallPolicy` extraction, `CompactionEngine` summaries, and `RedLineGuardEngine` LLM escalation all fail simultaneously | They all share the same `LLMClient` instance (`c.llm_client`), which was constructed with an empty API key | Set one API key -- all LLM features share it |

#### EB_EMBEDDING_API_KEY

| Symptom | Cause | Fix |
|---|---|---|
| Health check at `/health/ready` shows `embedding: error` with 401 | `EB_EMBEDDING_API_KEY` not set; `EmbeddingService` sends empty Bearer token | Set `EB_EMBEDDING_API_KEY`. Note: `EB_LLM_API_KEY` does NOT fall back to embedding key -- the fallback is the other direction. |
| Retrieval returns zero results; dedup never fires; reranker semantic stage skipped | All embedding calls fail; `embed_text()` raises `httpx.HTTPStatusError`; callers catch the exception and return empty/passthrough | Set the embedding API key. Every retrieval source except structural Cypher depends on embeddings either directly (vector search) or indirectly (Cognee indexes). |

---

### 14.2 Wrong Database URIs

The `RuntimeContainer.from_config()` method handles database connection failures differently per service:

#### Neo4j (`EB_NEO4J_URI`)

| Symptom | Cause | Fix |
|---|---|---|
| Runtime starts normally; first API request that touches graph (e.g. `POST /memory/store`, `GET /health/ready`) hangs for ~30s then returns 500 | `EB_NEO4J_URI` points to wrong host/port. `GraphAdapter` uses lazy connection (`_get_driver()` creates driver on first use, line 36-38). The Neo4j async driver's connection pool times out on first Cypher execution. | Fix the URI. Check connectivity: `cypher-shell -a bolt://host:7687 -u neo4j -p password "RETURN 1"`. |
| `/health/ready` shows `neo4j: error` with `ServiceUnavailable` or `ConnectionRefusedError` | Neo4j container is down or URI is wrong | Start Neo4j container or fix `EB_NEO4J_URI`. The health check explicitly tests Neo4j via `query_cypher("RETURN 1", {})`. |
| Runtime starts; `/health/live` returns OK but `/health/ready` shows Neo4j error | `/health/live` does not check any infrastructure -- it unconditionally returns `{"alive": true}`. `/health/ready` is the deep check. | Use `/health/ready` for deployment readiness probes, `/health/live` for liveness only. |
| All memory store, actor, goal, procedure, and evidence operations fail with 500 | Every module that does graph reads/writes depends on `GraphAdapter` which holds the Neo4j driver | Neo4j is a hard runtime dependency. Nothing except the health-live endpoint works without it. |

#### Qdrant (`EB_QDRANT_URL`)

| Symptom | Cause | Fix |
|---|---|---|
| Runtime starts normally; first vector search or `add_data_points()` call fails | `VectorAdapter` uses lazy connection (`_get_client()` creates client on first use, line 39-41). Wrong URL only surfaces on first operation. | Fix `EB_QDRANT_URL`. Test with: `curl http://host:6333/collections`. |
| `/health/ready` shows `qdrant: error` with `ConnectError` | Qdrant container is down or URL is wrong | Start Qdrant or fix the URL. Health check uses `get_collections()` which is a lightweight ping. |
| Vector search returns empty results but no errors | Qdrant is reachable but the collection does not exist yet (no data has been stored). `search_similar()` on a nonexistent collection may raise or return empty depending on Qdrant version. | Store some facts first (`POST /memory/store`), which triggers `add_data_points()` and creates the collection. |
| `Qdrant 400 Bad Request: "wrong input: Vector params for 'text' are not specified"` | Qdrant server version mismatch (deployment fix section 37 pinned to v1.17.0). Named vector `using="text"` requires the collection to have been created with named vectors. | Pin Qdrant to v1.17.0. Check `docker-compose.yml` for `qdrant/qdrant:v1.17.0`. |

#### Redis (`EB_REDIS_URL`)

| Symptom | Cause | Fix |
|---|---|---|
| Runtime starts with warning: `"Redis client creation failed, continuing without: ..."` | `EB_REDIS_URL` points to unreachable Redis. Container line 168-170 catches the exception, logs a warning, and sets `c.redis = None`. | Fix `EB_REDIS_URL` or start Redis. The runtime starts but operates in severely degraded mode. |
| Runtime is "up" but: session goals don't persist, ingest buffering is disabled, working set snapshots are missing, embedding cache is disabled, guard history is empty, compact state is empty, HITL approval queue is unavailable | All these features require Redis. When `c.redis = None`, the container skips: `IngestBuffer` (line 414-418), `SessionGoalStore` still created but Redis ops will fail, `CachedEmbeddingService` falls through to uncached, `ApprovalQueue` set to None (line 323). | **Redis is a soft startup dependency but a hard runtime dependency.** The system starts without it but almost nothing works correctly. Fix the connection immediately. |
| `touch_session_keys()` raises `AttributeError: 'NoneType' has no attribute 'pipeline'` | Code paths that call `touch_session_keys()` may not guard against `redis=None` | Ensure Redis is available. This is a bug surface -- the lifecycle calls `touch_session_keys` on every turn. |

**Key insight: Neo4j and Qdrant are lazy-connect (fail on first use); Redis is eager-connect (fail at startup, but the failure is caught and degraded).**

---

### 14.3 Wrong LLM Configuration

#### Model Name Without `openai/` Prefix

| Symptom | Cause | Fix |
|---|---|---|
| **Cognee operations hang or fail.** `cognee.cognify()` calls Cognee's internal LLM for entity/relationship extraction. Cognee uses the model name as-is from `cognee.config.set_llm_config()`. Without the `openai/` prefix, Cognee may hang trying to route the model. | `EB_LLM_MODEL` set to `gemini/gemini-2.5-pro` instead of `openai/gemini/gemini-2.5-pro`. Cognee expects the `openai/` prefix for OpenAI-compatible endpoints. | Always use the `openai/` prefix: `EB_LLM_MODEL=openai/gemini/gemini-2.5-pro`. |
| **Direct `LLMClient` calls work fine** even without the prefix | `LLMClient.__init__()` strips the `openai/` prefix before sending to LiteLLM (line 24: `if model.startswith("openai/"): model = model[len("openai/"):]`). So the LLMClient works either way. | The prefix is required for Cognee, optional for LLMClient. Always include it. |
| **Embedding model — `openai/` prefix is OPTIONAL, depends on backend** | `configure_cognee()` passes `config.embedding_model` directly to Cognee's `embedding_cfg.embedding_model`. Unlike the LLM model, the embedding model name is just routed by LiteLLM — `gemini/text-embedding-004` works without prefix because LiteLLM recognizes the `gemini/` provider prefix. | Use the model name your LiteLLM proxy actually serves, e.g. `EB_EMBEDDING_MODEL=gemini/text-embedding-004` (default) or `openai/text-embedding-3-large`. |

#### Wrong LLM Endpoint

| Symptom | Cause | Fix |
|---|---|---|
| `httpx.ConnectError` on every LLM call; `POST /memory/ingest-messages` returns 500 or silently drops facts | `EB_LLM_ENDPOINT` points to wrong host/port. `LLMClient.complete()` calls `self._client.post(f"{self._endpoint}/chat/completions", ...)` which raises `ConnectError`. | Fix the endpoint. Verify with: `curl -X POST http://host:port/v1/chat/completions -H "Authorization: Bearer key" -d '{"model":"gemini-2.5-pro","messages":[{"role":"user","content":"test"}]}'` |
| `httpx.HTTPStatusError` with status 400 on every LLM call | Endpoint exists but doesn't understand the model name, or request format is wrong | Check that the LiteLLM proxy has the model configured. Check the proxy logs for the actual error. |
| Compaction summaries contain error text instead of summaries | `CompactionEngine` uses a separate `LLMClient` instance (`compaction_llm_client`). If `EB_COMPACTION_LLM_ENDPOINT` is wrong (or falls back to the primary endpoint which is also wrong), compaction's LLM calls fail but compaction may catch the error and continue with degraded output. | Check both `EB_LLM_ENDPOINT` and `EB_COMPACTION_LLM_ENDPOINT`. If compaction LLM uses the same endpoint/key as primary LLM, the container reuses the same client (line 395-404). |

#### Expired API Key

| Symptom | Cause | Fix |
|---|---|---|
| Runtime starts fine; every LLM-dependent operation fails with 401 | API key was valid during deployment but has since expired or been revoked | Rotate the key. The error surfaces on every `complete()` call because `response.raise_for_status()` raises `httpx.HTTPStatusError`. |
| Logs show `LLM returned invalid JSON` warnings | Some LLM proxies return HTML error pages on auth failure, which the `json.loads()` in `complete_json()` cannot parse | This is the auth error manifesting as a JSON parse error. Fix the API key. |

---

### 14.4 Profile Misconfiguration

#### Unknown Profile Name

| Symptom | Cause | Fix |
|---|---|---|
| `KeyError: "Unknown profile: badname"` returned as HTTP 404 (via error middleware) | `EB_DEFAULT_PROFILE` set to a name not in `PROFILE_PRESETS`. Valid names: `coding`, `research`, `managerial`, `worker`, `personal_assistant`. | The error middleware at `elephantbroker/api/middleware/errors.py` catches `KeyError` and returns 404. Set `EB_DEFAULT_PROFILE` to one of the 5 valid names. |
| `register_org_override()` raises `KeyError` | Admin API tries to register an override for a non-existent profile | Check the profile name. `ProfileRegistry.register_org_override()` explicitly validates: `if profile_id not in self._presets: raise KeyError(...)` (line 137-138). |
| First API call after startup returns 404 with `"Unknown profile"` | The profile resolution happens on the first request that needs a profile (e.g., memory search with auto-recall). If the default profile is wrong, it fails here. | Fix the profile name. There is no startup validation of `default_profile`. |

#### Org Override with Invalid Field Names

| Symptom | Cause | Fix |
|---|---|---|
| Warning logged: `"Ignoring unknown override key: bad_field"` | Org override dict contains a key not in `ProfilePolicy.model_fields`. The `ProfileInheritanceEngine._apply_org_overrides()` method skips unknown top-level keys with a warning (line 99). | Check the override dict. Valid top-level keys are the fields of `ProfilePolicy`: `id`, `name`, `extends`, `graph_mode`, `budgets`, `scoring_weights`, `compaction`, `autorecall`, `retrieval`, `verification`, `guards`, `session_data_ttl_seconds`, `assembly_placement`. |
| Warning logged: `"Ignoring unknown nested override key: scoring_weights.bad_nested"` | Nested dict override contains invalid sub-field. The engine checks `if nk in type(current_field).model_fields` (line 107) and logs a warning for unknown nested keys. | Check nested field names against the schema. For `scoring_weights`, valid sub-fields are: `turn_relevance`, `session_goal_relevance`, `global_goal_relevance`, `recency`, `successful_use_prior`, `confidence`, `evidence_strength`, `novelty`, `redundancy_penalty`, `contradiction_penalty`, `cost_penalty`, `recency_half_life_hours`, `evidence_refs_for_max_score`, `redundancy_similarity_threshold`, `contradiction_similarity_threshold`, `contradiction_confidence_gap`. |
| Override is accepted but profile behavior doesn't change | The override key exists but the value type is wrong (e.g., string where float expected). Pydantic's `model_validate()` in `_apply_org_overrides()` (line 115) will either coerce the value or raise `ValidationError`. | If validation fails, the error propagates as a 500. If Pydantic silently coerces (e.g., `"1.5"` -> `1.5`), the override works but may not be what was intended. |

#### Circular Profile Inheritance

| Symptom | Cause | Fix |
|---|---|---|
| `ValueError: "Circular inheritance detected: X already in chain [...]"` | A custom profile's `extends` field creates a cycle (e.g., A extends B, B extends A). The `ProfileInheritanceEngine.flatten()` method detects this via a visited set (line 41-49). | Fix the inheritance chain. Builtin profiles only extend `"base"`, so this only happens with custom profiles stored as org overrides. |

#### All Scoring Weights Set to Zero

| Symptom | Cause | Fix |
|---|---|---|
| Working set scores are all 0.0; facts selected randomly by insertion order | `ScoringWeights.weighted_sum()` computes `0*dim1 + 0*dim2 + ... = 0` for every candidate. All candidates tie at score 0. The `BudgetSelector` then fills the budget in arbitrary order. | Set at least `turn_relevance` and `recency` to non-zero values. The working set manager depends on score differentiation to select the most relevant facts. |
| All penalty weights set to zero means contradicting/redundant facts are never penalized | `redundancy_penalty: 0.0` and `contradiction_penalty: 0.0` neutralize the dedup and contradiction detection stages | Set penalty weights to negative values (spec defaults: `redundancy_penalty: -0.7`, `contradiction_penalty: -1.0`). |

---

### 14.5 YAML Config Errors

#### Malformed YAML

| Symptom | Cause | Fix |
|---|---|---|
| `yaml.scanner.ScannerError` or `yaml.parser.ParserError` at startup; server does not start | The YAML file has syntax errors (bad indentation, missing colons, tabs instead of spaces). `ElephantBrokerConfig.from_yaml()` (the internal reader called by `load()`) uses `yaml.safe_load(f)` which raises on parse errors. | Fix the YAML syntax. Use a YAML linter. Common errors: inconsistent indentation, unquoted special characters, tabs. |
| `ValidationError` from Pydantic at startup | YAML parses successfully but the data structure doesn't match `ElephantBrokerConfig` fields. `cls(**data)` passes the parsed dict to the Pydantic constructor — this happens BEFORE env overrides are applied, so the error message points at the YAML, not the env. | Check field names and nesting against the schema in `schemas/config.py`. |
| `FileNotFoundError` at startup | `--config path/to/config.yaml` points to a nonexistent file. The Click option uses `type=click.Path(exists=True)` (server.py line 24) which validates file existence. | Fix the file path. Click will print `Error: Invalid value for '--config': Path 'bad/path' does not exist.` and refuse to start. |

#### Missing Sections

| Symptom | Cause | Fix |
|---|---|---|
| No error -- defaults are used | Every section in `ElephantBrokerConfig` has `default_factory` (e.g., `cognee: CogneeConfig = Field(default_factory=CogneeConfig)`). Missing YAML sections simply use Pydantic defaults. | This is by design. Only specify sections you want to override. |
| `yaml.safe_load(f)` returns `None` for an empty file | Empty YAML file. The `from_yaml()` reader handles this: `data = yaml.safe_load(f) or {}`. An empty file produces a fully-defaulted config (which env overrides then layer onto). | An empty YAML file is valid -- it means "use all defaults plus env overrides". |

#### Wrong Types

| Symptom | Cause | Fix |
|---|---|---|
| `ValidationError: Input should be a valid integer` at startup | YAML value is a string where int is expected (e.g., `max_tokens: "eight thousand"`). Pydantic v2 strict mode is not enabled, so `"8192"` (string of digits) is coerced to `8192`, but `"eight thousand"` fails. | Use numeric values without quotes in YAML. `max_tokens: 8192` not `max_tokens: "eight thousand"`. |
| `ValidationError: Input should be greater than or equal to 1` at startup | Value violates a Pydantic `Field` constraint. For example, `embedding_dimensions: 0` fails because `Field(default=768, ge=1)`. | Check the `ge`, `le`, and value constraints defined in `schemas/config.py`. |
| Boolean field gets unexpected value | YAML `true`/`false` are native booleans. But `"true"` (quoted) is a string. Pydantic coerces `"true"` -> `True` in non-strict mode, but `"yes"` may not coerce. | Use unquoted `true`/`false` in YAML. |
| Float field loses precision | YAML float `0.1` is parsed as Python float. This is standard floating-point behavior, not a config error. | Not a problem in practice. |

---

### 14.6 Env Override Edge Cases (post-F2/F3)

> **Historical note.** This section used to document the asymmetry between
> `from_env()` (which treated empty string as a set value) and `from_yaml()`
> (which treated empty string as falsy and skipped the override). After the
> F2/F3 unification there is exactly one path through `_apply_env_overrides()`,
> and the empty-string asymmetry no longer exists. The `from_env()` behavior
> won — empty string IS an override.

#### Environment Variable Set to Empty String vs Not Set

`_apply_env_overrides()` uses `if env_var not in os.environ` as the gate.
Only "not in `os.environ` at all" skips the override. Empty string is treated
as a set value and goes through the coercer like any other input.

| Scenario | Result |
|---|---|
| `EB_GATEWAY_ID="prod-01"` | `gateway.gateway_id = "prod-01"` (override applied). |
| `EB_GATEWAY_ID=""` (set but empty) | `gateway.gateway_id = ""` (override applied as empty string — trips the startup safety guard, which refuses to boot with an empty gateway ID). |
| `EB_GATEWAY_ID` not set at all | YAML value (or default `"local"` from the packaged YAML) is preserved. |
| `EB_ORG_ID=""` (set but empty) | `gateway.org_id = None` because the binding uses the `str_or_none` coercer (`""` → `None`). |
| `EB_ORG_ID="acme"` | `gateway.org_id = "acme"` (override applied). |

| Symptom | Cause | Fix |
|---|---|---|
| Runtime refuses to boot with `EB_GATEWAY_ID="" ` | The startup safety guard rejects an empty gateway ID. After F2/F3 the empty string IS the value the env override applies — it does not silently fall back to the YAML default. | `unset EB_GATEWAY_ID` (do not export it as empty). |
| `EB_LLM_API_KEY=""` causes 401 errors at first LLM call | Empty string is now applied as the override and the inheritance fallback only fires if the field is empty AFTER overrides — and `""` IS empty, so the fallback DOES kick in and copy `cognee.embedding_api_key`. If that is also empty, you hit the 401. | Set `EB_LLM_API_KEY` to a real key, OR set `EB_EMBEDDING_API_KEY` so the Tier-1 inheritance can populate it. |

#### YAML `null` vs Absent Key

| Scenario | Behavior |
|---|---|
| Key absent from YAML | Pydantic uses the `default_factory` or default value. This is the normal case. Env overrides still apply on top. |
| Key set to `null` in YAML (e.g., `gateway: null`) | `yaml.safe_load()` parses `null` as Python `None`. Pydantic receives `gateway=None`. For a field typed as `GatewayConfig = Field(default_factory=GatewayConfig)`, Pydantic v2 rejects `None` with `ValidationError: Input should be a valid dictionary`. |
| Nested key set to `null` (e.g., `gateway: { org_id: null }`) | The field `org_id: str | None = None` accepts `None`. This is fine. But a field like `gateway_id: str = "local"` does NOT accept `None` because it's not `str | None`. |

| Symptom | Cause | Fix |
|---|---|---|
| `ValidationError: Input should be a valid dictionary` at startup | Top-level config section set to `null` in YAML instead of being omitted | Remove the line entirely, or provide a dict value `{}` |
| `ValidationError: Input should be a valid string` | String field set to `null` in YAML but the field type is `str` not `str | None` | Remove the line or provide a string value |

#### API Key Inheritance (post-unification)

| Scenario | Result |
|---|---|
| Only `EB_EMBEDDING_API_KEY` set | `cognee.embedding_api_key` gets the value from the env override. After overrides, `_apply_inheritance_fallbacks()` Tier 1 copies it to `llm.api_key` (which is empty), then Tier 2 copies that into `compaction_llm.api_key` and `successful_use.api_key`. Every subsystem ends up with the embedding key. |
| `EB_SUCCESSFUL_USE_API_KEY` not set | After overrides, the field is still empty, so Tier 2 copies `llm.api_key` into it. |
| `EB_LLM_API_KEY` set to a non-empty value, `EB_COMPACTION_LLM_API_KEY` not set | Tier 2 copies `llm.api_key` into `compaction_llm.api_key`. |
| `EB_LLM_API_KEY` set to a non-empty value, `EB_COMPACTION_LLM_API_KEY` also set | Both env overrides apply. The Tier-2 fallback only fires if the target is empty after overrides — explicit values are always respected. |

The chain runs identically in both YAML+env and env-only modes (via `load(None)` + the packaged YAML). There is no longer a separate "from_env() fallback chain" that YAML mode lacked — every load goes through the same `_apply_inheritance_fallbacks()` after env overrides.

---

### 14.7 Runtime Configuration Changes

#### Changing `EB_GATEWAY_ID` After Data Exists

| Symptom | Cause | Fix |
|---|---|---|
| All existing facts, actors, goals, procedures, and evidence become invisible | Every Cypher query includes `WHERE gateway_id = $gateway_id` (strict, no IS NULL fallback per CLAUDE.md). Data stored under the old gateway_id is filtered out. | **This is by design -- gateway isolation is strict.** To access old data, either: (a) revert to the old gateway_id, or (b) run a Cypher migration: `MATCH (n) WHERE n.gateway_id = "old_id" SET n.gateway_id = "new_id"`. |
| Redis keys are orphaned; session state, goals, snapshots from old gateway are inaccessible | All Redis keys are prefixed `eb:{gateway_id}:`. Changing gateway_id creates a new key namespace. | The old Redis keys will expire naturally per their TTL. To clean up immediately: `redis-cli KEYS "eb:old_id:*" | xargs redis-cli DEL`. |
| Cognee datasets are orphaned | Cognee datasets are named `f"{gateway_id}__{base}"`. Old datasets (`old_id__elephantbroker`) are not deleted and not searched by the new gateway. | Clean up manually via Cognee admin or leave them (they consume storage but cause no errors). |
| Prometheus metrics have both old and new `gateway_id` labels | `MetricsContext` is initialized once at startup with the new gateway_id. Existing metric series in Prometheus with the old label persist until they expire from TSDB retention. | Not a functional problem. The old series age out. |

#### Changing `EB_ORG_ID` Mid-Session

| Symptom | Cause | Fix |
|---|---|---|
| Profile resolution cache returns stale profiles | `ProfileRegistry` caches by `(profile_name, org_id)`. Changing `EB_ORG_ID` means new requests use a different cache key, so they miss the cache and fetch the new org's overrides. Old cached profiles expire after `profile_cache.ttl_seconds` (default 300s). | Wait 5 minutes for cache expiry, or restart the runtime. There is no cache invalidation trigger for org_id changes. |
| Persistent goal visibility changes | Scope-aware Cypher for persistent goals includes `WHERE ... org_id = $org_id` for ORGANIZATION-scoped goals. Changing org_id changes which organization goals are visible. | Expected behavior. Session goals (Redis-stored) are unaffected by org_id changes. |
| Authority checks may fail | Authority rules are stored per org in SQLite. Changing org_id means the admin API uses a different set of authority rules. | Expected behavior. Make sure the new org_id has authority rules configured via `ebrun` or the admin API. |

#### Changing `default_profile` Mid-Session

| Symptom | Cause | Fix |
|---|---|---|
| New sessions use the new profile; existing sessions are unaffected | Profile is resolved once at bootstrap and cached in the session context. Changing the default profile only affects new `session_start` calls. | Expected behavior. To change an existing session's profile, end it and start a new one. |
| If the new profile name is invalid, only new sessions fail | The profile is validated at resolution time, not at config load time. | Validate the profile name before deploying the config change. |

---

### 14.8 Infrastructure Error Patterns

#### Cognee SDK Configuration Errors

| Symptom | Cause | Fix |
|---|---|---|
| `ImportError: No module named 'cognee_community_vector_adapter_qdrant'` at startup | The community Qdrant adapter package is not installed. `configure_cognee()` imports it at line 20. | `pip install cognee-community-vector-adapter-qdrant>=0.2.2` |
| `cognee.cognify()` hangs indefinitely | (1) Model name without `openai/` prefix causes Cognee routing failure. (2) LLM endpoint unreachable -- Cognee's internal HTTP client may not have a timeout. (3) Cognee telemetry is trying to phone home and DNS resolution hangs. | (1) Add `openai/` prefix. (2) Fix endpoint. (3) Ensure `COGNEE_DISABLE_TELEMETRY=true` (set in `elephantbroker/__init__.py`). |
| `cognee.search()` returns empty list for all query types | (1) No data has been stored yet. (2) Wrong dataset name (deployment fix section 32). (3) `cognee.cognify()` was never run (keyword and semantic search require it). | (1) Store data first. (2) Check that `RetrievalOrchestrator` uses `self._dataset_name` not `session_key`. (3) Run ingest pipeline which calls `cognee.add()` + `cognee.cognify()`. |

#### OTEL / Tracing Errors

| Symptom | Cause | Fix |
|---|---|---|
| `setup_tracing()` completes but no spans appear in Jaeger/OTEL Collector | `EB_OTEL_ENDPOINT` not set; `config.otel_endpoint` is `None`. `setup_tracing()` creates a `TracerProvider` with no span processor (line 45-57). All `@traced` spans are created and immediately discarded. | Set `EB_OTEL_ENDPOINT` to the OTEL Collector gRPC endpoint (e.g., `http://localhost:4317`). |
| Warning: `"OTEL endpoint configured but opentelemetry-exporter-otlp-proto-grpc is not installed"` | The OTLP exporter package is missing. `setup_tracing()` catches the `ImportError` and logs a warning (line 51-57). Traces are silently dropped. | `pip install opentelemetry-exporter-otlp-proto-grpc` |
| TraceLedger events not appearing in ClickHouse | `EB_TRACE_OTEL_LOGS_ENABLED` not set to `true`, or `EB_OTEL_ENDPOINT` not set. The OTEL log bridge requires both. `setup_otel_logging()` returns `None` if either is missing. | Set both: `EB_OTEL_ENDPOINT=http://localhost:4317` and `EB_TRACE_OTEL_LOGS_ENABLED=true`. |

#### Reranker Connection Errors

| Symptom | Cause | Fix |
|---|---|---|
| Warning: `"Cross-encoder failed, falling back: ..."` in logs; retrieval still works but quality is lower | Reranker endpoint is unreachable. `cross_encoder_rerank()` catches the exception and falls back to pre-reranked order because `reranker.fallback_on_error` defaults to `true` (line 174-181). | Fix `EB_RERANKER_ENDPOINT`. Verify with: `curl http://host:1235/v1/rerank`. |
| `DEGRADED_OPERATION` trace events appearing | Same as above -- the fallback path emits a `TraceEvent` with `component: "reranker"` and the error message. | Monitor these trace events in Jaeger or ClickHouse to detect persistent reranker issues. |
| If `reranker.fallback_on_error: false`, reranker failure crashes the request | The exception is re-raised (line 182: `raise`). The error middleware catches it as an unhandled exception and returns 500. | Either fix the reranker or set `fallback_on_error: true` (the default). |

#### HITL Middleware Errors

| Symptom | Cause | Fix |
|---|---|---|
| Guard checks never escalate to human approval | `hitl.enabled` defaults to `false`. When disabled, `HitlClient` is not created (container.py line 321: `if config.hitl.enabled else None`), and `ApprovalQueue` requires Redis (line 323: `if c.redis else None`). | Set `hitl.enabled: true` in config and ensure Redis is available. |
| Approval callbacks fail with HMAC validation error | `hitl.callback_hmac_secret` is empty or doesn't match the HITL middleware's secret | Set `EB_HITL_CALLBACK_SECRET` to the same value in both the EB runtime and the HITL middleware. |

---

### 14.9 API Error Patterns

The error middleware (`elephantbroker/api/middleware/errors.py`) maps Python exceptions to HTTP status codes:

| Exception Type | HTTP Status | JSON Body |
|---|---|---|
| `RequestValidationError` (Pydantic) | 422 | `{"detail": [list of validation errors]}` |
| `KeyError` | 404 | `{"code": "not_found", "message": "..."}` |
| `ValueError` | 422 | `{"code": "validation_error", "message": "..."}` |
| Any other `Exception` | 500 | `{"code": "internal_error", "message": "..."}` |

| Symptom | Cause | Fix |
|---|---|---|
| 404 on a profile-related endpoint | Profile name not in presets. `ProfileRegistry.resolve_profile()` raises `KeyError("Unknown profile: ...")` which the middleware catches and returns as 404. | Use a valid profile name. |
| 422 with Pydantic validation details | Request body doesn't match the expected schema. Common: missing required field, wrong type, value out of range. | Read the `detail` array in the response -- it lists every validation error with the field path and expected type. |
| 500 with `"internal_error"` | Unhandled exception in the route handler. Could be: database connection error, LLM call failure, unexpected None where a value was expected. | Check the server logs -- the error middleware logs the full traceback at ERROR level (line 29). |
| 422 but the request looks correct | OpenClaw may send extra fields. The `AgentMessage` schema was fixed with `extra="allow"` (deployment fix section 22) but other schemas may still reject unknown fields. | Check which schema is rejecting. Pydantic v2's default is to ignore extra fields, but if `model_config` is set otherwise, it rejects them. |

---

### 14.10 Container Initialization Failure Modes Summary

`RuntimeContainer.from_config()` follows a specific pattern for each dependency:

| Dependency | Failure Mode | Startup Impact | Runtime Impact |
|---|---|---|---|
| **Cognee SDK** (`configure_cognee()`) | Exception propagates, **startup aborted** | Fatal | N/A |
| **Redis** | Caught, logged as warning, `c.redis = None` | Startup continues | Severe degradation (see 14.2) |
| **Neo4j** | Lazy connect -- no failure at startup | None | Fatal on first use |
| **Qdrant** | Lazy connect -- no failure at startup | None | Fatal on first vector operation |
| **OTEL exporter** | `ImportError` caught, warning logged | Startup continues | No tracing export |
| **OTEL log bridge** | Exception caught, returns `None` | Startup continues | No trace event export to ClickHouse |
| **TuningDeltaStore** | Exception caught (line 219) | Startup continues | Scoring tuner has no persistence |
| **ScoringLedgerStore** | Exception caught (line 225) | Startup continues | Working set scoring has no ledger |
| **ConsolidationReportStore** | Exception caught (line 357) | Startup continues | Consolidation reports not stored |
| **OtelTraceQueryClient** | Exception caught (line 363) | Startup continues | Consolidation Stage 7 (procedure refinement) falls back to pattern-based detection |
| **SuccessfulUseReasoningTask** | Exception caught (line 587) | Startup continues | No LLM-based successful-use reasoning |
| **ProcedureAuditStore** | `await init_db()` called; failure **propagates** | Fatal | N/A |
| **SessionGoalAuditStore** | `await init_db()` called; failure **propagates** | Fatal | N/A |
| **OrgOverrideStore** | `await init_db()` called; failure **propagates** | Fatal | N/A |
| **AuthorityRuleStore** | `await init_db()` called; failure **propagates** | Fatal | N/A |

**Key takeaway:** SQLite stores (audit, org overrides, authority rules) are hard startup dependencies -- they call `await init_db()` without try/except. If the SQLite database path is unwritable (permissions, disk full), the runtime will not start. The `data/` directory must exist and be writable.

| Symptom | Cause | Fix |
|---|---|---|
| Startup crash with `sqlite3.OperationalError: unable to open database file` | `data/` directory doesn't exist or is not writable. Default paths: `data/procedure_audit.db`, `data/session_goals_audit.db`, `data/org_overrides.db`, `data/authority_rules.db`, `data/consolidation_reports.db`, `data/tuning_deltas.db`, `data/scoring_ledger.db`. | `mkdir -p data && chmod 755 data`. Or set custom paths via `audit.*_db_path` config fields. |
| Startup crash with `PermissionError` | The process doesn't have write access to the data directory | Fix permissions: `chown <user>:<group> data/` |

---

*This troubleshooting section was generated from source code analysis on 2026-03-28. For the latest error behaviors, trace through `elephantbroker/runtime/container.py` (startup), `elephantbroker/api/middleware/errors.py` (API errors), and `elephantbroker/schemas/config.py` (validation rules).*


---

## 15. LLM Prompt Template Reference

## Table of Contents

1. [Fact Extraction (Conversation)](#1-fact-extraction-conversation)
2. [Fact Extraction (Tool Output)](#2-fact-extraction-tool-output)
3. [Memory Classification](#3-memory-classification)
4. [Artifact Summarization](#4-artifact-summarization)
5. [Compaction Summarization](#5-compaction-summarization)
6. [Guard LLM Escalation (Layer 5)](#6-guard-llm-escalation-layer-5)
7. [Fact Canonicalization (Consolidation Stage 2)](#7-fact-canonicalization-consolidation-stage-2)
8. [Procedure Refinement (Consolidation Stage 7)](#8-procedure-refinement-consolidation-stage-7)
9. [Successful Use Reasoning (RT-1)](#9-successful-use-reasoning-rt-1)
11. [Goal Refinement](#11-goal-refinement)
12. [Sub-Goal Creation](#12-sub-goal-creation)
13. [Subagent Context Summarization](#13-subagent-context-summarization)
14. [Health Check Smoke Tests](#14-health-check-smoke-tests)

---

## 1. Fact Extraction (Conversation)

**File:** `elephantbroker/runtime/adapters/cognee/tasks/extract_facts.py`

**Purpose:** Extract discrete, atomic facts from new conversation messages. Detects supersession and contradiction against previously extracted facts, tags goal relevance, and reports goal status changes.

**System prompt:**
```
You are a fact extraction engine for a {profile_name} agent.
Your task is to extract discrete, atomic facts from the NEW MESSAGES below.

{focus_section}
{goal_section}

Each fact MUST have:
- "text": a clean, atomic fact statement (one sentence)
- "category": one of the valid categories listed below
- "source_turns": list of message indices (0-based) that support this fact
- "supersedes_index": index into PREVIOUSLY EXTRACTED FACTS if this fact replaces an older one, or -1
- "contradicts_index": index into PREVIOUSLY EXTRACTED FACTS if this fact contradicts (but does not replace) an older one, or -1
- "goal_relevance": array tagging which goals each fact is relevant to (session goals only)
- "goal_status_hints": array reporting any session goal status changes detected in the new messages

VALID CATEGORIES: {valid_categories}

DECISION DOMAIN TAXONOMY (set decision_domain when category is "decision"):
financial, data_access, communication, code_change, scope_change, resource, info_share, delegation, record_mutation, uncategorized
When a fact has category "decision", also set decision_domain to classify the decision area.

INSTRUCTIONS:
- Extract discrete, atomic facts from the NEW MESSAGES only
- Use FOCUS AREAS to prioritize what is worth extracting
- Resolve contradictions within the new messages (extract the resolved version, not both sides)
- If a new fact replaces/updates a PREVIOUSLY EXTRACTED FACT, set supersedes_index to that fact's index
- Set supersedes_index to -1 if the fact does not supersede any previous fact
- If a new fact contradicts a PREVIOUSLY EXTRACTED FACT (without replacing it), set contradicts_index to that fact's index
- Set contradicts_index to -1 if the fact does not contradict any previous fact
- For each fact, populate goal_relevance with the indices of relevant ACTIVE SESSION GOALS and strength (direct/indirect/none)
- Do NOT produce goal_relevance or goal_status_hints for PERSISTENT GOALS
- source_turns = indices of new messages that contribute to this fact
- Return at most {max_facts} facts. Return {"facts": [], "goal_status_hints": []} if nothing worth extracting.

Return ONLY valid JSON matching the schema. Do not add commentary.
```

**User prompt construction:**
```
PREVIOUSLY EXTRACTED FACTS (reference only - set supersedes_index or contradicts_index if a new fact replaces or contradicts one):
[0] "fact text" (category, id=abcdef12)
[1] ...

NEW MESSAGES (extract facts from these - resolve contradictions within the batch):
[0] user (2025-01-01T00:00:00): message content
[1] assistant: response content
```

**Response format:** JSON matching `_RESPONSE_SCHEMA`:
```json
{
  "facts": [
    {
      "text": "string",
      "category": "string",
      "source_turns": [int],
      "supersedes_index": int,
      "contradicts_index": int,
      "decision_domain": "string (optional, for decision category only)",
      "goal_relevance": [{"goal_index": int, "strength": "direct|indirect|none"}]
    }
  ],
  "goal_status_hints": [
    {"goal_index": int, "hint": "completed|abandoned|blocked|progressed|refined|new_subgoal", "evidence": "string"}
  ]
}
```

**Temperature:** Not explicitly set per-call; `complete_json` hardcodes `0.0`

**Max tokens:** `config.extraction_max_output_tokens` (default: `16384`, env: `EB_LLM_EXTRACTION_MAX_OUTPUT_TOKENS`)

**Model:** `LLMConfig.model` (default: `gemini/gemini-2.5-pro`, env: `EB_LLM_MODEL`)

**When called:** During turn ingest pipeline (`extract_facts` task), triggered when a batch of messages accumulates (batch_size default 6, or batch_timeout 60s). Skipped if total message chars < 10.

**Failure behavior:** Returns `{"facts": [], "goal_status_hints": []}`. Logs warning. No retry.

**Parameters injected:**
- `profile_name` -- active profile name (default: "coding")
- `focus_section` -- from `extraction_focus` list (e.g., "FOCUS AREAS: topic1, topic2")
- `goal_section` -- built from `active_session_goals` (capped at `max_session_goals`, default 5) and `persistent_goals` (capped at `max_persistent_goals`, default 3), controlled by `GoalInjectionConfig`
- `valid_categories` -- `BUILTIN_CATEGORIES` + any `custom_categories`
- `max_facts` -- `config.extraction_max_facts_per_batch` (default: 10, env: `EB_LLM_EXTRACTION_MAX_FACTS`)
- Input truncation: user prompt truncated if token count > `extraction_max_input_tokens` (default: 4000)

---

## 2. Fact Extraction (Tool Output)

**File:** `elephantbroker/runtime/adapters/cognee/tasks/extract_facts.py`

**Purpose:** Extract key findings, results, and errors from structured tool output when the message batch is tool-only.

**System prompt:**
```
You are a fact extraction engine for a {profile_name} agent.
Your task is to extract key findings, results, and errors from structured tool output.

{focus_section}
{goal_section}

Each fact MUST have:
- "text": a clean, atomic fact statement (one sentence)
- "category": one of the valid categories listed below
- "source_turns": list of message indices (0-based) that produced this output
- "supersedes_index": index into PREVIOUSLY EXTRACTED FACTS if this fact replaces an older one, or -1
- "contradicts_index": index into PREVIOUSLY EXTRACTED FACTS if this fact contradicts (but does not replace) an older one, or -1
- "goal_relevance": array tagging which goals each fact is relevant to (session goals only)
- "goal_status_hints": array reporting any session goal status changes detected in the new messages

VALID CATEGORIES: {valid_categories}

DECISION DOMAIN TAXONOMY (set decision_domain when category is "decision"):
financial, data_access, communication, code_change, scope_change, resource, info_share, delegation, record_mutation, uncategorized
When a fact has category "decision", also set decision_domain to classify the decision area.

Focus on extracting: key results, error messages, configuration values, tool outputs that represent decisions or state.
Return at most {max_facts} facts. Return {"facts": [], "goal_status_hints": []} if nothing worth extracting.

Return ONLY valid JSON matching the schema. Do not add commentary.
```

**User prompt construction:** Same as Prompt 1.

**Response format:** Same JSON schema as Prompt 1.

**Temperature:** `0.0` (hardcoded in `complete_json`)

**Max tokens:** Same as Prompt 1 (`extraction_max_output_tokens`)

**Model:** Same as Prompt 1

**When called:** Same trigger as Prompt 1, but selected when `_is_tool_only_batch(messages)` returns True (all messages have role "tool").

**Failure behavior:** Same as Prompt 1.

**Parameters injected:** Same as Prompt 1.

---

## 3. Memory Classification

**File:** `elephantbroker/runtime/adapters/cognee/tasks/classify_memory.py`

**Purpose:** Classify a fact with unknown or "general" category into a memory tier (episodic, semantic, policy) via LLM fallback. Only called when the rule-based category map has no entry.

**System prompt:**
```
Classify the following fact into one of: episodic, semantic, policy. Return JSON: {"memory_class": "..."}
```

**User prompt:**
```
Fact: {fact.text}
Category: {fact.category}
```

**Response format:**
```json
{"memory_class": "episodic" | "semantic" | "policy"}
```
With `json_schema` enforcement:
```json
{
  "type": "object",
  "properties": {"memory_class": {"type": "string", "enum": ["episodic", "semantic", "policy"]}},
  "required": ["memory_class"]
}
```

**Temperature:** `0.0` (hardcoded in `complete_json`)

**Max tokens:** `50`

**Model:** `LLMConfig.model` (default: `gemini/gemini-2.5-pro`)

**When called:** During `classify_memory` task in turn ingest pipeline. Only fires for facts with category "general" or any category not in the hardcoded `_CATEGORY_MAP`. Skipped if no `llm_client` is provided (defaults to EPISODIC).

**Failure behavior:** Returns `MemoryClass.EPISODIC`. Logs warning. No retry.

**Parameters injected:**
- `fact.text` -- the fact's text content
- `fact.category` -- the fact's raw category string

---

## 4. Artifact Summarization

**File:** `elephantbroker/runtime/adapters/cognee/tasks/summarize_artifact.py`

**Purpose:** Generate a compact 1-3 sentence summary of a tool artifact's output.

**System prompt:**
```
You are a concise summarizer. Summarize the following tool output in 1-3 sentences. Focus on the key result, changes, or findings.
```

**User prompt:**
```
Tool: {artifact.tool_name}

Output:
{content}
```

**Response format:** Plain text (1-3 sentences).

**Temperature:** `0.0` (explicitly passed)

**Max tokens:** `config.summarization_max_output_tokens` (default: `200`, env: `EB_LLM_SUMMARIZATION_MAX_OUTPUT_TOKENS`)

**Model:** `LLMConfig.model` (default: `gemini/gemini-2.5-pro`)

**When called:** During artifact ingest pipeline. Only fires when artifact content length >= `summarization_min_artifact_chars` (default: 500) AND `llm_client` is provided.

**Failure behavior:** Falls back to truncation (first 200 chars of content). Logs warning. No retry.

**Parameters injected:**
- `artifact.tool_name` -- tool that produced the artifact
- `content` -- full artifact content string
- `summarization_min_artifact_chars` -- threshold to trigger LLM (default: 500)
- `summarization_max_output_tokens` -- max response tokens (default: 200)

---

## 5. Compaction Summarization

**File:** `elephantbroker/runtime/compaction/engine.py`

**Purpose:** Produce a concise summary of compressed conversation messages during context compaction, preserving decisions, open questions, technical details, evidence, and goal-relevant information.

**System prompt:**
```
You are a context compaction assistant. Your job is to produce a concise summary of the conversation below, preserving:
- Key decisions and their rationale
- Open questions that remain unanswered
- Technical details and code references
- Evidence and proof references
- Information relevant to active goals

Discard greetings, acknowledgments, and redundant exchanges. Output only the summary, no preamble.

Active goals (preserve information relevant to these):
- {goal_1_label}
- {goal_2_label}
```
(Goal section appended only if active goals exist.)

**User prompt:**
```
Summarize this conversation segment ({N} messages, ~{T} tokens) into a compact digest:

[user]: message 1
[assistant]: message 2
...
```

**Response format:** Plain text summary.

**Temperature:** `0.2` (explicitly passed)

**Max tokens:** `config.compaction_summary_max_tokens` (from `ContextAssemblyConfig`, default: `1000`)

**Model:** `CompactionLLMConfig.model` (default: `gemini/gemini-2.5-flash-lite`, env: `EB_COMPACTION_LLM_MODEL`)

**When called:** During `compact_with_context()` when the compress bucket is non-empty. Triggered when current token count exceeds `target_tokens * cadence_multiplier` (balanced=2.0, aggressive=1.5, minimal=3.0) or forced.

**Failure behavior:** Falls back to character truncation at `compaction_summary_max_tokens * 4` chars. Logs warning with exc_info. No retry.

**Parameters injected:**
- `compress_messages` -- messages classified as "compress" by the rule engine
- `goals` -- active goals (labels extracted for goal-aware prompting)
- `compaction_summary_max_tokens` -- from `ContextAssemblyConfig` (default: 1000)
- Cadence multipliers (aggressive=1.5, balanced=2.0, minimal=3.0) control trigger threshold

---

## 6. Guard LLM Escalation (Layer 5)

**File:** `elephantbroker/runtime/guards/engine.py`

**Purpose:** Final layer (Layer 5) of the 6-layer guard pipeline. Evaluates whether an agent action violates safety constraints when cheaper layers are inconclusive.

**System prompt:**
```
You are a safety evaluator for an AI agent system. Respond with JSON only.
```

**User prompt:**
```
The agent is attempting the following action:

Action type: {action.action_type.value}
Action target: {action.action_target or 'none'}
Action content: {action.action_content[:500]}
Action metadata: {json.dumps(dict(action.action_metadata))[:200]}

Active safety rules:
- [{rule.pattern_type}] {rule.pattern}: {rule.description} -> {rule.outcome}
...

Determine:
1. Does this action violate any active constraints?
2. If yes, should it be BLOCKED or does it REQUIRE_APPROVAL?
3. If no, is it safe to PASS?

Respond with JSON: {"outcome": "pass" | "block" | "require_approval", "explanation": "brief reason"}
```

**Response format:**
```json
{"outcome": "pass" | "block" | "require_approval", "explanation": "brief reason"}
```

**Temperature:** `0.0` (hardcoded in `complete_json`)

**Max tokens:** `config.llm_escalation_max_tokens` (from `GuardConfig`, default: `500`)

**Model:** `LLMConfig.model` (default: `gemini/gemini-2.5-pro`)

**When called:** Layer 5 of guard check pipeline. Only fires when:
1. `guard_policy.llm_escalation_enabled` is True
2. `StrictnessPreset.llm_escalation_on` is not "disabled" (loose preset disables; medium fires on "ambiguous"; strict fires on "any_non_pass")
3. `self._llm` client exists

**Failure behavior:** Returns `GuardOutcome.BLOCK` (fail-closed). On timeout (configurable via `llm_escalation_timeout_seconds`, default 10s), returns BLOCK with confidence 0.5. On any exception, returns BLOCK with confidence 0.5. Logs warning.

**Parameters injected:**
- `action.action_type` -- GuardActionType enum value
- `action.action_target` -- tool name or target
- `action.action_content` -- truncated to 500 chars
- `action.action_metadata` -- truncated to 200 chars as JSON
- Up to 20 enabled static rules with pattern, description, outcome
- `llm_escalation_max_tokens` -- from GuardConfig (default: 500)
- `llm_escalation_timeout_seconds` -- asyncio timeout (default: 10.0)

---

## 7. Fact Canonicalization (Consolidation Stage 2)

**File:** `elephantbroker/runtime/consolidation/stages/canonicalize.py`

**Purpose:** Synthesize a single canonical fact statement from a cluster of near-duplicate facts during consolidation. Only fires when cluster members have non-identical text.

**System prompt:**
```
You are a knowledge synthesizer.
```

**User prompt:**
```
These facts describe the same thing. Synthesize a single, precise, canonical statement that captures the best information from all versions.
Preserve specific details (names, numbers, versions) over vague ones.
Return ONLY the synthesized statement text, nothing else.

Facts:
- fact text 1
- fact text 2
- fact text 3
```

**Response format:** Plain text -- single synthesized statement.

**Temperature:** Not explicitly set; uses `LLMClient.complete()` default from `LLMConfig.temperature` (default: `0.1`)

**Max tokens:** `500`

**Model:** `LLMConfig.model` (default: `gemini/gemini-2.5-pro`)

**When called:** During consolidation Stage 2 (Canonicalize Stable Facts). Only fires when a duplicate cluster has members with non-identical text AND LLM call budget has not been exhausted (`context.llm_calls_used < context.llm_calls_cap`, default cap: 50).

**Failure behavior:** Returns `None` (cluster skipped, no canonicalization). Logs warning with exc_info. Original facts remain unmodified.

**Parameters injected:**
- `fact_texts` -- newline-separated bullet list of cluster member texts
- `context.llm_calls_cap` -- max LLM calls per consolidation run (default: 50 from `ConsolidationConfig.llm_calls_per_run_cap`)

---

## 8. Procedure Refinement (Consolidation Stage 7)

**File:** `elephantbroker/runtime/consolidation/stages/refine_procedures.py`

**Purpose:** Generate a procedure definition draft from a repeated tool call pattern detected across multiple sessions.

**System prompt:**
```
You are a procedure definition generator.
```

**User prompt:**
```
Based on the following repeated tool call pattern observed across {sessions} sessions, generate a procedure definition.

Pattern: {tool_1} -> {tool_2} -> {tool_3}
Description: Repeated sequence: {tool_1} -> {tool_2} -> {tool_3} (seen in {sessions} sessions)

Return a JSON object with:
- name: short procedure name
- description: what this procedure accomplishes
- steps: array of {instruction: str, order: int}

Return ONLY valid JSON.
```

**Response format:**
```json
{
  "name": "string",
  "description": "string",
  "steps": [{"instruction": "string", "order": 0}]
}
```
(Note: the response is currently not parsed into a ProcedureDefinition; `draft_procedure` is stored as `None`.)

**Temperature:** Not explicitly set; uses `LLMConfig.temperature` default (0.1)

**Max tokens:** `500`

**Model:** `LLMConfig.model` (default: `gemini/gemini-2.5-pro`)

**When called:** During consolidation Stage 7 (Refine Procedures). Fires for each detected recurring pattern (tool sequence appearing in >= `pattern_recurrence_threshold` sessions, default 3). Capped at `max_patterns_per_run` (default: 10) and `context.llm_calls_cap` (default: 50).

**Failure behavior:** Pattern skipped (`continue`). Logs warning with exc_info. Other patterns still processed.

**Parameters injected:**
- `sessions` -- number of sessions the pattern was observed in
- `sequence` -- arrow-joined tool names (e.g., "grep -> read -> edit")
- `description` -- auto-generated description of the pattern
- Data source: primary ClickHouse (`OtelTraceQueryClient`), fallback `ProcedureAuditStore` (SQLite)

---

## 9. Successful Use Reasoning (RT-1)

**File:** `elephantbroker/runtime/consolidation/successful_use_task.py`

**Purpose:** Batch LLM evaluation to determine which injected facts actually contributed to agent actions. Overrides Phase 6 heuristic attribution for evaluated turns.

**System prompt:**
```
You are a knowledge evaluation assistant.
```

**User prompt:**
```
You are evaluating whether injected knowledge was useful to the agent.

INJECTED FACTS:
[0] fact text 1
[1] fact text 2
...

SESSION GOALS:
- Goal Title: description
...

CONVERSATION (last {turn_count} turns):
[Turn 1] user: message content
[Turn 1] assistant: response content
[Turn 2] ...

For each fact, determine if it:
a) Was directly referenced or acted upon by the agent
b) Led to progress toward a session goal
c) Received positive user feedback (user accepted the agent's action)

Return a JSON object: {"used_fact_indices": [int], "reasoning": "brief explanation"}
Only include facts you are confident (>70%) actually contributed.
Do NOT mark a fact as used if the agent would have done the same without it.
```

**Response format:**
```json
{"used_fact_indices": [0, 2], "reasoning": "brief explanation"}
```

**Temperature:** `0.1` (hardcoded in request payload)

**Max tokens:** `500` (hardcoded in request payload)

**Model:** `SuccessfulUseConfig.model` (default: `gemini/gemini-2.5-flash-lite`, env: `EB_SUCCESSFUL_USE_MODEL`)

**When called:** Fires as background `asyncio.create_task` from `ContextLifecycle.after_turn()` every `batch_size` turns (default: 5) or `batch_timeout_seconds` (default: 120s). Off by default (`SuccessfulUseConfig.enabled=False`, env: `EB_SUCCESSFUL_USE_ENABLED`).

**Failure behavior:** Returns empty list `[]`. Logs warning with exc_info. No retry. Does not affect normal turn processing.

**Parameters injected:**
- `fact_list` -- indexed list of injected fact texts, capped at `feed_last_facts` (default: 20)
- `goal_list` -- session goal titles and descriptions, or "(none)"
- `conversation` -- all messages from `turn_messages` batches, each content truncated to 500 chars, total truncated to 4000 chars
- `turn_count` -- number of turn batches
- Uses own dedicated httpx client with `SuccessfulUseConfig.endpoint` (default: `http://localhost:8811/v1`)
- Uses raw OpenAI-compatible API (`/chat/completions`), not `LLMClient`

---

## 11. Goal Refinement

**File:** `elephantbroker/runtime/working_set/goal_refinement.py`

**Purpose:** Tier 2 LLM-powered goal refinement. Rewrites a goal's title, description, and success criteria based on new evidence.

**System prompt:**
```
You refine goal definitions.
```

**User prompt:**
```
Refine this goal based on new evidence:
Current: {goal.title} - {goal.description}
Evidence: {evidence}
Return JSON: {"title": ..., "description": ..., "success_criteria": [...]}
```

**Response format:**
```json
{"title": "string", "description": "string", "success_criteria": ["string"]}
```

**Temperature:** `0.0` (hardcoded in `complete_json`)

**Max tokens:** `500`

**Model:** `LLMConfig.model` (default: `gemini/gemini-2.5-pro`)

**When called:** When a `hint == "refined"` is processed by `GoalRefinementTask.process_hint()` AND `GoalRefinementConfig.refinement_task_enabled` is True (default: True). Hints come from fact extraction's `goal_status_hints` output.

**Failure behavior:** Returns original goal unchanged. Logs warning. No retry.

**Parameters injected:**
- `goal.title` -- current goal title
- `goal.description` -- current goal description
- `evidence` -- evidence string from the goal status hint

---

## 12. Sub-Goal Creation

**File:** `elephantbroker/runtime/working_set/goal_refinement.py`

**Purpose:** Create a sub-goal for an existing goal via LLM, with Jaccard similarity dedup against existing sibling sub-goals.

**System prompt:**
```
You create sub-goals.
```

**User prompt:**
```
Create a sub-goal for:
Parent: {parent.title}
Evidence: {evidence}
Return JSON: {"title": ..., "description": ..., "success_criteria": [...]}
```

**Response format:**
```json
{"title": "string", "description": "string", "success_criteria": ["string"]}
```

**Temperature:** `0.0` (hardcoded in `complete_json`)

**Max tokens:** `500`

**Model:** `LLMConfig.model` (default: `gemini/gemini-2.5-pro`)

**When called:** When a `hint == "new_subgoal"` is processed AND `refinement_task_enabled` is True. Subject to `max_subgoals_per_session` limit (default: 10, counts ALL subgoals in session, not per-parent). Created sub-goal must pass Jaccard dedup check against siblings (threshold from `subgoal_dedup_threshold`, default: 0.6). Without LLM, creates a simple sub-goal from evidence text.

**Failure behavior:** Returns `None` (no sub-goal created). Logs warning. No retry.

**Parameters injected:**
- `parent.title` -- parent goal title
- `evidence` -- evidence string from the goal status hint
- `max_subgoals_per_session` -- cap from `GoalRefinementConfig` (default: 10)
- `subgoal_dedup_threshold` -- Jaccard similarity threshold (default: 0.6)

---

## 13. Subagent Context Summarization

**File:** `elephantbroker/runtime/context/assembler.py`

**Purpose:** Summarize inherited context for a child agent when spawning a subagent. Uses the parent's working-set snapshot filtered by the child's goal.

**System prompt:** None (single-prompt call via duck-typed `client.complete(prompt)`)

**User prompt:**
```
Summarize the following context for a child agent. The child's goal is: {child_goal.title}

Context:
{all_text from must_inject items + top-3 scored items}
```

**Response format:** Plain text summary.

**Temperature:** Not explicitly set; depends on duck-typed client implementation. If using `LLMClient.complete()`, uses `LLMConfig.temperature` (default: 0.1).

**Max tokens:** Not explicitly set; depends on duck-typed client. The result is truncated to `budget` tokens after the call.

**Model:** Whatever model the duck-typed `llm_client` uses.

**When called:** During `build_subagent_packet_from_context()` when an LLM client is available (either passed as parameter or set on the assembler instance). Part of the subagent spawn lifecycle.

**Failure behavior:** Falls back to deterministic strategy: all `must_inject` items + top-3 items by score, rendered as formatted text blocks within budget. Logs warning with exc_info.

**Parameters injected:**
- `child_goal.title` -- the child agent's assigned goal
- Working set items from parent snapshot (must_inject items + top-3 by final score)
- `budget` -- token budget for the context packet

**Note:** This call uses a duck-typed interface (`client.complete(prompt)`) that takes a single string, not the standard `(system_prompt, user_prompt)` signature. This is intentionally different from the standard `LLMClient` API.

---

## 14. Health Check Smoke Tests

**Files:** `elephantbroker/api/routes/health.py`, `elephantbroker/api/routes/memory.py`

**Purpose:** Verify LLM connectivity during health checks. Not a real prompt -- just a connectivity smoke test.

**System prompt:**
```
respond with OK
```

**User prompt:**
```
test
```

**Response format:** Any response (ignored).

**Temperature:** Not set; uses `LLMConfig.temperature` default (0.1)

**Max tokens:** `5`

**Model:** `LLMConfig.model` (default: `gemini/gemini-2.5-pro`)

**When called:** On `GET /health/ready` and `GET /memory/health` endpoints.

**Failure behavior:** Health check reports `"status": "error"` for the LLM check. Does not affect overall service availability (only Neo4j + Qdrant are required for "ok" status).

---

## Summary Table

| # | Prompt | File | Method | Temperature | Max Tokens | Default Model | Enabled By Default |
|---|--------|------|--------|-------------|------------|---------------|-------------------|
| 1 | Fact Extraction (Conversation) | `extract_facts.py` | `complete_json` | 0.0 | 16384 | gemini-2.5-pro | Yes |
| 2 | Fact Extraction (Tool Output) | `extract_facts.py` | `complete_json` | 0.0 | 16384 | gemini-2.5-pro | Yes |
| 3 | Memory Classification | `classify_memory.py` | `complete_json` | 0.0 | 50 | gemini-2.5-pro | Yes (fallback) |
| 4 | Artifact Summarization | `summarize_artifact.py` | `complete` | 0.0 | 200 | gemini-2.5-pro | Yes |
| 5 | Compaction Summarization | `compaction/engine.py` | `complete` | 0.2 | 1000 | gemini-2.5-flash-lite | Yes |
| 6 | Guard LLM Escalation | `guards/engine.py` | `complete_json` | 0.0 | 500 | gemini-2.5-pro | Profile-dependent |
| 7 | Fact Canonicalization | `canonicalize.py` | `complete` | 0.1 (default) | 500 | gemini-2.5-pro | During consolidation |
| 8 | Procedure Refinement | `refine_procedures.py` | `complete` | 0.1 (default) | 500 | gemini-2.5-pro | During consolidation |
| 9 | Successful Use (RT-1) | `successful_use_task.py` | raw httpx | 0.1 | 500 | gemini-2.5-flash-lite | No (opt-in) |
| 11 | Goal Refinement | `goal_refinement.py` | `complete_json` | 0.0 | 500 | gemini-2.5-pro | Yes |
| 12 | Sub-Goal Creation | `goal_refinement.py` | `complete_json` | 0.0 | 500 | gemini-2.5-pro | Yes |
| 13 | Subagent Context Summary | `assembler.py` | duck-typed `complete` | varies | varies | varies | When LLM available |
| 14 | Health Check Smoke | `health.py`, `memory.py` | `complete` | 0.1 (default) | 5 | gemini-2.5-pro | Yes |

---

## 16. Configuration Value Guide

Previous passes documented WHAT parameters exist. This pass documents the VALUE SPECTRUM -- what happens at minimum, default, maximum, and recommended production values.

---

## 1. Identity and Isolation

### 1.1 `EB_GATEWAY_ID` / `gateway.gateway_id`

**Schema constraint:** `str`, default `"local"`. Used to build Redis key prefix `eb:{gateway_id}:` and as Cognee dataset prefix `{gateway_id}__`. Also used in all Cypher `WHERE gateway_id = $gateway_id` clauses, all Prometheus labels, and OTEL spans.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `"local"` (default) | Single-tenant dev mode. All Redis keys start with `eb:local:`. All Neo4j nodes carry `gateway_id="local"`. | Local development, single-instance testing. | Collision if two devs share infra without knowing. |
| `"gw-prod-assistant"` | Alphanumeric-dash format. Clean Redis prefix `eb:gw-prod-assistant:`. Clean dataset name `gw-prod-assistant__elephantbroker`. | Production per-gateway naming. | None -- recommended pattern. |
| `""` (empty string) | Redis prefix becomes `eb::` (double colon). Cypher matches `WHERE gateway_id = ""`. Cognee dataset `__elephantbroker`. | Never use. | Double-colon keys are fragile. Empty string matches uninitialized nodes. Data leakage across gateways. |
| Values with `:` or `{` | Redis keys become `eb:gw:id:name:ingest_buffer:...` -- the colon in the ID creates ambiguous key segments. | Never use. | Key parsing breaks if any tooling splits on `:`. Cognee dataset names may reject special chars. |
| Very long values (100+ chars) | Redis keys grow linearly. Every Cypher query carries the full string. Prometheus labels bloat cardinality. | Avoid. | Memory pressure on Prometheus/Redis. OTEL span attributes grow. |

**Recommended production:** Short alphanumeric-dash, 8-20 chars. Pattern: `gw-{purpose}` or `gw-{team}-{instance}`. Examples: `gw-prod-assistant`, `gw-prod-coding-01`, `gw-staging`.

### 1.2 `EB_ORG_ID` / `gateway.org_id` and `EB_TEAM_ID` / `gateway.team_id`

**Schema constraint:** `str | None`, default `None`. Set via env or YAML. Used for scope-aware goal visibility (4-clause Cypher: GLOBAL > ORGANIZATION > TEAM > ACTOR) and MEMBER_OF edge creation on actor registration.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| Both `None` (default) | No org/team hierarchy. Goals only visible at GLOBAL or ACTOR scope. `MEMBER_OF` edges not created. Admin org/team endpoints return empty. | Single-user dev or personal assistant. | Cannot use ORGANIZATION or TEAM scoped goals. Cannot use org-level profile overrides. |
| `org_id` set, `team_id` None | Actors registered under this org. ORGANIZATION-scoped goals visible. Profile org overrides apply. No team filtering. | Single-team organizations, flat orgs. | Team-scoped goals invisible. |
| Both set | Full 4-level scope hierarchy active. ORGANIZATION and TEAM scoped goals visible. MEMBER_OF edges created for both. Profile overrides checked for org. | Multi-team production environments. | Must ensure org/team actually exist (created via `ebrun org create` / `ebrun team create`). |
| Wrong/stale org_id | Actor registration succeeds but `MEMBER_OF` edge points to non-existent org node. Goal visibility queries match the org_id string even without the org node. | Never intentional. | Phantom org membership. No authority rules fire for the org. Profile overrides silently fail. |

**Recommended production:** Always set both if operating in multi-org mode. Create org/team via `ebrun` CLI first. Leave both `None` for single-user personal deployments.

---

## 2. LLM Tuning

### 2.1 `llm.temperature`

**Schema constraint:** `Field(default=0.1, ge=0.0, le=2.0)`. Env: `EB_LLM_TEMPERATURE`. Controls the LLM temperature for all extraction calls (fact extraction, memory class classification, supersession detection).

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `0.0` | Fully deterministic. Same input always produces same facts. No creative interpretation. | Compliance-sensitive environments where reproducibility matters. Debugging extraction issues. | May miss nuanced facts. Under-extracts from ambiguous conversations. Tends to repeat patterns. |
| `0.1` (default) | Near-deterministic with slight variation. Good balance of consistency and coverage. | General production use. Coding, research, worker profiles. | Negligible. This is the battle-tested default. |
| `0.3` | Moderate creativity. More varied fact phrasings. Occasionally surfaces non-obvious connections. | Research profiles needing broader extraction. | May extract "creative" facts not well-grounded in the text. Slightly less stable dedup (different phrasings of same fact). |
| `0.7` | High variation. Different extractions from same conversation each time. | Not recommended for production. | Fact instability. The same conversation might produce 5 facts one run and 8 different facts the next. Dedup threshold must be looser to compensate. |
| `1.0+` | Very creative/random. Hallucination risk in extraction increases. | Never for fact extraction. | Extracted facts may contain confabulated details. Confidence scores become meaningless. |

**Recommended production:** `0.1` (default). Only raise to `0.2-0.3` for research profiles if extraction feels too narrow.

### 2.2 `llm.extraction_max_facts_per_batch`

**Schema constraint:** `Field(default=10, ge=1)`. Env: `EB_LLM_EXTRACTION_MAX_FACTS`. Passed to extraction prompt as `{max_facts}` -- instructs the LLM on the maximum number of facts to return per batch.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `1` | One fact per batch. LLM picks the single most important fact. | Ultra-conservative extraction. Minimizes noise. | Loses most conversational information. Multi-topic conversations only retain one thread. |
| `5` | Conservative extraction. LLM prioritizes by importance. | Short conversations, simple tasks, worker profile. | May miss secondary facts in rich conversations. |
| `10` (default) | Balanced extraction. Captures main facts plus supporting context. | General production use. | Rare over-extraction on simple "hi/thanks" exchanges (mitigated by 10-char minimum batch check). |
| `20` | Aggressive extraction. Captures fine-grained details, minor observations. | Research profile with detailed audit needs. | LLM may pad with low-quality facts. Increases dedup work. Higher token cost per batch. More storage churn. |
| `50+` | Exhaustive. Tries to extract everything mentioned. | Not recommended. | Noise overwhelms signal. Dedup similarity checks become O(n^2) with existing facts. Token budget competition becomes meaningless when flooded with marginal facts. |

**Recommended production:** `10` (default). Lower to `5` for worker profiles. Raise to `15` for research.

### 2.3 `llm.ingest_batch_size`

**Schema constraint:** `Field(default=6, ge=1)`. Env: `EB_INGEST_BATCH_SIZE`. Controls how many messages accumulate in the Redis ingest buffer before triggering LLM extraction. Also interacts with `ingest_batch_timeout_seconds` (default 60s) which forces a flush regardless of size.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `1` | Extract after every single message. Near-real-time fact availability. One LLM call per message. | High-stakes conversations where every message matters immediately. Debugging. | Very expensive -- LLM cost scales linearly with message count. Latency per message increases because extraction is synchronous in the ingest path. No cross-message context for the LLM. |
| `3` | Small batches. Fast fact availability (3 messages behind). Moderate cost. | Short, rapid conversations (chat-style). | Extraction LLM sees limited context per call. May miss cross-message relationships. |
| `6` (default) | Balanced. Facts available within ~6 messages or 60 seconds (whichever comes first). Good cross-message context for extraction. | General production use. | 6-message lag is noticeable in slow conversations. The timeout_seconds (60s) mitigates this. |
| `12-20` | Large batches. LLM gets rich context. Fewer LLM calls. | Cost optimization. Long-running sessions with continuous dialogue. | Facts not available until batch completes. If messages come slowly, the 60s timeout fires first anyway. Very large batches may hit `extraction_max_input_tokens` (4000 default) and get truncated. |

**Recommended production:** `6` (default). Lower to `3` for interactive assistants. Raise to `10-12` only if LLM cost is a concern and latency tolerance is high.

### 2.4 `llm.extraction_max_input_tokens`

**Schema constraint:** `Field(default=4000, ge=100)`. Env: `EB_LLM_EXTRACTION_MAX_INPUT_TOKENS`. Controls the user prompt truncation before sending to LLM. Truncation is by character ratio: if `prompt_tokens > max_input_tokens`, the prompt is sliced to `int(len(prompt) * max_input_tokens / prompt_tokens)`.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `1000` | Very short context window for extraction. Only ~250 words of conversation visible to LLM. | Minimal extraction, cost-sensitive. | Loses most of the conversation batch. Cross-message references broken. Quality drops sharply for batches > 3 messages. |
| `4000` (default) | ~1000 words. Covers a typical 6-message batch comfortably. | General production use. | Some very long tool outputs or code blocks may still be truncated. |
| `8000` | ~2000 words. Handles long technical discussions and code review conversations. | Research and coding profiles with verbose tool outputs. | Higher token cost per extraction call. Marginal quality improvement on typical conversations. |
| `16000+` | Very large context. Almost never truncates. | Specialized use with large LLM context windows. | Expensive. Risk of extracting facts from irrelevant old context. Diminishing returns -- extraction prompt quality matters more than raw input size. |

**Recommended production:** `4000` (default). Raise to `6000-8000` for coding profiles where tool outputs are large.

---

## 3. Scoring and Retrieval

### 3.1 `scoring_weights.turn_relevance`

**Schema constraint:** `float`, default `1.0`. Profile presets range from `0.7` (managerial) to `1.5` (coding). The raw score is cosine similarity between the current turn embedding and the fact embedding, clamped to [0.0, 1.0].

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `0.0` | Turn relevance has zero weight. Facts selected purely by goals, recency, evidence, etc. Current conversation topic is ignored. | Never useful in practice. | Working set includes topically irrelevant facts. Agent sees facts about unrelated topics. |
| `0.5` | Low emphasis. Turn topic influences selection but does not dominate. Goal relevance and recency matter more. | Research profiles browsing across topics. | May surface tangential facts when the user is focused on one topic. |
| `1.0` (base default) | Standard weight. Turn relevance is one of the primary selection signals alongside goal relevance. | General balanced use. | None -- well-calibrated default. |
| `1.5` (coding preset) | Strong emphasis on current topic. Facts about the exact code/topic being discussed win the budget competition. | Coding sessions where context switching is frequent and precision matters. | Older, still-relevant facts may lose to freshly-relevant ones. Session-spanning context gets deprioritized. |
| `3.0` | Dominant weight. Turn relevance overwhelms almost all other dimensions. Working set becomes a "what's the topic right now" filter. | Never recommended. | Goal-relevant background facts get squeezed out. Evidence and confidence become meaningless. Effectively turns the 11-dimension system into single-dimension retrieval. |

**Recommended production:** Use profile presets. `1.5` for coding, `0.7-0.8` for managerial/research, `1.0` for general.

### 3.2 `scoring_weights.recency_half_life_hours`

**Schema constraint:** `Field(default=69.0, ge=1.0)`. Profile presets: coding=24, worker=12, managerial=72, research=168, personal_assistant=720. The recency score uses exponential decay: `score = exp(-ln(2) / half_life * hours_since)`.

| Value | Behavior | Example: fact from 24h ago scores... | Use Case | Risk |
|---|---|---|---|---|
| `1` | Extremely aggressive decay. Facts lose half their recency value every hour. | `0.0000` (essentially zero) | Never recommended. | Even facts from the same workday become invisible. |
| `12` (worker) | Fast decay. Half-life of half a day. Facts from yesterday score ~0.25. | `0.25` | Task-focused worker agents completing short tasks. Previous day's facts are low-priority. | Multi-day projects lose context. |
| `24` (coding) | Daily decay. Facts from yesterday score 0.5, from 2 days ago score 0.25. | `0.50` | Coding sessions where yesterday's decisions are still relevant but last week's are not. | Week-long refactoring efforts lose early context. |
| `69` (base default) | ~3 day half-life. Facts from 3 days ago score 0.5. Week-old facts score ~0.25. | `0.79` | Balanced general use. | Stale facts may linger in working set for projects with many facts. |
| `168` (research) | Weekly half-life. Facts from last week score 0.5. Month-old facts score ~0.06. | `0.90` | Research where accumulated knowledge spans weeks. | Old irrelevant facts compete with newer relevant ones. |
| `720` (personal assistant) | Monthly half-life. Facts retain relevance for a month. | `0.98` | Personal assistants remembering preferences and schedules long-term. | Contradicted or updated facts persist longer. Relies heavily on contradiction_penalty to suppress outdated info. |

**Recommended production:** Use profile presets. Never go below `12` in production.

### 3.3 `retrieval.isolation_level`

**Schema constraint:** `IsolationLevel` enum: `"none"`, `"loose"` (default), `"strict"`. Controls how memory is partitioned in the retrieval pipeline.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `none` (research preset) | No isolation filtering. All sources active including cross-session Cognee keyword search. Post-retrieval filter disabled. Facts from any session/actor are candidates. | Research agents that need to discover facts across all sessions and actors. | Data leakage in multi-tenant deployments. Agent sees facts from other users'/agents' sessions. Only safe when gateway_id provides sufficient isolation. |
| `loose` (default; coding, worker, managerial presets) | All sources active. Post-retrieval filter applies `isolation_scope` (default: `session_key`) -- facts not matching current session_key (or NULL) are removed. Cross-session Cognee keyword search runs but results filtered. | General production use. Agent sees its own session's facts plus global/shared facts. | Cross-session facts may appear if they have `session_key=None`. This is by design for promoted/global facts. |
| `strict` (personal_assistant preset) | Keyword search via Cognee disabled (skipped entirely). Vector search redirected to `_get_direct_vector_hits()` with session_key filter baked into the Qdrant query. Only structural Cypher (with session_key in WHERE clause) and filtered graph expansion active. | Personal assistant with strict privacy. Multi-user gateways where session isolation is critical. | Significantly reduced recall. Cross-session knowledge invisible even if highly relevant. Must rely on promoted facts (SEMANTIC class) for cross-session memory. |

**Recommended production:** `loose` for most profiles. `strict` only for personal_assistant or regulated environments. `none` only for research where the gateway is single-user.

### 3.4 `retrieval.graph_max_depth`

**Schema constraint:** `Field(default=2, ge=1, le=5)`. Passed as `depth` parameter to `get_graph_neighbors()` which calls `cognee.search(GRAPH_COMPLETION)`. Controls how many hops from the initial query match the graph expansion traverses.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `1` (coding, worker presets) | Single hop. Only directly connected entities/facts. Fast. Typically returns closely related facts. | Task-focused profiles where precision > recall. Coding sessions where facts are about specific files/functions. | Misses indirect relationships. A fact connected through an intermediate entity is invisible. |
| `2` (default; managerial, personal_assistant presets) | Two hops. Finds facts connected through one intermediate node. Discovers "friend-of-friend" relationships. | Balanced production use. Discovers relationships like: fact -> actor -> other facts by that actor. | Moderate expansion of candidate set. Some noise from loosely connected facts. |
| `3` (research preset) | Three hops. Broad graph exploration. Finds facts connected through two intermediates. | Research agents exploring knowledge graphs for non-obvious connections. | Significant expansion of candidate set. Many tangentially related facts. Cognee GRAPH_COMPLETION latency increases. May return 50-100+ candidates from graph alone. |
| `5` (schema maximum) | Five hops. Near-exhaustive graph traversal for small knowledge bases. | Only for small, densely connected graphs where you want to explore everything. | Very expensive. On a graph with 1000+ nodes, 5 hops can traverse most of the graph. Latency spikes. Candidate flood overwhelms scoring pipeline. |

**Recommended production:** `1-2` for coding/worker, `2-3` for research/managerial. Never use `5` unless the graph is known to be small.

### 3.5 `budgets.max_prompt_tokens`

**Schema constraint:** `Field(default=8000, ge=100)`. Profile presets: worker=6000, coding/managerial/personal_assistant=8000, research=12000. This is the token budget for the BudgetSelector greedy competition -- how many tokens of facts can be injected into the working set.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `2000` | Very tight budget. ~500 words of injected context. Only 3-5 facts typically fit. | Cost-constrained deployments with expensive LLMs. Simple task workers. | Agent has very limited memory context. Complex multi-fact reasoning impossible. Working set competition becomes ruthless -- most scored facts are discarded. |
| `4000` | Moderate budget. ~1000 words. Typically 8-12 facts fit. | Budget-conscious production. Worker agents with focused tasks. | Some relevant facts may not fit. Goal context competes with factual context for space. |
| `6000` (worker) | Comfortable budget for task-focused work. ~1500 words. 12-18 facts. | Worker profile default. Short-lived task sessions. | Adequate for most single-focus tasks. |
| `8000` (default) | Standard budget. ~2000 words. 15-25 facts depending on fact length. | General production use. Coding, managerial, personal assistant. | None -- well-calibrated for typical conversations. |
| `12000` (research) | Large budget. ~3000 words. 25-40 facts. | Research profiles with complex multi-source reasoning. | Higher token cost per LLM call. Some padding with marginal facts. |
| `16000` | Very large budget. Agent sees extensive context. | Custom profiles for complex analysis tasks. | Significantly higher LLM costs. Risk of "context overload" where the agent struggles to focus among too many facts. |

**Recommended production:** Use profile presets. Scale with LLM context window and cost tolerance.

### 3.6 `autorecall.dedup_similarity`

**Schema constraint:** `Field(default=0.95, ge=0.0, le=1.0)`. Env: not directly exposed (set via profile/YAML). Cosine similarity threshold for considering two facts as duplicates during extraction dedup.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `0.80` | Aggressive dedup. Facts that are ~80% similar are considered duplicates. Merges facts with different wording about the same topic. | Highly compacted environments where storage efficiency matters. | Over-dedup: distinct but related facts get merged. "User prefers TypeScript" and "User prefers TypeScript for frontend" become one fact. Nuance lost. |
| `0.90` | Moderate dedup. Facts must be ~90% similar. Catches obvious restatements but preserves nuance. | Research profiles where slight differences matter. | Some near-duplicates slip through. Marginal storage overhead. |
| `0.95` (default) | Conservative dedup. Only nearly identical facts are deduplicated. "The deployment uses Docker" and "The deployment uses Docker containers" merge, but "The deployment uses Docker" and "Docker is used for CI/CD" do not. | General production use. | Very similar facts may coexist if their embeddings differ by >5%. Acceptable in most cases. |
| `0.99` | Minimal dedup. Only essentially identical text is deduplicated. | Audit-sensitive environments where every distinct phrasing must be preserved. | Storage grows faster. Redundancy_penalty in scoring must do the heavy lifting to avoid duplicate injection. |

**Recommended production:** `0.95` (default). Lower to `0.90` for research profiles that accumulate many facts. Never go below `0.85`.

---

## 4. Guards

### 4.1 `guards.preflight_check_strictness`

**Schema constraint:** `str`, default `"medium"`. Must match a key in `GuardConfig.strictness_presets`. Profile presets: coding/worker=`"medium"`, research=`"loose"`, managerial/personal_assistant=`"strict"`. The strictness preset controls 6 parameters that affect all guard layers.

| Value | BM25 Threshold Multiplier | Semantic Override | Warn Upgrade | Structural Validators | Reinjection Trigger | LLM Escalation | Behavioral Summary |
|---|---|---|---|---|---|---|---|
| `"loose"` | `1.5x` (higher = harder to trigger) | `0.90` (very high bar) | None (warn stays warn) | Disabled | Block only | Disabled | Maximum agent freedom. Only explicit BLOCK rules fire. BM25 must score 50% higher to trigger. Semantic check requires 90% match. No structural validation. No LLM escalation. Research profile uses this. |
| `"medium"` (default) | `1.0x` (base thresholds) | None (uses per-profile `semantic_similarity_threshold`, default 0.80) | None (warn stays warn) | Enabled | On `elevated_risk` (block, require_approval, require_evidence, or warn in prior layers) | On `ambiguous` (no definitive result from L0-L3) | Balanced safety. All layers active at base sensitivity. LLM called only when prior layers are inconclusive. Coding and worker profiles use this. |
| `"strict"` | `0.7x` (lower = easier to trigger) | `0.70` (lower bar) | `"require_approval"` (warn becomes require_approval) | Enabled | On `any_non_pass` (any layer that is not PASS triggers reinjection) | On `any_non_pass` (LLM called for any non-PASS result) | Maximum safety. BM25 triggers 30% more easily. Semantic needs only 70% match. All WARNs are upgraded to REQUIRE_APPROVAL (queue for human review). Reinjection fires on any concern. LLM escalation on any concern. Managerial and personal_assistant profiles use this. |

**Recommended production:** Match to profile preset. Override only for specific org requirements.

### 4.2 Autonomy Domain Levels

**Schema constraint:** `AutonomyLevel` enum per `DecisionDomain`. Four levels map directly to `GuardOutcome`: AUTONOMOUS -> PASS, INFORM -> INFORM, APPROVE_FIRST -> REQUIRE_APPROVAL, HARD_STOP -> BLOCK.

| Level | GuardOutcome | Agent Effect | Example (coding profile) | Example (managerial profile) |
|---|---|---|---|---|
| `AUTONOMOUS` | `PASS` | Agent proceeds without any constraint. No trace event beyond normal logging. | `code_change`: Agent freely modifies code. `resource`: Agent allocates compute. `delegation`: Agent spawns subagents. | `scope_change`: Manager redefines project scope. `delegation`: Manager assigns tasks. |
| `INFORM` | `INFORM` | Agent proceeds but a constraint is injected into Block 1 (systemPromptAddition) informing the human what happened. The action is NOT blocked. | `communication`: Agent can send messages but human sees "[INFORM] Agent sent external message". `scope_change`: Agent can adjust scope but human is notified. | `resource`: Manager allocated resources (human informed). `record_mutation`: Data was modified (human informed). |
| `APPROVE_FIRST` | `REQUIRE_APPROVAL` | Action queued in HITL approval queue. Auto-goal created to track pending approval. Agent cannot proceed until human approves or approval times out (default 300s -> HARD_STOP). | `data_access`: Agent needs human approval before accessing sensitive data. | `financial`: Manager needs approval for financial decisions. `data_access`: Manager needs approval for data access. `communication`: Manager needs approval before external comms. |
| `HARD_STOP` | `BLOCK` | Action completely blocked. Constraint injected: "[BLOCKED] This action requires human intervention." No auto-goal. No timeout. Agent must find alternative approach. | `financial`: Agent cannot make financial decisions. | `code_change`: Manager cannot directly modify code (must delegate to worker). |

**Recommended production:** Use profile presets as starting point. Override specific domains per-org using authority rules (`ebrun authority create`). Common customization: `financial: HARD_STOP` across all profiles.

---

## 5. Compaction

### 5.1 `compaction.cadence`

**Schema constraint:** `str`, default `"balanced"`. Must be one of `"aggressive"`, `"balanced"`, `"minimal"` -- unknown values fall back to `"balanced"`. The cadence controls a multiplier on `target_tokens` that determines the trigger threshold: compaction fires when `current_tokens > target_tokens * multiplier`.

| Value | Multiplier | Threshold (at target_tokens=4000) | Behavior | Profile Usage |
|---|---|---|---|---|
| `"aggressive"` | `1.5x` | `6000 tokens` | Compaction triggers when context exceeds 1.5x the target. Fires frequently. Context is always close to target. More LLM summarization calls. | Coding (context switches fast, stale code discussions compress well), Managerial (decisions made, move on). |
| `"balanced"` | `2.0x` (default) | `8000 tokens` | Compaction triggers at 2x target. Moderate frequency. Context grows to double the target before compressing. | Worker, Personal Assistant (base default). |
| `"minimal"` | `3.0x` | `12000 tokens` | Compaction triggers only at 3x target. Context grows significantly before compressing. Preserves more raw conversation. Fewer LLM calls. | Research (preserving raw dialogue matters for evidence/citation tracking). |

**Token impact per session turn:** With `target_tokens=4000` and 10 turns averaging 200 tokens each (2000 total), `aggressive` would not trigger (2000 < 6000). At 35 turns (7000 tokens), `aggressive` triggers, `balanced` does not (7000 < 8000). At 45 turns (9000 tokens), both `aggressive` and `balanced` trigger but `minimal` does not (9000 < 12000).

**Recommended production:** Use profile presets. `aggressive` for high-throughput agents, `minimal` only for research.

### 5.2 `compaction.target_tokens`

**Schema constraint:** `Field(default=4000, ge=100)`. The target size after compaction completes. Also used with cadence multiplier to determine trigger threshold.

| Value | Post-Compaction Size | Trigger at "aggressive" (1.5x) | Trigger at "balanced" (2.0x) | Use Case | Risk |
|---|---|---|---|---|---|
| `1000` | ~250 words after compaction | Triggers at 1500 tokens (~375 words) | Triggers at 2000 tokens | Ultra-aggressive compression. Only summary + last 1-2 exchanges survive. | Massive information loss. Open questions and evidence references may be dropped despite `preserve_*` flags because the target is so small. |
| `2000` | ~500 words | Triggers at 3000 | Triggers at 4000 | Tight sessions, simple tasks, cost-conscious. | Earlier context compressed heavily. Multi-step procedures may lose intermediate state. |
| `4000` (default) | ~1000 words | Triggers at 6000 | Triggers at 8000 | Standard production use. Retains good context while keeping prompts manageable. | None -- well-calibrated. |
| `8000` | ~2000 words | Triggers at 12000 | Triggers at 16000 | Research or complex multi-step sessions. | Late compaction means LLM calls operate on large contexts for many turns before compaction kicks in. Higher per-call cost. |

**Recommended production:** `4000` (default). Adjust with `max_prompt_tokens` -- target_tokens should generally be about half of max_prompt_tokens.

---

## 6. Consolidation

### 6.1 `consolidation.cluster_similarity_threshold`

**Schema constraint:** `Field(default=0.92, ge=0.5, le=1.0)`. Stage 1 of consolidation uses embedding cosine similarity to find near-duplicate fact clusters via union-find.

| Value | Behavior | Use Case | Risk |
|---|---|---|---|
| `0.80` | Aggressive clustering. Facts with 80% similarity are grouped as duplicates. | Rapid knowledge base cleanup. Environments with many paraphrased facts. | Over-merging: semantically distinct facts get clustered. "Server uses PostgreSQL" and "Server uses MySQL" could cluster if embeddings are close. Canonical merge loses nuance. |
| `0.92` (default) | Conservative clustering. Only near-identical facts cluster. "The API uses JWT authentication" and "JWT auth is used for the API" cluster, but "The API uses JWT" and "The API uses OAuth2" do not. | General production use. | Some duplicates may not cluster. Acceptable -- redundancy_penalty in scoring handles the scoring side. |
| `0.98` | Very conservative. Only essentially identical facts cluster. | Audit-sensitive environments. | Very few clusters found. Most duplicates survive consolidation. Storage grows over time. |

**Recommended production:** `0.92` (default). Lower to `0.88` if knowledge base grows beyond 10K facts and dedup is needed.

### 6.2 `consolidation.decay_recalled_unused_factor`

**Schema constraint:** `Field(default=0.85, ge=0.1, le=1.0)`. Stage 4: for facts that were recalled (injected into working set) but never successfully used by the agent, confidence is multiplied by this factor each consolidation cycle.

| Value | Behavior | Confidence After 3 Cycles | Use Case | Risk |
|---|---|---|---|---|
| `0.5` | Aggressive decay. Facts lose half their confidence per cycle when recalled but not used. | `0.125` (below archival threshold) | Aggressive pruning of unhelpful facts. | Useful facts that were recalled during a quiet session get penalized unfairly. Reduces knowledge base size quickly but may lose valuable dormant knowledge. |
| `0.85` (default) | Moderate decay. ~15% confidence loss per cycle. A fact with confidence 1.0 drops to 0.61 after 3 cycles. | `0.614` | General production use. Gradually demotes facts that do not prove useful. | Slow to clean up truly useless facts. Takes 10+ cycles to reach archival threshold (0.05). |
| `0.95` | Gentle decay. ~5% confidence loss per cycle. Almost imperceptible per cycle. | `0.857` | Research profiles where recalled-but-unused facts may become useful later. Long-term knowledge preservation. | Very slow cleanup. Useless facts persist for many cycles. |
| `0.99` | Near-zero decay. Facts essentially never lose confidence from non-use. | `0.970` | Archival/reference systems where all recalled facts should persist indefinitely. | No learning from usage patterns. Knowledge base grows monotonically. |

**Recommended production:** `0.85` (default). Lower to `0.80` for worker profiles that process short tasks.

### 6.3 `consolidation.max_weight_adjustment_pct`

**Schema constraint:** `Field(default=0.05, ge=0.01, le=0.20)`. Stage 9: caps how much scoring weights can drift from their BASE values (profile + org override) via adaptive tuning. The cap is referenced to base weight, NOT current tuned weight, preventing convergence traps.

| Value | Max Drift from Base | Example (base turn_relevance=1.5) | Use Case | Risk |
|---|---|---|---|---|
| `0.01` | 1% of base weight | 1.5 can drift to [1.485, 1.515] | Ultra-stable. Scoring behavior barely changes over time. | Adaptive tuning effectively disabled. System cannot learn from usage patterns. |
| `0.05` (default) | 5% of base weight | 1.5 can drift to [1.425, 1.575] | Standard adaptive range. Allows meaningful but bounded learning. | None -- well-calibrated. |
| `0.10` | 10% of base weight | 1.5 can drift to [1.35, 1.65] | More responsive to usage patterns. Faster adaptation to user behavior. | Weights may oscillate if usage patterns are inconsistent. Profile-specific tuning becomes less predictable. |
| `0.20` (schema maximum) | 20% of base weight | 1.5 can drift to [1.2, 1.8] | Aggressive adaptation. Weights can shift significantly from profile defaults. | Risk of drifting to suboptimal weights. May need periodic manual resets via `ebrun`. Profile semantics break down -- a "coding" profile could behave like a "research" profile after enough drift. |

**Recommended production:** `0.05` (default). Raise to `0.10` only if you have good observability and can monitor weight drift via the scoring ledger.

---

## 7. Session and TTL

### 7.1 `session_data_ttl_seconds`

**Schema constraint:** `Field(default=86400, ge=3600)` on `ProfilePolicy`. Profile presets: coding/worker=86400 (1 day), managerial=172800 (2 days), research=259200 (3 days), personal_assistant=604800 (7 days). Controls Redis TTL on all session-scoped keys refreshed by `touch_session_keys()` on every turn (10 key families: context, messages, goals, artifacts, snapshot, compact state, procedures, guard history, fact domains, parent).

| Value | TTL | Behavior | Use Case | Risk |
|---|---|---|---|---|
| `3600` (minimum) | 1 hour | Session data expires 1 hour after last turn. Working set, compact state, goals, guard history all gone. | Very short-lived tasks. CI/CD agents. | If user returns after 1 hour, session state is lost. No session goal continuity. Compact state gone -- agent loses compaction context on reconnect. |
| `86400` (default) | 1 day | Standard daily TTL. Session survives overnight. Typical for coding/worker sessions that last one workday. | Coding, worker profiles. | Session from Friday may expire over the weekend. |
| `172800` | 2 days | Two-day TTL. Session survives a weekend if last turn was Friday afternoon. | Managerial profiles with multi-day decision cycles. | Higher Redis memory usage per session. |
| `259200` (research) | 3 days | Three-day TTL. Research sessions spanning multiple days. | Research profiles. Long-running investigations. | ~3x Redis memory vs 1-day TTL per session. |
| `604800` (personal assistant) | 7 days | Weekly TTL. Session state persists for a full week. | Personal assistant with infrequent interaction. | ~7x Redis memory. Stale session data occupies memory for a week even if abandoned. Mitigated by the `max_concurrent_sessions` limit (100 default). |

**Recommended production:** Use profile presets. The TTL is refreshed on every turn, so active sessions never expire.

### 7.2 `consolidation_min_retention_seconds`

**Schema constraint:** `Field(default=172800, ge=3600)` on `ElephantBrokerConfig`. Env: `EB_CONSOLIDATION_MIN_RETENTION_SECONDS`. Defines how old facts must be before consolidation is allowed to decay, archive, or merge them. Protects recently created facts from premature consolidation.

| Value | Retention | Behavior | Use Case | Risk |
|---|---|---|---|---|
| `3600` (minimum) | 1 hour | Facts become eligible for consolidation after 1 hour. Very aggressive. | Development/testing. Not for production. | Active session facts get decayed/merged while the session is still running. Working set loses facts it just stored. |
| `86400` | 1 day | Facts from yesterday are eligible. Today's facts protected. | Fast-churning environments where knowledge has short shelf life. | If consolidation runs during an active multi-hour session, some facts may be eligible even though still in use. The `active_session_protection_hours` (default 1.0) provides an additional guard. |
| `172800` (default) | 2 days | Facts must be at least 2 days old. Protects all of today's and yesterday's work. | General production use. | None -- safe default. |

**Recommended production:** `172800` (default). Lower to `86400` only for high-churn environments with frequent consolidation runs.

---

## 8. Infrastructure

### 8.1 `infra.trace.memory_max_events`

**Schema constraint:** `Field(default=10_000, ge=100)`. Env: `EB_TRACE_MEMORY_MAX_EVENTS`. Controls the in-memory ring buffer size for the TraceLedger. When full, oldest events are evicted.

| Value | Buffer Size | Memory (~200 bytes/event) | Behavior | Use Case | Risk |
|---|---|---|---|---|---|
| `100` (minimum) | 100 events | ~20 KB | Very small buffer. Only last ~10-20 turns of trace data visible via `/trace/query`. | Testing. Memory-constrained environments. | Trace queries for older events return nothing. Debugging becomes impossible for anything beyond the last few minutes. |
| `1000` | 1000 events | ~200 KB | Small buffer. ~100 turns of history. Covers a typical session. | Light production use, single gateway. | Multi-session gateways may push events out before queries can access them. |
| `10000` (default) | 10K events | ~2 MB | Standard buffer. ~1000 turns across all sessions on this gateway. | General production use. | None -- well-calibrated for typical load. |
| `50000` | 50K events | ~10 MB | Large buffer. Retains many sessions of history. | High-throughput gateways with multiple concurrent sessions. Deep debugging needs. | 10 MB of memory. Acceptable for most servers. Trace query response size may be large. |

**Recommended production:** `10000` (default). Raise to `50000` if you need deep trace history AND have not enabled OTEL log export (`otel_logs_enabled=true`) or ClickHouse bridge.

### 8.2 `embedding_cache.ttl_seconds`

**Schema constraint:** `Field(default=3600, ge=60)`. Env: `EB_EMBEDDING_CACHE_TTL`. Controls Redis TTL for cached embeddings. Embedding cache keys are global (`eb:emb_cache:{hash}`), not gateway-scoped.

| Value | TTL | Behavior | Use Case | Risk |
|---|---|---|---|---|
| `300` (5 minutes) | Short cache. Embeddings recomputed frequently. | Development where embedding model changes often. | Higher embedding API cost. More latency on cache misses. Negligible benefit from caching. |
| `3600` (default) | 1 hour. Same text re-embedded within an hour hits cache. | General production use. Good balance of freshness and cost. | None. Embeddings for the same text are deterministic -- longer TTL is always safe for the same model. |
| `86400` (1 day) | Daily cache. All embeddings computed today are reused. | Production with stable embedding model. Cost optimization. | If embedding model changes (model update, endpoint change), stale embeddings persist for up to 24 hours. Must flush Redis `eb:emb_cache:*` keys on model change. |

**Recommended production:** `3600` (default) or `86400` if embedding model is stable. Embedding cache has no correctness risk -- same text always produces same embedding for a given model.

### 8.3 `reranker.timeout_seconds`

**Schema constraint:** `Field(default=10.0, ge=1.0)`. Controls the HTTP timeout for calls to the Qwen3-Reranker-4B service. Also has `fallback_on_error: bool = True` which falls back to scoring-only ranking if reranker fails.

| Value | Timeout | Behavior | Use Case | Risk |
|---|---|---|---|---|
| `2` | 2 seconds. Aggressive timeout. Reranker must respond within 2s or request falls back. | Low-latency environments. Reranker on same host with GPU. | Reranker may timeout on batches > 20 documents. Frequent fallbacks degrade ranking quality. |
| `5` | 5 seconds. Reasonable for local GPU inference. | Reranker on local GPU (RTX 3090/4090). Batch size <= 32. | Occasional timeouts on cold starts. |
| `10` (default) | 10 seconds. Comfortable for most GPU setups and moderate batches. | General production use. | None -- well-calibrated. |
| `30` | 30 seconds. Very generous. Allows large batches and slow hardware. | Reranker on CPU or shared GPU. Batch size up to 100. | Perceived latency on working set build increases. `assemble` response can take 30+ seconds if reranker is slow. |

**Recommended production:** `10` (default). Reduce to `5` only if reranker is on dedicated local GPU with confirmed low latency.

### 8.4 `max_concurrent_sessions`

**Schema constraint:** `Field(default=100, ge=1)`. Env: `EB_MAX_CONCURRENT_SESSIONS`. Controls the maximum number of concurrent sessions the runtime accepts.

| Value | Capacity | Behavior | Use Case | Risk |
|---|---|---|---|---|
| `10` | 10 concurrent sessions | Tight limit. Useful for development or single-user gateways. | Dev/test, personal use. | Production agents may be rejected during peak load. |
| `50` | 50 concurrent sessions | Moderate capacity. | Small team deployment, 5-10 concurrent users. | Peak hour spikes may exceed limit. |
| `100` (default) | 100 concurrent sessions | Standard capacity. Each session holds Redis state (goals, context, compact state, guard history -- ~10 key families). | General production use. | At 100 sessions with 604800s TTL (personal_assistant), Redis memory usage can grow significantly. Monitor Redis memory. |
| `1000` | 1000 concurrent sessions | High capacity. | Enterprise multi-gateway deployment with dedicated infrastructure. | Redis memory: 1000 sessions x ~10 keys x average 5KB per key = ~50 MB baseline. Neo4j connection pool pressure. LLM API rate limits may become the real bottleneck. |

**Recommended production:** `100` (default). Scale up with infrastructure capacity. Monitor Redis memory usage as the real constraint.

---

## Source Files Referenced

- `elephantbroker/schemas/config.py` -- all config schemas with defaults and constraints
- `elephantbroker/schemas/profile.py` -- ProfilePolicy, RetrievalPolicy, AutorecallPolicy, CompactionPolicy, GuardPolicy, Budgets
- `elephantbroker/schemas/working_set.py` -- ScoringWeights with 11 dimensions + detection thresholds
- `elephantbroker/schemas/guards.py` -- AutonomyLevel, DecisionDomain, GuardOutcome mapping
- `elephantbroker/schemas/consolidation.py` -- ConsolidationConfig with all 9 stage parameters
- `elephantbroker/runtime/profiles/presets.py` -- 5 profile presets with concrete values for all parameters
- `elephantbroker/runtime/compaction/engine.py` -- CADENCE_MULTIPLIERS and trigger logic
- `elephantbroker/runtime/working_set/scoring.py` -- recency decay formula and scoring computations
- `elephantbroker/runtime/retrieval/orchestrator.py` -- isolation filtering and graph depth usage
- `elephantbroker/runtime/guards/engine.py` -- strictness preset application across 6 guard layers
- `elephantbroker/runtime/redis_keys.py` -- RedisKeyBuilder prefix pattern and touch_session_keys

---

## 17. Production Security Hardening Guide

## 1. Secrets Management

### 1.1 Complete Secrets Inventory

| Secret | Source | Default Value | Where Used | Risk if Exposed |
|--------|--------|---------------|------------|-----------------|
| `EB_NEO4J_PASSWORD` / `cognee.neo4j_password` | env / YAML / `config.py` default | `elephant_dev` | GraphAdapter, Cognee config | Full graph database access; read/modify/delete all knowledge data |
| `EB_LLM_API_KEY` / `llm.api_key` | env / YAML | `""` (empty) | LLMClient, Cognee LLM config, compaction LLM, successful-use LLM | Cost exposure on LLM provider; prompt/completion data visible to key holder |
| `EB_EMBEDDING_API_KEY` / `cognee.embedding_api_key` | env / YAML | `""` (empty) | EmbeddingService, Cognee embedding config | Cost exposure on embedding provider |
| `EB_RERANKER_API_KEY` / `reranker.api_key` | env / YAML | `""` (empty) | RerankOrchestrator | Cost exposure on reranker endpoint |
| `EB_HITL_CALLBACK_SECRET` / `hitl.callback_hmac_secret` | env / YAML | `""` (empty) | HITL middleware HMAC validation, runtime HitlClient | Allows forging approve/reject callbacks; attacker can auto-approve any guard check |
| `EB_COMPACTION_LLM_API_KEY` | env | Falls back to `EB_LLM_API_KEY` | CompactionLLMConfig | Cost exposure |
| `EB_SUCCESSFUL_USE_API_KEY` | env | Falls back to `EB_LLM_API_KEY` | SuccessfulUseConfig | Cost exposure |
| `NEO4J_AUTH` | `docker-compose.yml` env | `neo4j/elephant_dev` | Neo4j container authentication | Full database access |
| `CLICKHOUSE_PASSWORD` | `docker-compose.yml` env | `""` (empty) | ClickHouse container, OTEL collector config | Access to all OTEL trace/log analytics data |
| Redis password | Not configured | None (no auth) | Redis container | Access to all session state, working sets, embedding cache, guard history, HITL queue |

### 1.2 Storage Recommendations

| Secret | Acceptable Storage | NOT Acceptable |
|--------|--------------------|----------------|
| Database passwords (Neo4j, ClickHouse, Redis) | Secrets manager (Vault/AWS SM/GCP SM), systemd `LoadCredential=`, encrypted env file with `0600` perms | YAML config file, docker-compose.yml, git repository |
| API keys (LLM, embedding, reranker) | Secrets manager, systemd `LoadCredential=`, encrypted env file | YAML config, committed code |
| HMAC secret (`EB_HITL_CALLBACK_SECRET`) | Secrets manager, systemd `LoadCredential=` | Any file readable by non-service users |
| `EB_GATEWAY_ID` | env file, YAML config | These are identity, not secrets -- but treat as sensitive metadata |

### 1.3 Rotation Procedures

**Neo4j password:**
1. Update Neo4j: `ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'` via `cypher-shell`
2. Update `docker-compose.yml` `NEO4J_AUTH` (for container restart)
3. Update `EB_NEO4J_PASSWORD` in `/etc/elephantbroker/env`
4. `systemctl restart elephantbroker`
5. Verify: `curl http://localhost:8420/health/`

**LLM/Embedding API keys:**
1. Generate new key on provider dashboard
2. Update `EB_LLM_API_KEY` / `EB_EMBEDDING_API_KEY` in `/etc/elephantbroker/env`
3. `systemctl restart elephantbroker`
4. Revoke old key on provider dashboard after confirming health

**HITL HMAC secret:**
1. Generate: `openssl rand -hex 32`
2. Update `EB_HITL_CALLBACK_SECRET` in BOTH `/etc/elephantbroker/env` AND `/etc/elephantbroker/hitl.env`
3. `systemctl restart elephantbroker elephantbroker-hitl`
4. Note: any pending HITL approvals with old HMAC will fail validation (expected)

**Redis password (currently unset):**
1. Set `requirepass` in redis.conf or `--requirepass` flag
2. Update `EB_REDIS_URL` to `redis://:password@localhost:6379`
3. `systemctl restart elephantbroker`

---

## 2. Network Security

### 2.1 Port Exposure Matrix

| Port | Service | Protocol | Bind To | External Exposure |
|------|---------|----------|---------|-------------------|
| 8420 | ElephantBroker Runtime | HTTP | `0.0.0.0` (current) | OpenClaw VM only -- firewall to gateway IP |
| 8421 | HITL Middleware | HTTP | `0.0.0.0` (current) | OpenClaw VM only -- firewall to gateway IP |
| 7474 | Neo4j HTTP | HTTP | Docker network | NEVER expose externally |
| 7687 | Neo4j Bolt | TCP | Docker network | NEVER expose externally |
| 6333 | Qdrant HTTP | HTTP | Docker network | NEVER expose externally |
| 6334 | Qdrant gRPC | gRPC | Docker network | NEVER expose externally |
| 6379 | Redis | TCP | Docker network | NEVER expose externally |
| 4317 | OTEL Collector gRPC | gRPC | Docker network | NEVER expose externally |
| 8123 | ClickHouse HTTP | HTTP | Docker network | NEVER expose externally |
| 9000 | ClickHouse native | TCP | Docker network | NEVER expose externally |
| 16686 | Jaeger UI | HTTP | Docker network | Admin access only (VPN/bastion) |
| 13000 | Grafana UI | HTTP | Docker network | Admin access only (VPN/bastion) |

### 2.2 Recommended Firewall Rules (iptables/nftables)

```bash
# DB VM: Allow runtime port from OpenClaw VM only
iptables -A INPUT -p tcp --dport 8420 -s OPENCLAW_VM_IP -j ACCEPT
iptables -A INPUT -p tcp --dport 8420 -j DROP

iptables -A INPUT -p tcp --dport 8421 -s OPENCLAW_VM_IP -j ACCEPT
iptables -A INPUT -p tcp --dport 8421 -j DROP

# Block all infrastructure ports from external access
for port in 7474 7687 6333 6334 6379 4317 8123 9000; do
    iptables -A INPUT -p tcp --dport $port -s 127.0.0.1 -j ACCEPT
    iptables -A INPUT -p tcp --dport $port -j DROP
done

# Admin UIs: VPN/bastion only
for port in 16686 13000; do
    iptables -A INPUT -p tcp --dport $port -s VPN_SUBNET -j ACCEPT
    iptables -A INPUT -p tcp --dport $port -j DROP
done
```

### 2.3 Docker Compose Network Isolation

The current `docker-compose.yml` publishes ports on the host with mapped numbers (e.g., `17474:7474`). For production, use internal-only Docker networks:

```yaml
services:
  neo4j:
    # Remove "ports" section entirely for production
    # ports:                          # DO NOT EXPOSE
    #   - "17474:7474"
    #   - "17687:7687"
    networks:
      - eb-internal

  qdrant:
    networks:
      - eb-internal

  redis:
    networks:
      - eb-internal

networks:
  eb-internal:
    driver: bridge
    internal: true   # No external access
```

The runtime process connects to infrastructure via `localhost` (venv-based, not Docker), so Docker port mapping is needed only if the runtime runs on a different host than Docker. In the current single-VM setup, use Docker's host network mode or explicit container-name DNS.

### 2.4 TLS Termination

The runtime and HITL middleware serve plain HTTP. For production:

**Option A: Reverse proxy (recommended)**
```nginx
# /etc/nginx/conf.d/elephantbroker.conf
server {
    listen 443 ssl;
    server_name eb.internal.example.com;

    ssl_certificate     /etc/ssl/certs/eb.pem;
    ssl_certificate_key /etc/ssl/private/eb.key;
    ssl_protocols       TLSv1.3;

    # Only allow OpenClaw VM IP
    allow OPENCLAW_VM_IP;
    deny all;

    location / {
        proxy_pass http://127.0.0.1:8420;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        # Preserve EB identity headers
        proxy_pass_request_headers on;
    }
}
```

**Option B: Mutual TLS (mTLS)** for gateway-to-runtime authentication:
- Issue client cert to OpenClaw VM
- Configure nginx `ssl_client_certificate` + `ssl_verify_client on`
- This replaces or supplements the missing AuthMiddleware

---

## 3. Authentication & Authorization

### 3.1 AuthMiddleware: CRITICAL GAP

**Current state:** The `AuthMiddleware` in `elephantbroker/api/middleware/auth.py` is a stub that always passes. Every API endpoint is completely unauthenticated.

```python
class AuthMiddleware(BaseHTTPMiddleware):
    """Stub: always passes. Real auth in a future phase."""
    async def dispatch(self, request: Request, call_next) -> Response:
        return await call_next(request)
```

**Risk:** Any network-reachable client can call any endpoint, including:
- `/memory/store` -- inject arbitrary facts into any gateway's knowledge graph
- `/memory/{id}` DELETE -- delete facts (GDPR endpoint, no auth)
- `/admin/organizations` POST -- create orgs
- `/admin/actors` POST -- register actors with arbitrary authority levels
- `/consolidation/trigger` POST -- trigger consolidation pipeline
- `/context/assemble` POST -- read assembled context (data exfiltration)

**Minimum viable fix:** Implement API key validation in AuthMiddleware:
```python
class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path.startswith("/health"):
            return await call_next(request)
        api_key = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if not api_key or not hmac.compare_digest(api_key, self._expected_key):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
        return await call_next(request)
```

### 3.2 Gateway Identity Header Trust Model

**Current state:** `GatewayIdentityMiddleware` trusts `X-EB-Gateway-ID`, `X-EB-Agent-Key`, `X-EB-Agent-ID`, `X-EB-Session-Key`, and `X-EB-Actor-Id` headers blindly from any caller.

**Risks:**
- An attacker can set `X-EB-Gateway-ID: victim-gateway` and access/modify another gateway's data
- An attacker can set `X-EB-Actor-Id` to any UUID and inherit that actor's authority level for admin API calls
- The fallback to `default_gateway_id` ("local") when header is absent means unidentified requests still get routed

**Mitigations (short-term):**
1. Implement AuthMiddleware to validate callers before trusting headers
2. Pin expected gateway_id per deployment -- reject requests with unexpected gateway_id values
3. For admin API: require the actor_id to match a registered actor and validate via a signed token (not just a UUID in a header)

**Mitigations (long-term):**
1. mTLS between gateway and runtime, with gateway_id bound to client certificate CN/SAN
2. JWT-based identity tokens signed by the gateway, validated by the runtime

### 3.3 Admin API Authority Model

**Current state:** The admin API (`/admin/*`) uses `check_authority()` from `elephantbroker/api/routes/_authority.py`. Authority checks are based on:
- `X-EB-Actor-Id` header (self-asserted, not authenticated)
- Actor's `authority_level` from Neo4j
- Per-action rules in `AuthorityRuleStore`

**Bootstrap mode risk:** When the graph is empty (`bootstrap_mode=True`), the BOOTSTRAP_ACTIONS (`create_org`, `create_team`, `register_actor`) bypass ALL authority checks. The bootstrap admin gets `authority_level=90` with a synthetic `ActorRef`. After first actor creation, bootstrap mode is disabled by setting `container._bootstrap_mode = False` -- but this is an in-memory flag, reset on restart if the graph is wiped.

**Risks:**
1. If an attacker reaches the API before legitimate bootstrap, they own the org hierarchy
2. Bootstrap mode detection is lazy -- it depends on `container.check_bootstrap_mode()` or `container._bootstrap_mode`, which checks the graph for existing actors. If Neo4j is slow or the check fails, the fallback may incorrectly allow bootstrap
3. `register_actor` in bootstrap can set `authority_level` to any value -- an attacker can create a level-90 actor

**Mitigations:**
1. Bootstrap should require a one-time bootstrap token (generated at deploy time, used once, then invalidated)
2. After bootstrap, explicitly lock the bootstrap endpoint via a persistent flag (not just in-memory)
3. Add rate limiting to admin endpoints

### 3.4 HITL HMAC Validation

**Current state:** The HITL middleware validates HMAC-SHA256 signatures on callback endpoints (`/callbacks/approve`, `/callbacks/reject`). The implementation in `hitl-middleware/hitl_middleware/security.py` uses `hmac.compare_digest()` (timing-safe) and includes token expiry checking (`is_token_expired`, 3600-second window).

**Strengths:**
- Timing-safe comparison prevents timing attacks
- Token includes `request_id` and `created_at` timestamp -- not replayable across requests
- Empty secret causes validation to return `False` (fail-closed)

**Gaps:**
- If `EB_HITL_CALLBACK_SECRET` is not set (default: `""`), the HITL middleware rejects callbacks with 403 -- but the runtime's `HitlClient` still sends approval requests that will generate callback URLs with no HMAC protection
- The intent endpoints (`/intents/notify`, `/intents/approval`) have NO authentication -- any caller can submit fake guard events to the HITL middleware
- The 3600-second (1 hour) expiry window is generous; consider reducing to 300 seconds for approve/reject callbacks

### 3.5 Missing Security Controls

| Control | Status | Priority |
|---------|--------|----------|
| API authentication (AuthMiddleware) | STUB -- always passes | CRITICAL |
| Rate limiting | NOT IMPLEMENTED | HIGH |
| CORS policy | NOT CONFIGURED | MEDIUM (if any browser-based admin UI planned) |
| Request size limits | FastAPI defaults only | MEDIUM |
| Input sanitization on Cypher params | Parameterized queries used (safe) | DONE |
| HITL intent endpoint auth | NOT IMPLEMENTED | HIGH |

---

## 4. Data Isolation

### 4.1 Gateway Isolation Mechanism

The gateway identity model provides logical data isolation via `gateway_id` on all DataPoints. The rule (from CLAUDE.md): "All Cypher queries MUST include `WHERE ... gateway_id = $gateway_id`."

### 4.2 Known Isolation Gaps: `get_entity` Without gateway_id

The `GraphAdapter.get_entity()` method has an optional `gateway_id` parameter. When called without it (`gateway_id=None`), the query matches any node with the given `eb_id` regardless of gateway ownership.

**Calls WITHOUT gateway_id scoping (cross-gateway data leakage vectors):**

| File | Line | Method | Risk |
|------|------|--------|------|
| `runtime/actors/registry.py` | 95 | `resolve_actor()` | Can resolve actors from other gateways; used in authority checks |
| `runtime/goals/manager.py` | 79, 105 | `get_hierarchy()`, `update_goal_status()` | Can read/modify goals from other gateways |
| `runtime/retrieval/orchestrator.py` | 280 | structural search result hydration | Can hydrate facts from other gateways into retrieval results |
| `runtime/memory/facade.py` | 253, 274, 292, 303, 340 | `get()`, `update()`, `decay()`, `promote_scope()`, `delete()` | Can read/modify/delete facts from other gateways (GDPR delete has gateway pre-check, but read/update do not) |
| `runtime/context/lifecycle.py` | 212, 219 | `bootstrap()` org/team lookup | Can read org/team entities (these are intentionally cross-gateway, but worth noting) |
| `api/routes/admin.py` | 185, 250, 464, 474 | org/team/goal admin | Can read/modify entities across gateways |

**High-risk paths:**
- `memory/facade.py:get()` and `update()` -- a fact_id UUID from one gateway can be fetched/modified by a request with a different `X-EB-Gateway-ID`
- `actors/registry.py:resolve_actor()` -- used by `check_authority()`, meaning a request with a spoofed `X-EB-Actor-Id` pointing to a high-authority actor in ANY gateway would pass authority checks

**Note:** The GDPR delete path (`memory/facade.py:339-370`) correctly implements a gateway ownership pre-check: it reads the entity, compares `entity_gw` to `self._gateway_id`, and raises `PermissionError` on mismatch. This pattern should be replicated on all read/update paths.

### 4.3 OrganizationDataPoint / TeamDataPoint Cross-Gateway Design

Per the architecture: `OrganizationDataPoint` and `TeamDataPoint` intentionally do NOT carry `gateway_id` -- they are business entities that span gateways. This is correct by design but means:
- Any gateway can read any org/team
- The admin API's `list_organizations` and `list_teams` return all orgs/teams, not gateway-scoped ones
- Team membership (`MEMBER_OF` edges) is not gateway-scoped

### 4.4 Cognee Dataset Scoping

Cognee datasets are gateway-scoped: `f"{gateway_id}__{base}"`. This provides isolation for `cognee.search()` calls (keyword, semantic, graph completion). However:
- Direct Cypher queries bypass Cognee dataset scoping -- they query the shared Neo4j database
- Qdrant collections are collection-name-scoped (e.g., `FactDataPoint_text`) but NOT gateway-scoped within a collection -- vector search returns results across gateways unless post-filtered

### 4.5 GDPR Compliance Gaps

| Gap | Description | Impact |
|-----|-------------|--------|
| Cognee internal data | `cognee.add()` + `cognee.cognify()` creates internal chunks, entities, and triplets. The GDPR delete path removes the DataPoint node and Qdrant vector but does NOT clean up Cognee's internal pipeline artifacts | Residual PII in chunk nodes and entity extraction outputs |
| ClickHouse traces | OTEL trace logs exported to ClickHouse contain fact text, actor IDs, session data. No deletion API exists for trace data | Trace data persists beyond fact deletion |
| Redis session data | Session state, working sets, and embedding cache contain fact content. TTL-based expiry (default 86400s) but no immediate purge on GDPR delete | Cached copies survive deletion for up to 24 hours |
| SQLite audit stores | Procedure audit, session goal audit, consolidation reports contain fact references and possibly content | Audit trail retains references to deleted data |
| Qdrant best-effort delete | Vector deletion is wrapped in try/except with a warning log -- failure is non-fatal | Orphaned vectors in Qdrant after failed delete |

---

## 5. Default Credentials

### 5.1 ALL Default Passwords/Keys That MUST Be Changed

> **B7 line-number audit:** Path:line references in this table drift on every
> schema reorg (the F2/F3 unification alone shifted every `schemas/config.py`
> line by hundreds). Locations are now expressed as search anchors against the
> file — grep for the quoted token to find the current line.

| Default | Location (search anchor) | Replacement |
|---------|--------------------------|-------------|
| Neo4j: `elephant_dev` | `elephantbroker/schemas/config.py` (search `neo4j_password: str = "elephant_dev"` in `CogneeConfig`); `elephantbroker/config/default.yaml` (search `neo4j_password:`); `infrastructure/docker-compose.yml` (search `NEO4J_AUTH=neo4j/elephant_dev`); `_apply_inheritance_fallbacks()` does NOT cover this — set `EB_NEO4J_PASSWORD` explicitly | Strong random password (32+ chars) |
| ClickHouse: `""` (empty) | `infrastructure/docker-compose.yml` (search `CLICKHOUSE_PASSWORD`), `infrastructure/otel-collector-config.yaml` (search `clickhouse:` exporter block) | Strong random password; update both compose and collector config |
| Redis: no auth | `infrastructure/docker-compose.yml` (no `--requirepass` in the `redis:` service command) | Set `requirepass` in redis.conf; update `EB_REDIS_URL` to include password |
| HITL HMAC: `""` (empty) | `elephantbroker/schemas/config.py` (search `class HitlConfig` → `callback_hmac_secret: str = ""`) | `openssl rand -hex 32`; set in both runtime (`EB_HITL_CALLBACK_SECRET`) and HITL env files |
| Gateway ID: `"local"` | `elephantbroker/schemas/config.py` (search `class GatewayConfig` → `gateway_id: str = "local"`); `elephantbroker/config/default.yaml` (search `gateway_id:` under the top-level `gateway:` block) | Unique per-deployment identifier (e.g., `gw-prod-us-east-1`) — set via `EB_GATEWAY_ID` |
| Grafana: default `admin/admin` | Docker image default | Set `GF_SECURITY_ADMIN_PASSWORD` in compose env |
| Jaeger: no auth | Docker image default | Deploy behind reverse proxy with auth; or use production Jaeger with auth |

### 5.2 Pre-Deployment Security Checklist

> **B4 PR #3 status markers:** Items marked `(✓ PR #3)` are now implemented by
> `deploy/install.sh`, `deploy/update.sh`, the systemd unit files, or the
> packaged config. They still belong on the checklist as gates the operator
> should verify, but they no longer require manual scripting work.

```
SECRETS
[ ] Neo4j password changed from "elephant_dev" to strong random value
[ ] EB_NEO4J_PASSWORD set in /etc/elephantbroker/env
[ ] NEO4J_AUTH updated in docker-compose.yml
[ ] Redis requirepass configured
[ ] EB_REDIS_URL includes redis password (redis://:PASSWORD@host:6379)
[ ] ClickHouse password set (non-empty)
[ ] OTEL collector config updated with ClickHouse password
[ ] EB_HITL_CALLBACK_SECRET generated (openssl rand -hex 32)
    (✓ PR #3: install.sh F11 block auto-generates and writes the secret to
     both /etc/elephantbroker/env and /etc/elephantbroker/hitl.env on first
     install — operator only needs to verify post-install that both files
     contain the same value)
[ ] EB_HITL_CALLBACK_SECRET set in BOTH env and hitl.env
    (✓ PR #3: install.sh F11 enforces this; update.sh preserves the
     existing secret across upgrades)
[ ] EB_LLM_API_KEY set (not empty)
[ ] EB_EMBEDDING_API_KEY set (not empty)
[ ] Grafana admin password changed (if observability profile used)
[ ] No secrets committed to git repository
[ ] Env files have 0640 permissions, owned by root:elephantbroker
    (✓ PR #3 / Bucket C C2: install.sh writes /etc/elephantbroker/env and
     hitl.env as root:elephantbroker mode 0640 — root owns the files so a
     compromised runtime cannot rewrite its own secrets, but the service
     group can read them. The previous 0600/service-user form let the
     runtime overwrite its own credentials.)

NETWORK
[ ] Firewall rules restrict port 8420 to OpenClaw VM IP only
[ ] Firewall rules restrict port 8421 to OpenClaw VM IP only
[ ] Infrastructure ports (7474, 7687, 6333, 6334, 6379) blocked from external
[ ] TLS termination configured (reverse proxy or mTLS)
[ ] Docker Compose ports section removed or using internal-only network
[ ] Admin/observability UIs (Jaeger, Grafana) behind VPN or IP whitelist

AUTHENTICATION
[ ] AuthMiddleware stub replaced with real API key validation
[ ] API key shared securely with OpenClaw VM plugins
[ ] HITL intent endpoints authenticated (runtime -> HITL calls)
[ ] Bootstrap completed; bootstrap_mode confirmed disabled
[ ] No actors with authority_level >= 90 beyond intended admins

DATA ISOLATION
[ ] EB_GATEWAY_ID is unique per deployment and set consistently
[ ] EB_GATEWAY_ID matches between runtime env and plugin env
[ ] get_entity calls audited for gateway_id parameter usage
[ ] GDPR delete tested end-to-end (Neo4j + Qdrant + Redis purge)

CONTAINER/PROCESS
[ ] Runtime runs as non-root dedicated service user
    (✓ PR #3 / Bucket C C1: install.sh creates the dedicated `elephantbroker`
     system user with `useradd --system --shell /usr/sbin/nologin
     --home-dir /var/lib/elephantbroker` — no interactive login possible)
[ ] Dockerfile uses non-root USER directive
[ ] systemd service has security hardening directives
    (✓ PR #3 / Bucket D: ProtectSystem=strict, NoNewPrivileges=true,
     RestrictAddressFamilies, ProtectKernelTunables/Modules/Logs/Clock,
     RestrictNamespaces, LockPersonality, UMask=0027, MemoryMax/CPUQuota
     resource caps, and ReadWritePaths narrowed to /var/lib/elephantbroker
     plus /opt/elephantbroker/.venv/lib only — see deploy/systemd/)
[ ] Data directory permissions restricted to service user
    (✓ PR #3 / Bucket C C3: /opt/elephantbroker/ stays root-owned;
     only /var/lib/elephantbroker and the Cognee writable subdirs under
     .venv/lib/.../cognee/ are chowned to elephantbroker:elephantbroker)
[ ] Cognee writable directories have minimal permissions (not 777)
    (✓ PR #3 / Bucket C C3: install.sh now uses dedicated user ownership
     instead of the old `chmod -R 777` workaround — Cognee's
     .cognee_system/ and .data_storage/ are chowned to elephantbroker
     with the default 0750 mode)
[ ] Log files do not contain API keys or passwords
```

---

## 6. Container & Process Security

### 6.1 Dockerfile Improvements

> **STATUS: PARTIALLY IMPLEMENTED.** The `Dockerfile` was rewritten to use
> `uv sync --frozen` (matching the native install path) and the broken
> `pip install --force-reinstall mistralai` hack was removed. However, the
> Dockerfile is still labeled as dev/CI-only per `CLAUDE.md` — production
> deployments use `deploy/install.sh` on a real host with the dedicated
> `elephantbroker` system user.
>
> Remaining gaps for hardening the Dockerfile (if you want to run the
> container in production):
> - No `USER` directive (runs as root inside the container)
> - No `HEALTHCHECK` instruction
> - No read-only filesystem
> - No `.dockerignore` audit for secrets
>
> The Recommended Dockerfile below shows what a fully-hardened version
> would look like — it builds on the current uv-based Dockerfile and adds
> a non-root user and HEALTHCHECK. Apply only if you intend to run the
> container in production despite the CLAUDE.md guidance.

**Recommended Dockerfile (extends the current uv-based one):**
```dockerfile
FROM python:3.11-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY elephantbroker/ elephantbroker/

RUN uv sync --frozen --no-dev

FROM python:3.11-slim AS runtime
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /usr/local/bin/

# Create non-root user
RUN groupadd -r elephantbroker && useradd -r -g elephantbroker -d /app -s /sbin/nologin elephantbroker

WORKDIR /app
COPY --from=builder --chown=elephantbroker:elephantbroker /app/.venv /app/.venv
COPY --from=builder --chown=elephantbroker:elephantbroker /app/elephantbroker /app/elephantbroker
COPY --from=builder /app/pyproject.toml /app/pyproject.toml
COPY --chown=elephantbroker:elephantbroker elephantbroker/config/default.yaml /etc/elephantbroker/default.yaml

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8420/health/')" || exit 1

USER elephantbroker
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8420

# Read-only root filesystem (data volume mounted separately)
# Use --read-only flag in docker run / compose

ENTRYPOINT ["elephantbroker", "serve", "--config", "/etc/elephantbroker/default.yaml"]
```

### 6.2 HITL Middleware Dockerfile

`hitl-middleware/Dockerfile` has the same hardening gaps. Apply identical
hardening (non-root user, HEALTHCHECK, minimal base) if running in production.

### 6.3 Docker Compose Network Isolation

Add network segmentation to `infrastructure/docker-compose.yml`:

```yaml
networks:
  eb-data:
    driver: bridge
    internal: true      # No external access
  eb-observability:
    driver: bridge
    internal: true

services:
  neo4j:
    networks: [eb-data]
    # Remove ports section
  qdrant:
    networks: [eb-data]
  redis:
    networks: [eb-data]
  otel-collector:
    networks: [eb-observability]
  clickhouse:
    networks: [eb-observability]
  jaeger:
    networks: [eb-observability]
  grafana:
    networks: [eb-observability]
```

### 6.4 systemd Hardening Directives

> **STATUS: IMPLEMENTED.** The systemd unit files in `deploy/systemd/` ship
> with full security hardening directives. They are installed by
> `deploy/install.sh` and run under the dedicated `elephantbroker` system
> user (no shell, no interactive login). The list below documents the
> directives that are actually applied — see `deploy/systemd/elephantbroker.service`
> for the canonical source.

```ini
[Service]
Type=simple
User=elephantbroker
Group=elephantbroker
WorkingDirectory=/var/lib/elephantbroker

EnvironmentFile=/etc/elephantbroker/env
ExecStart=/opt/elephantbroker/.venv/bin/elephantbroker serve --config /etc/elephantbroker/default.yaml --host 0.0.0.0 --port 8420

Restart=on-failure
RestartSec=5

# Filesystem hardening
ProtectSystem=strict
# Bucket D D1: ReadWritePaths narrowed from the entire /opt/elephantbroker
# tree to /opt/elephantbroker/.venv/lib only. Cognee's writable state
# (.venv/lib/python*/site-packages/cognee/.cognee_system/ and .data_storage/)
# is reachable through .venv/lib, but source code, pyproject.toml, uv.lock,
# the venv binaries, and this systemd unit file are now read-only at the
# kernel MAC layer. Pairs with the C3 DAC narrowing where /opt/elephantbroker/
# stays root-owned.
ReadWritePaths=/var/lib/elephantbroker /opt/elephantbroker/.venv/lib
ProtectHome=true
PrivateTmp=true
PrivateDevices=true

# Resource limits (Bucket D D2)
MemoryMax=8G
MemoryHigh=6G
TasksMax=512
CPUQuota=400%
LimitNOFILE=65536

# Privilege hardening
NoNewPrivileges=true
RestrictSUIDSGID=true
LockPersonality=true

# Kernel & namespace hardening
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectKernelLogs=true
ProtectControlGroups=true
ProtectClock=true
RestrictNamespaces=true
RestrictRealtime=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6

# File creation mask (640 by default instead of 644)
UMask=0027
```

`elephantbroker-hitl.service` ships with the same hardening directives —
both unit files share identical security configuration since they run as the
same service user against the same filesystem layout.

### 6.5 Cognee Writable Directories

> **STATUS: IMPLEMENTED (post-Bucket C C3).** `deploy/install.sh` pre-creates
> Cognee's writable state directories and chowns ONLY those subdirs to the
> service user — `/opt/elephantbroker/` itself stays root-owned. The old
> `chmod -R 777` workaround from earlier docs has been removed (it left
> Cognee state world-writable), and the previous `chown -R
> elephantbroker:elephantbroker /opt/elephantbroker` has been narrowed to a
> targeted chown of just the Cognee writable subtree. Pairs with the
> Bucket D D1 systemd `ReadWritePaths` narrowing — together, the runtime
> can write Cognee state but cannot rewrite its own source, lockfile,
> venv binaries, or systemd unit file.

```bash
# What deploy/install.sh actually does after Bucket C:
# Step 5b — pre-create Cognee writable subdirs:
mkdir -p /opt/elephantbroker/.venv/lib/python3.*/site-packages/cognee/.cognee_system/databases
mkdir -p /opt/elephantbroker/.venv/lib/python3.*/site-packages/cognee/.data_storage

# Step 7 — narrowed ownership (NOT a recursive chown of /opt/elephantbroker):
chown -R elephantbroker:elephantbroker \
    /opt/elephantbroker/.venv/lib/python3.*/site-packages/cognee/.cognee_system \
    /opt/elephantbroker/.venv/lib/python3.*/site-packages/cognee/.data_storage
chown -R elephantbroker:elephantbroker /var/lib/elephantbroker
# /opt/elephantbroker/ itself, the venv binaries, source code, pyproject.toml,
# and uv.lock all remain root-owned. A compromised runtime cannot rewrite them.
```

### 6.6 Error Message Information Leakage

The error handler middleware in `elephantbroker/api/middleware/errors.py` passes exception messages directly to the client in JSON responses:

```python
detail = ErrorDetail(code="internal_error", message=str(exc))
return JSONResponse(status_code=500, content=detail.model_dump())
```

For production, internal exception messages should be replaced with generic messages. The full exception should only be logged server-side:

```python
# Production: don't leak internal details
detail = ErrorDetail(code="internal_error", message="An internal error occurred")
return JSONResponse(status_code=500, content=detail.model_dump())
```

---

## Summary of Critical Items by Priority

> **Status column key:** `(✓ PR #3)` indicates the item is implemented in the
> deploy/install + systemd hardening work that shipped with PR #3 (Buckets A,
> C, D, E, F). Items without a status marker are still open and require
> operator action or follow-up work.

| Priority | Item | Section | Status |
|----------|------|---------|--------|
| **CRITICAL** | AuthMiddleware is a stub -- all endpoints unauthenticated | 3.1 | open |
| **CRITICAL** | Neo4j default password `elephant_dev` hardcoded in 4 locations | 5.1 | open |
| **CRITICAL** | Redis has no authentication configured | 5.1 | open |
| **CRITICAL** | Gateway identity headers trusted without validation | 3.2 | open |
| **HIGH** | `get_entity()` calls without `gateway_id` enable cross-gateway data access | 4.2 | open |
| **HIGH** | Bootstrap mode allows unauthenticated org/actor creation | 3.3 | open |
| **HIGH** | HITL intent endpoints (`/intents/*`) have no authentication | 3.4 | open |
| **HIGH** | ClickHouse has empty password | 5.1 | open |
| **HIGH** | Runtime/HITL serve plain HTTP (no TLS) | 2.4 | open |
| **HIGH** | Dockerfile runs as root | 6.1 | open (Dockerfile is dev/CI-only per CLAUDE.md; production uses install.sh + native systemd as a non-root service user — see 6.4) |
| **MEDIUM** | Error handler leaks internal exception messages | 6.6 | open |
| **MEDIUM** | No rate limiting on any endpoint | 3.5 | open |
| **MEDIUM** | GDPR delete does not clean Cognee internal artifacts | 4.5 | open |
| **MEDIUM** | Cognee directories set to 777 | 6.5 | **(✓ PR #3 / Bucket C C3)** install.sh now uses dedicated user ownership instead of `chmod -R 777`; only Cognee writable subdirs are chowned to elephantbroker, the rest of /opt/elephantbroker stays root-owned |
| **MEDIUM** | systemd units lack security hardening | 6.4 | **(✓ PR #3 / Bucket D)** ProtectSystem=strict, NoNewPrivileges, RestrictAddressFamilies, ProtectKernel*, RestrictNamespaces, LockPersonality, UMask=0027, MemoryMax/CPUQuota caps, ReadWritePaths narrowed to /var/lib/elephantbroker + .venv/lib only |
| **MEDIUM** | Docker Compose exposes infrastructure ports on host | 2.3 | open |
| **LOW** | No CORS policy configured | 3.5 |
| **LOW** | Grafana default admin password | 5.1 |
| **LOW** | Qdrant vector delete is best-effort | 4.5 |
