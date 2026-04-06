# ElephantBroker

**A knowledge-grounded cognitive runtime for trustworthy AI agents.**

Elephants never forget, brokers always deliver — ElephantBroker is a unified cognitive runtime that gives AI agents durable memory, goal-aware context assembly, evidence-backed verification, and cheap-first safety enforcement. Built on a Cognee-powered knowledge plane (Neo4j graph + Qdrant vectors), it replaces flat vector stores with a structured, trustworthy, and profile-tuned memory system.

> **Paper:** [arXiv:2603.25097](https://arxiv.org/abs/2603.25097) — *ElephantBroker: A Knowledge-Grounded Cognitive Runtime for Trustworthy AI Agents*

## Key Capabilities

- **Hybrid five-source retrieval** — structural graph queries, lexical matching, semantic vector search, ego-net graph expansion, and artifact retrieval, merged with configurable per-source weights
- **Eleven-dimension competitive scoring** — turn relevance, goal relevance, recency, successful-use prior, confidence with verification multipliers, evidence strength, novelty, redundancy/contradiction penalties, and token cost. Budget-constrained greedy selection with a (1−1/e) submodularity guarantee
- **Four-state evidence verification** — UNVERIFIED → SELF-SUPPORTED → TOOL-SUPPORTED → SUPERVISOR-VERIFIED, with graded confidence multipliers (0.5, 0.7, 0.9, 1.0) feeding into scoring
- **Five-stage context lifecycle** — bootstrap, ingest, assemble, compact, afterTurn. Four-block assembly across two prompt surfaces, six-signal successful-use detection, subagent spawn/end hooks
- **Six-layer guard pipeline** — autonomy domain classification → static rules → semantic matching → structural validators → forced constraint reinjection → optional LLM escalation
- **Nine-stage consolidation engine** — cluster, canonicalize, strengthen, decay, prune, promote, refine procedures, identify verification gaps, recompute salience. Seven of nine stages require zero LLM calls
- **Authority-based multi-org identity** — four-tier numeric permission hierarchy, eleven configurable authority rules, organizations and teams as first-class graph entities

## Architecture

ElephantBroker is organized into four layers:

```
Layer A  Agent Integration     TypeScript plugins (24 tools, 8 lifecycle hooks)
           ↓ HTTP + X-EB-* identity headers
Layer B  Cognitive Runtime     Python (17 modules, 84 API endpoints, FastAPI)
           ↓ Cognee SDK
Layer C  Knowledge Plane       Cognee v0.5.x (7 DataPoint subclasses, graph+vector search)
           ↓
Layer D  Infrastructure        Neo4j · Qdrant · Redis · PostgreSQL · OTEL/Prometheus
```

The runtime exposes two thin TypeScript plugin surfaces:

- **MemoryPlugin** — 24 tools across 6 groups (memory, goals, procedures, artifacts, guards, admin) + session lifecycle hooks
- **ContextEnginePlugin** — 7 lifecycle methods (bootstrap, ingest, assemble, compact, afterTurn, subagent spawn/end) with degraded buffered mode

All intelligence, scoring, storage, and policy logic lives in Python. The plugins are pass-through HTTP adapters.

## Runtime Modules

| Module | Responsibility |
|--------|---------------|
| **ActorRegistry** | Actor CRUD, platform-qualified handles, MEMBER_OF edges, authority chains |
| **GoalManager** | Scope-aware goal CRUD (8 scopes), OWNS_GOAL filtering, blocker tracking |
| **MemoryStoreFacade** | Fact CRUD, dedup at 0.95 cosine similarity, graph edges, GDPR delete |
| **WorkingSetManager** | 11-dimension scoring, budget-constrained greedy selection |
| **ContextAssembler** | 4-block assembly across 2 prompt surfaces |
| **CompactionEngine** | Two-stage: rule-classify (zero LLM) + LLM-summarize. Goal-aware |
| **RetrievalOrchestrator** | 5-source concurrent retrieval with weighted merge |
| **RerankOrchestrator** | 4-stage: cheap prune → semantic → cross-encoder (Qwen3-Reranker-4B) → dedup |
| **ProcedureEngine** | Procedure CRUD, step execution, completion gates, proof validation |
| **EvidenceEngine** | 4-state claim tracking, proof validation, reject with mandatory reason |
| **RedLineGuardEngine** | 6-layer cheap-first pipeline, 5 policy presets, near-miss escalation |
| **ToolArtifactStore** | Tool output CRUD, SHA-256 dedup, summary + pointer model |
| **ConsolidationEngine** | 9-stage "sleep" pipeline, Redis distributed lock, ClickHouse bridge |
| **ProfileRegistry** | Base → named → org override inheritance, 5-minute TTL cache |
| **TraceLedger** | Append-only trace (47 event types), OTEL export, session timeline queries |
| **StatsEngine** | 49 Prometheus counters/gauges |
| **ScoringTuner** | Per-gateway weight persistence (PostgreSQL), EMA-smoothed deltas (±5% cap) |

### Adapters

| Adapter | Purpose |
|---------|---------|
| **GraphAdapter** | Structural reads (`query_cypher`, `get_neighbors`), custom edges, GDPR delete. No writes. |
| **VectorAdapter** | Filtered vector search on Cognee-managed collections. GDPR delete. No writes. |
| **EmbeddingService** | Dedup similarity checks, query embedding |
| **LLMClient** | LiteLLM wrapper with model prefix routing, token estimation |

> **Cognee is the knowledge plane.** All DataPoints are stored via `add_data_points()`. Adapter write methods have been removed — adapters handle reads, deletes, edges, and structural queries only.

## Data Model

**Seven DataPoint subclasses** map schemas to graph-vector storage:

| Entity | Indexed Fields | Vector Collection |
|--------|---------------|-------------------|
| FactDataPoint | text | FactDataPoint_text |
| ActorDataPoint | display_name | ActorDataPoint_display_name |
| GoalDataPoint | title, description | GoalDataPoint_title |
| ProcedureDataPoint | name, description | ProcedureDataPoint_name |
| ArtifactDataPoint | summary | ArtifactDataPoint_summary |
| OrganizationDataPoint | name | OrganizationDataPoint_name |
| TeamDataPoint | name | TeamDataPoint_name |

**Five memory classes:** EPISODIC (decays, promotable to SEMANTIC after 3+ sessions), SEMANTIC (durable knowledge), PROCEDURAL (workflows, no decay), POLICY (non-negotiable constraints, always injected), WORKING_MEMORY (session-only, dropped at end).

**Eight scopes:** GLOBAL → ORGANIZATION → TEAM → ACTOR → SESSION → TASK → SUBAGENT → ARTIFACT, each with distinct visibility rules, decay cadence, and promotion targets.

**Twelve fact categories:** identity, preference, event, decision, system, relationship, trait, project, general, constraint, procedure_ref, verification.

## Identity Model

Three-level isolation enforced across all infrastructure:

| Level | Identifier | Scope |
|-------|-----------|-------|
| **Gateway** | `EB_GATEWAY_ID` | Redis keys, Cypher WHERE clauses, dataset prefixes, metrics, traces, logs |
| **Agent** | `agent_key = {gateway_id}:{agentId}` | Deterministic UUID v5, registers as ActorRef |
| **Session** | `sessionKey` (stable) + `sessionId` (ephemeral) | Routing + lifecycle |

Four `X-EB-*` headers transmitted on every request. `GatewayIdentityMiddleware` extracts them into `request.state`.

**Authority model:** Regular actor (0-49) < Team lead (50-69) < Org admin (70-89) < System admin (90+). Eleven configurable rules with matching-exempt semantics.

## Retrieval Pipeline

Formalized as **f(α) = σ(ρ(φ(α)))** — multi-source search (recall) → four-stage reranker (precision) → eleven-dimension scoring + budget selection (value optimization).

**Five sources** dispatched concurrently:

| Source | Method | Default Weight |
|--------|--------|---------------|
| Structural | Neo4j Cypher (session_key, actor, scope filters) | 0.4 |
| Keyword | Cognee `CHUNKS_LEXICAL` (BM25-like) | 0.3 |
| Semantic | Cognee `CHUNKS` or direct vector | 0.5 |
| Graph expansion | Cognee `GRAPH_COMPLETION` (ego-net traversal) | 0.2 |
| Artifact | Vector search over artifact collections | 0.5 |

**Four-stage reranking:** cheap prune (token overlap + retrieval score) → semantic reranking (cosine similarity) → cross-encoder (Qwen3-Reranker-4B) → near-duplicate merge (union-find, cosine > 0.95).

Five-level graceful degradation — no single backend failure blocks the pipeline.

## Scoring

**Pass 1 — Nine independent dimensions:** turn relevance, session goal relevance, global goal relevance, recency (exponential decay with per-profile half-life), successful-use prior, confidence × verification multiplier, evidence strength, novelty, cost penalty.

**Pass 2 — Two interaction-dependent dimensions** (computed during greedy selection): redundancy penalty (cosine similarity to already-selected), contradiction penalty (graph edges + semantic confidence gap).

Mandatory items (constraints, goals with blockers, procedures requiring proof) pre-allocated unconditionally. Remaining candidates selected greedily under token budget.

**Per-profile presets** (coding, research, managerial, worker, personal_assistant) control all weights, half-life, budget, isolation level, graph mode, and graph depth.

## Guard Pipeline

Six layers, each cheaper than the next:

| Layer | Method | Cost |
|-------|--------|------|
| 0. Autonomy classification | 10 domains × 4 autonomy levels | Microseconds |
| 1. Static rules | 12 builtins + profile + procedure-bound + custom | Microseconds |
| 2. Semantic matching | BM25 + cosine similarity against exemplars | Milliseconds |
| 3. Structural validation | Graph queries for approvals, evidence, confirmations | Milliseconds |
| 4. Forced reinjection | Constraints injected into Block 1 of context assembly | Zero LLM |
| 5. LLM escalation | Safety-focused prompt for ambiguous cases (opt-in) | One LLM call |

Near-miss escalation: three `warn` results within five turns auto-tightens strictness.

**Procedures-Guards-Evidence triangle:** procedure activation loads guard bindings → guard violation demands evidence → procedure completion validates all proof requirements.

## Consolidation ("Sleep") Pipeline

Nine stages, gateway-scoped with Redis distributed lock:

1. **Cluster near-duplicates** — cosine > 0.92, union-find
2. **Canonicalize** — majority voting, LLM only for ambiguous (<5%)
3. **Strengthen** — `c' = min(c + s/u × 0.3, 1.0)` for high-success facts
4. **Decay** — recalled-unused: `c × 0.9^t_r`, never-recalled: `c × 0.95^t_d`
5. **Prune autorecall** — blacklist 5+ recalls with 0 successful uses
6. **Promote** — EPISODIC → SEMANTIC after 3+ sessions
7. **Refine procedures** — trace pattern detection + LLM draft (1 LLM call)
8. **Verification gaps** — missing evidence flagged for supervisor
9. **Recompute salience** — EMA weight adjustment, capped at ±5%

Seven of nine stages require zero LLM calls.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Runtime | Python 3.11+, FastAPI (port 8420), Pydantic v2 |
| Knowledge plane | Cognee v0.5.x SDK |
| Graph store | Neo4j 5 (community) |
| Vector store | Qdrant v1.17.0 |
| Cache | Redis 7 |
| Audit stores | PostgreSQL (procedures, session goals, authority rules, tuning deltas) |
| LLM | Configurable via LiteLLM proxy (default: gemini/gemini-2.5-pro) |
| Embeddings | openai/text-embedding-3-large |
| Reranker | Qwen3-Reranker-4B (external HTTP endpoint) |
| Observability | OpenTelemetry + Prometheus (49 metrics), ClickHouse, Jaeger, Grafana |
| Plugins | TypeScript (thin HTTP adapters for OpenClaw) |

## Project Structure

```
elephantbroker/
  runtime/                     # 17 modules + adapters
    actors/                    # ActorRegistry
    goals/                     # GoalManager
    memory/                    # MemoryStoreFacade
    working_set/               # WorkingSetManager + SessionGoalStore
    context/                   # ContextAssembler + ContextLifecycle
    compaction/                # CompactionEngine
    retrieval/                 # RetrievalOrchestrator
    rerank/                    # RerankOrchestrator
    procedures/                # ProcedureEngine
    evidence/                  # EvidenceEngine
    guards/                    # RedLineGuardEngine
    artifacts/                 # ToolArtifactStore
    consolidation/             # ConsolidationEngine (9-stage)
    profiles/                  # ProfileRegistry
    trace/                     # TraceLedger
    stats/                     # StatsEngine (Prometheus)
    adapters/
      cognee/                  # GraphAdapter, VectorAdapter, EmbeddingService, DataPoints
      llm/                     # LLMClient (LiteLLM wrapper)
  pipelines/                   # Cognee Task chains (turn/artifact/procedure ingest, verification)
  schemas/                     # 20 Pydantic v2 models (source of truth)
  api/                         # FastAPI (84 endpoints, 16 route groups)
    routes/
    middleware/                # GatewayIdentityMiddleware
  config/                      # Default YAML configuration
  server.py                    # uvicorn entry point
  cli.py                       # ebrun admin CLI

openclaw-plugins/
  elephantbroker-memory/       # 24 tools + 8 lifecycle hooks
  elephantbroker-context/      # ContextEnginePlugin (7 lifecycle methods)

infrastructure/                # Docker Compose (Neo4j, Qdrant, Redis, observability)
hitl-middleware/               # HITL approval queue service (port 8421)
tests/                         # 2405+ unit tests, 156 integration tests
```

## Quick Start

### Infrastructure

```bash
cd infrastructure
docker compose up -d              # Neo4j, Qdrant, Redis
docker compose --profile observability up -d  # + ClickHouse, Jaeger, Grafana (optional)
```

### Runtime

```bash
python -m venv .venv && source .venv/bin/activate
uv sync

export EB_GATEWAY_ID="my-gateway"
export EB_NEO4J_URI="bolt://localhost:17687"
export EB_QDRANT_URL="http://localhost:16333"
export EB_REDIS_URL="redis://localhost:16379"
export EB_LLM_MODEL="gemini/gemini-2.5-pro"

elephantbroker serve --port 8420
```

### Bootstrap

```bash
ebrun bootstrap --org-name "Acme" --team-name "Engineering" --admin-name "Admin"
```

### Health Check

```bash
curl http://localhost:8420/health/ready
```

## API Overview

84 endpoints across 16 route groups:

| Group | Endpoints | Purpose |
|-------|-----------|---------|
| `/memory` | 15 | Store, search, update, forget, ingest messages |
| `/context` | 6 | Bootstrap, ingest, assemble, compact, dispose |
| `/goals` | 10 | Create, list, update status, add blockers, refine |
| `/procedures` | 5 | Create, activate, execute steps, complete |
| `/guards` | 8 | Preflight check, refresh, events, policy |
| `/working-set` | 3 | Build, status, explain |
| `/trace` | 6 | Query, timeline, summary, sessions, event types |
| `/consolidation` | 7 | Run, cancel, status, reports, tuning deltas |
| `/sessions` | 5 | New, list, get, context state, dispose |
| `/admin` | 20+ | Bootstrap, org/team/actor CRUD, authority rules, profile overrides |
| `/health` | 2 | Liveness + readiness (checks Neo4j, Qdrant, Redis, LLM, reranker) |
| `/metrics` | 1 | Prometheus exposition |
| + others | | actors, profiles, claims, artifacts, rerank, stats |

## Testing

```bash
# Unit tests (fast, no external deps)
pytest tests/unit/ -x

# Integration tests (requires infrastructure running)
pytest tests/integration/ -x

# Scenario tests
python -m tests.scenarios.runner --json
```

2,405+ unit tests, 156 integration tests, 7 end-to-end scenarios (basic memory, multi-turn consistency, goal-driven context, context lifecycle, subagent delegation, procedure execution, guard pipeline).

## Documentation

| Document | Description |
|----------|-------------|
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Full configuration reference (76 environment variables) |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Production deployment guide (native venv + docker infrastructure) |
| [docs/OPENCLAW-SETUP.md](docs/OPENCLAW-SETUP.md) | OpenClaw plugin installation and configuration |
| [elephantbroker_architecture_v1.html](../elephantbroker_architecture_v1.html) | Full architecture specification |

## License

AGPL-3.0
