# ElephantBroker Deployment Guide

## Recommended: Production (Docker Compose)

The recommended production deployment uses `docker-compose.prod.yml` which brings up the **complete stack** as containers — no native venv required.

```
Host
├─ docker-compose.prod.yml
│  ├─ postgres       :5432   (audit stores — authority rules, procedures, etc.)
│  ├─ neo4j          :7474/:7687
│  ├─ qdrant         :6333
│  ├─ redis          :6379
│  ├─ elephantbroker :8420   (runs `migrate` then `serve` on start)
│  └─ hitl           :8421
└─ OpenClaw VM → HTTP to host:8420
```

### 1. Setup Environment

```bash
cd infrastructure/
cp .env.prod.example .env.prod
# Edit .env.prod with real credentials and API keys
```

### 2. Start the Stack

```bash
docker compose -f docker-compose.prod.yml --env-file .env.prod up -d
```

Migrations run automatically before the server starts (`elephantbroker migrate` is the container entrypoint). The runtime only becomes healthy after all infra dependencies pass their health checks.

### 3. Bootstrap (first run only)

```bash
docker compose -f docker-compose.prod.yml exec elephantbroker \
  ebrun bootstrap \
    --org-name "YourOrg" \
    --team-name "YourTeam" \
    --admin-name "admin" \
    --admin-handles "email:you@example.com"
```

### 4. Verify

```bash
curl http://localhost:8420/health/ready
curl http://localhost:8421/health
docker compose -f docker-compose.prod.yml logs elephantbroker
```

Logs are structured JSON — pipe through `jq` for readability:
```bash
docker compose -f docker-compose.prod.yml logs elephantbroker | jq .
```

### 5. Observability (optional)

```bash
docker compose -f docker-compose.prod.yml --profile observability up -d
# Jaeger UI: http://localhost:16686
# Grafana:   http://localhost:3000
```

### 6. Update (after git pull)

```bash
git pull
docker compose -f docker-compose.prod.yml --env-file .env.prod build
docker compose -f docker-compose.prod.yml --env-file .env.prod up -d
# Migrations applied automatically on restart
```

---

## Legacy / Development: Native venv + Docker infra

> **Use this for local development only.** The runtime runs as a native Python process (venv) while infrastructure services (Neo4j, Qdrant, Redis) run via Docker Compose.

### Architecture

```
Dev Machine
├─ Python venv (uv)
│  ├─ elephantbroker serve  :8420
│  └─ python -m hitl_middleware :8421
├─ Docker Compose (infra only)
│  ├─ Neo4j     :17474/:17687
│  ├─ Qdrant    :16333
│  └─ Redis     :16379
└─ PostgreSQL (local or Docker)
```

### Prerequisites

- Python 3.11+ and [`uv`](https://github.com/astral-sh/uv)
- Docker + Docker Compose
- Node.js 18+ (OpenClaw VM only)
- LiteLLM proxy or OpenAI-compatible endpoint

### 1. Infrastructure Services

```bash
cd infrastructure/
docker compose up -d neo4j qdrant redis

# With observability (optional):
docker compose --profile observability up -d
```

### 2. Python venv (via uv)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
cd /opt/elephant-broker
uv sync          # creates .venv and installs all deps from uv.lock
source .venv/bin/activate

# HITL middleware
cd hitl-middleware && uv sync && cd ..
```

> **Note:** The `mistralai` conflict is resolved automatically by `uv`'s lock file — no post-install scripts needed.

> **Cognee writable dirs** — still required in native venv mode:
> ```bash
> mkdir -p .venv/lib/python3.*/site-packages/cognee/.cognee_system/databases
> mkdir -p .venv/lib/python3.*/site-packages/cognee/.data_storage
> chmod -R 777 .venv/lib/python3.*/site-packages/cognee/
> touch .venv/lib/python3.*/site-packages/.anon_id && chmod 666 .venv/lib/python3.*/site-packages/.anon_id
> ```

### 3. Configuration

Create `/etc/elephantbroker/default.yaml` — see `elephantbroker/config/default.yaml` for template.

**Critical: LLM model prefix.** Cognee requires `openai/` prefix:
```yaml
llm:
  model: "openai/gemini/gemini-2.5-pro"
infra:
  log_format: "text"   # use "json" in production
audit:
  postgres_dsn: "postgresql://elephantbroker:yourpassword@localhost:5432/elephantbroker"
```

### 4. Run Migrations

```bash
export EB_POSTGRES_DSN="postgresql://elephantbroker:yourpassword@localhost:5432/elephantbroker"
elephantbroker migrate --config /etc/elephantbroker/default.yaml
```

### 5. Environment Files

**Runtime** — `/etc/elephantbroker/env`:
```bash
EB_GATEWAY_ID=gw-prod
EB_NEO4J_URI=bolt://localhost:17687
EB_QDRANT_URL=http://localhost:16333
EB_REDIS_URL=redis://localhost:16379
EB_POSTGRES_DSN=postgresql://elephantbroker:yourpassword@localhost:5432/elephantbroker
EB_LLM_API_KEY=your-key
EB_EMBEDDING_API_KEY=your-key
EB_ORG_ID=your-org
EB_TEAM_ID=your-team
EB_HITL_CALLBACK_SECRET=<openssl rand -hex 32>
EB_LOG_FORMAT=json
COGNEE_DISABLE_TELEMETRY=true
```

**HITL** — `/etc/elephantbroker/hitl.env`:
```bash
HITL_HOST=0.0.0.0
HITL_PORT=8421
HITL_LOG_LEVEL=info
EB_HITL_CALLBACK_SECRET=<same secret as runtime>
EB_RUNTIME_URL=http://localhost:8420
```

### 6. systemd Units

**`/etc/systemd/system/elephantbroker.service`** (see repo root `elephantbroker.service`):
```bash
systemctl daemon-reload
systemctl enable elephantbroker elephantbroker-hitl
systemctl start elephantbroker elephantbroker-hitl
```

### 7. Bootstrap

```bash
source .venv/bin/activate
ebrun --runtime-url http://localhost:8420 bootstrap \
  --org-name "YourOrg" --team-name "YourTeam" \
  --admin-name "admin" --admin-handles "email:you@example.com"
```

### 8. Verify

```bash
curl http://localhost:8420/health/    # note trailing slash
curl http://localhost:8421/health
```

---

## OpenClaw VM Setup

> Same for both deployment modes — the plugins only need `EB_RUNTIME_URL` pointing to the runtime.

### 1. Install Plugins

> **Deployment mode:** Installing both plugins configures **FULL mode** — the recommended operating mode for all production deployments.

```bash
git clone https://github.com/<your-org>/elephant-broker.git /opt/elephant-broker

ln -s /opt/elephant-broker/openclaw-plugins/elephantbroker-memory ~/.openclaw/extensions/elephantbroker-memory
ln -s /opt/elephant-broker/openclaw-plugins/elephantbroker-context ~/.openclaw/extensions/elephantbroker-context

cd ~/.openclaw/extensions/elephantbroker-memory && npm install
cd ~/.openclaw/extensions/elephantbroker-context && npm install
```

### 2. Environment

```bash
EB_GATEWAY_ID=gw-prod                  # must match runtime
EB_RUNTIME_URL=http://DB_VM_IP:8420
EB_GATEWAY_SHORT_NAME=prod
```

### 3. Plugin Registration

```bash
openclaw config set plugins.slots.memory elephantbroker-memory
openclaw config unset tools.profile   # must be "full" — "coding" blocks 22/24 EB tools
openclaw hooks disable session-memory
openclaw gateway restart
```

---

## Firewall

| Port | Service | Expose to |
|------|---------|-----------| 
| 8420 | Runtime | OpenClaw VM |
| 8421 | HITL | OpenClaw VM |
| 7687 | Neo4j | Internal only |
| 6333 | Qdrant | Internal only |
| 6379 | Redis | Internal only |
| 5432 | PostgreSQL | Internal only |

---

## Known Gotchas

1. **mistralai conflict** — Resolved automatically by `uv` lock. In native venv mode with `pip`, the post-install fix (remove + chmod) is still required. With `uv sync` it is not.
2. **Cognee writable dirs** — In native venv mode, Cognee creates `.cognee_system/` and `.data_storage/` inside its package dir. These must be writable by the service user. Not an issue in Docker (runs as root).
3. **LLM model prefix** — Cognee needs `openai/gemini/gemini-2.5-pro`. Without `openai/` prefix, Cognee hangs on LLM connection test.
4. **Health endpoint trailing slash** — `/health` returns 307 redirect; use `/health/` or `/health/ready`.
5. **HITL log level** — Does not support `verbose`. Use `info` or `debug`.
6. **Bootstrap is one-shot** — Only works on empty graph. If it fails halfway, `docker compose down -v` and retry.
7. **Qdrant version pairing** — Qdrant server pinned to `v1.17.0` — must stay aligned with `qdrant-client` in `pyproject.toml`.
8. **Migrations before serve** — `elephantbroker migrate` must run before `elephantbroker serve`. In `docker-compose.prod.yml` this is handled by the container command. In native deploys, run it manually after `git pull`.
9. **Alembic offline mode** — To generate SQL without a live DB: `alembic -c elephantbroker/db/alembic.ini upgrade head --sql`.


```
DB VM                                    OpenClaw VM
├─ Python venv                           ├─ elephantbroker-memory
│  ├─ elephantbroker serve  :8420  ←──── │  └─ HTTP to DB_VM:8420
│  ├─ python -m hitl_middleware :8421 ←──│
│  └─ ebrun CLI                          ├─ elephantbroker-context
├─ Docker Compose (infra only)           │  └─ HTTP to DB_VM:8420
│  ├─ Neo4j     :7474/:7687             └─ EB_GATEWAY_ID must match DB VM
│  ├─ Qdrant    :6333/:6334
│  ├─ Redis     :6379
│  └─ (optional) OTEL/ClickHouse/Jaeger/Grafana
```

## Prerequisites

- Python 3.11+ (3.12 tested)
- Docker + Docker Compose
- Node.js 18+ (OpenClaw VM only)
- LiteLLM proxy or OpenAI-compatible endpoint for LLM + embeddings



## OpenClaw VM Setup

### 1. Install Plugins

> **Deployment mode:** Installing both plugins configures **FULL mode** — the recommended operating mode for all production deployments. FULL mode enables the complete ElephantBroker stack: durable memory (Neo4j + Qdrant), working set scoring, context assembly, compaction, and guards. Omitting `elephantbroker-context` puts the runtime in MEMORY_ONLY mode (memory storage without context lifecycle features). Install both plugins for all standard deployments.

```bash
# Clone the repo on the gateway host (if not already present)
git clone https://github.com/<your-org>/elephant-broker.git /opt/elephant-broker

# Symlink plugins into OpenClaw extensions directory (FULL mode — both plugins)
ln -s /opt/elephant-broker/openclaw-plugins/elephantbroker-memory ~/.openclaw/extensions/elephantbroker-memory
ln -s /opt/elephant-broker/openclaw-plugins/elephantbroker-context ~/.openclaw/extensions/elephantbroker-context

# Install dependencies in each plugin directory
cd ~/.openclaw/extensions/elephantbroker-memory && npm install
cd ~/.openclaw/extensions/elephantbroker-context && npm install
```

### 2. Environment

```bash
EB_GATEWAY_ID=gw-prod                  # must match DB VM
EB_RUNTIME_URL=http://DB_VM_IP:8420
EB_GATEWAY_SHORT_NAME=prod
```

### 3. Workspace Files (Surgical Edit)

Edit the agent's workspace files to use EB's durable memory instead of file-based memory.
**Do NOT overwrite the files** — they contain user customizations. Only modify the memory-related sections.

**`~/.openclaw/workspace/AGENTS.md`** — make these changes:
1. Session Startup: remove steps that read memory files (keep SOUL.md/USER.md reads)
2. Memory section: replace dual-system (files + EB) with EB-only memory
3. Remove "MEMORY.md - Your Long-Term Memory" subsection entirely
4. Remove "Write It Down - No Mental Notes" subsection entirely
5. Heartbeat section: remove "Memory Maintenance" subsection only (keep everything else)

**`~/.openclaw/workspace/TOOLS.md`** — add the ElephantBroker tool documentation section if not already present. Keep existing "Local Notes" and any user customizations.

See `openclaw-plugins/elephantbroker-memory/workspace/` for reference templates showing the EB-specific sections to splice in. See docs/OPENCLAW-SETUP.md for detailed change instructions.

### 4. Plugin Registration & Gateway Configuration

```bash
# Register plugins in slots
openclaw config set plugins.slots.memory elephantbroker-memory

# CRITICAL: tools.profile must be "full" — "coding" blocks 22/24 EB tools
openclaw config unset tools.profile   # defaults to "full"
# OR: openclaw config set tools.profile full

# Disable OpenClaw's built-in session-memory hook (EB replaces it)
openclaw hooks disable session-memory

# Restart gateway to apply changes
openclaw gateway restart
```

Memory plugin (`kind: "memory"`), Context engine plugin (`kind: "context-engine"`).
See docs/OPENCLAW-SETUP.md for tool definitions and agent prompt instructions.

## Firewall

| Port | Service | Expose to |
|------|---------|-----------|
| 8420 | Runtime | OpenClaw VM |
| 8421 | HITL | OpenClaw VM |
| 7474, 7687 | Neo4j | Internal only |
| 6333, 6334 | Qdrant | Internal only |
| 6379 | Redis | Internal only |

## Updating the Runtime (after git pull)

**IMPORTANT:** Use `--no-deps` to avoid re-pulling broken transitive deps (mistralai).

### DB VM (runtime)

```bash
cd /opt/elephant-broker
git pull origin deployment-fixes
source venv/bin/activate
pip install --no-deps .
sudo systemctl restart elephantbroker
```

If you need to update dependencies (new dep added to pyproject.toml):

```bash
pip install .
# Then re-apply post-install fixes:
pip uninstall -y mistralai 2>/dev/null
rm -rf venv/lib/python3.*/site-packages/mistralai/
chmod -R 777 venv/lib/python3.*/site-packages/cognee/
sudo systemctl restart elephantbroker
```

### Gateway VM (plugins)

```bash
cd /opt/elephant-broker
git pull origin deployment-fixes

# Re-install npm deps if package.json changed
cd openclaw-plugins/elephantbroker-memory && npm install
cd ../elephantbroker-context && npm install

# Restart gateway to reload plugins
openclaw gateway restart
```

## Known Deployment Gotchas

1. **mistralai ghost package** — `cognee==0.5.3` pulls in a broken `mistralai` namespace package that crashes `instructor`. Remove it after every `pip install .` (full install). Use `pip install --no-deps .` for code-only updates to avoid this.
2. **Cognee writable dirs** — Cognee creates `.cognee_system/` and `.data_storage/` inside its own package directory. These must be writable by the service user.
3. **LLM model prefix** — Cognee needs `openai/gemini/gemini-2.5-pro`. Without `openai/` prefix, Cognee hangs on LLM connection test.
4. **Health endpoint trailing slash** — `/health` returns 307 redirect, use `/health/`.
5. **HITL log level** — Does not support `verbose`. Use `info` or `debug`.
6. **venv portability** — Shebangs in `venv/bin/` are absolute paths. If you move/copy the venv, recreate it in place.
7. **Bootstrap is one-shot** — Only works on empty graph. If it fails halfway, `docker compose down -v` and retry.
8. **`pip install .` vs `pip install --no-deps .`** — Full install re-resolves all deps and re-pulls mistralai. Use `--no-deps` for code-only updates.
9. **Qdrant version pairing** — Qdrant server is pinned to v1.17.0 in both `docker-compose.yml` and `docker-compose.test.yml` — must stay aligned with `qdrant-client` version in `pyproject.toml` (currently `>=1.7`). If upgrading the client, update both compose files to match.
