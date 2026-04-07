# ElephantBroker Deployment Guide

## Architecture

The runtime runs as a native Python process (venv), NOT in Docker. Infrastructure services (Neo4j, Qdrant, Redis) run via Docker Compose.

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

- Python 3.11 or 3.12 (pinned via `requires-python = ">=3.11,<3.13"` in pyproject.toml)
- [`uv`](https://docs.astral.sh/uv/) — installed automatically by `deploy/install.sh` if missing
- Docker + Docker Compose (for the Neo4j / Qdrant / Redis infrastructure)
- **Node.js 24+** on the OpenClaw VM (pinned via `engines.node` in each plugin's package.json — earlier versions may run but are not supported)
- LiteLLM proxy or OpenAI-compatible endpoint for LLM + embeddings
- Root access to the DB VM (install runs via `sudo`)

> **About uv:** ElephantBroker uses [`uv`](https://docs.astral.sh/uv/) instead of
> plain `pip` for reproducible builds. The lockfile (`uv.lock` at the repo root)
> pins every dependency — direct and transitive — to exact versions and integrity
> hashes. `uv sync --frozen` (the install path) always installs exactly what the
> lockfile specifies. See [`deploy/UPDATING-DEPS.md`](../deploy/UPDATING-DEPS.md)
> for the dep-upgrade workflow.

## Service User and Directory Layout

The runtime runs under a dedicated `elephantbroker` system user — never as
root. The install script (`deploy/install.sh`) creates this user and the
canonical directory layout below; all services and update scripts assume it
exists.

| Path | Owner | Mode | Purpose |
|---|---|---|---|
| `/opt/elephantbroker` | `elephantbroker:elephantbroker` | 755 | Source repo + venv install |
| `/opt/elephantbroker/.venv` | `elephantbroker:elephantbroker` | 755 | Python virtual environment (uv-managed) |
| `/etc/elephantbroker` | `elephantbroker:elephantbroker` | 750 | Config directory |
| `/etc/elephantbroker/default.yaml` | `elephantbroker:elephantbroker` | 640 | Non-secret config (template) |
| `/etc/elephantbroker/env` | `root:elephantbroker` | 640 | Runtime secrets (root writes, service reads) |
| `/etc/elephantbroker/hitl.env` | `root:elephantbroker` | 640 | HITL middleware secrets |
| `/var/lib/elephantbroker` | `elephantbroker:elephantbroker` | 750 | Runtime data dir (SQLite stores, working dir) |
| `/etc/systemd/system/elephantbroker*.service` | `root:root` | 644 | systemd unit files |

### Why a dedicated service user

- **No root execution.** A compromised runtime process cannot escalate to
  root or modify system binaries — it can only act inside the directories
  it owns.
- **No interactive login.** The user is created with `--shell /usr/sbin/nologin`,
  so even if its credentials leak, no shell session can be opened.
- **Defense in depth via systemd hardening.** The systemd units pair the
  service user with `ProtectSystem=strict`, `ProtectHome=true`,
  `NoNewPrivileges=true`, `PrivateTmp=true`, `PrivateDevices=true`, and
  restricted address families. See `deploy/systemd/elephantbroker.service`
  for the full list.
- **Secrets stay readable only by the service.** The two env files are
  `mode 640` with `root:elephantbroker` ownership, so only the service
  user (via group membership) and root (via DAC override) can read them.

### Verify the layout on a running host

```bash
id elephantbroker
ls -ld /opt/elephantbroker /etc/elephantbroker /var/lib/elephantbroker
stat -c '%U:%G %a %n' /etc/elephantbroker/env /etc/elephantbroker/hitl.env /etc/elephantbroker/default.yaml
```

## DB VM Setup

### 1. Infrastructure Services

```bash
cd infrastructure/
docker compose up -d neo4j qdrant redis

# With observability (optional — Jaeger UI at http://localhost:16686):
docker compose --profile observability up -d
```

### 2. Run the install script

The repo ships an idempotent installer at `deploy/install.sh` that creates
the dedicated `elephantbroker` system user, sets up the canonical directory
layout, builds the venv, installs the runtime + HITL middleware, applies the
post-install fixes (mistralai purge, Cognee writable dirs), copies the
config + env templates into `/etc/elephantbroker/`, and installs the systemd
unit files. It runs entirely as root via `sudo` — no `sudo -u` switching.

The installer expects the repo to be cloned **into** `/opt/elephantbroker`
(not alongside it). This makes the install dir and the source dir the same
location, which simplifies update flows later.

```bash
# Clone directly into the install prefix
sudo git clone https://github.com/elephant-broker/elephant-broker.git /opt/elephantbroker

# Run the installer (idempotent — safe to re-run)
sudo /opt/elephantbroker/deploy/install.sh
```

What the installer does, in order:

1. Creates the `elephantbroker` system user (no shell, home `/var/lib/elephantbroker`)
2. Creates `/opt/elephantbroker`, `/etc/elephantbroker`, `/var/lib/elephantbroker` with the ownership/modes from the table above
3. Runs `uv sync --frozen --no-dev` — builds the venv at `/opt/elephantbroker/.venv` and installs the EXACT pinned versions from `uv.lock`
4. `pip install` the runtime and HITL middleware into the venv
5. Applies post-install fixes:
   - Removes the `mistralai` ghost package (broken cognee 0.5.3 transitive dep)
   - Pre-creates `cognee/.cognee_system/databases` and `cognee/.data_storage` writable subdirs
   - Touches the `cognee/.anon_id` telemetry file (telemetry is disabled at import time anyway)
6. Copies `default.yaml` from the repo into `/etc/elephantbroker/`
7. Copies `env.example` and `hitl.env.example` into `/etc/elephantbroker/env` and `hitl.env` (only on first install — never overwrites existing secrets)
8. Final `chown -R elephantbroker:elephantbroker /opt/elephantbroker`
9. Installs the systemd unit files from `deploy/systemd/` and enables both services (does not start them — operator must edit secrets first)

> **Note:** The installer does NOT use `chmod 777` on the Cognee directories.
> Earlier versions of these docs recommended that as a workaround for permission
> errors, but it was wrong — it left Cognee state world-writable. The correct
> fix is the dedicated service user with `chown -R` of the venv tree (which the
> installer does in step 8).

Optional flags:

```bash
sudo /opt/elephantbroker/deploy/install.sh --no-systemd   # skip installing unit files
sudo /opt/elephantbroker/deploy/install.sh --prefix /custom/path
sudo /opt/elephantbroker/deploy/install.sh --help
```

### 3. Edit secrets

```bash
sudo nano /etc/elephantbroker/env       # fill in EB_LLM_API_KEY, EB_NEO4J_PASSWORD, etc
sudo nano /etc/elephantbroker/hitl.env  # fill in EB_HITL_CALLBACK_SECRET (same value as in env!)
```

The installer copies `env.example` and `hitl.env.example` as starting
templates. Required secret variables are uncommented at the top of each
file with blank values — fill them in before starting the services. See
`elephantbroker/config/env.example` for the complete annotated reference.

> **Critical: `EB_HITL_CALLBACK_SECRET` must be identical** in `/etc/elephantbroker/env`
> and `/etc/elephantbroker/hitl.env`. Generate it once with `openssl rand -hex 32`
> and paste the same value in both files. A mismatch causes HITL callbacks to
> fail silently with 401 responses, leaving approvals stuck in pending state.

### 4. Review default.yaml

Most operators only need to edit a handful of fields in
`/etc/elephantbroker/default.yaml`:

- `gateway.gateway_id`, `gateway.org_id`, `gateway.team_id` — your deployment identity
- `cognee.neo4j_uri`, `cognee.qdrant_url`, `infra.redis_url` — only if your
  databases are not on the same host
- `reranker.enabled` — set to `false` if you do not have a Qwen3-Reranker server
- `compaction_llm.model` and `goal_refinement.model` — override if your
  LiteLLM proxy does not serve `gemini-2.5-flash`

**Critical: LLM model prefix.** Cognee requires the `openai/` prefix on the
LLM model name (it strips the prefix internally before sending to LiteLLM):

```yaml
llm:
  model: "openai/gemini/gemini-2.5-pro"   # Cognee strips "openai/", sends "gemini/gemini-2.5-pro"
```

Without the prefix, Cognee hangs at startup on the LLM connection test.

### 5. Bootstrap your org/team/admin

```bash
sudo -u elephantbroker /opt/elephantbroker/.venv/bin/ebrun \
  --runtime-url http://localhost:8420 bootstrap \
  --org-name "YourOrg" \
  --team-name "YourTeam" \
  --admin-name "admin" \
  --admin-handles "email:you@example.com"
```

(This is the one place we use `sudo -u elephantbroker` — `ebrun` is the
admin CLI and should run as the service user so any local state it creates
inherits the right ownership.)

Bootstrap is one-shot — only works on an empty graph. If it fails partway,
`docker compose -f infrastructure/docker-compose.yml down -v` to wipe the
infra and retry.

### 6. Start the services

```bash
sudo systemctl start elephantbroker elephantbroker-hitl
```

The installer already enabled both services in step 9 above, so they will
also come up automatically on the next reboot.

### 7. Verify

```bash
systemctl status elephantbroker elephantbroker-hitl
curl http://localhost:8420/health/    # note trailing slash
curl http://localhost:8421/health
journalctl -u elephantbroker -f       # follow runtime logs
```

## OpenClaw VM Setup

### 1. Install Plugins

> **Deployment mode:** Installing both plugins configures **FULL mode** — the recommended operating mode for all production deployments. FULL mode enables the complete ElephantBroker stack: durable memory (Neo4j + Qdrant), working set scoring, context assembly, compaction, and guards. Omitting `elephantbroker-context` puts the runtime in MEMORY_ONLY mode (memory storage without context lifecycle features). Install both plugins for all standard deployments.

```bash
# Clone the repo on the gateway host (if not already present)
git clone https://github.com/elephant-broker/elephant-broker.git /opt/elephantbroker

# Symlink plugins into OpenClaw extensions directory (FULL mode — both plugins)
ln -s /opt/elephantbroker/openclaw-plugins/elephantbroker-memory ~/.openclaw/extensions/elephantbroker-memory
ln -s /opt/elephantbroker/openclaw-plugins/elephantbroker-context ~/.openclaw/extensions/elephantbroker-context

# Install dependencies — use `npm ci` (NOT `npm install`).
# `npm ci` is the lockfile-driven install: it reads package-lock.json and
# installs EXACTLY those versions, errors out if the lockfile is missing or
# out of sync with package.json. This is the npm equivalent of
# `uv sync --frozen` on the DB VM.
cd ~/.openclaw/extensions/elephantbroker-memory && npm ci
cd ~/.openclaw/extensions/elephantbroker-context && npm ci
```

> **Why `npm ci` and not `npm install`:** `npm install` resolves package.json
> ranges to whatever's latest today, regenerates the lockfile if needed, and
> can silently install different versions on different hosts. `npm ci` reads
> the committed `package-lock.json` and installs bit-for-bit the same tree
> every time. Use it for any production deployment, CI run, or anywhere you
> care about reproducibility.

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

## Updating the Runtime

### DB VM (runtime + HITL)

The repo ships an idempotent updater at `deploy/update.sh`. It pulls from
the current branch, reinstalls into the venv (using `--no-deps` by default
to preserve the mistralai workaround), re-chowns the install tree, and
restarts both systemd services.

```bash
sudo /opt/elephantbroker/deploy/update.sh
```

If you added a new dependency to `pyproject.toml`, use `--full` to do a
full reinstall and re-apply the post-install fixes:

```bash
sudo /opt/elephantbroker/deploy/update.sh --full
```

The updater refuses to run on a dirty git tree — commit or stash any local
changes first. See `deploy/update.sh --help` for all flags.

### Gateway VM (plugins)

```bash
cd /opt/elephantbroker
git pull origin main

# Re-install npm deps from the committed lockfile (use `npm ci`, NOT
# `npm install`, so the install is reproducible — same as the DB VM).
cd openclaw-plugins/elephantbroker-memory && npm ci
cd ../elephantbroker-context && npm ci

# Restart gateway to reload plugins
openclaw gateway restart
```

## Known Deployment Gotchas

1. **mistralai ghost package** — `cognee==0.5.3` pulls in a broken `mistralai` namespace package that crashes `instructor`. The installer (`deploy/install.sh`) and updater (`deploy/update.sh --full`) remove it automatically. The fast `update.sh` path uses `pip install --no-deps .` to avoid re-pulling it.
2. **Cognee writable dirs** — Cognee creates `.cognee_system/` and `.data_storage/` inside its own site-packages directory at runtime. The installer pre-creates these dirs and the final `chown -R elephantbroker:elephantbroker /opt/elephantbroker` makes them writable by the service user. Earlier docs recommended `chmod -R 777 venv/.../cognee/` as a workaround — that was wrong (world-writable Cognee state). Use the dedicated service user instead.
3. **LLM model prefix** — Cognee needs `openai/gemini/gemini-2.5-pro`. Without `openai/` prefix, Cognee hangs on LLM connection test.
4. **Embedding model + tiktoken** — Cognee tokenizes via tiktoken which only knows OpenAI model names. If you set `EB_EMBEDDING_MODEL` to a non-OpenAI model name (e.g. `gemini/text-embedding-004`), the runtime will crash at first embedding call with `KeyError: Could not automatically map ... to a tokeniser`. Stick to `openai/text-embedding-3-large` (1024 dim) unless you have verified your specific model works with tiktoken.
5. **Health endpoint trailing slash** — `/health` returns 307 redirect, use `/health/`.
6. **HITL log level** — Does not support `verbose`. Use `info` or `debug`.
7. **venv portability** — Shebangs in `.venv/bin/` are absolute paths. If you move/copy the venv, run `uv sync` to rebuild in place. The installer always creates the venv at `/opt/elephantbroker/.venv` (uv's default location) so this only matters for unusual deployments.
8. **Bootstrap is one-shot** — Only works on empty graph. If it fails halfway, `docker compose -f infrastructure/docker-compose.yml down -v` and retry.
9. **`pip install .` vs `pip install --no-deps .`** — Full install re-resolves all deps and re-pulls mistralai. The updater's default fast path uses `--no-deps` for code-only updates; use `update.sh --full` when bumping a dependency.
10. **Qdrant version pairing** — Qdrant server is pinned to v1.17.0 in both `docker-compose.yml` and `docker-compose.test.yml` — must stay aligned with `qdrant-client` version in `pyproject.toml`. If upgrading the client, update both compose files to match.
11. **Service user ownership** — Files inside `/opt/elephantbroker`, `/etc/elephantbroker`, and `/var/lib/elephantbroker` must be owned by `elephantbroker:elephantbroker` (with `root:elephantbroker` for the env files specifically). If you copy files in manually, follow up with `sudo chown -R elephantbroker:elephantbroker <path>` or the systemd unit will fail to read them.
