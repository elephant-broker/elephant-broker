# ElephantBroker Deployment Guide

## Architecture

The runtime runs as a native Python process (venv), NOT in Docker. Infrastructure services (Neo4j, Qdrant, Redis) run via Docker Compose.

```
DB VM                                    OpenClaw VM
├─ Python venv                           ├─ elephantbroker-memory
│  ├─ elephantbroker serve  :8420  ←──── │  └─ HTTP to DB_VM:8420
│  ├─ .venv/bin/hitl-middleware :8421 ←──│
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
| `/opt/elephantbroker` | `root:root` | 755 | Source repo + venv install (**root-owned by design** — C3 defense-in-depth narrowing, see below) |
| `/opt/elephantbroker/.venv` | `root:root` | 755 | Python virtual environment (uv-managed). Only the 3 Cognee runtime paths inside it are chowned to the service user |
| `/opt/elephantbroker/.venv/lib/pythonX.Y/site-packages/cognee/.cognee_system` | `elephantbroker:elephantbroker` | 750 | Cognee runtime SQLite + state (chowned by install.sh step 6) |
| `/opt/elephantbroker/.venv/lib/pythonX.Y/site-packages/cognee/.data_storage` | `elephantbroker:elephantbroker` | 750 | Cognee chunk/artifact storage (chowned by install.sh step 6) |
| `/opt/elephantbroker/.venv/lib/pythonX.Y/site-packages/.anon_id` | `elephantbroker:elephantbroker` | 644 | Cognee anonymous-telemetry id file (chowned by install.sh step 6) |
| `/etc/elephantbroker` | `elephantbroker:elephantbroker` | 750 | Config directory |
| `/etc/elephantbroker/default.yaml` | `elephantbroker:elephantbroker` | 640 | Non-secret config (template) |
| `/etc/elephantbroker/env` | `root:elephantbroker` | 640 | Runtime secrets (root writes, service reads) |
| `/etc/elephantbroker/hitl.env` | `root:elephantbroker` | 640 | HITL middleware secrets |
| `/var/lib/elephantbroker` | `elephantbroker:elephantbroker` | 750 | Runtime data dir (SQLite stores, working dir) |
| `/var/cache/elephantbroker` | `elephantbroker:elephantbroker` | 750 | `PYTHONPYCACHEPREFIX` target for editable-source .pyc writes (post-C3/D-R3 narrowing, see systemd unit `Environment=PYTHONPYCACHEPREFIX=`) |
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

The repo ships an idempotent installer at `deploy/install.sh` that installs
[`uv`](https://docs.astral.sh/uv/) (if missing), creates the dedicated
`elephantbroker` system user, sets up the canonical directory layout, runs
`uv sync --frozen --no-dev` to install the EXACT pinned dependencies from
`uv.lock`, installs the HITL middleware, pre-creates Cognee's writable
state directories, copies the config + env templates into `/etc/elephantbroker/`,
and installs the systemd unit files. It runs entirely as root via `sudo`
— no `sudo -u` switching.

The installer expects the repo to be cloned **into** `/opt/elephantbroker`
(not alongside it). This makes the install dir and the source dir the same
location, which simplifies update flows later.

```bash
# Clone directly into the install prefix
sudo git clone https://github.com/elephant-broker/elephant-broker.git /opt/elephantbroker

# Run the installer (idempotent — safe to re-run)
sudo /opt/elephantbroker/deploy/install.sh
```

What the installer does, in order (8 steps + Step 0, matching `install.sh` exactly):

**Step 0/8 — Install uv.** Installs `uv` to `/usr/local/bin` via Astral's official versioned installer `https://astral.sh/uv/0.11.3/install.sh` (pinned to 0.11.3 to match `Dockerfile:14`/`:70`). Skipped if `uv` is already on PATH.

**Step 1/8 — Create service user.** Creates the `elephantbroker` system user with `useradd --system --shell /usr/sbin/nologin --home-dir /var/lib/elephantbroker`. Skipped if the user already exists.

**Step 2/8 — Create directories.** Creates `$PREFIX=/opt/elephantbroker` (`root:root 755` — **intentionally root-owned** per C3 defense-in-depth), `$CONFIG_DIR=/etc/elephantbroker` (`elephantbroker:elephantbroker 750`), `$DATA_DIR=/var/lib/elephantbroker` (`elephantbroker:elephantbroker 750`), and `$CACHE_DIR=/var/cache/elephantbroker` (`elephantbroker:elephantbroker 750`, added in D-R3 for the `PYTHONPYCACHEPREFIX` redirect target).

**Step 3/8 — Install runtime + HITL middleware via `uv sync` (workspace mode).** Runs `uv sync --frozen --no-dev` from `$REPO_DIR`, which builds the venv at `/opt/elephantbroker/.venv` and installs BOTH the `elephantbroker` runtime AND the `hitl-middleware` workspace member in a single call. The root `pyproject.toml` declares `[tool.uv.workspace] members = ["hitl-middleware"]`, so the root `uv.lock` is the single source of truth for both packages — no separate `uv pip install hitl-middleware` step. All ~188 direct + transitive deps are installed at exact pinned versions with integrity hashes.

**Step 4/8 — Post-install fixes (Cognee writable dirs + mistralai safety net).**
  - Resolves the venv's `site-packages` dir authoritatively via `uv run python -c 'import site; print(site.getsitepackages()[0])'` (the `find ... | head -n 1` approach was replaced in C8/TODO-3-325 with this strict resolution to eliminate silent mis-detection).
  - Belt-and-suspenders `mistralai` cleanup: shape-checks the mistralai directory first, only acts if the broken namespace-package shape is confirmed (missing `dist-info/METADATA`). With `uv` (the supported install path) this is a no-op because uv resolves `mistralai==1.12.4` (a working modern version). The cleanup exists for the edge case of someone running `pip install` against the venv.
  - Pre-creates `cognee/.cognee_system/databases` and `cognee/.data_storage` writable subdirs.
  - Touches `cognee/.anon_id` telemetry file (telemetry is disabled at import time via `COGNEE_DISABLE_TELEMETRY=true` in `elephantbroker/__init__.py`).

**Step 5/8 — Install config files into `$CONFIG_DIR`.**
  - `default.yaml` → copies from `$REPO_DIR/elephantbroker/config/default.yaml` on first install only, preserves operator customizations on re-run.
  - `env` → copies from `elephantbroker/config/env.example` on first install only.
  - `hitl.env` → copies from `hitl-middleware/hitl.env.example` on first install only.
  - **F11 — auto-generate `EB_HITL_CALLBACK_SECRET` (TODO-3-614).** When BOTH `env` and `hitl.env` were freshly copied in this run (fresh install, not a re-run), the installer runs `openssl rand -hex 32` once and patches the same secret into both files via a `mktemp + sed + cat > mv` pattern that preserves the `640 root:elephantbroker` ownership/mode. If only one of the two files was freshly copied (the other already has operator content), auto-gen is skipped with a warning — auto-generating one half would silently break the existing HMAC pair. If `openssl` is missing, falls back to a warning with the manual instructions. On first install with both env files copied, **you do NOT need to manually generate the HITL secret** — the installer does it for you.

**Step 6/8 — Chown writable subdirs only (defense in depth, C3 narrowed model).** The previous version of the installer ran `chown -R elephantbroker:elephantbroker /opt/elephantbroker`, which transferred ownership of the entire install tree (source + venv + binaries) to the runtime user. A compromised runtime process could then rewrite its own code, the cognee binaries, or the config templates. **C3 (TODO-3-010) narrows this to exactly 3 paths:**
  - `$COGNEE_DIR/.cognee_system` — Cognee's runtime SQLite + state
  - `$COGNEE_DIR/.data_storage` — Cognee's chunk/artifact storage
  - `$ANON_ID_PATH` — Cognee's anonymous-telemetry id file

  Everything else — `/opt/elephantbroker` itself, the `.venv` binaries, the runtime source tree — stays **root-owned**. Default file modes from `uv sync` are 644/755 (other-readable + other-executable for dirs), so the service user reads and traverses the venv without owning it. The systemd unit's `ReadWritePaths=/var/lib/elephantbroker /opt/elephantbroker/.venv/lib /var/cache/elephantbroker` permits writes to those specific paths through the MAC layer, but DAC ownership now blocks unintended writes from a compromised runtime that didn't go through the pre-created Cognee paths.

**Step 7/8 — Install systemd unit files.** Installs `deploy/systemd/elephantbroker.service` and `deploy/systemd/elephantbroker-hitl.service` to `/etc/systemd/system/` (`root:root 644`), runs `systemctl daemon-reload`, and enables both units. The services are **not started** — the operator must review `/etc/elephantbroker/default.yaml` and confirm secrets in `env`/`hitl.env` before starting. Skipped if `--no-systemd` was passed.

**Step 8/8 — Smoke test.** Runs `$REPO_DIR/.venv/bin/elephantbroker config validate --config $CONFIG_DIR/default.yaml` as a pre-systemd-start sanity check (C4/TODO-3-013/TODO-3-222). Any structural failure (`extra="forbid"` violation, embedding model/dim mismatch, malformed YAML, env coercion error) surfaces here as a clear install-log error with recovery hints, instead of a confusing journalctl failure 30 seconds later. Hard-dies on failure — the fresh-install path should pass, and a genuine schema error blocks the install.

> **Note:** The installer does NOT use `chmod 777` on the Cognee directories.
> Earlier versions of these docs recommended that as a workaround for permission
> errors, but it was wrong — it left Cognee state world-writable. The correct
> fix is the **C3 narrowed chown model** (install.sh step 6): the dedicated
> service user with targeted ownership of exactly the 3 Cognee runtime paths
> (`cognee/.cognee_system`, `cognee/.data_storage`, `cognee/.anon_id`), with
> everything else in `/opt/elephantbroker` left `root:root`.

Optional flags:

```bash
sudo /opt/elephantbroker/deploy/install.sh --no-systemd   # skip installing unit files
sudo /opt/elephantbroker/deploy/install.sh --prefix /custom/path
sudo /opt/elephantbroker/deploy/install.sh --help
```

### 3. Edit secrets

```bash
sudo nano /etc/elephantbroker/env       # fill in EB_LLM_API_KEY, EB_NEO4J_PASSWORD, etc
sudo nano /etc/elephantbroker/hitl.env  # should already have EB_HITL_CALLBACK_SECRET populated by F11 — verify
```

The installer copies `env.example` and `hitl.env.example` as starting
templates. Required secret variables are uncommented at the top of each
file with blank values — fill them in before starting the services. See
`elephantbroker/config/env.example` for the complete annotated reference.

> **`EB_HITL_CALLBACK_SECRET` is auto-generated on first install.** The installer's
> F11 step (TODO-3-614) detects when both `env` and `hitl.env` are freshly
> copied in the same run and auto-generates a single `openssl rand -hex 32`
> secret, patching the same value into both files. Verify with
> `grep EB_HITL_CALLBACK_SECRET /etc/elephantbroker/env /etc/elephantbroker/hitl.env`
> — both should show the same 64-hex value. **Manual generation is only needed**
> in these cases:
> - `openssl` was missing from the install host (installer falls back to a warning)
> - Only one of `env` or `hitl.env` was freshly copied (e.g., you preserved one from a previous install) — F11 skips auto-gen and warns, to avoid clobbering an existing operator-rotated value
> - You intentionally rotate the secret post-install (set the same new value in both files, then `sudo systemctl restart elephantbroker elephantbroker-hitl`)
>
> Both values MUST be identical. A mismatch causes HITL callbacks to fail silently
> with 401 responses, leaving approvals stuck in pending state.

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

Plugin env vars are set via the `env.vars` block in `~/.openclaw/openclaw.json`
(NOT via shell exports). OpenClaw interpolates `${VAR}` references in the plugin
config block from this env.vars map at config load time. This is the single
source of truth for plugin configuration — see **docs/OPENCLAW-SETUP.md §
Plugin Installation → Step 2** for the complete `openclaw.json` example.

Minimum required env vars (set in `~/.openclaw/openclaw.json` → `env.vars`):

```json
{
  "env": {
    "vars": {
      "EB_GATEWAY_ID": "gw-prod",
      "EB_RUNTIME_URL": "http://DB_VM_IP:8420",
      "EB_GATEWAY_SHORT_NAME": "prod"
    }
  }
}
```

- **`EB_GATEWAY_ID`** — required. Must match the `gateway.gateway_id` set in the DB VM's `default.yaml`. Example: `gw-prod-us-east-1`. Used by both plugins and stamped on every tenant-scoped API call.
- **`EB_RUNTIME_URL`** — optional, default `http://localhost:8420`. Points at the DB VM's ElephantBroker runtime. Use the DB VM's routable IP or DNS name from the OpenClaw VM's perspective (NOT `localhost` unless you're running both on the same host).
- **`EB_GATEWAY_SHORT_NAME`** — optional, defaults to first 8 chars of `EB_GATEWAY_ID`. Human-friendly label for log/trace display.
- **`EB_ACTOR_ID`** — optional. Fallback actor ID for the `X-EB-Actor-Id` header when OpenClaw's context doesn't provide `actorId`.
- **`EB_PROFILE`** — optional, default `coding`. Profile name override (can also be set in the per-plugin `config.profileName` field).

See `docs/OPENCLAW-SETUP.md § Required Environment Variables` for the full reference.

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
# Register both plugins in their respective slots (FULL mode)
openclaw config set plugins.slots.memory elephantbroker-memory
openclaw config set plugins.slots.contextEngine elephantbroker-context

# CRITICAL: tools.profile must be "full" — "coding" blocks 22/24 EB tools
openclaw config unset tools.profile   # defaults to "full"
# OR: openclaw config set tools.profile full

# Disable OpenClaw's built-in session-memory hook (EB replaces it)
openclaw hooks disable session-memory

# Restart gateway to apply changes
openclaw gateway restart
```

The memory plugin uses `kind: "memory"` and the context engine plugin uses
`kind: "context-engine"` — they register into two different slots and coexist
without conflict. The context engine plugin sets `ownsCompaction: true`,
meaning OpenClaw delegates compaction decisions to ElephantBroker.

**Both slots must be registered for FULL mode.** Omitting the `contextEngine`
slot registration puts the runtime in MEMORY_ONLY mode even if the plugin is
installed — the context lifecycle features (bootstrap, assemble, compact,
afterTurn, subagent lifecycle) won't run without the slot binding.

See `docs/OPENCLAW-SETUP.md § Plugin Installation` for tool definitions, agent
prompt instructions, and the complete `openclaw.json` config example.

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
the current branch, runs `uv sync --frozen --no-dev` to install exactly
what `uv.lock` specifies (zero drift), re-chowns the install tree, and
restarts both systemd services.

```bash
sudo /opt/elephantbroker/deploy/update.sh
```

The default path uses `--frozen` mode, which **errors out if `pyproject.toml`
has been modified since the last `uv lock`** — preventing accidental dep drift
on production hosts. If you intentionally want to upgrade dependencies, use
`--upgrade` to regenerate `uv.lock` from the current `pyproject.toml`:

```bash
sudo /opt/elephantbroker/deploy/update.sh --upgrade
```

The updater refuses to run on a dirty git tree — commit or stash any local
changes first. See `deploy/update.sh --help` for all flags, and
[`deploy/UPDATING-DEPS.md`](../deploy/UPDATING-DEPS.md) for the full
dependency upgrade workflow.

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

1. **mistralai ghost package (legacy pip path only)** — `cognee==0.5.3` ships a broken `mistralai` namespace package as a transitive dep that conflicts with `instructor`. With `uv` (the supported install path), this is NOT an issue: uv's holistic resolver picks `mistralai==1.12.4` (a working modern version) automatically. The installer keeps a belt-and-suspenders cleanup step in case someone bypasses uv and runs `pip install` against the venv by habit.
2. **Cognee writable dirs** — Cognee creates `.cognee_system/` and `.data_storage/` inside its own site-packages directory at runtime. The installer pre-creates these dirs in Step 4 and then Step 6 **targeted-chowns exactly those 3 paths** (`.cognee_system`, `.data_storage`, `.anon_id`) to the service user via the **C3 narrowed model** (TODO-3-010). `/opt/elephantbroker` itself stays `root:root` — the service user can traverse and read the venv without owning it. Earlier docs recommended `chmod -R 777 venv/.../cognee/` as a workaround — that was wrong (world-writable Cognee state). Earlier versions of the installer also ran a broad `chown -R elephantbroker:elephantbroker /opt/elephantbroker` which was also wrong (a compromised runtime could then rewrite its own source). The current narrowed model is the right fix.
3. **LLM model prefix** — Cognee needs `openai/gemini/gemini-2.5-pro`. Without `openai/` prefix, Cognee hangs on LLM connection test.
4. **Embedding model + tiktoken** — Cognee tokenizes via tiktoken which only knows OpenAI model names. If you set `EB_EMBEDDING_MODEL` to a non-OpenAI model name (e.g. `gemini/text-embedding-004`), the runtime will crash at first embedding call with `KeyError: Could not automatically map ... to a tokeniser`. Stick to `openai/text-embedding-3-large` (1024 dim) unless you have verified your specific model works with tiktoken.
5. **Health endpoint trailing slash** — `/health` returns 307 redirect, use `/health/`.
6. **HITL log level** — Does not support `verbose`. Use `info` or `debug`.
7. **venv portability** — Shebangs in `.venv/bin/` are absolute paths. If you move/copy the venv, run `uv sync --frozen` to rebuild in place. The installer always creates the venv at `/opt/elephantbroker/.venv` (uv's default location) so this only matters for unusual deployments.
8. **Bootstrap is one-shot** — Only works on empty graph. If it fails halfway, `docker compose -f infrastructure/docker-compose.yml down -v` and retry.
9. **`uv sync --frozen` errors on lockfile drift** — If `pyproject.toml` has been modified since the last `uv lock`, `update.sh` will refuse to install. Run `update.sh --upgrade` to regenerate the lockfile, OR commit a fresh `uv lock` from a dev machine first. See `deploy/UPDATING-DEPS.md` for the full upgrade workflow.
10. **Qdrant version pairing** — Qdrant server is pinned to v1.17.0 in both `docker-compose.yml` and `docker-compose.test.yml` — must stay aligned with `qdrant-client` version in `pyproject.toml`. If upgrading the client, update both compose files to match.
11. **Service user ownership (C3 narrowed model)** — `/opt/elephantbroker` stays `root:root 755` (service user can read + traverse but not write). Only the 3 Cognee runtime paths inside the venv are chowned to `elephantbroker:elephantbroker`. `/etc/elephantbroker` is `elephantbroker:elephantbroker 750` with `root:elephantbroker 640` for the env files specifically. `/var/lib/elephantbroker` and `/var/cache/elephantbroker` are `elephantbroker:elephantbroker 750`. If you copy files in manually under these paths, follow the C3 model: `/opt/elephantbroker` manual copies stay `root:root`, `/etc/elephantbroker`/`/var/lib/elephantbroker`/`/var/cache/elephantbroker` manual copies get `chown elephantbroker:elephantbroker`, and env files get `chown root:elephantbroker`. Re-running `deploy/install.sh` is the safest way to restore correct ownership.
