#!/usr/bin/env bash
# =============================================================================
# ElephantBroker DB-VM installer
# =============================================================================
#
# Sets up a fresh DB VM for ElephantBroker:
#   0. Installs uv (Astral's Python package manager) if missing
#   1. Creates the dedicated `elephantbroker` system user
#   2. Creates /opt/elephantbroker, /etc/elephantbroker, /var/lib/elephantbroker
#   3. Runs `uv sync --frozen --no-dev` — installs the venv at
#      /opt/elephantbroker/.venv with the EXACT versions pinned in
#      pyproject.toml + uv.lock. This covers BOTH the elephantbroker runtime
#      AND the hitl-middleware package, because hitl-middleware is declared
#      as a uv workspace member in the root pyproject.toml.
#   4. Applies belt-and-suspenders post-install fixes (Cognee writable dirs)
#   5. Copies default.yaml + env.example + hitl.env.example into /etc/elephantbroker
#   6. Installs the systemd units (unless --no-systemd)
#   7. Sets ownership/permissions per the canonical layout
#
# Why uv (not pip):
#   - The lockfile (uv.lock) is mandatory by default — `uv sync` always uses it.
#   - Reproducible builds: bit-for-bit identical installs across machines.
#   - 10-100x faster than pip.
#   - Resolves cognee 0.5.3 + mistralai cleanly without the force-reinstall hack
#     pip needed (uv's holistic resolver picks a working mistralai version).
#   - See deploy/UPDATING-DEPS.md for the dep upgrade workflow.
#
# Usage (typical):
#   sudo git clone https://github.com/elephant-broker/elephant-broker.git /opt/elephantbroker
#   sudo /opt/elephantbroker/deploy/install.sh
#   sudo nano /etc/elephantbroker/env       # fill in EB_LLM_API_KEY etc
#   sudo nano /etc/elephantbroker/hitl.env  # fill in EB_HITL_CALLBACK_SECRET
#   sudo systemctl start elephantbroker elephantbroker-hitl
#
# This script is idempotent — safe to re-run on a partially-installed host.
# It runs entirely as root (no `sudo -u` switching). All ownership is set via
# `chown` after the privileged operations complete.
#
# Flags:
#   --no-systemd     Skip installing systemd unit files
#   --prefix PATH    Override the install prefix (default: /opt/elephantbroker)
#   --help           Show this message
# =============================================================================

set -euo pipefail

# --- Defaults ---
PREFIX="/opt/elephantbroker"
INSTALL_SYSTEMD=1
SERVICE_USER="elephantbroker"
SERVICE_GROUP="elephantbroker"
CONFIG_DIR="/etc/elephantbroker"
DATA_DIR="/var/lib/elephantbroker"

# --- Parse flags ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-systemd) INSTALL_SYSTEMD=0; shift ;;
        --prefix) PREFIX="$2"; shift 2 ;;
        --help|-h)
            cat <<'HELP'
ElephantBroker DB-VM installer

Usage:
  sudo ./install.sh [--no-systemd] [--prefix PATH]

Flags:
  --no-systemd     Skip installing systemd unit files
  --prefix PATH    Override the install prefix (default: /opt/elephantbroker)
  --help, -h       Show this message

Typical workflow:
  sudo git clone <repo-url> /opt/elephantbroker
  sudo /opt/elephantbroker/deploy/install.sh
  sudo nano /etc/elephantbroker/env       # fill in EB_LLM_API_KEY etc
  sudo nano /etc/elephantbroker/hitl.env  # fill in EB_HITL_CALLBACK_SECRET
  sudo systemctl start elephantbroker elephantbroker-hitl

The script is idempotent — safe to re-run on a partially-installed host.
It runs entirely as root (no sudo -u switching). All ownership is set
via chown after the privileged operations complete.
HELP
            exit 0
            ;;
        *)
            echo "ERROR: unknown flag: $1" >&2
            echo "Run with --help for usage." >&2
            exit 1
            ;;
    esac
done

# --- Helpers ---
log()  { printf "\033[1;34m==>\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m!!\033[0m  %s\n" "$*" >&2; }
die()  { printf "\033[1;31mXX\033[0m  %s\n" "$*" >&2; exit 1; }

# --- Pre-flight ---
[[ $EUID -eq 0 ]] || die "must run as root (use sudo)"

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
log "Source repo:    $REPO_DIR"
log "Install prefix: $PREFIX"

if [[ "$REPO_DIR" != "$PREFIX" ]]; then
    warn "Source repo ($REPO_DIR) is NOT the install prefix ($PREFIX)."
    warn "The recommended workflow is to clone directly into $PREFIX:"
    warn "  sudo git clone <repo-url> $PREFIX"
    warn "  sudo $PREFIX/deploy/install.sh"
    warn "Continuing anyway — make sure the source repo will not be moved/deleted later."
fi

command -v python3 >/dev/null || die "python3 not found in PATH"
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
log "Python version: $PYTHON_VERSION"
case "$PYTHON_VERSION" in
    3.11|3.12) ;;
    *) warn "Python $PYTHON_VERSION is not 3.11 or 3.12 (the supported versions per pyproject.toml). Continuing anyway." ;;
esac

# =============================================================================
log "Step 0/8: install uv (Astral's Python package manager)"
# =============================================================================
# uv is a single static binary, ~30MB, no Python dependencies. We install it
# system-wide to /usr/local/bin so the systemd service user can also find it
# if needed for ad-hoc operations.
if command -v uv &>/dev/null; then
    log "  uv already installed: $(uv --version)"
else
    log "  uv not found — installing via Astral's official installer"
    # UV_INSTALL_DIR forces install to /usr/local/bin (default is ~/.local/bin)
    # so the binary is on PATH for all users including the service user.
    UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 \
        sh -c 'curl -LsSf https://astral.sh/uv/install.sh | sh' >/dev/null
    if ! command -v uv &>/dev/null; then
        die "uv install completed but binary not on PATH — check /usr/local/bin"
    fi
    log "  installed: $(uv --version)"
fi

# =============================================================================
log "Step 1/8: create system user '$SERVICE_USER'"
# =============================================================================
if id "$SERVICE_USER" &>/dev/null; then
    log "  user '$SERVICE_USER' already exists — skipping"
else
    useradd \
        --system \
        --home-dir "$DATA_DIR" \
        --shell /usr/sbin/nologin \
        --comment "ElephantBroker Cognitive Runtime" \
        "$SERVICE_USER"
    log "  created system user '$SERVICE_USER' (no shell, home=$DATA_DIR)"
fi

# =============================================================================
log "Step 2/8: create directories"
# =============================================================================
# install -d is idempotent and sets owner/group/mode in one call.
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 755 "$PREFIX"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 750 "$CONFIG_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 750 "$DATA_DIR"
log "  $PREFIX            (755 $SERVICE_USER:$SERVICE_GROUP)"
log "  $CONFIG_DIR  (750 $SERVICE_USER:$SERVICE_GROUP)"
log "  $DATA_DIR    (750 $SERVICE_USER:$SERVICE_GROUP)"

# =============================================================================
log "Step 3/8: install runtime + HITL middleware via uv sync (workspace mode)"
# =============================================================================
# `uv sync` does ALL of these in one command:
#   - Creates $REPO_DIR/.venv if missing (with the Python version pinned in
#     pyproject.toml requires-python)
#   - Reads pyproject.toml + uv.lock and installs the EXACT pinned versions
#   - Removes any packages not in the lockfile (full sync = zero drift)
#   - Installs the elephantbroker project AND the hitl-middleware workspace
#     member in editable mode (both share the same venv)
#
# Workspace mode: hitl-middleware is declared as a [tool.uv.workspace] member
# in the root pyproject.toml. The root uv.lock is the single source of truth
# for both packages, so a separate `uv pip install hitl-middleware` is no
# longer needed (and would in fact reintroduce the dependency-drift bug it
# was being used to "fix").
#
# We pass `--frozen` to refuse to modify uv.lock at install time. If the
# lockfile is missing or out of sync with pyproject.toml, the operator must
# regenerate it via `uv lock` BEFORE running install.sh. This prevents
# accidental dep drift on production hosts.
cd "$REPO_DIR"
log "  uv sync --frozen --no-dev (production install, no test deps)"
uv sync --frozen --no-dev

# =============================================================================
log "Step 4/8: post-install fixes (Cognee writable dirs + mistralai safety net)"
# =============================================================================

# 4a) mistralai cleanup (belt-and-suspenders, only matters for the pip path)
# cognee==0.5.3 ships a broken `mistralai` namespace package as a transitive
# dep. With uv (the supported install path) this is NOT an issue — uv's
# holistic resolver picks mistralai 1.12.4 (a working modern version). But
# if anyone runs `pip install` against this venv (e.g. by habit), pip's
# greedy resolver may install the broken namespace package.
#
# This cleanup is a defensive safety net for the pip path. With uv it's a
# no-op (mistralai is the modern version, not a namespace package).
uv pip uninstall mistralai 2>/dev/null || true
MISTRAL_DIR=$(find "$REPO_DIR/.venv/lib" -type d -name mistralai -prune 2>/dev/null | head -n 1 || true)
if [[ -n "$MISTRAL_DIR" ]]; then
    # Only remove if it's the broken namespace-package shape (no METADATA file)
    if [[ ! -f "$(dirname "$MISTRAL_DIR")"/mistralai-*.dist-info/METADATA ]]; then
        rm -rf "$MISTRAL_DIR"
        log "  removed mistralai ghost package (pip safety net): $MISTRAL_DIR"
    fi
fi

# 4b) Cognee writable directories
# Cognee creates `.cognee_system/` and `.data_storage/` inside its own
# site-packages directory at runtime. We pre-create them so first-run
# doesn't fail. Final chown -R later in step 6 makes them owned by the
# service user (no chmod 777 hack needed — that was the wrong fix).
COGNEE_DIR=$(find "$REPO_DIR/.venv/lib" -maxdepth 4 -type d -name cognee -path '*/site-packages/cognee' | head -n 1)
if [[ -z "$COGNEE_DIR" ]]; then
    die "could not locate cognee site-packages dir under $REPO_DIR/.venv/lib — did uv sync fail?"
fi
mkdir -p "$COGNEE_DIR/.cognee_system/databases"
mkdir -p "$COGNEE_DIR/.data_storage"
log "  cognee writable dirs ready: $COGNEE_DIR/{.cognee_system,.data_storage}"

# 4c) Cognee anonymous-telemetry id file
# Cognee writes a uuid here on first run for opt-in telemetry. We pre-create
# it empty so the runtime user has a writable target (avoids permission
# warnings). The runtime sets COGNEE_DISABLE_TELEMETRY=true at import time
# anyway (elephantbroker/__init__.py), so this file stays empty — but
# pre-creating it avoids log noise.
ANON_ID_PATH=$(find "$REPO_DIR/.venv/lib" -maxdepth 3 -type d -name site-packages | head -n 1)/.anon_id
touch "$ANON_ID_PATH"
chmod 644 "$ANON_ID_PATH"
log "  cognee anon_id touched: $ANON_ID_PATH"

# =============================================================================
log "Step 5/8: install config files into $CONFIG_DIR"
# =============================================================================
# default.yaml: always overwrite (it's the structural template; secrets and
# operator overrides live in env). Owner eb:eb mode 640.
install -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 640 \
    "$REPO_DIR/elephantbroker/config/default.yaml" \
    "$CONFIG_DIR/default.yaml"
log "  $CONFIG_DIR/default.yaml  (640 $SERVICE_USER:$SERVICE_GROUP, overwritten)"

# env files: NEVER overwrite — they contain operator secrets. Owner root:eb
# mode 640 (root writes, service reads). On first install, copy from .example.
ENV_FRESHLY_COPIED=0
HITL_ENV_FRESHLY_COPIED=0
if [[ -f "$CONFIG_DIR/env" ]]; then
    log "  $CONFIG_DIR/env           (already exists — preserved)"
else
    install -o root -g "$SERVICE_GROUP" -m 640 \
        "$REPO_DIR/elephantbroker/config/env.example" \
        "$CONFIG_DIR/env"
    ENV_FRESHLY_COPIED=1
    log "  $CONFIG_DIR/env           (640 root:$SERVICE_GROUP, FROM TEMPLATE — edit before starting)"
fi

if [[ -f "$CONFIG_DIR/hitl.env" ]]; then
    log "  $CONFIG_DIR/hitl.env      (already exists — preserved)"
else
    install -o root -g "$SERVICE_GROUP" -m 640 \
        "$REPO_DIR/hitl-middleware/hitl.env.example" \
        "$CONFIG_DIR/hitl.env"
    HITL_ENV_FRESHLY_COPIED=1
    log "  $CONFIG_DIR/hitl.env      (640 root:$SERVICE_GROUP, FROM TEMPLATE — edit before starting)"
fi

# F11 (TODO-3-614): auto-generate EB_HITL_CALLBACK_SECRET on first install.
#
# The runtime AND the hitl-middleware must agree on the same HMAC secret or
# every HITL approval callback fails verification. Historically the operator
# was told (in `Next steps:` below) to run `openssl rand -hex 32` and paste
# the result into BOTH /etc/elephantbroker/env AND /etc/elephantbroker/hitl.env.
# In practice this was the #1 cause of "first start works for everything
# except HITL" because operators routinely (a) forgot, (b) generated different
# values for the two files, or (c) pasted with surrounding whitespace.
#
# When BOTH env files were freshly copied in this run, we generate one secret
# and patch it into both. If only one was freshly copied (the other already
# exists with operator-customized contents), we leave the placeholder alone
# and warn the operator — auto-generating one half would silently break the
# existing pair.
if [[ "$ENV_FRESHLY_COPIED" -eq 1 && "$HITL_ENV_FRESHLY_COPIED" -eq 1 ]]; then
    if command -v openssl >/dev/null 2>&1; then
        HITL_SECRET=$(openssl rand -hex 32)
        # Use a temp file + mv pattern instead of `sed -i` to keep ownership/mode
        # intact (sed -i on Linux re-creates the file with the invoking user's
        # umask, which would clobber the 640 root:elephantbroker we just set).
        for env_file in "$CONFIG_DIR/env" "$CONFIG_DIR/hitl.env"; do
            tmp_file=$(mktemp)
            sed "s|^EB_HITL_CALLBACK_SECRET=$|EB_HITL_CALLBACK_SECRET=$HITL_SECRET|" \
                "$env_file" > "$tmp_file"
            cat "$tmp_file" > "$env_file"
            rm -f "$tmp_file"
        done
        log "  EB_HITL_CALLBACK_SECRET   (auto-generated, written to env + hitl.env)"
    else
        warn "  openssl not found — cannot auto-generate EB_HITL_CALLBACK_SECRET."
        warn "  You MUST set the same value manually in env + hitl.env before starting HITL."
    fi
elif [[ "$ENV_FRESHLY_COPIED" -eq 1 || "$HITL_ENV_FRESHLY_COPIED" -eq 1 ]]; then
    warn "  Only ONE of env / hitl.env was freshly copied. Skipping HITL secret"
    warn "  auto-generation — paste the existing value from the preserved file"
    warn "  into the freshly-copied one, or both halves will fail HMAC verification."
fi

# =============================================================================
log "Step 6/8: ensure ownership across $PREFIX (final pass)"
# =============================================================================
# All operations above run as root, which means files inside the venv may have
# root ownership. Final chown -R sets the whole install tree to the service
# user. This is the LAST thing before systemd setup so any subsequent file
# access (e.g. by the systemd service) finds correct ownership.
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$PREFIX"
log "  chowned $PREFIX → $SERVICE_USER:$SERVICE_GROUP (recursive)"

# =============================================================================
log "Step 7/8: install systemd unit files"
# =============================================================================
if [[ "$INSTALL_SYSTEMD" -eq 0 ]]; then
    log "  --no-systemd flag set — skipping"
else
    install -o root -g root -m 644 \
        "$REPO_DIR/deploy/systemd/elephantbroker.service" \
        /etc/systemd/system/elephantbroker.service
    install -o root -g root -m 644 \
        "$REPO_DIR/deploy/systemd/elephantbroker-hitl.service" \
        /etc/systemd/system/elephantbroker-hitl.service
    log "  installed /etc/systemd/system/elephantbroker{,-hitl}.service"

    systemctl daemon-reload
    log "  systemctl daemon-reload"

    systemctl enable elephantbroker elephantbroker-hitl >/dev/null 2>&1 || true
    log "  systemctl enable elephantbroker elephantbroker-hitl"
fi

# =============================================================================
log "Step 8/8: verify install"
# =============================================================================
# Quick smoke test: invoke the elephantbroker entry point with --help to
# confirm the venv is functional and the binary is on the right path.
if [[ -x "$REPO_DIR/.venv/bin/elephantbroker" ]]; then
    if "$REPO_DIR/.venv/bin/elephantbroker" --help >/dev/null 2>&1; then
        log "  elephantbroker entry point works ✓"
    else
        warn "  elephantbroker --help returned non-zero — check the install"
    fi
else
    warn "  elephantbroker binary not found at $REPO_DIR/.venv/bin/elephantbroker"
fi

# =============================================================================
log "Install complete."
# =============================================================================
cat <<EOF

Next steps:
  1. Edit secrets in /etc/elephantbroker/env :
        EB_LLM_API_KEY=...
        EB_EMBEDDING_API_KEY=...
        EB_NEO4J_PASSWORD=...                   # REQUIRED — runtime refuses to boot if empty
        EB_HITL_CALLBACK_SECRET=\$(openssl rand -hex 32)

  2. Edit /etc/elephantbroker/hitl.env and put the SAME EB_HITL_CALLBACK_SECRET
     value as the runtime env file.

  3. Set gateway_id in /etc/elephantbroker/default.yaml to a deployment-
     specific value (REQUIRED — the runtime refuses to boot with the empty
     sentinel default; two hosts that share the same gateway_id collide on
     Redis, ClickHouse, and Neo4j). For example:
        gateway:
          gateway_id: "gw-prod-eu1"     # any unique label per host
        # Override at runtime via EB_GATEWAY_ID if you prefer env-based config.

  4. Review the rest of /etc/elephantbroker/default.yaml (org_id, team_id,
     reranker, etc.) — most operators only need to change those few fields.

  5. Make sure your infrastructure (Neo4j / Qdrant / Redis) is running. The
     project ships a docker-compose file at infrastructure/docker-compose.yml:
        cd $REPO_DIR/infrastructure && docker compose up -d

  6. Start the services:
        sudo systemctl start elephantbroker elephantbroker-hitl

  7. Verify:
        systemctl status elephantbroker elephantbroker-hitl
        curl http://localhost:8420/health/    # note trailing slash
        curl http://localhost:8421/health
        journalctl -u elephantbroker -f

EOF
