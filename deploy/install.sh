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
#   4. Applies belt-and-suspenders post-install fixes (Cognee writable dirs
#      + mistralai ghost-package safety net for the pip path)
#   5. Copies default.yaml + env.example + hitl.env.example into /etc/elephantbroker
#      and auto-generates EB_HITL_CALLBACK_SECRET on first install
#   6. Chowns ONLY the Cognee writable subdirs to the service user;
#      $PREFIX itself stays root-owned for defense in depth
#   7. Installs the systemd units (unless --no-systemd)
#   8. Smoke-test: invoke `elephantbroker --help` to verify the venv is
#      functional and the entry-point binary is on the right path
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
#   --no-systemd            Skip installing systemd unit files
#   --prefix PATH           Override the install prefix (default: /opt/elephantbroker)
#   --allow-out-of-tree     Permit running install.sh from a directory other
#                           than $PREFIX. WITHOUT this flag, the script refuses
#                           to run unless the source repo IS the prefix — the
#                           supported workflow is to clone directly into
#                           /opt/elephantbroker. See C2 (TODO-3-202/602/011)
#                           for the rationale.
#   --help                  Show this message
# =============================================================================

set -euo pipefail

# --- Defaults ---
PREFIX="/opt/elephantbroker"
INSTALL_SYSTEMD=1
ALLOW_OOT=0
SERVICE_USER="elephantbroker"
SERVICE_GROUP="elephantbroker"
CONFIG_DIR="/etc/elephantbroker"
DATA_DIR="/var/lib/elephantbroker"

# --- Parse flags ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-systemd) INSTALL_SYSTEMD=0; shift ;;
        --prefix) PREFIX="$2"; shift 2 ;;
        --allow-out-of-tree) ALLOW_OOT=1; shift ;;
        --help|-h)
            cat <<'HELP'
ElephantBroker DB-VM installer

Usage:
  sudo ./install.sh [--no-systemd] [--prefix PATH] [--allow-out-of-tree]

Flags:
  --no-systemd            Skip installing systemd unit files
  --prefix PATH           Override the install prefix (default: /opt/elephantbroker)
  --allow-out-of-tree     Permit running install.sh from a directory other than
                          $PREFIX. WITHOUT this flag, the script refuses to run
                          unless the source repo IS the prefix. See note below.
  --help, -h              Show this message

Typical workflow:
  sudo git clone <repo-url> /opt/elephantbroker
  sudo /opt/elephantbroker/deploy/install.sh
  sudo nano /etc/elephantbroker/env       # fill in EB_LLM_API_KEY etc
  sudo nano /etc/elephantbroker/hitl.env  # fill in EB_HITL_CALLBACK_SECRET
  sudo systemctl start elephantbroker elephantbroker-hitl

The script is idempotent — safe to re-run on a partially-installed host.
It runs entirely as root (no sudo -u switching). All ownership is set
via chown after the privileged operations complete.

Out-of-tree installs (--allow-out-of-tree):
  uv sync writes the venv to <source-repo>/.venv. The systemd units
  hardcode /opt/elephantbroker/.venv/bin/elephantbroker. If the source
  repo is NOT the prefix, the venv lives at the wrong path and systemd
  startup fails. The --allow-out-of-tree flag exists only as an explicit
  opt-in for advanced users who know they will fix the venv path themselves.
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

# C2 (TODO-3-202, TODO-3-602, TODO-3-011): refuse out-of-tree installs by
# default. uv sync writes the venv to $REPO_DIR/.venv, but the systemd units
# hardcode $PREFIX/.venv/bin/elephantbroker. If $REPO_DIR != $PREFIX the venv
# lives at the wrong path and systemd will fail to start the service. The
# previous code only emitted a warning and proceeded, which routinely produced
# "install succeeded but `systemctl start` fails" reports from operators who
# missed the warning in the install log. Hard-fail unless --allow-out-of-tree
# is explicitly passed.
if [[ "$REPO_DIR" != "$PREFIX" ]]; then
    if [[ "$ALLOW_OOT" -eq 1 ]]; then
        warn "Source repo ($REPO_DIR) is NOT the install prefix ($PREFIX)."
        warn "--allow-out-of-tree was passed: proceeding, but note that uv sync will"
        warn "write the venv to $REPO_DIR/.venv NOT $PREFIX/.venv. The systemd units"
        warn "hardcode $PREFIX/.venv/bin/elephantbroker — startup will fail until you"
        warn "manually relocate the venv or edit the unit files. You take responsibility."
    else
        die "Source repo ($REPO_DIR) is not the install prefix ($PREFIX).

The supported workflow is to clone directly into $PREFIX:
  sudo git clone <repo-url> $PREFIX
  sudo $PREFIX/deploy/install.sh

If you really need to install from a different location, re-run with
--allow-out-of-tree (you take responsibility for the venv/systemd path
mismatch — see install.sh --help for details)."
    fi
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
#
# C3 (TODO-3-010): $PREFIX is intentionally root-owned (755). The service
# user must be able to READ and TRAVERSE the install tree but must NOT be
# able to WRITE to its own source code or venv binaries. The only paths the
# service user actually needs to write to are the Cognee runtime subdirs
# inside .venv (chowned individually in step 6) and $DATA_DIR. A compromised
# runtime process can no longer rewrite its own binaries or config templates.
install -d -o root -g root -m 755 "$PREFIX"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 750 "$CONFIG_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 750 "$DATA_DIR"
log "  $PREFIX            (755 root:root — defense in depth, see C3 comment)"
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
# holistic resolver picks mistralai 1.12.4 (a working modern version with a
# proper dist-info). But if anyone runs `pip install` against this venv (e.g.
# by habit), pip's greedy resolver may install the broken namespace package
# on top of the modern one.
#
# C7 (TODO-3-320): the previous version of this block always ran the
# `uv pip uninstall mistralai` step, even when mistralai wasn't present
# at all OR was the modern dist-info shape. That produced confusing log
# noise on the supported uv path and obscured the intent of the safety net.
# Now we shape-check FIRST and only act when the broken namespace-package
# shape is confirmed (no dist-info/METADATA file alongside the directory).
#
# C5 (TODO-3-012): the previous form swallowed all errors with
# `2>/dev/null || true`, which hid genuine failures (e.g. uv binary missing
# from PATH, permissions broken on the venv). The new form runs the
# uninstall ONLY when needed and warns loudly if it actually fails — the
# directory rm still happens as a fallback so the safety net delivers the
# end state regardless.
MISTRAL_DIR=$(find "$REPO_DIR/.venv/lib" -type d -name mistralai -prune 2>/dev/null | head -n 1 || true)
if [[ -z "$MISTRAL_DIR" ]]; then
    log "  mistralai not installed in venv — no cleanup needed (uv path)"
elif compgen -G "$(dirname "$MISTRAL_DIR")/mistralai-*.dist-info/METADATA" >/dev/null; then
    log "  mistralai present as proper dist-info package — no cleanup needed"
else
    # Confirmed broken namespace-package shape. Try uv pip uninstall first
    # so the venv's installer metadata stays consistent; fall through to a
    # filesystem rm -rf if uninstall reports nothing or fails outright.
    log "  mistralai ghost detected (no dist-info/METADATA) — running cleanup"
    if uv pip uninstall mistralai; then
        log "  uv pip uninstall mistralai → handled"
    else
        warn "  uv pip uninstall mistralai exited non-zero — continuing with directory removal"
    fi
    if [[ -d "$MISTRAL_DIR" ]]; then
        rm -rf "$MISTRAL_DIR"
        log "  removed mistralai ghost package (pip safety net): $MISTRAL_DIR"
    fi
fi

# 4b) Cognee writable directories
# Cognee creates `.cognee_system/` and `.data_storage/` inside its own
# site-packages directory at runtime. We pre-create them so first-run
# doesn't fail. Step 6 below targeted-chowns these specific subdirs to
# the service user (NOT a recursive chown of $PREFIX — see C3 comment).
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
# default.yaml: NEVER overwrite — operators routinely edit gateway_id, org_id,
# team_id, profile weights, and other deployment-specific knobs in this file.
# C1 (TODO-3-600): the previous unconditional `install` clobbered those edits
# on every install.sh re-run, which is the expected behavior of an idempotent
# installer except when the operator has customized the file. The fix mirrors
# the env / hitl.env handling below: copy from the packaged template only on
# first install, and track a "freshly copied" flag for downstream steps that
# may want to know whether the file is template-shape or operator-edited.
YAML_FRESHLY_COPIED=0
if [[ -f "$CONFIG_DIR/default.yaml" ]]; then
    log "  $CONFIG_DIR/default.yaml  (already exists — preserved)"
else
    install -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 640 \
        "$REPO_DIR/elephantbroker/config/default.yaml" \
        "$CONFIG_DIR/default.yaml"
    YAML_FRESHLY_COPIED=1
    log "  $CONFIG_DIR/default.yaml  (640 $SERVICE_USER:$SERVICE_GROUP, FROM TEMPLATE — set gateway_id before starting)"
fi

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
log "Step 6/8: chown writable subdirs only (defense in depth)"
# =============================================================================
# C3 (TODO-3-010): the previous version did `chown -R $SERVICE_USER $PREFIX`
# which transferred ownership of the entire install tree (source + venv +
# binaries) to the runtime user. A compromised runtime process could then
# rewrite its own code, the cognee binaries, or the config templates — the
# whole point of running as a dedicated unprivileged service user vanishes.
#
# The minimal set of paths that genuinely need to be writable by the
# service user is:
#   * $COGNEE_DIR/.cognee_system   — Cognee's runtime SQLite + state
#   * $COGNEE_DIR/.data_storage    — Cognee's chunk/artifact storage
#   * $ANON_ID_PATH                — Cognee's anonymous-telemetry id file
#
# Everything else stays root-owned. Default file modes from `uv sync` are
# 644/755 (other-readable + other-executable for dirs), so the service
# user can read and traverse the venv without owning it. The systemd unit's
# `ReadWritePaths=/opt/elephantbroker` permits writes to $PREFIX through
# its MAC layer, but DAC ownership now blocks unintended writes from a
# compromised runtime that didn't go through the pre-created Cognee paths.
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$COGNEE_DIR/.cognee_system"
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$COGNEE_DIR/.data_storage"
chown "$SERVICE_USER:$SERVICE_GROUP" "$ANON_ID_PATH"
log "  chowned $COGNEE_DIR/.cognee_system    → $SERVICE_USER:$SERVICE_GROUP"
log "  chowned $COGNEE_DIR/.data_storage     → $SERVICE_USER:$SERVICE_GROUP"
log "  chowned $ANON_ID_PATH                 → $SERVICE_USER:$SERVICE_GROUP"
log "  $PREFIX itself remains root-owned (defense in depth)"

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
#
# C2: this MUST use $PREFIX (not $REPO_DIR) — that's the path the systemd
# units hardcode (/opt/elephantbroker/.venv/bin/elephantbroker). For an
# in-tree install ($REPO_DIR == $PREFIX) the two paths are identical, so
# this is a no-op behavior change. For an out-of-tree install (--allow-out-of-tree),
# this will warn that the binary isn't where systemd expects it — which IS
# the right thing to surface to the operator.
if [[ -x "$PREFIX/.venv/bin/elephantbroker" ]]; then
    if "$PREFIX/.venv/bin/elephantbroker" --help >/dev/null 2>&1; then
        log "  elephantbroker entry point works ✓"
    else
        warn "  elephantbroker --help returned non-zero — check the install"
    fi
else
    warn "  elephantbroker binary not found at $PREFIX/.venv/bin/elephantbroker"
    warn "  (out-of-tree install? venv lives at $REPO_DIR/.venv — systemd will fail to start)"
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
