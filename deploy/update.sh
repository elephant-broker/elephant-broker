#!/usr/bin/env bash
# =============================================================================
# ElephantBroker DB-VM updater
# =============================================================================
#
# In-place upgrade for an existing install. Pulls the latest source from git,
# syncs the venv to match the lockfile via `uv sync`, re-chowns the install
# tree, and restarts both systemd services.
#
# Usage:
#   sudo /opt/elephantbroker/deploy/update.sh           # default: uv sync --frozen
#   sudo /opt/elephantbroker/deploy/update.sh --upgrade # regenerate uv.lock first
#
# This script is idempotent and runs entirely as root (no sudo -u switching).
# It refuses to run on a dirty git tree so the operator does not lose
# uncommitted local changes.
#
# Default behavior (no --upgrade flag):
#   - git pull --ff-only origin <current-branch>
#   - uv sync --frozen --no-dev (installs EXACTLY what uv.lock specifies)
#   - chown -R + restart services
#
# With --upgrade:
#   - git pull
#   - uv lock --upgrade (regenerates uv.lock from current pyproject.toml,
#     picking the latest versions allowed by the constraints)
#   - uv sync --no-dev (installs the new lockfile)
#   - This is the path for "I bumped a version in pyproject.toml" workflows.
#     See deploy/UPDATING-DEPS.md for the full upgrade procedure.
#
# Flags:
#   --upgrade        Regenerate uv.lock before syncing. Use when a new
#                    dependency was added or a version was bumped in
#                    pyproject.toml.
#   --no-restart     Do not restart systemd services after install (useful for
#                    multi-step upgrades, or when running on a host with no
#                    systemd units installed).
#   --prefix PATH    Override the install prefix (default: /opt/elephantbroker)
#   --help           Show this message
# =============================================================================

set -euo pipefail

# --- Defaults ---
PREFIX="/opt/elephantbroker"
SERVICE_USER="elephantbroker"
SERVICE_GROUP="elephantbroker"
UPGRADE_LOCK=0
RESTART=1

# --- Parse flags ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --upgrade) UPGRADE_LOCK=1; shift ;;
        --no-restart) RESTART=0; shift ;;
        --prefix) PREFIX="$2"; shift 2 ;;
        --help|-h)
            cat <<'HELP'
ElephantBroker DB-VM updater

Usage:
  sudo ./update.sh [--upgrade] [--no-restart] [--prefix PATH]

Flags:
  --upgrade        Regenerate uv.lock before syncing. Use when a new dependency
                   was added or a version was bumped in pyproject.toml. WITHOUT
                   --upgrade, the script installs EXACTLY what uv.lock specifies
                   (frozen mode) — the safe default for in-place updates.
  --no-restart     Do not restart systemd services after install.
  --prefix PATH    Override install prefix (default: /opt/elephantbroker)
  --help, -h       Show this message

Default behavior (no --upgrade):
  - git pull --ff-only origin <current-branch>
  - uv sync --frozen --no-dev (installs EXACTLY what uv.lock specifies)
  - chown -R elephantbroker:elephantbroker /opt/elephantbroker
  - systemctl restart elephantbroker elephantbroker-hitl

With --upgrade:
  - git pull --ff-only origin <current-branch>
  - uv lock --upgrade (regenerate the lockfile)
  - uv sync --no-dev (install the new lockfile)
  - chown + restart

The script refuses to run on a dirty git tree.
See deploy/UPDATING-DEPS.md for the full dep upgrade procedure.
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
[[ -d "$PREFIX/.git" ]] || die "$PREFIX is not a git working tree — was the repo cloned in place?"
[[ -d "$PREFIX/.venv" ]] || die "$PREFIX/.venv not found — run install.sh first"
command -v uv &>/dev/null || die "uv not found in PATH — run install.sh first (it installs uv)"

cd "$PREFIX"

# Refuse dirty tree (operator might lose uncommitted changes)
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    warn "Working tree at $PREFIX has uncommitted changes:"
    git status --short
    die "refusing to update on a dirty tree — commit or stash your changes first"
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
log "Install prefix: $PREFIX"
log "Current branch: $CURRENT_BRANCH"

# =============================================================================
log "Step 1/4: git pull"
# =============================================================================
BEFORE_SHA="$(git rev-parse HEAD)"
git pull --ff-only origin "$CURRENT_BRANCH"
AFTER_SHA="$(git rev-parse HEAD)"
if [[ "$BEFORE_SHA" == "$AFTER_SHA" ]]; then
    log "  already up to date ($AFTER_SHA)"
else
    log "  $BEFORE_SHA -> $AFTER_SHA"
fi

# =============================================================================
log "Step 2/4: uv sync"
# =============================================================================
if [[ "$UPGRADE_LOCK" -eq 1 ]]; then
    log "  --upgrade flag: regenerating uv.lock from pyproject.toml"
    uv lock --upgrade
    log "  uv sync --no-dev"
    uv sync --no-dev
else
    log "  uv sync --frozen --no-dev (installs exactly what uv.lock specifies)"
    uv sync --frozen --no-dev
fi

# Workspace mode: hitl-middleware is a [tool.uv.workspace] member of the
# root pyproject.toml, so the `uv sync` above already covers it. Before
# the workspace conversion this script ran a separate `uv pip install` —
# that bypassed the lockfile entirely and let the HITL service drift
# from the runtime on every update.

# Cognee writable directories: re-create in case a fresh sync wiped them
COGNEE_DIR=$(find "$PREFIX/.venv/lib" -maxdepth 4 -type d -name cognee -path '*/site-packages/cognee' | head -n 1 || true)
if [[ -n "$COGNEE_DIR" ]]; then
    mkdir -p "$COGNEE_DIR/.cognee_system/databases" "$COGNEE_DIR/.data_storage"
fi

# =============================================================================
log "Step 3/4: re-apply ownership across $PREFIX"
# =============================================================================
# Files written by `uv sync` (during root execution above) may have root
# ownership. Re-chown the whole tree to the service user so the systemd unit
# can read everything.
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$PREFIX"
log "  chowned $PREFIX -> $SERVICE_USER:$SERVICE_GROUP (recursive)"

# =============================================================================
log "Step 4/4: restart services"
# =============================================================================
if [[ "$RESTART" -eq 0 ]]; then
    log "  --no-restart flag set — skipping (run 'systemctl restart elephantbroker' manually)"
else
    if systemctl list-unit-files elephantbroker.service &>/dev/null; then
        systemctl restart elephantbroker
        log "  restarted elephantbroker"
    else
        warn "  elephantbroker.service not installed — skipping"
    fi
    if systemctl list-unit-files elephantbroker-hitl.service &>/dev/null; then
        systemctl restart elephantbroker-hitl
        log "  restarted elephantbroker-hitl"
    else
        warn "  elephantbroker-hitl.service not installed — skipping"
    fi
fi

# =============================================================================
log "Update complete."
# =============================================================================
cat <<EOF

Verify:
  systemctl status elephantbroker elephantbroker-hitl
  curl http://localhost:8420/health/    # note trailing slash
  curl http://localhost:8421/health
  journalctl -u elephantbroker -n 50

EOF
