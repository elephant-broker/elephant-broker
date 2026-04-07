#!/usr/bin/env bash
# =============================================================================
# ElephantBroker DB-VM updater
# =============================================================================
#
# In-place upgrade for an existing install. Pulls the latest source from git,
# reinstalls the runtime + HITL middleware (without re-pulling broken
# transitive deps unless --full is given), restarts both services.
#
# Usage:
#   sudo /opt/elephantbroker/deploy/update.sh           # fast: --no-deps install
#   sudo /opt/elephantbroker/deploy/update.sh --full    # full reinstall + post-install fixes
#
# This script is idempotent and runs entirely as root (no sudo -u switching).
# It refuses to run on a dirty git tree so the operator does not lose
# uncommitted local changes.
#
# Flags:
#   --full           Run a full reinstall (pip install . without --no-deps) and
#                    re-apply the post-install fixes (mistralai purge, Cognee
#                    writable dirs). Use this when a new dependency was added
#                    to pyproject.toml.
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
FULL_INSTALL=0
RESTART=1

# --- Parse flags ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --full) FULL_INSTALL=1; shift ;;
        --no-restart) RESTART=0; shift ;;
        --prefix) PREFIX="$2"; shift 2 ;;
        --help|-h)
            cat <<'HELP'
ElephantBroker DB-VM updater

Usage:
  sudo ./update.sh [--full] [--no-restart] [--prefix PATH]

Flags:
  --full           Full reinstall (pip install .) + re-run post-install fixes.
                   Use when a new dependency was added to pyproject.toml.
  --no-restart     Do not restart systemd services after install.
  --prefix PATH    Override install prefix (default: /opt/elephantbroker)
  --help, -h       Show this message

Default behavior:
  - git pull origin <current-branch>
  - pip install --no-deps .  (preserves the mistralai workaround)
  - chown -R elephantbroker:elephantbroker /opt/elephantbroker
  - systemctl restart elephantbroker elephantbroker-hitl

The script refuses to run on a dirty git tree.
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
[[ -f "$PREFIX/venv/bin/pip" ]] || die "$PREFIX/venv not found — run install.sh first"

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
log "Step 2/4: pip install"
# =============================================================================
if [[ "$FULL_INSTALL" -eq 1 ]]; then
    log "  --full flag: full reinstall (will re-apply post-install fixes)"
    "$PREFIX/venv/bin/pip" install --quiet "$PREFIX"
    "$PREFIX/venv/bin/pip" install --quiet "$PREFIX/hitl-middleware"

    # Re-apply post-install fixes (full install re-pulls broken transitive deps)
    log "  applying post-install fixes (mistralai purge + Cognee dirs)"
    "$PREFIX/venv/bin/pip" uninstall -y mistralai 2>/dev/null || true
    MISTRAL_DIR=$(find "$PREFIX/venv/lib" -type d -name mistralai -prune 2>/dev/null | head -n 1 || true)
    [[ -n "$MISTRAL_DIR" ]] && rm -rf "$MISTRAL_DIR" && log "    removed mistralai ghost: $MISTRAL_DIR"

    COGNEE_DIR=$(find "$PREFIX/venv/lib" -maxdepth 4 -type d -name cognee -path '*/site-packages/cognee' | head -n 1)
    [[ -n "$COGNEE_DIR" ]] && mkdir -p "$COGNEE_DIR/.cognee_system/databases" "$COGNEE_DIR/.data_storage"
else
    log "  fast path: pip install --no-deps . (preserves mistralai workaround)"
    "$PREFIX/venv/bin/pip" install --quiet --no-deps "$PREFIX"
    "$PREFIX/venv/bin/pip" install --quiet --no-deps "$PREFIX/hitl-middleware"
fi

# =============================================================================
log "Step 3/4: re-apply ownership across $PREFIX"
# =============================================================================
# Files written by the pip install (during root execution above) may have root
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
