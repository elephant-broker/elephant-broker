#!/usr/bin/env bash
# =============================================================================
# ElephantBroker DB-VM installer
# =============================================================================
#
# Sets up a fresh DB VM for ElephantBroker:
#   1. Creates the dedicated `elephantbroker` system user
#   2. Creates /opt/elephantbroker, /etc/elephantbroker, /var/lib/elephantbroker
#   3. Builds the Python venv at /opt/elephantbroker/venv
#   4. Installs the runtime + HITL middleware
#   5. Applies post-install fixes (mistralai purge, Cognee writable dirs, etc.)
#   6. Copies default.yaml + env.example + hitl.env.example into /etc/elephantbroker
#   7. Installs the systemd units (unless --no-systemd)
#   8. Sets ownership/permissions per the canonical layout
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
    *) warn "Python $PYTHON_VERSION is not 3.11 or 3.12 (the tested versions). Continuing anyway." ;;
esac

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
log "Step 3/8: create or reuse Python venv at $PREFIX/venv"
# =============================================================================
if [[ -f "$PREFIX/venv/bin/python" ]]; then
    log "  venv already exists — skipping creation"
else
    python3 -m venv "$PREFIX/venv"
    log "  created venv with python3 ($PYTHON_VERSION)"
fi

# Always upgrade pip (cheap, ensures lockfile-style installs work later)
"$PREFIX/venv/bin/pip" install --quiet --upgrade pip

# =============================================================================
log "Step 4/8: install ElephantBroker runtime + HITL middleware"
# =============================================================================
log "  pip install $REPO_DIR (this can take a few minutes)"
"$PREFIX/venv/bin/pip" install --quiet "$REPO_DIR"

log "  pip install $REPO_DIR/hitl-middleware"
"$PREFIX/venv/bin/pip" install --quiet "$REPO_DIR/hitl-middleware"

# =============================================================================
log "Step 5/8: apply post-install fixes (mistralai + Cognee writable dirs)"
# =============================================================================

# 5a) mistralai ghost package
# cognee==0.5.3 pulls in a broken `mistralai` namespace package that conflicts
# with `instructor`. We don't use Mistral, so the safe fix is to remove it.
"$PREFIX/venv/bin/pip" uninstall -y mistralai 2>/dev/null || true
MISTRAL_DIR=$(find "$PREFIX/venv/lib" -type d -name mistralai -prune 2>/dev/null | head -n 1 || true)
if [[ -n "$MISTRAL_DIR" ]]; then
    rm -rf "$MISTRAL_DIR"
    log "  removed mistralai ghost package: $MISTRAL_DIR"
else
    log "  mistralai already absent"
fi

# 5b) Cognee writable directories
# Cognee creates `.cognee_system/` and `.data_storage/` inside its own
# site-packages directory at runtime — these need to exist and be writable
# by the service user. We pre-create them so the first run doesn't fail.
COGNEE_DIR=$(find "$PREFIX/venv/lib" -maxdepth 4 -type d -name cognee -path '*/site-packages/cognee' | head -n 1)
if [[ -z "$COGNEE_DIR" ]]; then
    die "could not locate cognee site-packages dir under $PREFIX/venv/lib — did pip install fail?"
fi
mkdir -p "$COGNEE_DIR/.cognee_system/databases"
mkdir -p "$COGNEE_DIR/.data_storage"
log "  cognee writable dirs ready: $COGNEE_DIR/{.cognee_system,.data_storage}"

# 5c) Cognee anonymous-telemetry id file
# Cognee writes a uuid here on first run for opt-in telemetry. We create the
# file empty so the runtime user has somewhere to write (avoids permission
# warnings). The runtime also sets COGNEE_DISABLE_TELEMETRY=true at import
# time, so this file is mostly cosmetic — but pre-creating avoids noise.
ANON_ID_PATH=$(find "$PREFIX/venv/lib" -maxdepth 3 -type d -name site-packages | head -n 1)/.anon_id
touch "$ANON_ID_PATH"
chmod 644 "$ANON_ID_PATH"
log "  cognee anon_id touched: $ANON_ID_PATH"

# =============================================================================
log "Step 6/8: install config files into $CONFIG_DIR"
# =============================================================================
# default.yaml: always overwrite (it's the structural template; secrets and
# operator overrides live in env). Owner eb:eb mode 640.
install -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 640 \
    "$REPO_DIR/elephantbroker/config/default.yaml" \
    "$CONFIG_DIR/default.yaml"
log "  $CONFIG_DIR/default.yaml  (640 $SERVICE_USER:$SERVICE_GROUP, overwritten)"

# env files: NEVER overwrite — they contain operator secrets. Owner root:eb
# mode 640 (root writes, service reads). On first install, copy from .example.
if [[ -f "$CONFIG_DIR/env" ]]; then
    log "  $CONFIG_DIR/env           (already exists — preserved)"
else
    install -o root -g "$SERVICE_GROUP" -m 640 \
        "$REPO_DIR/elephantbroker/config/env.example" \
        "$CONFIG_DIR/env"
    log "  $CONFIG_DIR/env           (640 root:$SERVICE_GROUP, FROM TEMPLATE — edit before starting)"
fi

if [[ -f "$CONFIG_DIR/hitl.env" ]]; then
    log "  $CONFIG_DIR/hitl.env      (already exists — preserved)"
else
    install -o root -g "$SERVICE_GROUP" -m 640 \
        "$REPO_DIR/hitl-middleware/hitl.env.example" \
        "$CONFIG_DIR/hitl.env"
    log "  $CONFIG_DIR/hitl.env      (640 root:$SERVICE_GROUP, FROM TEMPLATE — edit before starting)"
fi

# =============================================================================
log "Step 7/8: ensure ownership across $PREFIX (final pass)"
# =============================================================================
# All operations above run as root, which means files inside the venv may have
# root ownership. Final chown -R sets the whole install tree to the service
# user. This is the LAST thing before systemd setup so any subsequent file
# access (e.g. by the systemd service) finds correct ownership.
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$PREFIX"
log "  chowned $PREFIX → $SERVICE_USER:$SERVICE_GROUP (recursive)"

# =============================================================================
log "Step 8/8: install systemd unit files"
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
log "Install complete."
# =============================================================================
cat <<EOF

Next steps:
  1. Edit secrets in /etc/elephantbroker/env :
        EB_LLM_API_KEY=...
        EB_EMBEDDING_API_KEY=...
        EB_NEO4J_PASSWORD=...
        EB_HITL_CALLBACK_SECRET=\$(openssl rand -hex 32)

  2. Edit /etc/elephantbroker/hitl.env and put the SAME EB_HITL_CALLBACK_SECRET
     value as the runtime env file.

  3. Review /etc/elephantbroker/default.yaml (gateway_id, org_id, team_id,
     reranker, etc.) — most operators only need to change these few fields.

  4. Make sure your infrastructure (Neo4j / Qdrant / Redis) is running. The
     project ships a docker-compose file at infrastructure/docker-compose.yml:
        cd $REPO_DIR/infrastructure && docker compose up -d

  5. Start the services:
        sudo systemctl start elephantbroker elephantbroker-hitl

  6. Verify:
        systemctl status elephantbroker elephantbroker-hitl
        curl http://localhost:8420/health/    # note trailing slash
        curl http://localhost:8421/health
        journalctl -u elephantbroker -f

EOF
