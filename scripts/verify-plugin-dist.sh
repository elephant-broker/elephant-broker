#!/usr/bin/env bash
# verify-plugin-dist.sh — enforce that committed openclaw-plugins/*/dist/
# matches a fresh `npm ci && npm run build` off committed src/ + lockfile.
#
# Why: OpenClaw loads openclaw-plugins/*/dist/index.js at runtime per each
# plugin's package.json "main" + openclaw.plugin.json "entry". Committed
# dist is the source of truth for what deploys; committed src is the
# source of truth for what was written. Drift silently ships stale bytes
# (the incident that produced TODO-6-501 on PR #6).
#
# Two modes:
#   Manual:   bash scripts/verify-plugin-dist.sh
#             (or `scripts/verify-plugin-dist.sh` if executable)
#             → rebuilds both plugins unconditionally, fails on drift.
#
#   Hook:     scripts/verify-plugin-dist.sh --hook
#             → only rebuilds plugins whose src/** has staged changes
#               (invoked from .githooks/pre-commit). Silent no-op when
#               no plugin src touched.
#
# Exits 0 on clean, 1 on drift or build failure.

set -euo pipefail

# Repo root — resolve relative to this script so it works from any cwd.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"
cd "${REPO_ROOT}"

PLUGINS=(elephantbroker-context elephantbroker-memory)
MODE="manual"
if [[ "${1:-}" == "--hook" ]]; then
    MODE="hook"
fi

# In hook mode, figure out which plugins have staged src/** changes.
# In manual mode, check both unconditionally.
PLUGINS_TO_CHECK=()
if [[ "${MODE}" == "hook" ]]; then
    # git diff --cached --name-only prints staged file paths.
    STAGED="$(git diff --cached --name-only --diff-filter=ACMR)"
    for plugin in "${PLUGINS[@]}"; do
        if printf '%s\n' "${STAGED}" | grep -q "^openclaw-plugins/${plugin}/src/"; then
            PLUGINS_TO_CHECK+=("${plugin}")
        fi
    done
    if [[ ${#PLUGINS_TO_CHECK[@]} -eq 0 ]]; then
        # No plugin src staged — hook silently passes.
        exit 0
    fi
else
    PLUGINS_TO_CHECK=("${PLUGINS[@]}")
fi

# Rebuild each affected plugin.
for plugin in "${PLUGINS_TO_CHECK[@]}"; do
    plugin_dir="openclaw-plugins/${plugin}"
    if [[ ! -d "${plugin_dir}" ]]; then
        echo "verify-plugin-dist: plugin directory missing: ${plugin_dir}" >&2
        exit 1
    fi
    echo "verify-plugin-dist: rebuilding ${plugin}..."
    build_log="$(mktemp)"
    trap 'rm -f "${build_log}"' EXIT
    (
        cd "${plugin_dir}"
        # --prefer-offline + --no-audit + --no-fund keeps pre-commit fast
        # when node_modules is already warm; falls through to network only
        # when the cache is cold.
        npm ci --prefer-offline --no-audit --no-fund >"${build_log}" 2>&1
        npm run build >>"${build_log}" 2>&1
    ) || {
        echo "verify-plugin-dist: build failed for ${plugin}" >&2
        echo "--- build output ---" >&2
        cat "${build_log}" >&2
        exit 1
    }
    rm -f "${build_log}"
    trap - EXIT
done

# After rebuild, committed dist must match the fresh build bytes.
# git diff --exit-code returns 0 iff there's no diff.
DIFF_PATHS=()
for plugin in "${PLUGINS_TO_CHECK[@]}"; do
    DIFF_PATHS+=("openclaw-plugins/${plugin}/dist/")
done

if ! git diff --exit-code -- "${DIFF_PATHS[@]}" >/dev/null; then
    {
        echo ""
        echo "verify-plugin-dist: ERROR — committed dist/ does not match a fresh build."
        echo ""
        echo "Plugins that drifted:"
        git diff --name-only -- "${DIFF_PATHS[@]}" | sed 's/^/  /'
        echo ""
        echo "To fix:"
        for plugin in "${PLUGINS_TO_CHECK[@]}"; do
            echo "  cd openclaw-plugins/${plugin} && npm ci && npm run build"
        done
        echo "  git add openclaw-plugins/*/dist/"
        echo ""
        echo "Then re-run the commit. Manual verify: bash scripts/verify-plugin-dist.sh"
    } >&2
    exit 1
fi

if [[ "${MODE}" == "manual" ]]; then
    echo "verify-plugin-dist: OK — all plugin dist/ subtrees match fresh builds."
fi
exit 0
