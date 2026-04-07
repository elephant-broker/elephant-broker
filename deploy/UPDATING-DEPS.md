# Updating Python Dependencies

ElephantBroker uses [`uv`](https://docs.astral.sh/uv/) (Astral's Python package
manager) for reproducible builds. This doc explains how to bump, add, or remove
a Python dependency safely.

## TL;DR

```bash
# 1. Edit pyproject.toml — change a version, add a dep, etc.
$ vi pyproject.toml

# 2. Regenerate the lockfile
$ uv lock

# 3. Sync your local venv
$ uv sync --extra dev

# 4. Run the tests
$ pytest tests/unit/

# 5. Commit pyproject.toml AND uv.lock together
$ git add pyproject.toml uv.lock
$ git commit -m "deps: bump <package> to <version>"
```

The cardinal rule: **`pyproject.toml` and `uv.lock` are committed together,
always**. Never commit one without the other.

---

## Why uv (not pip)

Reproducible builds and dependency hygiene. The reasoning:

1. **Lockfile is mandatory.** `uv sync` always reads `uv.lock` and refuses to
   silently drift. With pip, an operator who runs `pip install .` (without
   `--constraint`) gets whatever the `>=` ranges resolve to today, not the
   versions you tested with. uv removes that footgun by design.

2. **Holistic resolver.** uv's parallel resolver picks versions that satisfy
   ALL constraints simultaneously. pip's greedy resolver installs whatever the
   first dep asks for, then leaves later conflicts unresolved. The classic
   example: `cognee==0.5.3` ships a broken `mistralai` namespace package as a
   transitive dep that conflicts with `instructor`. pip happily installs the
   broken version; uv picks `mistralai==1.12.4` (a working modern version)
   automatically because it has to satisfy `instructor`'s constraints too.

3. **Speed.** uv resolves the entire ~187-package transitive tree in ~40ms
   (when nothing changed) or ~3 seconds (full re-resolution from scratch).
   pip takes minutes.

4. **Drift detection.** `uv sync --frozen` errors out if `pyproject.toml` has
   been modified since the last `uv lock`. The CI / install scripts use
   `--frozen` so dep changes can never reach production without going through
   a fresh `uv lock` step.

5. **Defense in depth.** Direct deps are pinned to exact versions in
   `pyproject.toml` AND in `uv.lock`. Even if someone runs `pip install .`
   manually (bypassing uv), they still get the right direct versions —
   only the transitive resolution may drift slightly.

---

## Common operations

### Bump a single dependency to a newer version

You want to update `pydantic` from `2.12.5` to `2.13.0`:

```bash
# 1. Edit pyproject.toml
$ vi pyproject.toml
# Change "pydantic==2.12.5" → "pydantic==2.13.0"

# 2. Regenerate uv.lock for just this package (faster than full --upgrade)
$ uv lock --upgrade-package pydantic

# 3. Sync the local venv
$ uv sync --extra dev

# 4. Run tests against the new version
$ pytest tests/unit/

# 5. Commit
$ git add pyproject.toml uv.lock
$ git commit -m "deps: bump pydantic to 2.13.0"
```

### Add a new dependency

```bash
# 1. Add the dep to pyproject.toml's [project] dependencies list, pinned
$ vi pyproject.toml
# Add "newpackage==1.2.3" to the dependencies list

# 2. Lock + sync
$ uv lock
$ uv sync --extra dev

# 3. Test that the new dep doesn't break anything
$ pytest tests/unit/

# 4. Commit
$ git add pyproject.toml uv.lock
$ git commit -m "deps: add newpackage==1.2.3 for <reason>"
```

### Remove a dependency

```bash
# 1. Remove from pyproject.toml
$ vi pyproject.toml

# 2. Lock + sync — uv sync will UNINSTALL the removed package from your venv
$ uv lock
$ uv sync --extra dev

# 3. Verify nothing imports it anymore
$ grep -rn "import <package>" elephantbroker tests
$ pytest tests/unit/

# 4. Commit
$ git add pyproject.toml uv.lock
$ git commit -m "deps: remove <package> (no longer used)"
```

### Full upgrade — bump everything to latest allowed by pyproject.toml

Use this for periodic maintenance, e.g. monthly security upgrades:

```bash
# 1. Regenerate uv.lock against current pyproject.toml ranges
$ uv lock --upgrade

# 2. Inspect the diff to see what changed
$ git diff uv.lock | head -100

# 3. Sync + test
$ uv sync --extra dev
$ pytest tests/unit/

# 4. Run integration tests too — full upgrade is the highest-risk scenario
$ ./scripts/run-integration-tests.sh

# 5. Commit
$ git add uv.lock
$ git commit -m "deps: full upgrade (security maintenance)"
```

Note: full upgrade only changes `uv.lock`, not `pyproject.toml`, because
direct deps are already pinned. To bump direct dep pins, edit pyproject.toml
manually before `uv lock --upgrade`.

### Security advisory: pin a transitive dep to fix a CVE

Sometimes the offending package isn't your direct dep but a transitive one
several levels deep. uv's `[tool.uv] override-dependencies` lets you force a
specific version of any package in the tree:

```toml
# In pyproject.toml
[tool.uv]
override-dependencies = [
    "vulnerable-package==2.0.0",  # CVE-2026-XXXXX, was pinned to <1.5 by upstream
]
```

Then `uv lock` will respect the override even though it conflicts with the
upstream's stated range. Use sparingly — it's a workaround, not a fix.

---

## Production deployment after a dep update

The `deploy/install.sh` and `deploy/update.sh` scripts on the DB VM use
`uv sync --frozen` by default — they install EXACTLY what `uv.lock` says,
nothing more, nothing less. After committing a dep update locally and merging
to main:

```bash
# On the DB VM
sudo /opt/elephantbroker/deploy/update.sh
```

This pulls the latest `pyproject.toml` + `uv.lock`, runs `uv sync --frozen`
to install exactly the new lockfile, re-chowns the install tree, and restarts
both systemd services.

If you want to upgrade dependencies on the DB VM directly (without a commit
on dev first), use `--upgrade`:

```bash
sudo /opt/elephantbroker/deploy/update.sh --upgrade
```

This runs `uv lock --upgrade` on the DB VM, which regenerates `uv.lock` against
the current `pyproject.toml`, then syncs. The new lockfile is left in
`/opt/elephantbroker/uv.lock` — commit it back to git afterwards if you want
to share the upgrade with other deployments.

---

## Lockfile conflicts during merge

If two branches both modify `pyproject.toml` AND `uv.lock`, the merge can
produce conflicts in `uv.lock` that are hard to read (it's a 3000+ line
TOML file). The fix is to take both branches' `pyproject.toml` changes,
discard the conflicted `uv.lock` entirely, and regenerate from scratch:

```bash
# After merging pyproject.toml manually:
$ rm uv.lock
$ uv lock          # regenerates from the merged pyproject.toml
$ uv sync --extra dev
$ pytest tests/unit/
$ git add pyproject.toml uv.lock
$ git commit -m "merge: regenerate uv.lock"
```

---

## Verifying reproducibility

To prove that the install is bit-for-bit reproducible, do a clean install
on two machines (or two `rm -rf .venv` cycles on the same machine) and
compare:

```bash
$ rm -rf .venv
$ uv sync --extra dev
$ uv pip freeze | sort > /tmp/install-1.txt

$ rm -rf .venv
$ uv sync --extra dev
$ uv pip freeze | sort > /tmp/install-2.txt

$ diff /tmp/install-1.txt /tmp/install-2.txt   # should be empty
```

---

## Troubleshooting

### `uv sync --frozen` fails with "lockfile is out of sync with pyproject.toml"

You modified `pyproject.toml` without regenerating `uv.lock`. Run:

```bash
$ uv lock
$ uv sync --extra dev
```

Then commit both files.

### `uv lock` fails with a resolution conflict

Two of your direct deps (or their transitive deps) require incompatible
versions of a shared package. The error message names the conflict — usually
the fix is to relax one of the constraints (or pick a different version of
one of the conflicting direct deps).

### `uv` not found on the DB VM

The installer (`deploy/install.sh`) installs uv via Astral's curl one-liner.
If something went wrong:

```bash
$ which uv
$ uv --version
# If missing, re-install:
$ curl -LsSf https://astral.sh/uv/install.sh | sudo UV_INSTALL_DIR=/usr/local/bin sh
```

### I need to install a specific Python version for uv to use

uv can manage Python versions itself:

```bash
$ uv python install 3.11.10   # downloads and installs Python 3.11.10
$ uv python pin 3.11.10        # pin this venv to use 3.11.10
$ rm -rf .venv && uv sync --extra dev
```

This is rarely needed in production (use the system Python), but useful for
local development on machines with multiple Python versions.

---

## Files involved

| File | Purpose | Commit? |
|---|---|---|
| `pyproject.toml` | Direct dependency declarations + project metadata | Yes |
| `uv.lock` | Full transitive lock with versions, hashes, sources | **Yes — always commit alongside pyproject.toml** |
| `.venv/` | Local virtual environment built by `uv sync` | **No** (in .gitignore) |
| `deploy/install.sh` | Production installer — runs `uv sync --frozen` | Yes |
| `deploy/update.sh` | Production updater — runs `uv sync --frozen` | Yes |
| `Dockerfile` | Dev/CI container — also uses `uv sync --frozen` | Yes |

---

## Further reading

- [uv documentation](https://docs.astral.sh/uv/)
- [Lockfile format spec (uv.lock TOML)](https://docs.astral.sh/uv/concepts/projects/sync/)
- [uv vs pip-tools comparison](https://docs.astral.sh/uv/pip/compatibility/)
