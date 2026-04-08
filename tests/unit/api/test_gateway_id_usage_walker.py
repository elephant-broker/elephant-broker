"""Static walker — refuses any of the four R2 gateway_id anti-patterns.

Bucket A-R2 (TODO-3-227) repeatedly fixed the same family of mistakes in
``elephantbroker/api/routes/`` where a route handler combined caller-supplied
``body.gateway_id`` with the middleware-stamped ``request.state.gateway_id``
in a way that let a tenant read or write another tenant's data. The
canonical safe pattern lives at ``elephantbroker/api/routes/trace.py:25-39``::

    gw_id = getattr(request.state, "gateway_id", None)
    if gw_id is not None:
        target.gateway_id = gw_id

Three properties of the canonical pattern are load-bearing:

1. ``None`` (not ``""``) is the ``getattr`` default — ``""`` is a *valid*
   gateway_id from the middleware's perspective and must not collide with
   the "middleware not wired" sentinel.
2. The check is ``is not None`` (not truthiness) — post-Bucket-A the
   default ``gateway_id`` is ``""`` (falsy), and a truthiness check would
   silently skip the override and let the caller's value win.
3. The middleware value wins UNCONDITIONALLY — there is no ``or`` clause,
   no fallback to ``body.gateway_id``, no "use the body if state is empty".

This walker scans every ``.py`` file under ``elephantbroker/api/routes/``
for the four anti-patterns Bucket A-R2 had to remove by hand, and fails the
build with a precise file:line for each violation. The test exists so a
future PR that re-introduces the anti-pattern is caught at CI time, not
in a tenant-isolation incident.

The detection is regex-based rather than AST-based for two reasons:
the patterns are line-local (they don't span function boundaries), and a
regex is auditable in fewer than 200 lines whereas an AST visitor would
need ~600. Both approaches were considered acceptable per the R2 dispatch.
False-positive risk is bounded by anchoring every pattern on the literal
string ``gateway_id`` and on either ``request.state`` or a body identifier,
so a regex hit is essentially always a real anti-pattern.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

# Resolve elephantbroker/api/routes/ relative to this file so the test runs
# regardless of the working directory pytest is invoked from.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ROUTES_DIR = _REPO_ROOT / "elephantbroker" / "api" / "routes"


def _route_files() -> list[Path]:
    """Return every ``.py`` route file. Excludes ``__init__.py`` and the
    ``trace_event_descriptions.py`` data module (which contains no handlers
    and no ``request.state`` access)."""
    files: list[Path] = []
    for path in sorted(_ROUTES_DIR.glob("*.py")):
        if path.name in {"__init__.py", "trace_event_descriptions.py"}:
            continue
        files.append(path)
    return files


# ---------------------------------------------------------------------------
# Anti-pattern regexes
# ---------------------------------------------------------------------------

# Pattern 1 — body-OR (caller-supplied wins via truthy ``or``).
#
# Matches things like::
#
#     query.gateway_id = body.gateway_id or request.state.gateway_id
#     target.gateway_id = body.gateway_id or getattr(request.state, "gateway_id", None)
#
# The bug: when the caller supplies a non-empty ``body.gateway_id``, the
# ``or`` short-circuits to it and the middleware-stamped value is never
# consulted. A malicious tenant can read another tenant's data by posting
# ``{"gateway_id": "victim-tenant"}`` to any handler with this pattern.
_PATTERN_BODY_OR = re.compile(
    r"""\.gateway_id\s*or\s+(?:request\.state\.gateway_id|getattr\s*\(\s*request\.state\b)"""
)

# Pattern 2 — truthiness check on the middleware value.
#
# Matches things like::
#
#     gw_id = getattr(request.state, "gateway_id", None)
#     if gw_id:                  # <-- BUG: empty string is a valid identity
#         query.gateway_id = gw_id
#
# Post-Bucket-A the default gateway_id is ``""`` (empty string, falsy), so a
# truthiness check silently skips the override and the caller's value wins.
# The canonical pattern is ``if gw_id is not None:``.
#
# TODO-3-230 (Bucket A-R3, BSR MED): earlier the regex anchored exclusively on
# the literal identifier ``gw_id``, which assumed every handler used the same
# variable name. Callers routinely use variants like ``gateway_id``,
# ``caller_gw``, ``_state_gw``, or bare ``gw``, and the narrow anchor let
# those shapes through silently. We now enumerate every identifier that has
# plausibly held a middleware-extracted gateway_id in this codebase. This is
# deliberately a closed list (not a catch-all ``\w+``) so unrelated
# truthiness checks on other variables remain out of scope. If a new handler
# introduces a new name, add it here as part of the review that lands the
# handler.
_PATTERN_IF_TRUTHY = re.compile(
    r"""^\s*if\s+(?:gw_id|gateway_id|caller_gw|_state_gw|gw)\s*:\s*(?:#.*)?$""",
    re.MULTILINE,
)

# Pattern 3 — reverse-OR (middleware OR body).
#
# Matches things like::
#
#     query.gateway_id = request.state.gateway_id or body.gateway_id
#     target.gateway_id = getattr(request.state, "gateway_id", "") or body.gateway_id
#
# The bug: post-Bucket-A the middleware default is ``""`` (falsy), so the
# ``or`` falls through to the body-supplied value. Same tenant-spoofing
# consequence as the body-OR pattern. The ``getattr`` branch is anchored
# specifically on the ``gateway_id`` attribute name so other state attrs
# (``actor_id``, ``session_key``) reading the same way do not false-fire.
_PATTERN_REVERSE_OR = re.compile(
    r"""=\s*(?:request\.state\.gateway_id|"""
    r"""getattr\s*\(\s*request\.state\s*,\s*['"]gateway_id['"][^)]*\))"""
    r"""\s*or\s+\w""",
)

# Pattern 4 — sentinel literal default in ``getattr(request.state, ...)``.
#
# Matches things like::
#
#     gw_id = getattr(request.state, "gateway_id", "local")
#     gw_id = getattr(request.state, "gateway_id", "default")
#
# The bug: a hardcoded sentinel like ``"local"`` conflates "middleware not
# wired" with a stable but unrelated identity. Earlier R2 commits had to
# migrate every ``"local"`` default in the runtime — see Bucket A commit
# d850186 — and this walker prevents the regression.
#
# Two defaults are explicitly allowed:
#
# * ``None`` — the canonical default. Pairs with the canonical
#   ``if gw_id is not None:`` override check.
# * ``""`` — the post-Bucket-A convention. Acceptable when the call site
#   reads the value and passes it through (e.g. to a logger, a Cypher
#   ``$gw`` parameter, or a DataPoint constructor) without making an
#   override decision. The empty string is a valid stamp from the
#   middleware and a sane "no gateway" sentinel for the no-middleware
#   path. Use ``None`` instead when the value will drive an
#   ``is not None``-style override.
#
# The negative lookahead anchors on the comma after the attribute name,
# then checks that the next token (after optional whitespace) is one of
# ``None``, ``""``, or ``''`` followed by a closing paren — any other
# default (sentinel string literal, identifier, expression) trips the
# walker. The whitespace MUST live inside the lookahead so the regex
# engine cannot backtrack the leading ``\s*`` and sneak past the
# negative-match check.
_PATTERN_LITERAL_FALLBACK = re.compile(
    r"""getattr\s*\(\s*request\.state\s*,\s*['"]gateway_id['"]\s*,"""
    r"""(?!\s*(?:None|""|'')\s*\))"""
)


_PATTERNS: list[tuple[str, re.Pattern[str], str]] = [
    (
        "body-OR",
        _PATTERN_BODY_OR,
        "caller-supplied gateway_id wins over middleware via truthy `or` "
        "— use `gw_id = getattr(request.state, 'gateway_id', None); "
        "if gw_id is not None: target.gateway_id = gw_id` instead",
    ),
    (
        "if-truthy",
        _PATTERN_IF_TRUTHY,
        "truthiness check on `gw_id` skips override when middleware value "
        "is the empty string — use `if gw_id is not None:` instead",
    ),
    (
        "reverse-OR",
        _PATTERN_REVERSE_OR,
        "middleware value falls back to body via truthy `or` when state is "
        "empty string — use the unconditional `if gw_id is not None:` "
        "assignment instead",
    ),
    (
        "literal-fallback",
        _PATTERN_LITERAL_FALLBACK,
        "non-None default in getattr(request.state, 'gateway_id', ...) "
        "conflates 'middleware not wired' with a real empty value — use "
        "`None` as the default and check `is not None`",
    ),
]


# ---------------------------------------------------------------------------
# Walker
# ---------------------------------------------------------------------------


def _scan_file(path: Path) -> list[tuple[int, str, str, str]]:
    """Return a list of (line_no, pattern_name, line_text, advice) violations
    for a single source file. Empty list = clean."""
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    violations: list[tuple[int, str, str, str]] = []
    for name, pattern, advice in _PATTERNS:
        for match in pattern.finditer(text):
            # Convert match offset to a 1-indexed line number by counting
            # newlines before the match start. This is cheap (O(file size))
            # and correct for both single-line and multiline regex flags.
            line_no = text.count("\n", 0, match.start()) + 1
            line_text = lines[line_no - 1].rstrip()
            violations.append((line_no, name, line_text, advice))
    violations.sort()
    return violations


def test_routes_dir_exists():
    """Sanity guard — if someone moves the routes directory the walker
    silently scans nothing and the test trivially passes. Fail loudly
    instead."""
    assert _ROUTES_DIR.is_dir(), (
        f"Expected routes directory at {_ROUTES_DIR}; the walker test is "
        "obsolete or the layout changed."
    )
    files = _route_files()
    assert len(files) > 0, (
        f"Expected at least one .py route file under {_ROUTES_DIR}; got "
        "zero. The walker test is obsolete or the discovery filter is wrong."
    )


@pytest.mark.parametrize("path", _route_files(), ids=lambda p: p.name)
def test_no_gateway_id_anti_patterns(path: Path):
    """Per-file walker. One parametrized case per route file so a regression
    surfaces with a precise file name in the pytest output."""
    violations = _scan_file(path)
    if violations:
        rel = path.relative_to(_REPO_ROOT)
        lines = [f"{rel}: {len(violations)} gateway_id anti-pattern violation(s):"]
        for line_no, name, line_text, advice in violations:
            lines.append(f"  {rel}:{line_no} [{name}] {line_text}")
            lines.append(f"      → {advice}")
        lines.append("")
        lines.append(
            "See elephantbroker/api/routes/trace.py:25-39 for the canonical "
            "safe pattern. The walker is in tests/unit/api/"
            "test_gateway_id_usage_walker.py."
        )
        pytest.fail("\n".join(lines))


def test_canonical_pattern_present_in_trace_route():
    """Positive control — the canonical pattern from the docstring must
    actually exist in trace.py. If a future refactor accidentally removes
    or rewrites it, the walker's reference is stale and we want to know."""
    trace = _ROUTES_DIR / "trace.py"
    text = trace.read_text(encoding="utf-8")
    assert "getattr(request.state, \"gateway_id\", None)" in text, (
        "Canonical pattern reference in trace.py was removed or rewritten. "
        "Update tests/unit/api/test_gateway_id_usage_walker.py to point at "
        "the new canonical site."
    )
    assert "if gw_id is not None:" in text, (
        "Canonical `if gw_id is not None:` check missing from trace.py. "
        "Update tests/unit/api/test_gateway_id_usage_walker.py to point at "
        "the new canonical site."
    )
