"""R2-P7 / #1165 RESOLVED — strict ``relation_type`` charset
sanitization in :class:`GraphAdapter`.

Pre-fix: ``relation_type.upper().replace(" ", "_")`` only stripped
spaces. Cypher 5 rejects ``-``, ``.``, and other non-identifier
characters in relationship-type literals. A caller passing
``"has-child"`` produced an invalid ``[r:HAS-CHILD]`` clause that
raised at the driver. Existing #1165 pin in ``test_graph.py``
documented the gap.

Post-fix: the new ``_sanitize_rel_type()`` helper replaces any
character outside ``[A-Za-z0-9_]`` with ``_`` after upper-casing.
Tests below pin the helper's contract (alphanumeric pass-through,
hyphen replacement, dot replacement, idempotence on already-clean
inputs).
"""
from __future__ import annotations

import pytest

from elephantbroker.runtime.adapters.cognee.graph import _sanitize_rel_type


class TestSanitizeRelType:
    """Helper-level tests so the contract is pinned independently of
    any caller. The legacy uppercased+space-stripped behavior is a
    subset of the new contract — both ``"has child"`` and
    ``"has-child"`` resolve to ``HAS_CHILD``.
    """

    @pytest.mark.parametrize(
        "raw,expected",
        [
            # Already-clean inputs — idempotent.
            ("OWNS_GOAL", "OWNS_GOAL"),
            ("CREATED_BY", "CREATED_BY"),
            ("CHILD_OF", "CHILD_OF"),
            # Lower-case → upper-case.
            ("owns_goal", "OWNS_GOAL"),
            # Spaces → underscore (legacy behavior preserved).
            ("has child", "HAS_CHILD"),
            # R2-P7 / #1165: hyphen → underscore (was passing through).
            ("has-child", "HAS_CHILD"),
            # Defense-in-depth: dots / colons / brackets all get
            # collapsed to underscore.
            ("ns.relation", "NS_RELATION"),
            ("ns:relation", "NS_RELATION"),
            ("rel[type]", "REL_TYPE_"),
            # Mixed corruption.
            ("a-b.c d", "A_B_C_D"),
        ],
        ids=[
            "owns_goal_idempotent",
            "created_by_idempotent",
            "child_of_idempotent",
            "lowercase_uppercased",
            "space_to_underscore",
            "hyphen_to_underscore_1165",
            "dot_to_underscore",
            "colon_to_underscore",
            "bracket_to_underscore",
            "mixed_corruption",
        ],
    )
    def test_sanitize_rel_type(self, raw, expected):
        """Helper produces only ``[A-Za-z0-9_]`` output."""
        result = _sanitize_rel_type(raw)
        assert result == expected
        # Defensive shape check — no character outside the safe set.
        assert all(c.isalnum() or c == "_" for c in result)

    def test_sanitize_rel_type_idempotent_on_clean_input(self):
        """Sanitizing an already-clean rel_type twice yields the same
        result — guards against accidental double-application
        regressions.
        """
        clean = "OWNS_GOAL"
        once = _sanitize_rel_type(clean)
        twice = _sanitize_rel_type(once)
        assert once == twice == clean
