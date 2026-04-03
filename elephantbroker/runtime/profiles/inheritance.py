"""Profile inheritance engine — resolves base → named → org override chain.

Resolution order:
1. Walk ``extends`` chain from leaf to root (detect circular via visited set).
2. Return the leaf profile (builtins are complete objects, not sparse overrides).
3. Apply org-specific overrides from SQLite (sparse dict on top of resolved profile).
4. Return deep copy (immutable for session lifetime).
"""
from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any

from pydantic import BaseModel

from elephantbroker.schemas.profile import ProfilePolicy

logger = logging.getLogger(__name__)


class ProfileInheritanceEngine:
    """Resolves profile inheritance chains and applies org overrides."""

    def flatten(
        self,
        profile: ProfilePolicy,
        presets: dict[str, ProfilePolicy],
        org_overrides: dict[str, Any] | None = None,
    ) -> ProfilePolicy:
        """Resolve full inheritance chain and apply org overrides.

        For builtin presets (which are complete ``ProfilePolicy`` objects),
        the chain walk validates ancestry but the leaf already has all values.
        The real work is ``_apply_org_overrides()`` which takes a sparse dict
        and merges it on top of the resolved profile.

        Raises ``ValueError`` on circular inheritance.
        """
        # Walk the extends chain to validate (detect circular)
        visited: set[str] = set()
        current = profile
        chain: list[ProfilePolicy] = []
        while current is not None:
            pid = current.id
            if pid in visited:
                raise ValueError(
                    f"Circular inheritance detected: {pid} already in chain {[p.id for p in chain]}"
                )
            visited.add(pid)
            chain.append(current)
            parent_name = current.extends
            if parent_name is None:
                break
            current = presets.get(parent_name)
            if current is None:
                logger.warning("Parent profile %r not found in presets, stopping chain", parent_name)
                break

        # The leaf profile (first in chain) is the resolved profile.
        # For builtins this is the complete ProfilePolicy with all fields.
        resolved = deepcopy(profile)

        # Apply org overrides if provided
        if org_overrides:
            resolved = self._apply_org_overrides(resolved, org_overrides)

        return resolved

    def _merge_policy(self, base: ProfilePolicy, overlay: ProfilePolicy) -> ProfilePolicy:
        """Merge overlay fields on top of base.

        For builtin presets that are complete objects, this is equivalent to
        returning the overlay. For custom profiles with sparse overrides,
        each non-default field from overlay replaces the base value.
        Nested Pydantic models are merged field-by-field.
        """
        overlay_data = overlay.model_dump(exclude_defaults=True)
        return self._apply_org_overrides(base, overlay_data)

    def _apply_org_overrides(
        self, policy: ProfilePolicy, overrides: dict[str, Any]
    ) -> ProfilePolicy:
        """Apply a sparse override dict on top of a resolved policy.

        Only specified keys are changed. For nested models (scoring_weights,
        budgets, retrieval, etc.), the override is applied field-by-field
        via ``model_copy(update=...)``.

        Example override::

            {"scoring_weights": {"evidence_strength": 0.9},
             "budgets": {"max_prompt_tokens": 10000}}
        """
        policy_data = policy.model_dump()

        for key, value in overrides.items():
            if key not in ProfilePolicy.model_fields:
                logger.warning("Ignoring unknown override key: %s", key)
                continue

            current_field = getattr(policy, key)
            if isinstance(current_field, BaseModel) and isinstance(value, dict):
                # Nested Pydantic model — merge field-by-field
                nested_data = current_field.model_dump()
                for nk, nv in value.items():
                    if nk in type(current_field).model_fields:
                        nested_data[nk] = nv
                    else:
                        logger.warning("Ignoring unknown nested override key: %s.%s", key, nk)
                policy_data[key] = nested_data
            else:
                policy_data[key] = value

        return ProfilePolicy.model_validate(policy_data)
