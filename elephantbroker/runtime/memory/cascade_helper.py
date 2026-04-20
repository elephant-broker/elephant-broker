"""Shared TD-50 Cognee-cascade helper used by the memory facade and the
consolidation canonicalize stage.

Extracted from `facade._cascade_cognee_data` (TODO-5-314) so both call
sites share one implementation, including the Cfx TD-Cognee-Qdrant-404
recovery branch. Prior to this module canonicalize carried an
"intentionally duplicated" copy without the recovery branch — a drift
hazard, since a cluster canonicalized against a never-cognify()'d member
would 404 in Qdrant mid-cascade and leave the Data↔Dataset association
orphaned.

PIN INVARIANT (TODO-5-006): This cascade calls Cognee internal paths
that are NOT part of Cognee's public API stability contract:
  - `cognee.modules.users.methods.get_default_user`
  - `cognee.modules.data.methods.get_datasets_by_name`
  - `cognee.datasets.delete_data(mode="soft", delete_dataset_if_empty=False)`
  - `cognee.modules.data.methods.get_dataset_data` (TD-Cognee-Qdrant-404
    recovery path — re-fetches Data rows to complete metadata cleanup
    when the Qdrant step throws UnexpectedResponse mid-cascade)
  - `cognee.modules.data.methods.delete_data` (same recovery path —
    removes the Data ↔ Dataset association that Cognee's outer
    delete_data would have finalized on its last line)
The `cognee==0.5.3` pin in pyproject.toml is load-bearing — bumping
Cognee without re-verifying each call site (signature, kwargs, return
shape) will silently break the TD-50 cascade path.
See local/TECHNICAL-DEBT.md §"Load-bearing dependency pins" for the
full impact surface + bump protocol, and §"TD-Cognee-Qdrant-404" for
the upstream bug that necessitates the recovery branch.

BULK-DELETE CAVEAT (TODO-5-312): This cascade is per-fact by design;
`get_default_user` + `get_datasets_by_name` are re-fetched on every
call. No bulk-delete caller exists today. When one is added (e.g. a
GDPR bulk-purge endpoint), implement a batch variant that caches the
two lookups once per batch rather than looping this function N times.
"""
from __future__ import annotations

import logging
import uuid
from typing import Literal

import cognee
from cognee.modules.data.methods import (
    delete_data as _delete_data_row,
    get_dataset_data,
    get_datasets_by_name,
)
from cognee.modules.users.methods import get_default_user
from qdrant_client.http.exceptions import UnexpectedResponse

CascadeStatus = Literal[
    "ok",
    "ok_idempotent",
    "failed",
    "skipped_no_dataset",
    "skipped_bad_data_id",
    "skipped_no_data_id",
]


async def cascade_cognee_data(
    cognee_data_id: uuid.UUID | str | None,
    *,
    dataset_name: str,
    fact_id: uuid.UUID,
    context: str,
    log: logging.Logger | logging.LoggerAdapter,
) -> CascadeStatus:
    """Remove the Cognee-owned document for a single data_id.

    Returns a status string so callers can include the outcome in
    downstream audit events (GDPR_DELETE cascade_status):
      "ok"                   — Cognee cleanup completed.
      "ok_idempotent"        — TD-Cognee-Qdrant-404 recovery: the inner
                               Qdrant delete raised UnexpectedResponse
                               404 (collection/point not present — data
                               was added but never cognify()'d, so the
                               derived vector collection does not
                               exist). We manually complete the dataset
                               metadata removal that Cognee's outer
                               delete_data would have finalized after
                               the Qdrant step. Audit-distinguishable
                               from "ok" so operators can tell a clean
                               delete from an upstream-bug workaround.
      "skipped_no_dataset"   — Dataset lookup returned nothing (the
                               EB-side delete still proceeds; there
                               is nothing Cognee-side left to clean).
                               TODO-5-309: datasets[0].id is indexed
                               only after the `if not datasets` guard,
                               so the cascade is safe against empty-
                               list IndexError.
      "skipped_bad_data_id"  — TODO-5-109: stored cognee_data_id is
                               not UUID-parseable (legacy row from
                               before the TODO-5-003 capture-time
                               coercion was added, or a corrupted
                               value). Distinguished from "failed"
                               because no Cognee call is even
                               attempted — nothing to retry at the
                               Cognee layer.
      "skipped_no_data_id"   — TODO-5-700: fact carries no
                               cognee_data_id (None/missing) — the
                               capture step on store/update never ran
                               or returned a shape we could not parse.
                               Stamped by the facade caller before
                               dispatch (no Cognee call attempted) and
                               surfaced here on the alias so the audit
                               cascade_status is statically typeable.
      "failed"               — Cognee raised; partial cleanup, the
                               step is reported via DEGRADED_OPERATION
                               trace + metric by the caller.
    """
    try:
        user = await get_default_user()
        datasets = await get_datasets_by_name([dataset_name], user.id)
        if not datasets:
            log.warning(
                "TD-50 cascade skipped (%s): dataset %s not found for fact %s",
                context, dataset_name, fact_id,
            )
            return "skipped_no_dataset"
        try:
            data_id_uuid = (
                cognee_data_id if isinstance(cognee_data_id, uuid.UUID)
                else uuid.UUID(str(cognee_data_id))
            )
        except (ValueError, TypeError) as exc:
            log.warning(
                "TD-50 cascade skipped (%s): cognee_data_id=%r on fact %s "
                "is not UUID-parseable (%s: %s) — no Cognee call attempted",
                context, cognee_data_id, fact_id, type(exc).__name__, exc,
            )
            return "skipped_bad_data_id"
        try:
            result = await cognee.datasets.delete_data(
                dataset_id=datasets[0].id,
                data_id=data_id_uuid,
                mode="soft",
                delete_dataset_if_empty=False,
            )
        except UnexpectedResponse as exc:
            # TD-Cognee-Qdrant-404 upstream-bug workaround:
            # Cognee 0.5.3's delete_data → delete_data_nodes_and_edges
            # → delete_from_graph_and_vector calls
            # QdrantAdapter.delete_data_points without a has_collection
            # guard. When a Data row exists (cognee.add()) but was never
            # cognify()'d, the derived vector collection doesn't exist →
            # Qdrant returns 404 → UnexpectedResponse propagates out,
            # aborting Cognee's outer delete_data before its last line
            # (`await delete_data(data, dataset_id)`) removes the
            # Data ↔ Dataset association. Left as-is, the OLD data_id
            # stays in the dataset and subsequent reads still see it.
            # Recovery: on 404 we treat the vector side as already-clean
            # (idempotent) and complete the metadata removal manually.
            # Non-404 responses (5xx, auth, malformed) fall through to
            # the broad except and report "failed" — we don't silently
            # swallow a genuinely broken Cognee.
            if exc.status_code == 404:
                try:
                    rows = [
                        d for d in await get_dataset_data(datasets[0].id)
                        if d.id == data_id_uuid
                    ]
                    if rows:
                        await _delete_data_row(rows[0], datasets[0].id)
                    log.info(
                        "TD-50 cascade ok_idempotent (%s): fact_id=%s "
                        "data_id=%s — Qdrant 404 on vector cleanup, "
                        "metadata removed manually (%r)",
                        context, fact_id, cognee_data_id, exc,
                    )
                    return "ok_idempotent"
                except Exception as inner:
                    log.warning(
                        "TD-50 cascade failed after UnexpectedResponse "
                        "recovery (%s, fact_id=%s, data_id=%s): "
                        "outer=%r / inner=%r",
                        context, fact_id, cognee_data_id, exc, inner,
                    )
                    return "failed"
            raise
        log.info(
            "TD-50 cascade complete (%s): fact_id=%s data_id=%s cognee_result=%r",
            context, fact_id, cognee_data_id, result,
        )
        return "ok"
    except Exception as exc:
        log.warning(
            "TD-50 cascade failed (%s, fact_id=%s, data_id=%s): %r",
            context, fact_id, cognee_data_id, exc,
        )
        return "failed"
