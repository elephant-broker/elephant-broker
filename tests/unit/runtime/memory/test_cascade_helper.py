"""TODO-5-314: dedicated unit tests for the shared Cognee-cascade helper.

These pin the status-code contract that both the memory facade and the
consolidation canonicalize stage rely on as thin wrappers. Prior to the
extraction, `facade._cascade_cognee_data` was covered by
`TestCascadeCogneeDataGuards` in `tests/unit/runtime/test_memory_facade.py`
and `canonicalize._cascade_superseded_data_id` carried an intentionally-
duplicated body with NO tests at all — a drift hazard that these tests,
together with the canonicalize regression companion, now close.
"""
from __future__ import annotations

import logging
import uuid
from unittest.mock import AsyncMock

import pytest
from httpx import Headers
from qdrant_client.http.exceptions import UnexpectedResponse


async def _call(monkeypatch, *, data_id, user=None, datasets=None,
                delete_side_effect=None, delete_return=None,
                dataset_rows=None, delete_data_row_side_effect=None):
    """Assemble monkeypatches against the helper's bound symbols and invoke it.

    Centralizes the ~5 patch paths so each test only sets the shape it
    cares about. The helper is called with a fresh logger and a fixed
    context string so assertions can pin log shape when needed.
    """
    from elephantbroker.runtime.memory import cascade_helper

    if user is None:
        user = type("U", (), {"id": uuid.uuid4()})()
    monkeypatch.setattr(
        cascade_helper, "get_default_user", AsyncMock(return_value=user),
    )
    monkeypatch.setattr(
        cascade_helper, "get_datasets_by_name",
        AsyncMock(return_value=datasets if datasets is not None else []),
    )

    mock_cognee = type("C", (), {})()
    mock_cognee.datasets = type("D", (), {})()
    if delete_side_effect is not None:
        mock_cognee.datasets.delete_data = AsyncMock(side_effect=delete_side_effect)
    else:
        mock_cognee.datasets.delete_data = AsyncMock(return_value=delete_return)
    monkeypatch.setattr(cascade_helper, "cognee", mock_cognee)

    monkeypatch.setattr(
        cascade_helper, "get_dataset_data",
        AsyncMock(return_value=dataset_rows if dataset_rows is not None else []),
    )
    monkeypatch.setattr(
        cascade_helper, "_delete_data_row",
        AsyncMock(side_effect=delete_data_row_side_effect)
        if delete_data_row_side_effect is not None
        else AsyncMock(return_value=None),
    )

    return await cascade_helper.cascade_cognee_data(
        data_id,
        dataset_name="test_ds",
        fact_id=uuid.uuid4(),
        context="unit",
        log=logging.getLogger("cascade_helper_test"),
    ), mock_cognee


class TestCascadeHelper:
    """TODO-5-314: status contract — ok, ok_idempotent, failed, skipped_*."""

    async def test_ok_happy_path(self, monkeypatch):
        """Dataset found, UUID parses, delete returns cleanly → "ok".
        Pins the default-green path both the facade and canonicalize rely
        on — the cascade reports success and the caller audits 'cognee_data=ok'.
        """
        fake_ds = type("D", (), {"id": uuid.uuid4()})()
        status, mock_cognee = await _call(
            monkeypatch,
            data_id=uuid.uuid4(),
            datasets=[fake_ds],
            delete_return={"deleted": True},
        )
        assert status == "ok"
        mock_cognee.datasets.delete_data.assert_awaited_once()

    async def test_ok_idempotent_on_qdrant_404(self, monkeypatch):
        """TD-Cognee-Qdrant-404 recovery: inner Qdrant delete raises 404
        (collection never existed because the Data row was added but
        never cognify()'d) → helper swallows, re-fetches the Data row,
        completes the Data↔Dataset unbind manually, and returns
        "ok_idempotent" (audit-distinguishable from clean "ok")."""
        fake_ds_id = uuid.uuid4()
        fake_ds = type("D", (), {"id": fake_ds_id})()
        data_id = uuid.uuid4()
        fake_row = type("Data", (), {"id": data_id, "__tablename__": "data"})()
        qdrant_404 = UnexpectedResponse(
            status_code=404,
            reason_phrase="Not Found",
            content=b'{"status":{"error":"Collection not found"}}',
            headers=Headers({}),
        )
        from elephantbroker.runtime.memory import cascade_helper
        status, _ = await _call(
            monkeypatch,
            data_id=data_id,
            datasets=[fake_ds],
            delete_side_effect=qdrant_404,
            dataset_rows=[fake_row],
        )
        assert status == "ok_idempotent"
        # Recovery path actually completed the Data↔Dataset unbind.
        cascade_helper._delete_data_row.assert_awaited_once_with(
            fake_row, fake_ds_id,
        )

    async def test_skipped_no_dataset(self, monkeypatch):
        """Dataset lookup returns [] → "skipped_no_dataset", Cognee delete
        NEVER attempted. TODO-5-309: pins that datasets[0].id is only
        indexed after the empty-list guard, so the cascade is safe."""
        status, mock_cognee = await _call(
            monkeypatch,
            data_id=uuid.uuid4(),
            datasets=[],
        )
        assert status == "skipped_no_dataset"
        mock_cognee.datasets.delete_data.assert_not_called()

    async def test_skipped_bad_data_id(self, monkeypatch):
        """TODO-5-109: stored cognee_data_id is non-UUID-parseable (legacy
        row from pre-TODO-5-003 coercion, or corrupted value) →
        "skipped_bad_data_id", no Cognee call attempted. Distinct from
        "failed" so operators can tell bad-data-at-rest from a genuine
        Cognee-side failure."""
        fake_ds = type("D", (), {"id": uuid.uuid4()})()
        status, mock_cognee = await _call(
            monkeypatch,
            data_id="this-is-not-a-uuid",
            datasets=[fake_ds],
        )
        assert status == "skipped_bad_data_id"
        mock_cognee.datasets.delete_data.assert_not_called()

    async def test_failed_on_500(self, monkeypatch):
        """Non-404 UnexpectedResponse (e.g. Qdrant 5xx) → "failed", recovery
        branch NOT taken. Guarantees the TD-Cognee-Qdrant-404 workaround
        does not silently mask genuine Qdrant failures."""
        fake_ds = type("D", (), {"id": uuid.uuid4()})()
        qdrant_503 = UnexpectedResponse(
            status_code=503,
            reason_phrase="Service Unavailable",
            content=b'{"error":"qdrant down"}',
            headers=Headers({}),
        )
        from elephantbroker.runtime.memory import cascade_helper
        status, _ = await _call(
            monkeypatch,
            data_id=uuid.uuid4(),
            datasets=[fake_ds],
            delete_side_effect=qdrant_503,
        )
        assert status == "failed"
        # Recovery helpers must NOT be called on non-404.
        cascade_helper.get_dataset_data.assert_not_awaited()
        cascade_helper._delete_data_row.assert_not_awaited()
