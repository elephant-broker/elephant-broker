"""Unit tests for DatasetManager with mocked Cognee datasets API."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from elephantbroker.runtime.adapters.cognee.datasets import DatasetManager
from elephantbroker.schemas.config import CogneeConfig


def _make_manager() -> DatasetManager:
    return DatasetManager(CogneeConfig())


class TestDatasetManager:
    def test_make_key(self):
        assert DatasetManager._make_key("org1", "facts") == "org1__facts"

    async def test_ensure_dataset_calls_cognee_add(self):
        mgr = _make_manager()
        with patch("elephantbroker.runtime.adapters.cognee.datasets.cognee") as mock_cognee:
            mock_cognee.add = AsyncMock()
            key = await mgr.ensure_dataset("org1", "test")
            assert key == "org1__test"
            mock_cognee.add.assert_awaited_once_with("", dataset_name="org1__test")

    async def test_list_datasets_filters_by_org(self):
        mgr = _make_manager()
        ds1 = MagicMock(name="org1__facts")
        ds1.name = "org1__facts"
        ds2 = MagicMock(name="org2__goals")
        ds2.name = "org2__goals"

        with patch("elephantbroker.runtime.adapters.cognee.datasets.cognee") as mock_cognee:
            mock_cognee.datasets.list_datasets = AsyncMock(return_value=[ds1, ds2])
            result = await mgr.list_datasets("org1")
            assert result == ["org1__facts"]

    async def test_delete_dataset_calls_empty(self):
        mgr = _make_manager()
        import uuid
        ds_obj = MagicMock()
        ds_obj.name = "org1__del"
        ds_obj.id = uuid.uuid4()

        with patch("elephantbroker.runtime.adapters.cognee.datasets.cognee") as mock_cognee:
            mock_cognee.datasets.list_datasets = AsyncMock(return_value=[ds_obj])
            mock_cognee.datasets.empty_dataset = AsyncMock()
            await mgr.delete_dataset("org1", "del")
            mock_cognee.datasets.empty_dataset.assert_awaited_once_with(ds_obj.id)

    async def test_delete_nonexistent_dataset_is_noop(self):
        mgr = _make_manager()
        with patch("elephantbroker.runtime.adapters.cognee.datasets.cognee") as mock_cognee:
            mock_cognee.datasets.list_datasets = AsyncMock(return_value=[])
            mock_cognee.datasets.empty_dataset = AsyncMock()
            await mgr.delete_dataset("org1", "ghost")
            mock_cognee.datasets.empty_dataset.assert_not_awaited()
