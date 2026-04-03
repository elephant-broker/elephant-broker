"""Integration tests for DatasetManager against live Cognee."""
from __future__ import annotations

import pytest

from elephantbroker.runtime.adapters.cognee.datasets import DatasetManager


@pytest.fixture
def dataset_manager(cognee_config):
    return DatasetManager(cognee_config)


@pytest.mark.pipeline  # Uses cognee.add() internally — needs isolated event loop
@pytest.mark.asyncio(loop_scope="session")
class TestDatasetManagerIntegration:
    async def test_ensure_and_list_dataset(self, dataset_manager):
        key = await dataset_manager.ensure_dataset("org1", "testds")
        assert key == "org1__testds"
        datasets = await dataset_manager.list_datasets("org1")
        assert "org1__testds" in datasets

    async def test_delete_dataset(self, dataset_manager):
        await dataset_manager.ensure_dataset("org2", "tobedeleted")
        await dataset_manager.delete_dataset("org2", "tobedeleted")

    async def test_cross_org_isolation(self, dataset_manager):
        await dataset_manager.ensure_dataset("orgA", "shared")
        await dataset_manager.ensure_dataset("orgB", "shared")
        a_datasets = await dataset_manager.list_datasets("orgA")
        b_datasets = await dataset_manager.list_datasets("orgB")
        assert all(d.startswith("orgA__") for d in a_datasets)
        assert all(d.startswith("orgB__") for d in b_datasets)
