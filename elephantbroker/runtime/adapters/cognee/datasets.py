"""Cognee dataset management adapter."""
from __future__ import annotations

import cognee

from elephantbroker.schemas.config import CogneeConfig


class DatasetManager:
    """Manages Cognee datasets scoped by organization.

    Dataset keys follow the convention ``{org_id}__{dataset_name}``
    to ensure cross-org isolation.
    """

    def __init__(self, config: CogneeConfig) -> None:
        self._default_dataset = config.default_dataset

    @staticmethod
    def _make_key(org_id: str, dataset_name: str) -> str:
        return f"{org_id}__{dataset_name}"

    async def ensure_dataset(self, org_id: str, dataset_name: str) -> str:
        """Create a dataset if it doesn't exist. Returns the dataset key."""
        key = self._make_key(org_id, dataset_name)
        await cognee.add("", dataset_name=key)
        return key

    async def delete_dataset(self, org_id: str, dataset_name: str) -> None:
        """Empty a dataset by its org-scoped key."""
        key = self._make_key(org_id, dataset_name)
        datasets = await cognee.datasets.list_datasets()
        for dataset in datasets:
            ds_name = getattr(dataset, "name", None)
            if ds_name == key:
                ds_id = getattr(dataset, "id", None)
                if ds_id is not None:
                    await cognee.datasets.empty_dataset(ds_id)
                return

    async def list_datasets(self, org_id: str) -> list[str]:
        """List dataset names belonging to an organization."""
        prefix = f"{org_id}__"
        datasets = await cognee.datasets.list_datasets()
        return [
            getattr(d, "name", str(d))
            for d in datasets
            if getattr(d, "name", str(d)).startswith(prefix)
        ]
