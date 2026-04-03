"""Shared fixtures for unit tests."""
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_cognee():
    """Mock cognee module for unit tests."""
    mock = MagicMock()
    mock.add = AsyncMock(return_value=None)
    mock.search = AsyncMock(return_value=[])
    return mock


@pytest.fixture
def mock_add_data_points():
    """Mock add_data_points — records calls and returns input unchanged."""
    calls = []

    async def fake(data_points, context=None, custom_edges=None, embed_triplets=False):
        dp_list = list(data_points)
        calls.append({"data_points": dp_list, "context": context})
        return dp_list

    fake.calls = calls
    return fake
