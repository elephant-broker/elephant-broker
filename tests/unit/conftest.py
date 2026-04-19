"""Shared fixtures for unit tests."""
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_cognee():
    """Mock cognee module for unit tests.

    ``.add`` returns a shape compatible with the real cognee SDK
    (``data_ingestion_info=[{"data_id": UUID}]``) so the facade's
    ``cognee_data_id`` capture path runs the SUCCESS branch by default.
    Tests that want to exercise the capture-failure branch can override
    with ``mock_cognee.add = AsyncMock(return_value=None)`` or a
    malformed object.
    """
    mock = MagicMock()
    mock.add = AsyncMock(return_value=SimpleNamespace(
        data_ingestion_info=[{"data_id": uuid.uuid4()}],
    ))
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
