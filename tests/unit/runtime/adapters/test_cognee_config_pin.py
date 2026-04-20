"""TODO-5-006: verify _verify_cognee_pin warns on version drift."""
from __future__ import annotations

import logging
from importlib import metadata as _importlib_metadata
from unittest.mock import patch

from elephantbroker.runtime.adapters.cognee.config import (
    _SUPPORTED_COGNEE_VERSION,
    _verify_cognee_pin,
)


class TestCogneePinVerify:
    """The load-bearing ``cognee==0.5.3`` pin (TODO-5-006) protects the TD-50
    cascade's internal-API call sites. ``_verify_cognee_pin`` emits a WARNING
    when the installed version drifts so operators testing a bump see the
    signal on boot."""

    def test_matching_version_emits_no_warning(self, caplog):
        with patch.object(
            _importlib_metadata, "version", return_value=_SUPPORTED_COGNEE_VERSION,
        ):
            with caplog.at_level(
                logging.WARNING, logger="elephantbroker.adapters.cognee.config",
            ):
                _verify_cognee_pin()
        assert caplog.text == ""

    def test_mismatched_version_emits_warning_with_cascade_pointer(self, caplog):
        with patch.object(_importlib_metadata, "version", return_value="0.6.0"):
            with caplog.at_level(
                logging.WARNING, logger="elephantbroker.adapters.cognee.config",
            ):
                _verify_cognee_pin()
        assert "0.6.0" in caplog.text
        assert _SUPPORTED_COGNEE_VERSION in caplog.text
        assert "TD-50" in caplog.text
        assert "TECHNICAL-DEBT.md" in caplog.text

    def test_missing_package_metadata_emits_warning(self, caplog):
        def _raise_not_found(name):
            raise _importlib_metadata.PackageNotFoundError(name)

        with patch.object(_importlib_metadata, "version", side_effect=_raise_not_found):
            with caplog.at_level(
                logging.WARNING, logger="elephantbroker.adapters.cognee.config",
            ):
                _verify_cognee_pin()
        assert "metadata not found" in caplog.text
