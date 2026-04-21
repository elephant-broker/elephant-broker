"""Tests for elephantbroker.runtime.context._utils."""
from __future__ import annotations

from elephantbroker.runtime.context._utils import _extract_key_phrases


class TestExtractKeyPhrases:
    """T-1: `_extract_key_phrases` strips edge punctuation so paraphrased
    responses (which typically drop the original punctuation) can still
    substring-match the generated phrases. Hyphens MUST be preserved so
    compound tokens like "time-series" survive as single semantic units.
    """

    def test_strips_trailing_punctuation(self):
        """Commas and periods on edges are stripped; inner text untouched."""
        phrases = _extract_key_phrases("TimescaleDB, the extension.")
        assert "timescaledb extension" in phrases, (
            f"expected 'timescaledb extension' in phrases, got {phrases}"
        )
        # The unstripped forms must NOT appear — that was the T-1 bug.
        assert "timescaledb," not in phrases
        assert "extension." not in phrases

    def test_preserves_hyphens(self):
        """Hyphens survive — `time-series` is a single semantic token."""
        phrases = _extract_key_phrases("time-series database systems")
        assert "time-series database" in phrases, (
            f"hyphen-preserved phrase missing; got {phrases}"
        )

    def test_handles_backticks_and_brackets(self):
        """Backticks and bracket chars stripped from token edges.

        The key invariant is no backtick/bracket appears in ANY output
        phrase — regardless of stop-word interactions.
        """
        phrases = _extract_key_phrases("use `TimescaleDB` in [production] code")
        # Stop-word "in" is filtered, so the adjacency becomes
        # use → timescaledb → production → code.
        assert "timescaledb production" in phrases or "timescaledb code" in phrases
        for p in phrases:
            assert "`" not in p, f"backtick leaked into phrase: {p!r}"
            assert "[" not in p, f"[ leaked into phrase: {p!r}"
            assert "]" not in p, f"] leaked into phrase: {p!r}"

    def test_empty_after_strip_skipped(self):
        """A token that becomes '' after strip (e.g. '...') is filtered out,
        not turned into an empty-string phrase. The `and s` guard in the
        comprehension handles this short-circuit."""
        phrases = _extract_key_phrases("... real content here")
        # Non-empty output (the real words still form phrases).
        assert phrases, "expected non-empty phrases list"
        for p in phrases:
            assert p.strip(), f"got empty/whitespace-only phrase: {p!r}"
