"""Shared utilities for LLM response handling."""
from __future__ import annotations


def strip_markdown_fences(content: str) -> str:
    """Strip leading/trailing markdown code fences from LLM JSON output.

    Empirical curl testing against the staging LiteLLM proxy
    (/tmp/observer-cheap-client-curl-verify.md) showed that all three working
    Gemini models wrap `message.content` in ```json\\n...\\n``` fences even
    when `response_format={"type": "json_object"}` is set. json.loads() fails
    on fenced input, so we strip the fences before parsing.

    Handles:
    - ```json\\n{...}\\n``` (language tag + newlines)
    - ```\\n{...}\\n```     (no language tag)
    - {...}                  (no fences — backward compat)
    - Leading/trailing whitespace.

    Idempotent and safe on fence-free content: for unfenced input the only
    transformation is ``.strip()`` on leading/trailing whitespace, which
    json.loads tolerates anyway.
    """
    content = content.strip()
    if content.startswith("```"):
        # Drop the opening fence line (with or without language tag)
        if "\n" in content:
            content = content.split("\n", 1)[1]
        # Drop the closing fence if present
        if content.endswith("```"):
            content = content.rsplit("```", 1)[0]
    return content.strip()
