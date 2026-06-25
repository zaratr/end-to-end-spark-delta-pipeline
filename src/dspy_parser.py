"""DSPy module: structured log extraction via a local Ollama model.

This module is imported lazily by :class:`src.parsers.DspyParser` so the
rest of the pipeline (and the test suite) import cleanly without DSPy or
Ollama installed. ``parse_log`` configures the LM on first use.

The previous version of this file had a malformed docstring (``" " "``)
that broke module import; this is the corrected, working module.
"""

from __future__ import annotations

import json
import re

import dspy

_DEFAULT_MODEL = "gemma"
_LM_CONFIGURED = False
_CONFIGURED_MODEL: str | None = None


def _ensure_lm(model: str = _DEFAULT_MODEL) -> None:
    global _LM_CONFIGURED, _CONFIGURED_MODEL
    if _LM_CONFIGURED and _CONFIGURED_MODEL == model:
        return
    dspy.settings.configure(lm=dspy.OllamaLocal(model=model, max_tokens=500))
    _LM_CONFIGURED = True
    _CONFIGURED_MODEL = model


class LogExtraction(dspy.Signature):
    """Extract structured fields from a raw unstructured log line."""

    raw_log = dspy.InputField(desc="The raw unstructured text log")
    structured_output = dspy.OutputField(
        desc="A JSON object with keys: timestamp, level, service, message"
    )


_FENCE_RE = re.compile(r"^```(?:json)?|```$", re.IGNORECASE | re.MULTILINE)


def _coerce_json(raw: str, fallback_text: str) -> dict:
    """Best-effort parse of the model's JSON output."""
    text = _FENCE_RE.sub("", raw).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {
            "timestamp": None,
            "level": "UNKNOWN",
            "service": "unknown",
            "message": fallback_text.strip(),
        }


def parse_log(raw_text: str, model: str = _DEFAULT_MODEL) -> dict:
    """Parse a raw log line into {timestamp, level, service, message}.

    Requires Ollama running locally with the named model pulled.
    """
    _ensure_lm(model)
    extractor = dspy.Predict(LogExtraction)
    result = extractor(raw_log=raw_text)
    return _coerce_json(result.structured_output, raw_text)
