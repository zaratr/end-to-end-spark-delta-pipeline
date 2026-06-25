"""Log parsers.

The pipeline is parser-agnostic: any object implementing
:meth:`LogParser.parse` can drive the bronze→silver transform. Two
implementations ship:

* :class:`RegexParser` — deterministic, dependency-free, used by default
  and in tests. Handles the bracketed log format the sample data uses.
* :class:`DspyParser` — wraps :mod:`src.dspy_parser` to extract fields
  via a local Ollama model (DSPy + Gemma). Imported lazily so the
  pipeline (and the test suite) run without ``dspy`` installed.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

# Matches: [2026-05-09 15:30:45] WARN (auth-service): login failed after 5000ms
_LOG_RE = re.compile(
    r"\[(?P<timestamp>[^\]]+)\]\s+"
    r"(?P<level>[A-Z]+)\s+"
    r"\((?P<service>[^)]+)\):\s*"
    r"(?P<message>.+)"
)


class LogParser(ABC):
    name: str = "base"

    @abstractmethod
    def parse(self, raw: str) -> Dict[str, Any]:
        """Return a dict with timestamp/level/service/message keys."""

    def parse_batch(self, raws):
        return [self.parse(r) for r in raws]


class RegexParser(LogParser):
    name = "regex"

    def parse(self, raw: str) -> Dict[str, Any]:
        match = _LOG_RE.search(raw or "")
        if not match:
            return {
                "timestamp": None,
                "level": "UNKNOWN",
                "service": "unknown",
                "message": (raw or "").strip(),
            }
        groups = match.groupdict()
        return {
            "timestamp": groups["timestamp"].strip(),
            "level": groups["level"].strip().upper(),
            "service": groups["service"].strip(),
            "message": groups["message"].strip(),
        }


class DspyParser(LogParser):
    name = "dspy-gemma"

    def __init__(self, model: str = "gemma") -> None:
        # Imported lazily so dspy/ollama are only required for the LLM path.
        from src.dspy_parser import parse_log

        self._parse_log = parse_log
        self._model = model

    def parse(self, raw: str) -> Dict[str, Any]:
        result = self._parse_log(raw, model=self._model)
        return {
            "timestamp": result.get("timestamp"),
            "level": (result.get("level") or "UNKNOWN").upper(),
            "service": result.get("service") or "unknown",
            "message": result.get("message") or (raw or "").strip(),
        }


def get_parser(name: Optional[str] = None, model: str = "gemma") -> LogParser:
    """Construct a parser by name.

    ``name`` in {None, "regex"} → RegexParser; {"dspy", "gemma", "llm"} →
    DspyParser (which requires the optional dspy dependency).
    """
    if name in {"dspy", "gemma", "llm"}:
        return DspyParser(model=model)
    return RegexParser()
