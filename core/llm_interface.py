from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class LLMClient(Protocol):
    def generate(self, prompt: str) -> str:
        """Generiert eine Antwort für einen Prompt."""


@dataclass
class EchoLLM:
    """Lokaler Fallback ohne externe API-Abhängigkeit."""

    prefix: str = "[LLM-Fallback]"

    def generate(self, prompt: str) -> str:
        return f"{self.prefix} {prompt}"
