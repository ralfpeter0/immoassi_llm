from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Protocol


_ALLOWED_STEP_TYPES = {
    "filter_mieter",
    "filter_year",
    "group_by_konto",
    "map_konto",
    "aggregate_sum",
    "output",
}


class LLMClient(Protocol):
    def generate(self, prompt: str) -> str:
        """Generiert eine Antwort für einen Prompt."""


@dataclass
class EchoLLM:
    """Lokaler Fallback ohne externe API-Abhängigkeit.

    Gibt immer ein JSON-Objekt mit `plan` zurück.
    """

    def generate(self, prompt: str) -> str:
        response = interpret_query(prompt)
        return json.dumps(response, ensure_ascii=False)


def _extract_year(text: str) -> int | None:
    match = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if match:
        return int(match.group(1))
    return None


def _extract_mieter(raw_text: str) -> str | None:
    patterns = [
        r"\b(?:von|für|fuer)\s+([A-Za-zÄÖÜäöüß-]+)",
        r"\b([A-Za-zÄÖÜäöüß-]+)\s+bezahlt",
        r"\b([A-Za-zÄÖÜäöüß-]+)\s+202\d\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip().title()
    return None


def _extract_output_format(text: str) -> str:
    if any(token in text for token in ["zeige", "liste", "und", "tabelle"]):
        return "table"
    return "value"


def _build_plan(user_text: str) -> list[dict]:
    text = user_text.lower().strip()
    plan: list[dict] = []

    mieter = _extract_mieter(user_text)
    year = _extract_year(text)

    if mieter:
        plan.append({"type": "filter_mieter", "value": mieter})
    if year is not None:
        plan.append({"type": "filter_year", "value": year})

    plan.extend(
        [
            {"type": "group_by_konto"},
            {"type": "map_konto"},
            {"type": "aggregate_sum"},
            {"type": "output", "format": _extract_output_format(text)},
        ]
    )
    return plan


def _is_valid_plan(plan: list[dict]) -> bool:
    if not isinstance(plan, list) or not plan:
        return False

    for step in plan:
        if not isinstance(step, dict):
            return False
        step_type = step.get("type")
        if step_type not in _ALLOWED_STEP_TYPES:
            return False

        if step_type == "filter_mieter" and not isinstance(step.get("value"), str):
            return False
        if step_type == "filter_year" and not isinstance(step.get("value"), int):
            return False
        if step_type == "output" and step.get("format") not in {"table", "value"}:
            return False

    return True


def interpret_query(user_text: str) -> dict:
    """Interpretation einer Benutzeranfrage als strikt strukturierten Plan."""
    plan = _build_plan(user_text)
    if not _is_valid_plan(plan):
        return {"plan": []}
    return {"plan": plan}
