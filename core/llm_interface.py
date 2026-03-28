from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol


_ALLOWED_STEP_TYPES = {
    "filter_mieter",
    "filter_year",
    "filter_konto",
    "group_by_konto",
    "map_konto",
    "aggregate_sum",
    "output",
}

_KONTO_LABEL_TO_NUMBERS = {
    "miete": [8105, 8400],
    "nebenkosten": [8195],
}


class LLMClient(Protocol):
    def generate(self, prompt: str) -> str:
        """Generiert eine Antwort für einen Prompt."""


@dataclass
class EchoLLM:
    """Lokaler Fallback ohne externe API-Abhängigkeit."""

    def generate(self, prompt: str) -> str:
        return str(interpret_query(prompt))


def _extract_year(text: str) -> str | None:
    match = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if match:
        return match.group(1)
    return None


def _extract_mieter(raw_text: str) -> str | None:
    patterns = [
        r"\b(?:von|für|fuer|hat)\s+([A-Za-zÄÖÜäöüß-]+)",
        r"\b([A-Za-zÄÖÜäöüß-]+)\s+bezahlt",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_text, flags=re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            if candidate.lower() not in {"miete", "nebenkosten", "zahlung"}:
                return candidate.title()
    return None


def _extract_konten(text: str) -> list[str] | None:
    result: list[str] = []
    if "miete" in text:
        result.append("Miete")
    if "nebenkosten" in text:
        result.append("Nebenkosten")
    return result or None


def _konto_numbers(konten: list[str]) -> list[int]:
    numbers: list[int] = []
    for konto in konten:
        values = _KONTO_LABEL_TO_NUMBERS.get(konto.lower(), [])
        for value in values:
            if value not in numbers:
                numbers.append(value)
    return numbers


def _build_plan(filters: dict[str, Any]) -> list[dict[str, Any]]:
    konten = filters.get("konten") or []
    year = filters.get("zeitraum")
    mieter = filters.get("mieter")

    plan = [
        {"type": "filter_mieter", "value": mieter},
        {"type": "filter_year", "value": int(year)},
        {"type": "filter_konto", "value": _konto_numbers(konten)},
        {"type": "group_by_konto"},
        {"type": "map_konto"},
        {"type": "aggregate_sum"},
        {"type": "output", "format": "table"},
    ]
    return plan


def _is_valid_plan(plan: list[dict[str, Any]]) -> bool:
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
        if step_type == "filter_konto" and not isinstance(step.get("value"), list):
            return False
        if step_type == "output" and step.get("format") not in {"table", "value"}:
            return False

    return True


def interpret_query(user_text: str, context_filters: dict[str, Any] | None = None) -> dict[str, Any]:
    text = user_text.lower().strip()
    context_filters = context_filters or {}

    extracted_filters = {
        "mieter": _extract_mieter(user_text),
        "konten": _extract_konten(text),
        "zeitraum": _extract_year(text),
    }

    filters = {
        "mieter": extracted_filters["mieter"] or context_filters.get("mieter"),
        "konten": extracted_filters["konten"] or context_filters.get("konten"),
        "zeitraum": extracted_filters["zeitraum"] or context_filters.get("zeitraum"),
    }

    missing: list[str] = []
    for key in ["mieter", "konten", "zeitraum"]:
        if not filters.get(key):
            missing.append(key)

    understanding_parts = []
    if filters["mieter"]:
        understanding_parts.append(f"Zahlungen von {filters['mieter']}")
    else:
        understanding_parts.append("Zahlungen eines Mieters")

    if filters["konten"]:
        understanding_parts.append(f"für {', '.join(filters['konten'])}")
    if filters["zeitraum"]:
        understanding_parts.append(f"im Zeitraum {filters['zeitraum']}")

    clarification = None
    plan = None

    if missing:
        questions = {
            "mieter": "Für welchen Mieter soll ich suchen?",
            "konten": "Meinst du Miete oder Nebenkosten?",
            "zeitraum": "Für welchen Zeitraum soll ich suchen?",
        }
        clarification = " ".join(questions[field] for field in missing)
    else:
        plan = _build_plan(filters)
        if not _is_valid_plan(plan):
            return {
                "understanding": "Anfrage konnte nicht sicher interpretiert werden",
                "missing": ["plan"],
                "filters": filters,
                "plan": None,
                "clarification": "Bitte formuliere die Anfrage noch einmal.",
            }

    return {
        "understanding": "; ".join(understanding_parts),
        "missing": missing,
        "filters": filters,
        "plan": plan,
        "clarification": clarification,
    }
