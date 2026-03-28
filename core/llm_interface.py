from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Protocol


class ChatLLMClient(Protocol):
    def generate(self, messages: list[dict[str, str]]) -> str:
        """Generiert eine Chat-Antwort als String."""


@dataclass
class HybridLLM:
    """Regelbasierter Fallback mit optionalem LLM-Client."""

    client: ChatLLMClient | None = None

    def respond(
        self,
        user_input: str,
        available_tools: list[dict[str, Any]],
        dataset_description: dict[str, Any],
        history: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        history = history or []

        if self.client:
            messages = self._build_messages(user_input, available_tools, dataset_description, history)
            raw = self.client.generate(messages)
            parsed = _try_parse_tool_call(raw)
            if parsed:
                return parsed
            return {"type": "answer", "content": raw}

        return _rule_based_response(user_input=user_input, history=history)

    def explain_with_data(
        self,
        user_input: str,
        data_markdown: str,
        history: list[dict[str, str]] | None = None,
    ) -> str:
        history = history or []

        if self.client:
            messages = history + [
                {"role": "system", "content": "Erkläre die Daten kurz, klar und freundlich. Nutze Deutsch."},
                {"role": "user", "content": f"Nutzerfrage: {user_input}\n\nDaten:\n{data_markdown}"},
            ]
            return self.client.generate(messages)

        return (
            "Ich habe die Zahlungen als Tabelle aufbereitet. "
            "Wenn du möchtest, kann ich zusätzlich eine Gesamtsumme oder einen Monatsvergleich zeigen."
        )

    @staticmethod
    def _build_messages(
        user_input: str,
        available_tools: list[dict[str, Any]],
        dataset_description: dict[str, Any],
        history: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        system_prompt = (
            "Du bist ein hilfreicher Assistent für Immobilien-Daten. "
            "Antworte natürlich. Frage nach, wenn Angaben fehlen. "
            "Wenn Datenabfrage nötig ist, gib NUR JSON im Format "
            '{"tool":"query_data","arguments":{...}} zurück.'
        )

        tool_text = json.dumps(available_tools, ensure_ascii=False)
        dataset_text = json.dumps(dataset_description, ensure_ascii=False)

        return [
            {"role": "system", "content": system_prompt},
            {"role": "system", "content": f"Verfügbare Tools: {tool_text}"},
            {"role": "system", "content": f"Dataset: {dataset_text}"},
            *history,
            {"role": "user", "content": user_input},
        ]


def _try_parse_tool_call(raw: str) -> dict[str, Any] | None:
    raw = raw.strip()
    if not raw:
        return None

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(parsed, dict):
        return None
    if parsed.get("tool") != "query_data":
        return None

    arguments = parsed.get("arguments", {})
    if not isinstance(arguments, dict):
        return None

    return {"type": "tool_call", "tool": "query_data", "arguments": arguments}


def _extract_year(text: str) -> int | None:
    match = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    return int(match.group(1)) if match else None


def _extract_mieter(text: str) -> str | None:
    patterns = [
        r"was hat\s+([A-Za-zÄÖÜäöüß-]+)\s+bezahlt",
        r"zahlungen?\s+von\s+([A-Za-zÄÖÜäöüß-]+)",
        r"([A-Za-zÄÖÜäöüß-]+)\s+bezahlt",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).title()
    return None


def _extract_konten(text: str) -> list[str] | None:
    konten: list[str] = []
    lower = text.lower()
    if "miete" in lower:
        konten.append("Miete")
    if "nebenkosten" in lower:
        konten.append("Nebenkosten")
    return konten or None


def _rule_based_response(user_input: str, history: list[dict[str, str]]) -> dict[str, Any]:
    text = user_input.strip()
    lower = text.lower()

    if "wohnt" in lower:
        return {
            "type": "answer",
            "content": "Fragen zu 'wohnt' laufen später über die Mietmatrix. Aktuell kann ich Zahlungen ('bezahlt') auswerten.",
        }

    mieter = _extract_mieter(text)
    jahr = _extract_year(lower)
    konten = _extract_konten(lower)

    if any(word in lower for word in ["bezahlt", "zahlung", "zahlungen", "miete", "nebenkosten"]):
        if not mieter:
            return {
                "type": "answer",
                "content": "Für welchen Mieter soll ich suchen?",
            }
        if not konten or not jahr:
            return {
                "type": "answer",
                "content": "Meinst du Miete oder Nebenkosten und für welches Jahr?",
            }

        return {
            "type": "tool_call",
            "tool": "query_data",
            "arguments": {
                "mieter": mieter,
                "jahr": jahr,
                "konten": konten,
            },
        }

    if "tabelle" in lower:
        return {
            "type": "answer",
            "content": "Gerne. Stelle bitte zuerst eine konkrete Zahlungsfrage (Mieter, Kontoart, Jahr), dann zeige ich dir die Tabelle.",
        }

    return {
        "type": "answer",
        "content": "Ich helfe dir gern bei Zahlungsfragen. Beispiel: 'Was hat Flury 2025 an Miete bezahlt?'",
    }
