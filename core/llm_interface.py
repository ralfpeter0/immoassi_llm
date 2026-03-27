from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Protocol


class LLMClient(Protocol):
    def generate(self, prompt: str) -> str:
        """Generiert eine Antwort für einen Prompt."""


@dataclass
class EchoLLM:
    """Lokaler Fallback ohne externe API-Abhängigkeit.

    Der Fallback antwortet im JSON-Format und erzwingt Rückfragen,
    wenn kritische Informationen fehlen.
    """

    def _extract_date_range(self, text: str) -> str | None:
        patterns = [
            r"\b\d{1,2}\.\d{1,2}\.\d{2,4}\b",
            r"\b\d{4}-\d{2}-\d{2}\b",
            r"\b(januar|februar|märz|maerz|april|mai|juni|juli|august|september|oktober|november|dezember)\b",
            r"\b\d{4}\b",
            r"\b(von|bis|zwischen|seit|im|monat|quartal|jahr|zeitraum)\b",
        ]
        if any(re.search(pattern, text) for pattern in patterns):
            return "angegeben"
        return None

    def _extract_payment_type(self, text: str) -> str | None:
        has_miete = "miete" in text
        has_nebenkosten = any(term in text for term in ["nebenkosten", "betriebskosten", "bk"])

        if has_miete and not has_nebenkosten:
            return "miete"
        if has_nebenkosten and not has_miete:
            return "nebenkosten"
        if has_miete and has_nebenkosten:
            return "mehrdeutig"
        return None

    def _extract_tenant(self, prompt: str) -> str | None:
        match = re.search(r"\b(?:hat|von|für|fuer)\s+([A-ZÄÖÜ][\wÄÖÜäöüß-]+)", prompt)
        if match:
            return match.group(1)
        match = re.search(r"\b([A-ZÄÖÜ][\wÄÖÜäöüß-]+)\s+bezahlt", prompt)
        if match:
            return match.group(1)
        return None

    def generate(self, prompt: str) -> str:
        text = prompt.lower().strip()
        date_range = self._extract_date_range(text)
        payment_type = self._extract_payment_type(text)

        multiple_entities = any(token in text for token in [" und ", ",", " sowie "])
        ambiguous_request = not any(token in text for token in ["wie viel", "summe", "bezahlt", "zahlung"])

        need_clarification = False
        question = ""

        if date_range is None:
            need_clarification = True
            question = "Für welchen Zeitraum soll die Berechnung erfolgen?"

        if payment_type in (None, "mehrdeutig"):
            need_clarification = True
            if not question:
                question = "Meinst du Miete oder Nebenkosten?"

        if multiple_entities:
            need_clarification = True
            if not question:
                question = "Ich habe mehrere passende Einträge gefunden. Welchen genau meinst du?"

        if ambiguous_request:
            need_clarification = True
            if not question:
                question = "Kannst du deine Anfrage genauer formulieren?"

        intent = "unknown"
        if payment_type == "miete":
            intent = "sum_miete"
        elif payment_type == "nebenkosten":
            intent = "sum_nebenkosten"

        response = {
            "intent": intent,
            "mieter": self._extract_tenant(prompt),
            "need_clarification": need_clarification,
            "question": question,
        }
        return json.dumps(response, ensure_ascii=False)
