from __future__ import annotations

from pathlib import Path
import json

import pandas as pd

from core.llm_interface import EchoLLM, LLMClient

BASE_DIR = Path(__file__).resolve().parent.parent
DATAOUT_DIR = BASE_DIR / "dataout"


class QueryEngine:
    """Einfache Query-Engine als Vorbereitung für LLM-Abfragen."""

    def __init__(self, llm: LLMClient | None = None) -> None:
        self.llm = llm or EchoLLM()
        self.df_ist_soll = pd.DataFrame()
        self.df_zahlung = pd.DataFrame()

    def load(self) -> None:
        ist_soll_path = DATAOUT_DIR / "miete_ist_soll.csv"
        zahlung_path = DATAOUT_DIR / "tbl_zahlung_mit_mieter.csv"

        if ist_soll_path.exists():
            self.df_ist_soll = pd.read_csv(ist_soll_path)
            print(f"[query_engine] geladen: {ist_soll_path}")
        else:
            print(f"[query_engine] fehlt: {ist_soll_path}")

        if zahlung_path.exists():
            self.df_zahlung = pd.read_csv(zahlung_path)
            print(f"[query_engine] geladen: {zahlung_path}")
        else:
            print(f"[query_engine] fehlt: {zahlung_path}")

    def ask(self, question: str) -> str:
        llm_response = self.llm.generate(question)

        try:
            payload = json.loads(llm_response)
        except json.JSONDecodeError:
            payload = {
                "need_clarification": False,
                "question": "",
            }

        if payload.get("need_clarification"):
            return str(payload.get("question") or "Bitte präzisiere deine Anfrage.")

        q = question.lower().strip()

        if "offen" in q and not self.df_ist_soll.empty and "diff" in self.df_ist_soll.columns:
            offene = self.df_ist_soll[self.df_ist_soll["diff"] < 0]
            return f"Offene Positionen: {len(offene)}"

        if "zahlung" in q and not self.df_zahlung.empty:
            return f"Geladene Zahlungen: {len(self.df_zahlung)}"

        return llm_response
