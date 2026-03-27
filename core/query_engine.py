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
        self.df_mietmatrix = pd.DataFrame()

    def load(self) -> None:
        ist_soll_path = DATAOUT_DIR / "miete_ist_soll.csv"
        zahlung_path = DATAOUT_DIR / "tbl_zahlung_mit_mieter.csv"
        mietmatrix_path = DATAOUT_DIR / "mietmatrix.csv"

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

        if mietmatrix_path.exists():
            self.df_mietmatrix = pd.read_csv(mietmatrix_path)
            print(f"[query_engine] geladen: {mietmatrix_path}")
        else:
            print(f"[query_engine] fehlt: {mietmatrix_path}")

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


_ENGINE: QueryEngine | None = None


def _get_engine() -> QueryEngine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = QueryEngine()
        _ENGINE.load()
    return _ENGINE


def _filter_zeitraum(df: pd.DataFrame, zeitraum: str | None) -> pd.DataFrame:
    if zeitraum is None or df.empty:
        return df

    if "monat" in df.columns:
        return df[df["monat"].astype(str).str.startswith(str(zeitraum))]

    if "datum" in df.columns:
        work = df.copy()
        work["_datum"] = pd.to_datetime(work["datum"], errors="coerce", dayfirst=True, format="mixed")
        return work[work["_datum"].dt.year == int(zeitraum)].drop(columns=["_datum"])

    return df


def _filter_zahlungsart(df: pd.DataFrame, zahlungsart: str | None) -> pd.DataFrame:
    if zahlungsart is None or df.empty:
        return df

    konto_map = {
        "miete": {"8105", "8115"},
        "nebenkosten": {"8195"},
    }
    allowed = konto_map.get(zahlungsart, set())
    if not allowed:
        return df

    for col in ["konto", "zahlung_konto"]:
        if col in df.columns:
            return df[df[col].astype(str).isin(allowed)]

    return df


def _filter_mieter(df: pd.DataFrame, mieter: str | None) -> pd.DataFrame:
    if mieter is None or df.empty:
        return df

    needle = str(mieter).strip().lower()
    for col in ["mieter_name", "buchungstext", "mieterid"]:
        if col in df.columns:
            mask = df[col].astype(str).str.lower().str.contains(needle, na=False)
            filtered = df[mask]
            if not filtered.empty:
                return filtered
    return df.iloc[0:0]


def execute_query(dialog_state: dict):
    engine = _get_engine()
    intent = dialog_state.get("intent")

    if intent == "mieter_info":
        df = engine.df_mietmatrix.copy()
        mieter = dialog_state.get("mieter")

        if mieter and not df.empty:
            left = df.get("mieter_name_1", pd.Series(index=df.index, dtype=object))
            right = df.get("mieter_name_2", pd.Series(index=df.index, dtype=object))
            df = df[
                left.astype(str).str.contains(mieter, case=False, na=False)
                | right.astype(str).str.contains(mieter, case=False, na=False)
            ]

        required_cols = [
            "mieter_name_1",
            "mieter_name_2",
            "einheit",
            "wohnung",
            "objekt",
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = ""

        return df[required_cols]

    if intent == "sum_miete":
        df = engine.df_zahlung.copy()
        df = _filter_mieter(df, dialog_state.get("mieter"))
        df = _filter_zeitraum(df, dialog_state.get("zeitraum"))
        df = _filter_zahlungsart(df, dialog_state.get("zahlungsart"))

        if df.empty or "betrag" not in df.columns:
            return 0.0

        betraege = pd.to_numeric(df["betrag"], errors="coerce").fillna(0)
        return float(betraege.sum())

    if intent == "list_payments":
        df = engine.df_zahlung.copy()
        df = _filter_mieter(df, dialog_state.get("mieter"))
        df = _filter_zeitraum(df, dialog_state.get("zeitraum"))
        df = _filter_zahlungsart(df, dialog_state.get("zahlungsart"))
        return df

    if intent == "ist_soll":
        df = engine.df_ist_soll.copy()
        df = _filter_zeitraum(df, dialog_state.get("zeitraum"))
        df = _filter_zahlungsart(df, dialog_state.get("zahlungsart"))
        return df

    return "Unbekannte Anfrage."
