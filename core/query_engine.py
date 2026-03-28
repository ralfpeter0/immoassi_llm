from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATAOUT_DIR = BASE_DIR / "dataout"
ZAHLUNG_PATH = DATAOUT_DIR / "tbl_zahlung_mit_mieter.csv"

ALLOWED_STEP_TYPES = {
    "filter_mieter",
    "filter_year",
    "filter_konto",
    "group_by_konto",
    "map_konto",
    "aggregate_sum",
    "output",
}


class QueryEngine:
    """Einfache Query-Engine zum Laden von Zahlungsdaten."""

    def __init__(self) -> None:
        self.df_zahlung = pd.DataFrame()

    def load(self) -> None:
        if ZAHLUNG_PATH.exists():
            self.df_zahlung = pd.read_csv(ZAHLUNG_PATH)
            print(f"[query_engine] geladen: {ZAHLUNG_PATH}")
        else:
            print(f"[query_engine] fehlt: {ZAHLUNG_PATH}")


def _validate_plan(plan) -> bool:
    if not isinstance(plan, list) or not plan:
        return False

    for step in plan:
        if not isinstance(step, dict):
            return False

        step_type = step.get("type")
        if step_type not in ALLOWED_STEP_TYPES:
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


def execute_plan(plan):
    if not _validate_plan(plan):
        return "Fehler im Plan"

    if not ZAHLUNG_PATH.exists():
        return "Fehler im Plan"

    df = pd.read_csv(ZAHLUNG_PATH)

    for step in plan:
        step_type = step["type"]

        if step_type == "filter_mieter":
            df = df[df["buchungstext"].astype(str).str.contains(step["value"], case=False, na=False)]

        elif step_type == "filter_year":
            df["datum"] = pd.to_datetime(df["datum"], errors="coerce", dayfirst=True, format="mixed")
            df = df[df["datum"].dt.year == int(step["value"])]

        elif step_type == "filter_konto":
            df = df[df["zahlung_konto"].isin(step["value"])]

        elif step_type == "group_by_konto":
            df = df.groupby("zahlung_konto")["betrag"].sum().reset_index()

        elif step_type == "map_konto":
            mapping = {
                8105: "Miete",
                8400: "Miete",
                8195: "Nebenkosten",
            }
            df["typ"] = df["zahlung_konto"].map(mapping)

        elif step_type == "aggregate_sum":
            df = df.groupby("typ")["betrag"].sum().reset_index()

        elif step_type == "output":
            if step["format"] == "table":
                return df
            if step["format"] == "value":
                if "betrag" in df.columns:
                    return pd.to_numeric(df["betrag"], errors="coerce").fillna(0).sum()
                return 0.0

    return df
