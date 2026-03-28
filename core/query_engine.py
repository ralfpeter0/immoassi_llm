from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATAOUT_DIR = BASE_DIR / "dataout"
ZAHLUNG_PATH = DATAOUT_DIR / "tbl_zahlung_mit_mieter.csv"

ALLOWED_STEP_TYPES = {
    "filter_mieter",
    "filter_year",
    "group_by_konto",
    "map_konto",
    "aggregate_sum",
    "output",
}

KONTO_TYP_MAPPING = {
    "8105": "Miete",
    "8400": "Miete",
    "8195": "Nebenkosten",
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


def _load_zahlung_data() -> pd.DataFrame:
    if not ZAHLUNG_PATH.exists():
        return pd.DataFrame()
    return pd.read_csv(ZAHLUNG_PATH)


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
        if step_type == "output" and step.get("format") not in {"table", "value"}:
            return False

    return True


def execute_plan(plan):
    if not _validate_plan(plan):
        return "Fehler im Plan"

    df = _load_zahlung_data()
    if df.empty:
        return "Fehler im Plan"

    grouped_df = pd.DataFrame()
    aggregated_df = pd.DataFrame()

    for step in plan:
        step_type = step["type"]

        if step_type == "filter_mieter":
            mieter = step["value"]
            df = df[df["buchungstext"].astype(str).str.contains(mieter, case=False, na=False)]

        elif step_type == "filter_year":
            year = step["value"]
            work = df.copy()
            work["_datum"] = pd.to_datetime(work["datum"], errors="coerce", dayfirst=True, format="mixed")
            df = work[work["_datum"].dt.year == year].drop(columns=["_datum"])

        elif step_type == "group_by_konto":
            grouped_df = df.groupby("zahlung_konto", as_index=False)["betrag"].sum()

        elif step_type == "map_konto":
            source = grouped_df if not grouped_df.empty else df
            work = source.copy()
            work["typ"] = work["zahlung_konto"].astype(str).map(KONTO_TYP_MAPPING)
            grouped_df = work.dropna(subset=["typ"])

        elif step_type == "aggregate_sum":
            source = grouped_df if not grouped_df.empty else df
            aggregated_df = source.groupby("typ", as_index=False)["betrag"].sum()

        elif step_type == "output":
            output_format = step["format"]
            source = aggregated_df if not aggregated_df.empty else grouped_df

            if output_format == "table":
                return source
            if output_format == "value":
                if source.empty:
                    return 0.0
                return float(pd.to_numeric(source["betrag"], errors="coerce").fillna(0).sum())

    return "Fehler im Plan"
