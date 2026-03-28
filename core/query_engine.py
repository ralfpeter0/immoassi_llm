from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATAOUT_DIR = BASE_DIR / "dataout"
ZAHLUNG_PATH = DATAOUT_DIR / "tbl_zahlung_mit_mieter.csv"

KONTO_MAP: dict[str, list[int]] = {
    "Miete": [8105, 8400],
    "Nebenkosten": [8195],
}


def query_data(
    mieter: str | None = None,
    jahr: int | None = None,
    konten: list[str] | None = None,
) -> pd.DataFrame:
    """Lädt Zahlungsdaten und liefert Summen nach Konto zurück."""
    if not ZAHLUNG_PATH.exists():
        return pd.DataFrame(columns=["zahlung_konto", "betrag"])

    df = pd.read_csv(ZAHLUNG_PATH)

    if mieter:
        df = df[df["buchungstext"].astype(str).str.contains(mieter, case=False, na=False)]

    if jahr:
        df["datum"] = pd.to_datetime(df["datum"], errors="coerce", dayfirst=True, format="mixed")
        df = df[df["datum"].dt.year == jahr]

    if konten:
        konto_values: list[int] = []
        for konto_name in konten:
            konto_values.extend(KONTO_MAP.get(konto_name, []))
        if konto_values:
            df = df[df["zahlung_konto"].isin(konto_values)]

    result = df.groupby("zahlung_konto", dropna=False)["betrag"].sum().reset_index()
    return result


def dataset_description() -> dict[str, Any]:
    return {
        "dataset": "tbl_zahlung_mit_mieter.csv",
        "path": str(ZAHLUNG_PATH),
        "intent_rules": {
            "bezahlt": "zahlungen",
            "wohnt": "mietmatrix (future)",
        },
        "konten": KONTO_MAP,
    }
