"""Schritt 3: Einfaches Matching von Mieter zu Objekt/Einheit."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DATAOUT = Path("dataout")
INPUT_PATH = DATAOUT / "01_cleaned.csv"
OUTPUT_PATH = DATAOUT / "03_mieter_match.csv"


def run() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Fehlende Eingabe: {INPUT_PATH}. Bitte zuerst 01_load_clean.py ausführen.")

    df = pd.read_csv(INPUT_PATH)
    matched = (
        df.sort_values("datum")
        .groupby(["objekt_id", "einheit"], as_index=False)
        .last()[["objekt_id", "einheit", "mieter_name", "datum"]]
        .rename(columns={"datum": "letztes_update"})
    )

    matched.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Mieter-Matching geschrieben: {OUTPUT_PATH}")
