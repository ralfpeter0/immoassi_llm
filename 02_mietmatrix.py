"""Schritt 2: Erzeugt eine Mietmatrix pro Objekt und Einheit."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DATAOUT = Path("dataout")
INPUT_PATH = DATAOUT / "01_cleaned.csv"
OUTPUT_PATH = DATAOUT / "02_mietmatrix.csv"


def run() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Fehlende Eingabe: {INPUT_PATH}. Bitte zuerst 01_load_clean.py ausführen.")

    df = pd.read_csv(INPUT_PATH)
    matrix = (
        df.pivot_table(
            index=["objekt_id", "einheit"],
            values=["miete_soll", "miete_ist"],
            aggfunc="mean",
        )
        .reset_index()
        .sort_values(["objekt_id", "einheit"])
    )

    matrix.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Mietmatrix geschrieben: {OUTPUT_PATH}")
