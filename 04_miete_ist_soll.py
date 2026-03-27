"""Schritt 4: Vergleich Ist- vs. Soll-Miete mit Abweichung."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DATAOUT = Path("dataout")
INPUT_PATH = DATAOUT / "02_mietmatrix.csv"
OUTPUT_PATH = DATAOUT / "04_miete_ist_soll.csv"


def run() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Fehlende Eingabe: {INPUT_PATH}. Bitte zuerst 02_mietmatrix.py ausführen.")

    df = pd.read_csv(INPUT_PATH)
    df["abweichung"] = df["miete_ist"] - df["miete_soll"]
    df["abweichung_prozent"] = df.apply(
        lambda row: (row["abweichung"] / row["miete_soll"] * 100) if row["miete_soll"] else 0.0,
        axis=1,
    )

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Ist/Soll-Vergleich geschrieben: {OUTPUT_PATH}")
