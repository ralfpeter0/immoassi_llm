"""Schritt 1: Laden und bereinigen der Rohdaten."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DATAIN = Path("datain")
DATAOUT = Path("dataout")
RAW_PATH = DATAIN / "mietdaten.csv"
CLEAN_PATH = DATAOUT / "01_cleaned.csv"


REQUIRED_COLUMNS = [
    "objekt_id",
    "mieter_name",
    "einheit",
    "miete_soll",
    "miete_ist",
    "datum",
]


def _create_sample_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "objekt_id": ["OBJ-001", "OBJ-001", "OBJ-002"],
            "mieter_name": ["Max Mustermann", "Erika Muster", "Ali Kaya"],
            "einheit": ["A1", "A2", "B1"],
            "miete_soll": [950.0, 870.0, 1100.0],
            "miete_ist": [950.0, 820.0, 1100.0],
            "datum": ["2026-01-01", "2026-01-01", "2026-01-01"],
        }
    )


def run() -> None:
    DATAIN.mkdir(parents=True, exist_ok=True)
    DATAOUT.mkdir(parents=True, exist_ok=True)

    if RAW_PATH.exists():
        df = pd.read_csv(RAW_PATH)
    else:
        df = _create_sample_data()
        df.to_csv(RAW_PATH, index=False)
        print(f"ℹ️ Keine Eingabedatei gefunden. Beispieldaten erzeugt: {RAW_PATH}")

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[REQUIRED_COLUMNS].copy()
    df["mieter_name"] = df["mieter_name"].astype(str).str.strip()
    df["einheit"] = df["einheit"].astype(str).str.strip().str.upper()
    df["miete_soll"] = pd.to_numeric(df["miete_soll"], errors="coerce").fillna(0.0)
    df["miete_ist"] = pd.to_numeric(df["miete_ist"], errors="coerce").fillna(0.0)
    df["datum"] = pd.to_datetime(df["datum"], errors="coerce")

    df = df.dropna(subset=["objekt_id", "einheit", "datum"]).sort_values(["objekt_id", "einheit", "datum"])
    df.to_csv(CLEAN_PATH, index=False)
    print(f"✅ Bereinigte Daten geschrieben: {CLEAN_PATH}")
