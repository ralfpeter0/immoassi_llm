from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATAIN_DIR = BASE_DIR / "datain"
DATAOUT_DIR = BASE_DIR / "dataout"


def _read_csv_flexible(path: Path) -> pd.DataFrame:
    """Liest CSV robust mit automatischer Trennzeichenerkennung."""
    if not path.exists():
        print(f"[data_loader] Datei fehlt: {path}")
        return pd.DataFrame()

    try:
        return pd.read_csv(path, sep=None, engine="python", dtype=str, encoding="utf-8-sig")
    except Exception as exc:  # noqa: BLE001
        print(f"[data_loader] Fehler beim Lesen {path}: {exc}")
        return pd.DataFrame()


def load_konto_mapping() -> pd.DataFrame:
    """Lädt konto_mapping aus dataout, fallback datain."""
    preferred = DATAOUT_DIR / "konto_mapping.csv"
    fallback = DATAIN_DIR / "konto_mapping.csv"

    df = _read_csv_flexible(preferred)
    if df.empty:
        df = _read_csv_flexible(fallback)

    if df.empty:
        print("[data_loader] konto_mapping.csv nicht gefunden oder leer")
        return pd.DataFrame(columns=["konto"])

    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _normalize_konto_values(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        konto = str(value).strip()
        if not konto or konto.lower() in {"nan", "none"}:
            continue
        out.append(konto)
    return out


def get_mietkonten(default: set[str] | None = None) -> set[str]:
    """
    Liefert Sachkonten für Mieten.

    Priorität:
    1) konto_mapping.csv (Spalte 'konto')
    2) fallback default
    """
    fallback = default or {"8105", "8115", "8195", "8400", "8401", "8402", "8403"}
    df = load_konto_mapping()

    if "konto" not in df.columns:
        print("[data_loader] Spalte 'konto' fehlt in konto_mapping.csv, nutze Fallback")
        return fallback

    konten = set(_normalize_konto_values(df["konto"].tolist()))
    if not konten:
        print("[data_loader] Keine Konten in konto_mapping.csv, nutze Fallback")
        return fallback

    print(f"[data_loader] {len(konten)} Miet-Sachkonten geladen")
    return konten
