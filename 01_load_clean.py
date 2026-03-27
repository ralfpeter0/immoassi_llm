import pandas as pd
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "datain"
OUTPUT_DIR = BASE_DIR / "dataout"
OUTPUT_DIR.mkdir(exist_ok=True)

UMLAUT_MAP = {
    "Ä": "AE",
    "Ö": "OE",
    "Ü": "UE",
    "ẞ": "SS",
    "ß": "SS",
}


def normalize_name(value: str) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip().upper()
    if not text:
        return ""

    for src, target in UMLAUT_MAP.items():
        text = text.replace(src, target)

    text = re.sub(r"\s+", " ", text)
    return text.strip()


def simplify_umlaut_alias(alias: str) -> str:
    return (
        alias.replace("AE", "A")
        .replace("OE", "O")
        .replace("UE", "U")
    )


def create_mieter_aliases(df_mieter: pd.DataFrame) -> pd.DataFrame:
    if "mieter_name" not in df_mieter.columns:
        return pd.DataFrame(columns=["name_original", "alias"])

    rows = []
    for _, row in df_mieter.iterrows():
        name_original = normalize_name(row.get("mieter_name", ""))
        laden = normalize_name(row.get("laden", ""))

        if not name_original:
            continue

        alias_candidates = set()

        if laden:
            alias_candidates.add(laden)

        parts = name_original.split()
        if parts:
            alias_candidates.add(parts[0])

            if len(parts) > 1:
                alias_candidates.add(f"{parts[0]} {parts[1]}")

            if len(parts) > 2:
                alias_candidates.add(f"{parts[0]} {parts[-1]}")

        for alias in alias_candidates:
            alias_clean = normalize_name(alias)
            if not alias_clean:
                continue

            rows.append({"name_original": name_original, "alias": alias_clean})

            alias_simple = simplify_umlaut_alias(alias_clean)
            if alias_simple and alias_simple != alias_clean:
                rows.append({"name_original": name_original, "alias": alias_simple})

    if not rows:
        return pd.DataFrame(columns=["name_original", "alias"])

    return pd.DataFrame(rows).drop_duplicates()


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    # --- Spaltennamen global sauber ---
    df.columns = (
        df.columns
        .str.replace("﻿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
        .str.lower()
    )

    # --- IDs ---
    for col in df.columns:
        if col.endswith("id"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # --- Beträge ---
    for col in df.columns:
        if "betrag" in col:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Datum ---
    for col in df.columns:
        if "datum" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    return df


def rename_columns(df, filename):
    # --- eindeutige Namen je Tabelle ---

    if filename == "tbl_mieter.csv":
        df = df.rename(columns={
            "name": "mieter_name"
        })

    if filename == "tbl_objekte.csv":
        df = df.rename(columns={
            "bezeichnung": "objekt_bezeichnung"
        })

    if filename == "tbl_einheiten.csv":
        df = df.rename(columns={
            "bezeichnung": "einheit_bezeichnung"
        })

    if filename == "tbl_vertrag_konto.csv":
        df = df.rename(columns={
            "betrag": "sollbetrag"
        })

    # --- vertrag bereinigen ---
    if filename == "tbl_vertrag.csv":
        df = df.drop(columns=["kostenstelle", "wohnung"], errors="ignore")

    return df


def main():
    files = list(INPUT_DIR.glob("*.csv"))

    for f in files:
        df = pd.read_csv(
            f,
            sep=None,
            engine="python",
            encoding="utf-8-sig",
            dtype=str
        )

        df = clean_df(df)
        df = rename_columns(df, f.name.lower())

        out_path = OUTPUT_DIR / f.name

        if out_path.exists():
            print(f"Überschreibe: {out_path}")

        df.to_csv(out_path, sep=",", index=False)

        print(f"OK: {f.name} -> {out_path}")

        if f.name.lower() == "tbl_mieter.csv":
            alias_df = create_mieter_aliases(df)
            alias_out_path = OUTPUT_DIR / "tbl_mieter_alias.csv"
            alias_df.to_csv(alias_out_path, sep=",", index=False)
            print(f"OK: Alias erzeugt -> {alias_out_path}")
            print("Alias erstellt:", len(alias_df))


if __name__ == "__main__":
    main()
