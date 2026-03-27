from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "dataout"
MIETMATRIX_PATH = DATA_DIR / "mietmatrix.csv"
ZAHLUNG_PATH = DATA_DIR / "tbl_zahlung_mit_mieter.csv"
OUTPUT_PATH = DATA_DIR / "miete_ist_soll.csv"
MONATSREPORT_PATH = DATA_DIR / "miete_monatsreport.csv"


def get_mietmonat(d: pd.Timestamp) -> str:
    if d.day >= 20:
        return (d + pd.DateOffset(months=1)).strftime("%Y-%m")
    return d.strftime("%Y-%m")


def get_status(row: pd.Series) -> str:
    if abs(row["diff"]) < 0.1:
        return "OK"
    if row["diff"] < 0:
        return "FEHLT"
    return "ZU VIEL"


def main() -> None:
    # 1. CSV laden
    mietmatrix = pd.read_csv(MIETMATRIX_PATH)
    df_ist = pd.read_csv(ZAHLUNG_PATH)

    # 2. Datum konvertieren (robust für gemischte Formate wie DD.MM.YYYY, YYYY-MM-DD etc.)
    raw_dates = df_ist["datum"].astype(str).str.strip()
    df_ist["datum"] = pd.to_datetime(
        raw_dates,
        dayfirst=True,
        format="mixed",
        errors="coerce",
    )

    invalid_dates = df_ist["datum"].isna().sum()
    if invalid_dates > 0:
        print(f"Achtung: {invalid_dates} ungültige Datumswerte entfernt")

    df_ist = df_ist.dropna(subset=["datum"])

    print("Datum nach Konvertierung:")
    print(df_ist["datum"].head(10))

    # 3. Mietmonat berechnen
    df_ist["monat"] = df_ist["datum"].apply(get_mietmonat)
    df_ist["monat_label"] = pd.to_datetime(df_ist["monat"] + "-01").dt.strftime("%b%y").str.lower()

    # 4. Zeitraum bestimmen
    monat_min = df_ist["monat"].min()
    monat_max = df_ist["monat"].max()
    print("Mietzeitraum:", monat_min, "bis", monat_max)

    # 5. Alle Monate im Bereich erzeugen
    monate = pd.date_range(
        start=f"{monat_min}-01",
        end=f"{monat_max}-01",
        freq="MS",
    )
    df_monate = pd.DataFrame(
        {
            "monat": monate.strftime("%Y-%m"),
            "monat_label": monate.strftime("%b%y").str.lower(),
        }
    )

    # 6. IST aggregieren
    df_ist_grouped = (
        df_ist.groupby(["vertragid", "zahlung_konto", "monat"])["betrag"]
        .sum()
        .reset_index()
    )

    # 7. MIT ALLEN MONATEN MERGEN (Monatsreport)
    df_ist_full = df_monate.merge(df_ist_grouped, on="monat", how="left")
    df_ist_full = df_ist_full.sort_values(["monat", "vertragid", "zahlung_konto"], na_position="last")
    df_ist_full.to_csv(MONATSREPORT_PATH, index=False)

    # 8. SOLL vorbereiten
    df_soll = (
        mietmatrix[["vertragid", "konto", "sollbetrag"]]
        .drop_duplicates()
        .rename(columns={"sollbetrag": "soll"})
    )

    # 9. IST für SOLL-Vergleich vorbereiten
    df_ist_grouped = df_ist_grouped.rename(columns={"zahlung_konto": "konto", "betrag": "ist"})

    # 10. Alle Kombinationen erzeugen
    df_soll_monate = df_soll.merge(df_monate[["monat"]], how="cross")

    # 11. Merge IST + SOLL
    df_final = df_soll_monate.merge(
        df_ist_grouped,
        on=["vertragid", "konto", "monat"],
        how="left",
    )
    df_final["ist"] = df_final["ist"].fillna(0)

    # 12. Differenz + Status
    df_final["diff"] = df_final["ist"] - df_final["soll"]
    df_final["status"] = df_final.apply(get_status, axis=1)

    # 13. Sortierung
    df_final = df_final.sort_values(["vertragid", "konto", "monat"])

    # 14. Speichern
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(OUTPUT_PATH, index=False)

    # 15. Debug
    print("Monatsreport erstellt:", df_ist_full.shape)
    print("Erstellt:", df_final.shape)
    print(df_final.head())


if __name__ == "__main__":
    main()
