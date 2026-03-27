from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

from core.data_loader import get_mietkonten

FUZZY_THRESHOLD = 85

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "dataout"
OUTPUT_DIR = BASE_DIR / "dataout"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_csv(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            return pd.read_csv(path, sep=None, engine="python", dtype=str, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, sep=None, engine="python", dtype=str)


def normalize_text(value: str) -> str:
    text = str(value or "").strip().upper()
    text = text.replace("Ä", "AE").replace("Ö", "OE").replace("Ü", "UE").replace("ß", "SS")
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_konto(value: str) -> str:
    konto = str(value or "").strip()
    if konto.lower() in {"", "nan", "none"}:
        return ""
    return konto


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]
    return out


def build_mieter_candidates(tbl_mieter: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, str]] = []

    for _, row in tbl_mieter.iterrows():
        mieterid = normalize_konto(row.get("mieterid"))
        if not mieterid:
            continue

        for source in ("mieter_name", "laden"):
            candidate = normalize_text(row.get(source, ""))
            if len(candidate) < 3:
                continue

            rows.append({"mieterid": mieterid, "candidate": candidate})

            alias = candidate.replace("AE", "A").replace("OE", "O").replace("UE", "U")
            if alias != candidate and len(alias) >= 3:
                rows.append({"mieterid": mieterid, "candidate": alias})

    result = pd.DataFrame(rows).drop_duplicates()
    print(f"[03_mieter_match] Mieter-Kandidaten: {len(result)}")
    return result


def build_vertrag_candidates(mietmatrix: pd.DataFrame) -> pd.DataFrame:
    matrix = mietmatrix.copy()
    matrix["vertragid"] = matrix["vertragid"].map(normalize_konto)
    matrix["konto"] = matrix["konto"].map(normalize_konto)

    left = matrix[["vertragid", "konto", "mieterid_1"]].rename(columns={"mieterid_1": "mieterid"})
    right = matrix[["vertragid", "konto", "mieterid_2"]].rename(columns={"mieterid_2": "mieterid"})

    out = pd.concat([left, right], ignore_index=True)
    out["mieterid"] = out["mieterid"].map(normalize_konto)
    out = out[(out["mieterid"] != "") & (out["vertragid"] != "")]
    out = out.drop_duplicates()

    print(f"[03_mieter_match] Vertrag-Kandidaten (mieterid+konto→vertragid): {len(out)}")
    return out


def choose_sachkonto(row: pd.Series, sachkonten: set[str]) -> str:
    sollkonto = normalize_konto(row.get("sollkonto"))
    habenkonto = normalize_konto(row.get("habenkonto"))

    # Priorität: Habenkonto (typisch Ertragskonto), dann Sollkonto.
    if habenkonto in sachkonten:
        return habenkonto
    if sollkonto in sachkonten:
        return sollkonto
    return ""


def select_mieterid(text: str, candidates: pd.DataFrame) -> tuple[str, str, float]:
    if not text:
        return "", "none", 0.0

    # 1) Exact: Kandidat als kompletter Ausdruck im Buchungstext
    exact = candidates[candidates["candidate"].apply(lambda c: c in text)]
    if not exact.empty:
        counts = exact.groupby("mieterid").size().sort_values(ascending=False)
        mieterid = str(counts.index[0])
        return mieterid, "exact", 100.0

    # 2) Fuzzy über alle Kandidaten
    best_per_mieter: dict[str, float] = {}
    for _, row in candidates.iterrows():
        mieterid = row["mieterid"]
        candidate = row["candidate"]
        score = float(fuzz.partial_ratio(text, candidate))
        if score >= FUZZY_THRESHOLD:
            if mieterid not in best_per_mieter or score > best_per_mieter[mieterid]:
                best_per_mieter[mieterid] = score

    if not best_per_mieter:
        return "", "none", 0.0

    mieterid = max(best_per_mieter, key=best_per_mieter.get)
    return mieterid, "fuzzy", round(best_per_mieter[mieterid], 2)


def determine_vertragid(vertrag_candidates: pd.DataFrame, mieterid: str, konto: str) -> str:
    if not mieterid:
        return ""

    subset = vertrag_candidates[vertrag_candidates["mieterid"] == mieterid]
    if subset.empty:
        return ""

    if konto:
        subset_konto = subset[subset["konto"] == konto]
        unique_konto = sorted(subset_konto["vertragid"].unique().tolist())
        if len(unique_konto) == 1:
            return unique_konto[0]

    unique_alle = sorted(subset["vertragid"].unique().tolist())
    if len(unique_alle) == 1:
        return unique_alle[0]

    return ""


def run_matching(zahlungen: pd.DataFrame, mietmatrix: pd.DataFrame, tbl_mieter: pd.DataFrame) -> pd.DataFrame:
    sachkonten = {str(k) for k in get_mietkonten()}
    print(f"[03_mieter_match] Sachkonten für Matching: {sorted(sachkonten)}")

    mieter_candidates = build_mieter_candidates(tbl_mieter)
    vertrag_candidates = build_vertrag_candidates(mietmatrix)

    output_rows = []
    for _, row in zahlungen.iterrows():
        text = normalize_text(row.get("buchungstext", ""))
        zahlung_konto = choose_sachkonto(row, sachkonten)

        mieterid, match_typ, match_score = select_mieterid(text, mieter_candidates)
        vertragid = determine_vertragid(vertrag_candidates, mieterid, zahlung_konto)

        output_rows.append(
            {
                "datum": row.get("datum", ""),
                "betrag": row.get("betrag", ""),
                "buchungstext": row.get("buchungstext", ""),
                "zahlung_konto": zahlung_konto,
                "mieterid": mieterid,
                "vertragid": vertragid,
                "match_typ": match_typ,
                "match_score": match_score,
            }
        )

    result = pd.DataFrame(output_rows)
    if "betrag" in result.columns:
        result["betrag"] = pd.to_numeric(result["betrag"], errors="coerce")

    print("[03_mieter_match] Match-Statistik:")
    print(result["match_typ"].value_counts(dropna=False))
    return result


def main() -> None:
    print("[03_mieter_match] Lade Daten...")

    zahlungen = normalize_columns(load_csv(INPUT_DIR / "tbl_zahlung.csv"))
    mietmatrix = normalize_columns(load_csv(INPUT_DIR / "mietmatrix.csv"))
    tbl_mieter = normalize_columns(load_csv(INPUT_DIR / "Tbl_mieter.csv"))

    required_zahlung = {"datum", "betrag", "buchungstext", "sollkonto", "habenkonto"}
    required_mietmatrix = {"vertragid", "konto", "mieterid_1", "mieterid_2"}
    required_mieter = {"mieterid", "mieter_name", "laden"}

    if not required_zahlung.issubset(zahlungen.columns):
        missing = required_zahlung.difference(set(zahlungen.columns))
        raise ValueError(f"tbl_zahlung.csv unvollständig: {missing}")
    if not required_mietmatrix.issubset(mietmatrix.columns):
        missing = required_mietmatrix.difference(set(mietmatrix.columns))
        raise ValueError(f"mietmatrix.csv unvollständig: {missing}")
    if not required_mieter.issubset(tbl_mieter.columns):
        missing = required_mieter.difference(set(tbl_mieter.columns))
        raise ValueError(f"Tbl_mieter.csv unvollständig: {missing}")

    result = run_matching(zahlungen, mietmatrix, tbl_mieter)

    out_path = OUTPUT_DIR / "tbl_zahlung_mit_mieter.csv"
    result.to_csv(out_path, index=False)
    print(f"[03_mieter_match] Fertig: {out_path}")


if __name__ == "__main__":
    main()
