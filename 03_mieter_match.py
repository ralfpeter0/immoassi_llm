from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime

import pandas as pd
from rapidfuzz import fuzz

FUZZY_THRESHOLD = 85

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "dataout"
FALLBACK_DIR = BASE_DIR / "datain"
OUTPUT_PATH = DATA_DIR / "tbl_zahlung_mit_mieter.csv"


def load_csv_flexible(path: Path) -> pd.DataFrame:
    """Liest CSV robust mit Encoding-/Delimiter-Erkennung."""
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            return pd.read_csv(path, sep=None, engine="python", dtype=str, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, sep=None, engine="python", dtype=str)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Lädt Zahlungsdaten, Mietmatrix und Mieterstamm."""
    zahlung_path = DATA_DIR / "tbl_zahlung.csv"
    matrix_path = DATA_DIR / "mietmatrix.csv"
    mieter_path = DATA_DIR / "Tbl_mieter.csv"

    if not zahlung_path.exists():
        zahlung_path = FALLBACK_DIR / "tbl_zahlung.csv"
    if not matrix_path.exists():
        matrix_path = FALLBACK_DIR / "mietmatrix.csv"
    if not mieter_path.exists():
        mieter_path = FALLBACK_DIR / "Tbl_mieter.csv"

    zahlungen = load_csv_flexible(zahlung_path)
    mietmatrix = load_csv_flexible(matrix_path)
    mieter = load_csv_flexible(mieter_path)

    for df in (zahlungen, mietmatrix, mieter):
        df.columns = [col.strip().lower() for col in df.columns]

    return zahlungen, mietmatrix, mieter


def normalize_text(value: str) -> str:
    text = str(value or "").upper().strip()
    text = (
        text.replace("Ä", "AE")
        .replace("Ö", "OE")
        .replace("Ü", "UE")
        .replace("ẞ", "SS")
        .replace("ß", "SS")
    )
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw or raw.lower() in {"nan", "none"}:
        return ""
    return raw


def normalize_konto(value: str) -> str:
    konto = normalize_id(value)
    if konto.endswith(".0"):
        konto = konto[:-2]
    return konto


def is_mietkonto(konto) -> bool:
    try:
        return str(int(konto)).startswith("8")
    except:
        return False


def choose_zahlungskonto(sollkonto, habenkonto) -> str:
    sollkonto = normalize_konto(sollkonto)
    habenkonto = normalize_konto(habenkonto)

    if is_mietkonto(sollkonto):
        konto = sollkonto
    elif is_mietkonto(habenkonto):
        konto = habenkonto
    else:
        return ""
    return konto


def _build_mieter_candidates(mieter: pd.DataFrame) -> pd.DataFrame:
    required = {"mieterid", "mieter_name", "laden"}
    missing = required.difference(set(mieter.columns))
    if missing:
        raise ValueError(f"Tbl_mieter.csv unvollständig, fehlende Spalten: {sorted(missing)}")

    base = mieter[["mieterid", "mieter_name", "laden"]].copy()
    base["mieterid"] = base["mieterid"].map(normalize_id)

    name_candidates = (
        base[["mieterid", "mieter_name"]]
        .rename(columns={"mieter_name": "candidate"})
        .assign(candidate=lambda d: d["candidate"].map(normalize_text))
    )
    laden_candidates = (
        base[["mieterid", "laden"]]
        .rename(columns={"laden": "candidate"})
        .assign(candidate=lambda d: d["candidate"].map(normalize_text))
    )

    candidates = pd.concat([name_candidates, laden_candidates], ignore_index=True)
    candidates = candidates[(candidates["mieterid"] != "") & (candidates["candidate"].str.len() >= 3)]
    candidates = candidates.drop_duplicates()

    # vereinfachte Umlaut-Varianten ergänzen
    alias = candidates.copy()
    alias["candidate"] = (
        alias["candidate"]
        .str.replace("AE", "A", regex=False)
        .str.replace("OE", "O", regex=False)
        .str.replace("UE", "U", regex=False)
    )
    alias = alias[alias["candidate"].str.len() >= 3]

    return pd.concat([candidates, alias], ignore_index=True).drop_duplicates()


def find_mieter(text: str, candidate_map: dict[str, list[str]]) -> tuple[str, str, float]:
    """Ermittelt mieterid per exact/fuzzy auf mieter_name + laden."""
    if not text:
        return "", "none", 0.0

    # exact: Kandidat als vollständiger Teilstring in normalisiertem Text
    exact_hits = [mieterid for mieterid, names in candidate_map.items() if any(name in text for name in names)]
    if len(exact_hits) == 1:
        return exact_hits[0], "exact", 100.0

    # fuzzy: bester Score je Mieter
    best_mieterid = ""
    best_score = 0.0
    tie = False
    for mieterid, names in candidate_map.items():
        local_best = max((float(fuzz.partial_ratio(text, n)) for n in names), default=0.0)
        if local_best > best_score:
            best_mieterid = mieterid
            best_score = local_best
            tie = False
        elif local_best == best_score and local_best >= FUZZY_THRESHOLD:
            tie = True

    if best_score >= FUZZY_THRESHOLD and not tie:
        return best_mieterid, "fuzzy", round(best_score, 2)

    return "", "none", 0.0


def _build_vertrag_lookup(mietmatrix: pd.DataFrame) -> dict[tuple[str, str], str]:
    required = {"vertragid", "konto", "mieterid_1", "mieterid_2"}
    missing = required.difference(set(mietmatrix.columns))
    if missing:
        raise ValueError(f"mietmatrix.csv unvollständig, fehlende Spalten: {sorted(missing)}")

    matrix = mietmatrix[["vertragid", "konto", "mieterid_1", "mieterid_2"]].copy()
    matrix["vertragid"] = matrix["vertragid"].map(normalize_id)
    matrix["konto"] = matrix["konto"].map(normalize_konto)
    matrix["mieterid_1"] = matrix["mieterid_1"].map(normalize_id)
    matrix["mieterid_2"] = matrix["mieterid_2"].map(normalize_id)

    left = matrix[["vertragid", "konto", "mieterid_1"]].rename(columns={"mieterid_1": "mieterid"})
    right = matrix[["vertragid", "konto", "mieterid_2"]].rename(columns={"mieterid_2": "mieterid"})
    expanded = pd.concat([left, right], ignore_index=True)
    expanded = expanded[(expanded["mieterid"] != "") & (expanded["konto"] != "") & (expanded["vertragid"] != "")]

    # Eindeutige Abbildung (mieterid + konto) -> vertragid
    grouped = expanded.groupby(["mieterid", "konto"])["vertragid"].agg(lambda s: sorted(set(s))).reset_index()

    lookup: dict[tuple[str, str], str] = {}
    for _, row in grouped.iterrows():
        if len(row["vertragid"]) == 1:
            lookup[(row["mieterid"], row["konto"])] = row["vertragid"][0]
    return lookup


def find_vertrag(mieterid: str, konto: str, lookup: dict[tuple[str, str], str]) -> str:
    """Ordnet vertragid strikt über (mieterid + konto) zu."""
    if not mieterid or not konto:
        return ""
    return lookup.get((mieterid, konto), "")


def main() -> None:
    zahlungen, mietmatrix, mieter = load_data()

    required = {"datum", "betrag", "buchungstext", "sollkonto", "habenkonto"}
    missing = required.difference(set(zahlungen.columns))
    if missing:
        raise ValueError(f"tbl_zahlung.csv unvollständig, fehlende Spalten: {sorted(missing)}")

    zahlungen = zahlungen[
        zahlungen.apply(
            lambda row: is_mietkonto(row.get("sollkonto")) or is_mietkonto(row.get("habenkonto")),
            axis=1,
        )
    ].copy()
    zahlungen["zahlung_konto"] = zahlungen.apply(
        lambda row: choose_zahlungskonto(row.get("sollkonto"), row.get("habenkonto")),
        axis=1,
    )
    print(f"[03_mieter_match] Relevante Zahlungen (8xxx): {len(zahlungen)}")

    mieter_candidates = _build_mieter_candidates(mieter)
    candidate_map = (
        mieter_candidates.groupby("mieterid")["candidate"].apply(lambda s: sorted(set(s))).to_dict()
    )
    vertrag_lookup = _build_vertrag_lookup(mietmatrix)

    working = zahlungen.copy()
    working["text_norm"] = working["buchungstext"].map(normalize_text)

    matches = working["text_norm"].map(lambda text: find_mieter(text, candidate_map))
    working[["mieterid", "match_typ", "match_score"]] = pd.DataFrame(matches.tolist(), index=working.index)

    working["vertragid"] = [
        find_vertrag(mieterid, konto, vertrag_lookup)
        for mieterid, konto in zip(working["mieterid"], working["zahlung_konto"])
    ]

    output = working[
        [
            "datum",
            "betrag",
            "buchungstext",
            "zahlung_konto",
            "mieterid",
            "vertragid",
            "match_typ",
            "match_score",
        ]
    ].copy()

    DATA_DIR.mkdir(exist_ok=True)
    written_path = OUTPUT_PATH
    try:
        output.to_csv(written_path, index=False)
    except PermissionError:
        fallback_name = f"{OUTPUT_PATH.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        written_path = OUTPUT_PATH.with_name(fallback_name)
        output.to_csv(written_path, index=False)
        print(
            "[03_mieter_match] WARN: Ziel-Datei war gesperrt, schreibe stattdessen nach "
            f"{written_path}"
        )

    total = len(output)
    matched = int((output["match_typ"] != "none").sum())
    unmatched = total - matched

    print(f"[03_mieter_match] Anzahl Gesamt: {total}")
    print(f"[03_mieter_match] Anzahl gematcht: {matched}")
    print(f"[03_mieter_match] Anzahl unmatched: {unmatched}")
    print(f"[03_mieter_match] Datei geschrieben: {written_path}")


if __name__ == "__main__":
    main()
