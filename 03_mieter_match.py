import pandas as pd
from rapidfuzz import fuzz
from pathlib import Path
import re

from core.data_loader import get_mietkonten

KONTO_MAP = {"8403": "8401"}
STOPWORDS = {"KG", "GMBH", "UND", "DER", "DIE", "DAS", "MBH"}

# --------------------------------------------------
# Pfade
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "dataout"
OUTPUT_DIR = BASE_DIR / "dataout"

# --------------------------------------------------
# Mietkonten-Filter aus konto_mapping.csv laden
# Fallback: hardcodierte Menge, falls Datei leer/fehlt
# --------------------------------------------------
_MIETKONTEN_FALLBACK = {"8105", "8195", "8115", "8197", "8400", "8401", "8402", "8403"}

def _load_mietkonten_filter() -> set:
    konten = get_konto_set(load_konto_mapping())
    if konten:
        print(f"Mietkonten aus konto_mapping.csv geladen: {sorted(konten)}")
        return konten
    print("konto_mapping.csv leer oder fehlt – Fallback auf hardcodierte Konten.")
    return _MIETKONTEN_FALLBACK

MIETKONTEN_FILTER = get_mietkonten()

OUTPUT_DIR.mkdir(exist_ok=True)

# --------------------------------------------------
# CSV laden (nur Komma!)
# --------------------------------------------------
def load_csv(path):
    last_error = None
    for enc in ("utf-8", "cp1252", "latin1"):
        try:
            return pd.read_csv(path, sep=",", dtype=str, encoding=enc)
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    return pd.read_csv(path, sep=",", dtype=str)

# --------------------------------------------------
# Hilfsfunktion
# --------------------------------------------------
def col(df, name):
    cols = {c.lower(): c for c in df.columns}
    if name.lower() in cols:
        return cols[name.lower()]
    raise Exception(f"Spalte nicht gefunden: {name} | vorhanden: {list(df.columns)}")

# --------------------------------------------------
# Textbereinigung
# --------------------------------------------------
def normalize(text):
    if text is None:
        return ""
    text = str(text)
    text = text.upper()
    text = text.replace("Ä", "AE").replace("Ö", "OE").replace("Ü", "UE").replace("ß", "SS")
    text = re.sub(r"[^A-Z ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_konto(value):
    text = str(value or "").strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def choose_zahlungskonto(row):
    sollkonto = normalize_konto(row.get("sollkonto"))
    habenkonto = normalize_konto(row.get("habenkonto"))
    if sollkonto in MIETKONTEN_FILTER:
        konto = sollkonto
    elif habenkonto in MIETKONTEN_FILTER:
        konto = habenkonto
    else:
        konto = sollkonto or habenkonto
    return KONTO_MAP.get(konto, konto)


def build_tokens_extended(text):
    tokens = normalize(text).split()
    tokens = [t for t in tokens if len(t) >= 4 and t not in STOPWORDS]
    tokens_extended = tokens + [tokens[i] + " " + tokens[i + 1] for i in range(len(tokens) - 1)]
    return tokens, tokens_extended


def build_mieter_candidates(tbl_mieter):
    if tbl_mieter.empty:
        return pd.DataFrame(columns=["mieterid", "candidate"])

    mieter = tbl_mieter.copy()
    mieter["mieterid"] = mieter.get("mieterid", "").fillna("").astype(str).str.strip()
    mieter["mieter_name"] = mieter.get("mieter_name", "").fillna("").astype(str)
    mieter["laden"] = mieter.get("laden", "").fillna("").astype(str)
    mieter = mieter[mieter["mieterid"].ne("")]
    mieter = mieter[~mieter["mieterid"].str.lower().isin({"nan", "none"})]

    candidates = []
    for _, row in mieter.iterrows():
        mieterid = row["mieterid"]
        for source in ("mieter_name", "laden"):
            candidate = normalize(row.get(source, ""))
            if candidate:
                candidates.append({"mieterid": mieterid, "candidate": candidate})

    return pd.DataFrame(candidates).drop_duplicates()


def collect_mieter_matches(tokens_extended, mieter_candidates, threshold=85):
    matches = {}
    if not tokens_extended or mieter_candidates.empty:
        return matches

    for token in tokens_extended:
        for _, row in mieter_candidates.iterrows():
            candidate = row["candidate"]
            score = fuzz.partial_ratio(token, candidate)
            if score >= threshold:
                mieterid = row["mieterid"]
                score = round(float(score), 2)
                if mieterid not in matches or score > matches[mieterid]:
                    matches[mieterid] = score
    return matches


def select_mieter_match(matches):
    if not matches:
        return None, "none", 0.0, 0

    if len(matches) == 1:
        selected_mieterid = next(iter(matches))
        return selected_mieterid, "fuzzy", matches[selected_mieterid], 1

    selected_mieterid = max(matches, key=matches.get)
    selected_score = matches[selected_mieterid]
    return selected_mieterid, "fuzzy", selected_score, len(matches)


def mieter_vertrag_kandidaten(mietmatrix):
    matrix = mietmatrix.copy()
    matrix["vertragid"] = matrix.get("vertragid", "").fillna("").astype(str).str.strip()
    matrix["konto"] = matrix.get("konto", "").map(normalize_konto)
    if "mieterid" in matrix.columns:
        matrix["mieterid"] = matrix["mieterid"].fillna("").astype(str).str.strip()
        out = matrix[["mieterid", "vertragid", "konto"]]
    else:
        left = matrix[["mieterid_1", "vertragid", "konto"]].rename(columns={"mieterid_1": "mieterid"})
        right = matrix[["mieterid_2", "vertragid", "konto"]].rename(columns={"mieterid_2": "mieterid"})
        out = pd.concat([left, right], ignore_index=True)

    out["mieterid"] = out["mieterid"].fillna("").astype(str).str.strip()
    out = out[out["mieterid"].ne("")]
    out = out[~out["mieterid"].str.lower().isin({"nan", "none"})]
    out = out[out["vertragid"].ne("")]
    return out.drop_duplicates()


# --------------------------------------------------
# Matching Vertrag
# --------------------------------------------------
def match_vertrag(zahlungen, mietmatrix, tbl_mieter):

    c_text = col(zahlungen, "buchungstext")
    c_betrag = col(zahlungen, "betrag")
    c_datum = col(zahlungen, "datum")

    mieter_candidates = build_mieter_candidates(tbl_mieter)
    vertrag_candidates = mieter_vertrag_kandidaten(mietmatrix)

    results = []

    for _, row in zahlungen.iterrows():
        zahlung_konto = choose_zahlungskonto(row)
        tokens, tokens_extended = build_tokens_extended(row.get(c_text, ""))
        raw_matches = collect_mieter_matches(tokens_extended, mieter_candidates, threshold=85)
        print("TOKENS:", tokens)
        print("MATCHES:", raw_matches)
        mieterid_match, match_typ, match_score, anzahl_treffer = select_mieter_match(raw_matches)

        result = {
            "datum": row.get(c_datum),
            "betrag": row.get(c_betrag),
            "buchungstext": row.get(c_text),
            "zahlung_konto": zahlung_konto,
            "mieterid_match": mieterid_match or "",
            "vertragid": None,
            "match_typ": match_typ,
            "match_score": match_score,
            "anzahl_treffer": anzahl_treffer,
        }

        if not mieterid_match:
            results.append(result)
            continue

        mieter_vertraege = vertrag_candidates[vertrag_candidates["mieterid"] == mieterid_match]
        vertragsliste = sorted(mieter_vertraege["vertragid"].drop_duplicates().tolist())

        if len(vertragsliste) == 1:
            result["vertragid"] = vertragsliste[0]
        elif len(vertragsliste) > 1 and zahlung_konto:
            konto_matches = mieter_vertraege[mieter_vertraege["konto"] == zahlung_konto]
            konto_vertraege = sorted(konto_matches["vertragid"].drop_duplicates().tolist())
            if len(konto_vertraege) == 1:
                result["vertragid"] = konto_vertraege[0]

        results.append(result)

    return pd.DataFrame(results)

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("Lade Daten...")

    zahlungen = pd.read_csv(
        INPUT_DIR / "tbl_zahlung.csv",
        sep=",",
        dtype=str,
        encoding="utf-8",
    )
    print("SPALTEN NACH LOAD:", zahlungen.columns.tolist())
    if len(zahlungen.columns) == 1:
        zahlungen = zahlungen.iloc[:, 0].str.split(",", expand=True)
        zahlungen.columns = [
            "datum",
            "betrag",
            "buchungstext",
            "zahlung_konto",
        ]
    mietmatrix = load_csv(INPUT_DIR / "mietmatrix.csv")
    mieter_file = INPUT_DIR / "tbl_mieter.csv"
    if not mieter_file.exists():
        mieter_file = INPUT_DIR / "Tbl_mieter.csv"
    tbl_mieter = load_csv(mieter_file)

    # Spalten normalisieren
    zahlungen.columns = [c.lower().strip() for c in zahlungen.columns]
    mietmatrix.columns = [c.lower().strip() for c in mietmatrix.columns]
    tbl_mieter.columns = [c.lower().strip() for c in tbl_mieter.columns]

    # nur relevante Mietkonten berücksichtigen
    zahlungen["sollkonto_clean"] = zahlungen["sollkonto"].map(normalize_konto)
    zahlungen["habenkonto_clean"] = zahlungen["habenkonto"].map(normalize_konto)
    zahlungen = zahlungen[
        zahlungen["sollkonto_clean"].isin(MIETKONTEN_FILTER)
        | zahlungen["habenkonto_clean"].isin(MIETKONTEN_FILTER)
    ].copy()
    zahlungen = zahlungen.drop(columns=["sollkonto_clean", "habenkonto_clean"])

    print("Spalten zahlung:", zahlungen.columns)
    print("Spalten mietmatrix:", mietmatrix.columns)
    print("Spalten mieter:", tbl_mieter.columns)

    # --------------------------------------------------
    # Schritt 1: Vertrag Matching
    # --------------------------------------------------
    df_result = match_vertrag(zahlungen, mietmatrix, tbl_mieter)
    print("Unmatched:", len(df_result[df_result["match_typ"] == "none"]))

    # --------------------------------------------------
    # Speichern
    # --------------------------------------------------
    df_result = df_result[
        [
            "datum",
            "betrag",
            "buchungstext",
            "zahlung_konto",
            "mieterid_match",
            "vertragid",
            "match_typ",
            "match_score",
            "anzahl_treffer",
        ]
    ]
    df_result["betrag"] = pd.to_numeric(df_result["betrag"], errors="coerce")

    print("FINAL SPALTEN:", df_result.columns.tolist())
    print(df_result.head())

    output_file = OUTPUT_DIR / "tbl_zahlung_mit_mieter.csv"
    df_result.to_csv(output_file, index=False, sep=",", encoding="utf-8")

    print("Fertig:", output_file)


# --------------------------------------------------
# Start
# --------------------------------------------------
if __name__ == "__main__":
    main()
