
import pandas as pd
from rapidfuzz import fuzz
from pathlib import Path
import re

KONTO_MAP = {"8403": "8401"}
STOPWORDS = {"KG", "GMBH", "UND", "DER", "DIE", "DAS", "MBH"}

# --------------------------------------------------
# Pfade
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "dataout"
OUTPUT_DIR = BASE_DIR / "dataout"

OUTPUT_DIR.mkdir(exist_ok=True)

# --------------------------------------------------
# NEU: 8xxx Regel
# --------------------------------------------------
def is_mietkonto(konto):
    try:
        return str(int(konto)).startswith("8")
    except:
        return False


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


# --------------------------------------------------
# Konto wählen (NEU)
# --------------------------------------------------
def choose_zahlungskonto(row):
    sollkonto = normalize_konto(row.get("sollkonto"))
    habenkonto = normalize_konto(row.get("habenkonto"))

    if is_mietkonto(sollkonto):
        konto = sollkonto
    elif is_mietkonto(habenkonto):
        konto = habenkonto
    else:
        return ""

    return KONTO_MAP.get(konto, konto)


# --------------------------------------------------
# Token
# --------------------------------------------------
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
        mieterid = next(iter(matches))
        return mieterid, "fuzzy", matches[mieterid], 1

    mieterid = max(matches, key=matches.get)
    return mieterid, "fuzzy", matches[mieterid], len(matches)


def mieter_vertrag_kandidaten(mietmatrix):
    matrix = mietmatrix.copy()
    matrix["vertragid"] = matrix.get("vertragid", "").fillna("").astype(str).str.strip()
    matrix["konto"] = matrix.get("konto", "").map(normalize_konto)

    left = matrix[["mieterid_1", "vertragid", "konto"]].rename(columns={"mieterid_1": "mieterid"})
    right = matrix[["mieterid_2", "vertragid", "konto"]].rename(columns={"mieterid_2": "mieterid"})

    out = pd.concat([left, right], ignore_index=True)

    out = out[out["mieterid"].notna()]
    out = out[out["mieterid"] != ""]

    return out.drop_duplicates()


# --------------------------------------------------
# Matching
# --------------------------------------------------
def match_vertrag(zahlungen, mietmatrix, tbl_mieter):

    c_text = col(zahlungen, "buchungstext")
    c_betrag = col(zahlungen, "betrag")
    c_datum = col(zahlungen, "datum")

    mieter_candidates = build_mieter_candidates(tbl_mieter)
    vertrag_candidates = mieter_vertrag_kandidaten(mietmatrix)

    results = []

    for _, row in zahlungen.iterrows():

        konto = choose_zahlungskonto(row)

        if not konto:
            continue

        tokens, tokens_extended = build_tokens_extended(row.get(c_text, ""))

        matches = collect_mieter_matches(tokens_extended, mieter_candidates)
        mieterid, match_typ, match_score, anzahl = select_mieter_match(matches)

        result = {
            "datum": row.get(c_datum),
            "betrag": row.get(c_betrag),
            "buchungstext": row.get(c_text),
            "zahlung_konto": konto,
            "mieterid": mieterid or "",
            "vertragid": None,
            "match_typ": match_typ,
            "match_score": match_score,
        }

        if mieterid:
            df = vertrag_candidates[vertrag_candidates["mieterid"] == mieterid]

            if len(df) == 1:
                result["vertragid"] = df.iloc[0]["vertragid"]
            else:
                df_konto = df[df["konto"] == konto]
                if len(df_konto) == 1:
                    result["vertragid"] = df_konto.iloc[0]["vertragid"]

        results.append(result)

    return pd.DataFrame(results)


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("Lade Daten...")

    zahlungen = load_csv(INPUT_DIR / "tbl_zahlung.csv")
    mietmatrix = load_csv(INPUT_DIR / "mietmatrix.csv")

    mieter_file = INPUT_DIR / "tbl_mieter.csv"
    if not mieter_file.exists():
        mieter_file = INPUT_DIR / "Tbl_mieter.csv"

    tbl_mieter = load_csv(mieter_file)

    zahlungen.columns = [c.lower().strip() for c in zahlungen.columns]
    mietmatrix.columns = [c.lower().strip() for c in mietmatrix.columns]
    tbl_mieter.columns = [c.lower().strip() for c in tbl_mieter.columns]

    df = match_vertrag(zahlungen, mietmatrix, tbl_mieter)

    output_file = OUTPUT_DIR / "tbl_zahlung_mit_mieter.csv"
    df.to_csv(output_file, index=False)

    print("Fertig:", output_file)


if __name__ == "__main__":
    main()