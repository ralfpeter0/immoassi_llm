import pandas as pd
from pathlib import Path

# --------------------------------------------------
# Pfade
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "dataout"
INPUT_DIR = DATA_DIR
OUTPUT_DIR = DATA_DIR

# --------------------------------------------------
# CSV laden
# --------------------------------------------------
def load_csv(name):
    return pd.read_csv(INPUT_DIR / name, dtype=str)


# --------------------------------------------------
# Hauptlogik
# --------------------------------------------------
def run():

    print("Lade Daten...")

    df_vm = load_csv("Tbl_vertrag_mieter.csv")
    df_mieter = load_csv("Tbl_mieter.csv")
    df_vertrag = load_csv("Tbl_vertrag.csv")
    df_vk = load_csv("Tbl_vertrag_konto.csv")
    df_einheiten = load_csv("tbl_einheiten.csv")
    df_objekte = load_csv("tbl_objekte.csv")

    # --------------------------------------------------
    # Spalten vereinheitlichen
    # --------------------------------------------------
    df_mieter.columns = [c.lower().strip() for c in df_mieter.columns]
    df_vk.columns = [c.lower().strip() for c in df_vk.columns]

    laden_lookup = pd.Series(dtype=str)
    if "laden" in df_mieter.columns:
        laden_lookup = (
            df_mieter[["mieterid", "laden"]]
            .assign(laden=lambda x: x["laden"].fillna("").astype(str).str.strip())
            .drop_duplicates(subset=["mieterid"], keep="first")
            .set_index("mieterid")["laden"]
        )

    # --------------------------------------------------
    # 1. Vertrag → max 2 Mieter pivotieren
    # --------------------------------------------------
    df_vm = df_vm.sort_values(["vertragid", "mieterid"])
    df_vm["pos"] = df_vm.groupby("vertragid").cumcount() + 1
    df_vm = df_vm[df_vm["pos"] <= 2]

    df_vm_pivot = df_vm.pivot(index="vertragid", columns="pos", values="mieterid").reset_index()
    df_vm_pivot.columns = ["vertragid", "mieterid_1", "mieterid_2"]

    # --------------------------------------------------
    # 2. Mieter 1
    # --------------------------------------------------
    df_vm_pivot = df_vm_pivot.merge(
        df_mieter,
        left_on="mieterid_1",
        right_on="mieterid",
        how="left"
    )

    df_vm_pivot = df_vm_pivot.rename(columns={
        "anrede": "anrede_1",
        "vorname": "vorname_1",
        "mieter_name": "mieter_name_1"
    }).drop(columns=["mieterid"])

    # --------------------------------------------------
    # 3. Mieter 2
    # --------------------------------------------------
    df_vm_pivot = df_vm_pivot.merge(
        df_mieter,
        left_on="mieterid_2",
        right_on="mieterid",
        how="left"
    )

    df_vm_pivot = df_vm_pivot.rename(columns={
        "anrede": "anrede_2",
        "vorname": "vorname_2",
        "mieter_name": "mieter_name_2"
    }).drop(columns=["mieterid"])

    # --------------------------------------------------
    # 4. Vertrag + Einheit + Objekt
    # --------------------------------------------------
    df = df_vm_pivot.merge(df_vertrag, on="vertragid", how="left")

    if "einheitid" in df.columns:
        df = df.merge(df_einheiten, on="einheitid", how="left")

    if "objektid" in df.columns:
        df = df.merge(df_objekte, on="objektid", how="left")

    # --------------------------------------------------
    # 5. Konten + Sollbeträge
    # --------------------------------------------------
    df = df.merge(df_vk, on="vertragid", how="left")

    df["konto"] = df["habenkonto"]
    df["sollbetrag"] = pd.to_numeric(df["sollbetrag"], errors="coerce")

    # --------------------------------------------------
    # 6. Vertragsart
    # --------------------------------------------------
    def get_art(konto):
        if konto == "8115":
            return "stellplatz"
        elif konto == "8105":
            return "wohnung"
        elif konto == "8195":
            return "nebenkosten"
        return "sonstiges"

    df["vertragsart"] = df["konto"].apply(get_art)

    # --------------------------------------------------
    # 7. Ergebnis
    # --------------------------------------------------
    df_out = pd.DataFrame()

    df_out["vertragid"] = df["vertragid"]

    df_out["mieterid_1"] = df["mieterid_1"]
    df_out["anrede_1"] = df["anrede_1"]
    df_out["vorname_1"] = df["vorname_1"]
    df_out["mieter_name_1"] = df["mieter_name_1"]

    df_out["mieterid_2"] = df["mieterid_2"]
    df_out["anrede_2"] = df["anrede_2"]
    df_out["vorname_2"] = df["vorname_2"]
    df_out["mieter_name_2"] = df["mieter_name_2"]

    df_out["laden"] = df_out["mieterid_1"].map(laden_lookup).fillna("")

    df_out["einheit"] = df["einheit_bezeichnung"]
    df_out["objekt"] = df["objekt_bezeichnung"]
    df_out["wohnung"] = df["wohnung"]
    df_out["m2"] = df["m2"]
    df_out["kellernummer"] = df["kellernummer"]
    df_out["strasse"] = df["strasse"]
    df_out["plz"] = df["plz"]
    df_out["ort"] = df["ort"]

    vertrag_cols = df_vertrag.columns.tolist()
    vertrag_cols = [c for c in vertrag_cols if c != "vertragid"]

    for col in vertrag_cols:
        df_out[col] = df.get(col, "")

    if "betrag" in df_out.columns:
        df_out["betrag"] = pd.to_numeric(df_out["betrag"], errors="coerce")

    df_out["konto"] = df["konto"]
    df_out["sollbetrag"] = df["sollbetrag"]

    df_out["vertragsart"] = df["vertragsart"]

    # --------------------------------------------------
    # 8. Duplikate entfernen
    # --------------------------------------------------
    df_out = df_out.drop_duplicates()

    # --------------------------------------------------
    # 9. Speichern
    # --------------------------------------------------
    invalid_suffix_cols = [c for c in df_out.columns if c.endswith("_x") or c.endswith("_y")]
    if invalid_suffix_cols:
        raise ValueError(f"Unerwartete _x/_y-Spalten im Output: {invalid_suffix_cols}")

    out_path = OUTPUT_DIR / "mietmatrix.csv"
    df_out.to_csv(out_path, index=False)

    print(f"Fertig: {out_path}")


# --------------------------------------------------
# Start
# --------------------------------------------------
if __name__ == "__main__":
    run()
