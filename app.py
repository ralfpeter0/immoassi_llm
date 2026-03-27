"""Streamlit UI für die Immobilien-Pipeline."""

from __future__ import annotations

import traceback

import streamlit as st

from run_pipeline import run_pipeline

st.set_page_config(page_title="Immobilienverwaltung", page_icon="🏢", layout="centered")

st.title("🏢 Immobilienverwaltung")
st.write("Führe die Datenpipeline aus, um Mietdaten aufzubereiten.")

if st.button("Pipeline ausführen"):
    with st.spinner("Pipeline läuft..."):
        try:
            run_pipeline()
            st.success("Pipeline erfolgreich ausgeführt.")
        except Exception as exc:  # pragma: no cover
            st.error(f"Fehler bei der Pipeline-Ausführung: {exc}")
            st.code(traceback.format_exc())
