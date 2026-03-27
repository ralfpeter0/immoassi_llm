from __future__ import annotations

import subprocess
import sys

import streamlit as st

from core.query_engine import QueryEngine

st.set_page_config(page_title="Immobilienverwaltung mit LLM", layout="wide")
st.title("Immobilienverwaltung – Datenpipeline & Abfragen")

if st.button("Pipeline ausführen"):
    with st.spinner("Pipeline läuft..."):
        process = subprocess.run(
            [sys.executable, "run_pipeline.py"],
            capture_output=True,
            text=True,
        )

    st.subheader("Pipeline-Log")
    st.code((process.stdout or "") + "\n" + (process.stderr or ""))

    if process.returncode == 0:
        st.success("Pipeline erfolgreich abgeschlossen")
    else:
        st.error(f"Pipeline fehlgeschlagen (Exit-Code {process.returncode})")

st.subheader("Natürliche Sprache")
question = st.text_input("Frage zu Mieten, Zahlungen oder Differenzen")

if question:
    engine = QueryEngine()
    engine.load()
    answer = engine.ask(question)
    st.write(answer)
