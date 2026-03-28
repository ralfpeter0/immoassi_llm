from __future__ import annotations

import subprocess
import sys

import pandas as pd
import streamlit as st

from core import llm_interface
from core.query_engine import execute_plan

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

if "messages" not in st.session_state:
    st.session_state.messages = []

if "context_filters" not in st.session_state:
    st.session_state.context_filters = {"mieter": None, "konten": None, "zeitraum": None}


for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if "dataframe" in message:
            st.dataframe(pd.DataFrame(message["dataframe"]))
        else:
            st.write(message["content"])

show_debug = st.checkbox("Debug anzeigen")
prompt = st.chat_input("Frage zu Mieten, Zahlungen oder Differenzen")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    response = llm_interface.interpret_query(prompt, st.session_state.context_filters)
    st.session_state.context_filters = response["filters"]

    with st.chat_message("assistant"):
        if show_debug:
            with st.expander("🧠 Verständnis"):
                st.write(response["understanding"])

            with st.expander("📊 Filter"):
                st.json(response["filters"])

            with st.expander("🔍 Plan"):
                st.json(response["plan"])

        if response["clarification"]:
            st.write(response["clarification"])
            st.session_state.messages.append({"role": "assistant", "content": response["clarification"]})
        elif response["plan"]:
            result = execute_plan(response["plan"])
            if isinstance(result, pd.DataFrame):
                st.dataframe(result)
                st.session_state.messages.append(
                    {
                        "role": "assistant",
                        "content": "[Tabelle]",
                        "dataframe": result.to_dict(orient="records"),
                    }
                )
            else:
                st.write(result)
                st.session_state.messages.append({"role": "assistant", "content": str(result)})
