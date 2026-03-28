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


def _normalize_result(result) -> tuple[str, pd.DataFrame | None]:
    if isinstance(result, pd.DataFrame):
        return "", result
    return str(result), None


def _format_value(value: str) -> str:
    try:
        number = float(value)
        formatted = f"{number:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{formatted} €"
    except ValueError:
        return value


for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if "dataframe" in message:
            st.dataframe(pd.DataFrame(message["dataframe"]))
        else:
            st.write(message["content"])

prompt = st.chat_input("Frage zu Mieten, Zahlungen oder Differenzen")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    response = llm_interface.interpret_query(prompt)

    if "plan" in response:
        result = execute_plan(response["plan"])
    else:
        result = "Fehler im Plan"

    assistant_text, assistant_df = _normalize_result(result)

    with st.chat_message("assistant"):
        if assistant_df is not None:
            st.dataframe(assistant_df)
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": "[Tabelle]",
                    "dataframe": assistant_df.to_dict(orient="records"),
                }
            )
        else:
            formatted_text = _format_value(assistant_text)
            st.write(formatted_text)
            st.session_state.messages.append({"role": "assistant", "content": formatted_text})
