from __future__ import annotations

import json
import subprocess
import sys

import pandas as pd
import streamlit as st

from core import llm_interface, query_engine
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

if "messages" not in st.session_state:
    st.session_state.messages = []

if "engine" not in st.session_state:
    st.session_state.engine = QueryEngine()
    st.session_state.engine.load()


def _run_query(user_text: str):
    interpreted = user_text
    if hasattr(llm_interface, "interpret_query"):
        interpreted = llm_interface.interpret_query(user_text)

    if hasattr(query_engine, "execute_query"):
        return query_engine.execute_query(interpreted)

    return st.session_state.engine.ask(user_text)


def _normalize_assistant_output(result) -> tuple[str, pd.DataFrame | None]:
    if isinstance(result, pd.DataFrame):
        return "", result

    if isinstance(result, dict):
        if result.get("need_clarification"):
            return str(result.get("question") or "Bitte präzisiere deine Anfrage."), None
        if "result" in result and isinstance(result["result"], pd.DataFrame):
            return "", result["result"]
        return str(result.get("answer") or result), None

    if isinstance(result, str):
        try:
            payload = json.loads(result)
            if isinstance(payload, dict) and payload.get("need_clarification"):
                return str(payload.get("question") or "Bitte präzisiere deine Anfrage."), None
        except json.JSONDecodeError:
            pass
        return result, None

    return str(result), None


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

    raw_result = _run_query(prompt)
    assistant_text, assistant_df = _normalize_assistant_output(raw_result)

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
            st.write(assistant_text)
            st.session_state.messages.append({"role": "assistant", "content": assistant_text})
