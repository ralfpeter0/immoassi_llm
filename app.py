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

if "dialog_state" not in st.session_state:
    st.session_state.dialog_state = {
        "intent": None,
        "mieter": None,
        "zeitraum": None,
        "zahlungsart": None,
        "output_format": None,
    }


def _empty_state() -> dict:
    return {
        "intent": None,
        "mieter": None,
        "zeitraum": None,
        "zahlungsart": None,
        "output_format": None,
    }


def _is_state_empty(state: dict) -> bool:
    return all(value is None for value in state.values())


def _is_state_complete(state: dict) -> bool:
    intent = state.get("intent")
    if intent == "mieter_info":
        return state.get("intent") is not None and state.get("mieter") is not None

    required = ["intent", "mieter", "zeitraum", "zahlungsart"]
    return all(state.get(key) is not None for key in required)


def _next_question(state: dict) -> str:
    if state.get("intent") == "mieter_info" and state.get("mieter") is None:
        return "Für welchen Mieter?"

    missing_zeitraum = state.get("zeitraum") is None
    missing_zahlungsart = state.get("zahlungsart") is None

    if missing_zeitraum and missing_zahlungsart:
        return "Meinst du Miete oder Nebenkosten und für welchen Zeitraum?"
    if missing_zahlungsart:
        return "Meinst du Miete oder Nebenkosten?"
    if missing_zeitraum:
        return "Für welchen Zeitraum?"
    if state.get("mieter") is None:
        return "Für welchen Mieter?"
    return "Bitte ergänze noch die fehlenden Angaben."


def _looks_like_new_topic(user_text: str, state: dict) -> bool:
    if _is_state_empty(state):
        return True

    updates = llm_interface.extract_slot_updates(user_text)
    has_explicit_intent = updates.get("intent") is not None
    has_mieter = updates.get("mieter") is not None
    has_zeitraum = updates.get("zeitraum") is not None
    has_zahlungsart = updates.get("zahlungsart") is not None

    return has_explicit_intent and (has_mieter or has_zeitraum or has_zahlungsart)


def _merge_missing_only(state: dict, updates: dict) -> dict:
    merged = dict(state)
    for key, value in updates.items():
        if merged.get(key) is None:
            merged[key] = value
    return merged


def _run_query(dialog_state: dict):
    if hasattr(query_engine, "execute_query"):
        return query_engine.execute_query(dialog_state)
    return st.session_state.engine.ask(json.dumps(dialog_state, ensure_ascii=False))


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

    current_state = st.session_state.dialog_state
    if _looks_like_new_topic(prompt, current_state):
        st.session_state.dialog_state = llm_interface.interpret_query(prompt)
    else:
        updates = llm_interface.extract_slot_updates(prompt)
        st.session_state.dialog_state = _merge_missing_only(st.session_state.dialog_state, updates)

    dialog_state = st.session_state.dialog_state

    if not _is_state_complete(dialog_state):
        assistant_text = _next_question(dialog_state)
        with st.chat_message("assistant"):
            st.write(assistant_text)
        st.session_state.messages.append({"role": "assistant", "content": assistant_text})
    else:
        raw_result = _run_query(dialog_state)
        assistant_text, assistant_df = _normalize_assistant_output(raw_result)

        with st.chat_message("assistant"):
            if dialog_state.get("output_format") == "table" and assistant_df is not None:
                st.dataframe(assistant_df)
                st.session_state.messages.append(
                    {
                        "role": "assistant",
                        "content": "[Tabelle]",
                        "dataframe": assistant_df.to_dict(orient="records"),
                    }
                )
            else:
                if not assistant_text and assistant_df is not None:
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

        st.session_state.dialog_state = _empty_state()
