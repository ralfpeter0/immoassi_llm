from __future__ import annotations

import subprocess
import sys

import pandas as pd
import streamlit as st

from core.llm_interface import HybridLLM
from core.query_engine import dataset_description, query_data

st.set_page_config(page_title="Immobilienverwaltung mit LLM", layout="wide")
st.title("Immobilienverwaltung – Hybrid Conversational Data System")

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

llm = HybridLLM()
tools = [
    {
        "name": "query_data",
        "description": "Filtert Zahlungsdaten nach Mieter, Jahr und Kontoart.",
        "arguments": {"mieter": "str|null", "jahr": "int|null", "konten": "list[str]|null"},
    }
]

if "messages" not in st.session_state:
    st.session_state.messages = []

st.subheader("Chat")
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message.get("content"):
            st.write(message["content"])
        if message.get("dataframe") is not None:
            st.dataframe(pd.DataFrame(message["dataframe"]))

prompt = st.chat_input("Frage z. B.: Was hat Flury 2025 an Miete bezahlt?")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    history_for_llm = [
        {"role": m["role"], "content": m.get("content", "")}
        for m in st.session_state.messages
        if m.get("content")
    ]

    llm_response = llm.respond(
        user_input=prompt,
        available_tools=tools,
        dataset_description=dataset_description(),
        history=history_for_llm,
    )

    with st.chat_message("assistant"):
        if llm_response["type"] == "tool_call" and llm_response.get("tool") == "query_data":
            args = llm_response.get("arguments", {})
            result = query_data(
                mieter=args.get("mieter"),
                jahr=args.get("jahr"),
                konten=args.get("konten"),
            )

            st.dataframe(result)

            table_markdown = result.to_markdown(index=False) if not result.empty else "(keine Treffer)"
            final_text = llm.explain_with_data(
                user_input=prompt,
                data_markdown=f"Here is the data:\n{table_markdown}",
                history=history_for_llm,
            )
            st.write(final_text)

            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": final_text,
                    "dataframe": result.to_dict(orient="records"),
                }
            )
        else:
            content = llm_response.get("content", "Ich habe gerade keine Antwort.")
            st.write(content)
            st.session_state.messages.append({"role": "assistant", "content": content})
