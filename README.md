# Immobilienverwaltung – Datenpipeline + Streamlit

Dieses Projekt stellt eine einfache, lauffähige Basis für eine Immobilienverwaltung bereit.

## Ziel
- Mietdaten einlesen, bereinigen und für weitere Analysen aufbereiten.
- Ergebnisse in `dataout/` schreiben.
- Pipeline per Skript oder per Streamlit-Button starten.
- Vorbereitung für eine spätere LLM-Abfrageschicht (`core/query_engine.py`, `core/llm_interface.py`).

## Projektstruktur
- `datain/` – Eingabedaten (CSV)
- `dataout/` – Ausgabedaten der Pipeline
- `core/` – spätere LLM-/Query-Logik
- `pages/` – optionale Streamlit-Seiten

## Nutzung
1. Optional eine Eingabedatei `datain/mietdaten.csv` bereitstellen.
2. Pipeline starten:
   ```bash
   python run_pipeline.py
   ```
3. Oder Streamlit starten:
   ```bash
   streamlit run app.py
   ```

## Abhängigkeiten
- pandas
- streamlit
