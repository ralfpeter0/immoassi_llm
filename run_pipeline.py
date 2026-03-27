"""Ausführung der Datenpipeline in fester Reihenfolge."""

from __future__ import annotations

import importlib
from typing import Callable

SCRIPT_MODULES = [
    "01_load_clean",
    "02_mietmatrix",
    "03_mieter_match",
    "04_miete_ist_soll",
]


def _load_runner(module_name: str) -> Callable[[], None]:
    module = importlib.import_module(module_name)
    run_func = getattr(module, "run", None)
    if run_func is None or not callable(run_func):
        raise AttributeError(f"Modul '{module_name}' hat keine callable run()-Funktion.")
    return run_func


def run_pipeline() -> None:
    for module_name in SCRIPT_MODULES:
        print(f"▶ Starte {module_name}.py")
        runner = _load_runner(module_name)
        runner()
    print("✅ Pipeline erfolgreich abgeschlossen.")


if __name__ == "__main__":
    run_pipeline()
