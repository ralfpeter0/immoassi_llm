import subprocess
import sys

STEPS = [
    "01_load_clean.py",
    "02_mietmatrix.py",
    "03_mieter_match.py",
    "04_miete_ist_soll.py",
]


def run_step(script):
    print(f"\n▶ Starte: {script}")

    result = subprocess.run(
        [sys.executable, script],
        stdout=sys.stdout,
        stderr=sys.stderr
    )

    if result.returncode != 0:
        print(f"\n❌ Fehler in {script}")
        sys.exit(1)

    print(f"✅ Fertig: {script}")


def main():
    print("=== Pipeline gestartet ===")

    for script in STEPS:
        run_step(script)

    print("\n=== Pipeline abgeschlossen ===")


if __name__ == "__main__":
    main()