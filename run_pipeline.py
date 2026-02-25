# run_pipeline.py
import sys
import subprocess
from pathlib import Path

PY = sys.executable  # uses current venv python
ROOT = Path(__file__).resolve().parent

STEPS = [
    ("Fetch raw matches (optional)", "fetch_data.py"),
    ("Build SQLite DB from raw JSON", "build_db.py"),
    ("Build stats: item1", "build_stats_item1.py"),
    ("Build stats: item2", "build_stats_item2.py"),
    ("Build stats: item3", "build_stats_item3.py"),
    ("Export CSV (optional)", "export_csv.py"),
]

def run_step(label: str, script: str):
    script_path = ROOT / script
    if not script_path.exists():
        print(f"[SKIP] {label}: missing {script}")
        return

    print(f"\n=== {label} ===")
    r = subprocess.run([PY, str(script_path)], cwd=str(ROOT))
    if r.returncode != 0:
        raise SystemExit(f"\nSTOP: step failed -> {script}")

def main():
    for label, script in STEPS:
        run_step(label, script)

    print("\n✅ Pipeline complete.")

if __name__ == "__main__":
    main()
