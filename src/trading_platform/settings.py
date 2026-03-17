from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
FEATURE_DATA_DIR = DATA_DIR / "features"
EXPERIMENT_DIR = DATA_DIR / "experiments"

for p in [RAW_DATA_DIR, FEATURE_DATA_DIR, EXPERIMENT_DIR]:
    p.mkdir(parents=True, exist_ok=True)