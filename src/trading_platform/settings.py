from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
FEATURES_DIR = DATA_DIR / "features"

ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
EXPERIMENT_DIR = ARTIFACTS_DIR / "experiments"

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
FEATURES_DIR.mkdir(parents=True, exist_ok=True)
EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)