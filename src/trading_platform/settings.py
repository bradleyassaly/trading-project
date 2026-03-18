from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
NORMALIZED_DATA_DIR = DATA_DIR / "normalized"
FEATURES_DIR = DATA_DIR / "features"
METADATA_DIR = DATA_DIR / "metadata"

ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
EXPERIMENT_DIR = ARTIFACTS_DIR / "experiments"

for path in [
    DATA_DIR,
    RAW_DATA_DIR,
    NORMALIZED_DATA_DIR,
    FEATURES_DIR,
    METADATA_DIR,
    ARTIFACTS_DIR,
    EXPERIMENT_DIR,
]:
    path.mkdir(parents=True, exist_ok=True)