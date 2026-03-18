from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from trading_platform.config.models import (
    ParameterSweepConfig,
    ResearchWorkflowConfig,
    WalkForwardConfig,
)

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def _read_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    suffix = path.suffix.lower()

    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))

    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError(
                "PyYAML is required for YAML config files. Install with `pip install pyyaml`."
            )
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data or {}

    raise ValueError(f"Unsupported config file type: {suffix}")


def load_research_workflow_config(path: str | Path) -> ResearchWorkflowConfig:
    data = _read_config_file(Path(path))
    return ResearchWorkflowConfig(**data)


def load_parameter_sweep_config(path: str | Path) -> ParameterSweepConfig:
    data = _read_config_file(Path(path))
    return ParameterSweepConfig(**data)

def load_walk_forward_config(path: str | Path) -> WalkForwardConfig:
    data = _read_config_file(Path(path))
    return WalkForwardConfig(**data)