from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _safe_read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def build_approved_model_state(
    *,
    artifact_dir: Path,
    approval_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    signal_diagnostics = _safe_read_json(artifact_dir / "signal_diagnostics.json")
    composite_diagnostics = _safe_read_json(artifact_dir / "composite_diagnostics.json")
    composite_inputs = _safe_read_json(artifact_dir / "composite_inputs.json")
    promoted_signals_df = _safe_read_csv(artifact_dir / "promoted_signals.csv")
    redundancy_df = _safe_read_csv(artifact_dir / "redundancy_report.csv")
    if redundancy_df.empty:
        redundancy_df = _safe_read_csv(artifact_dir / "redundancy_diagnostics.csv")

    approved_at = (
        (approval_metadata or {}).get("approved_at")
        or signal_diagnostics.get("run_timestamp")
        or datetime.fromtimestamp(artifact_dir.stat().st_mtime, tz=UTC).isoformat()
    )
    approval_status = (approval_metadata or {}).get("approval_status", "approved")
    portfolio_config = signal_diagnostics.get("composite_portfolio", {})

    return {
        "artifact_type": "approved_model_state",
        "schema_version": 1,
        "approved_at": approved_at,
        "approval_status": approval_status,
        "approval_metadata": approval_metadata or {},
        "source_artifact_dir": str(artifact_dir),
        "promoted_signals": promoted_signals_df.to_dict(orient="records"),
        "composite_inputs": composite_inputs,
        "composite_config": composite_diagnostics.get("config", {}),
        "signal_composition": signal_diagnostics.get("signal_composition", {}),
        "signal_composition_preset": signal_diagnostics.get("signal_composition_preset"),
        "dynamic_weighting_config": signal_diagnostics.get("signal_lifecycle", {}),
        "regime_aware_config": signal_diagnostics.get("regime", {}),
        "portfolio_construction": portfolio_config,
        "implementability_assumptions": {
            key: portfolio_config.get(key)
            for key in [
                "min_price",
                "min_volume",
                "min_avg_dollar_volume",
                "max_adv_participation",
                "max_position_pct_of_adv",
                "max_notional_per_name",
                "slippage_bps_per_turnover",
                "slippage_bps_per_adv",
            ]
            if key in portfolio_config
        },
        "redundancy_report": redundancy_df.to_dict(orient="records"),
        "artifacts": {
            "promoted_signals_path": str(artifact_dir / "promoted_signals.csv"),
            "redundancy_report_path": str(
                artifact_dir / ("redundancy_report.csv" if (artifact_dir / "redundancy_report.csv").exists() else "redundancy_diagnostics.csv")
            ),
            "composite_inputs_path": str(artifact_dir / "composite_inputs.json"),
            "signal_diagnostics_path": str(artifact_dir / "signal_diagnostics.json"),
            "composite_diagnostics_path": str(artifact_dir / "composite_diagnostics.json"),
        },
    }


def write_approved_model_state(
    *,
    artifact_dir: Path,
    approval_metadata: dict[str, Any] | None = None,
) -> dict[str, str]:
    approved_dir = artifact_dir / "approved"
    approved_dir.mkdir(parents=True, exist_ok=True)
    payload = build_approved_model_state(
        artifact_dir=artifact_dir,
        approval_metadata=approval_metadata,
    )
    root_path = artifact_dir / "approved_model_state.json"
    approved_path = approved_dir / "approved_model_state.json"
    for path in (root_path, approved_path):
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return {
        "approved_model_state_path": str(root_path),
        "approved_model_state_deployment_path": str(approved_path),
    }


def load_approved_model_state(path_or_dir: str | Path) -> dict[str, Any]:
    base_path = Path(path_or_dir)
    candidate_paths = (
        [base_path]
        if base_path.is_file()
        else [
            base_path / "approved" / "approved_model_state.json",
            base_path / "approved_model_state.json",
            base_path / "latest_approved_configuration.json",
        ]
    )
    for candidate_path in candidate_paths:
        payload = _safe_read_json(candidate_path)
        if payload:
            return payload
    raise FileNotFoundError(f"No approved model state artifact found under {base_path}")
