from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.research.registry import load_research_manifests


STRATEGY_VALIDATION_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class StrategyValidationPolicyConfig:
    schema_version: int = STRATEGY_VALIDATION_SCHEMA_VERSION
    min_folds: int = 3
    min_out_of_sample_sharpe: float = 0.5
    weak_out_of_sample_sharpe: float = 0.0
    min_mean_spearman_ic: float = 0.01
    weak_mean_spearman_ic: float = 0.0
    min_positive_fold_ratio: float = 0.5
    weak_positive_fold_ratio: float = 0.4
    max_metric_std: float | None = 0.15
    min_proxy_confidence_score: float = 0.5
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.schema_version != STRATEGY_VALIDATION_SCHEMA_VERSION:
            raise ValueError(f"Unsupported strategy validation schema_version: {self.schema_version}")
        if self.min_folds < 0:
            raise ValueError("min_folds must be >= 0")
        if self.min_positive_fold_ratio < 0 or self.weak_positive_fold_ratio < 0:
            raise ValueError("positive fold ratios must be >= 0")
        if self.min_proxy_confidence_score < 0:
            raise ValueError("min_proxy_confidence_score must be >= 0")
        if self.max_metric_std is not None and self.max_metric_std < 0:
            raise ValueError("max_metric_std must be >= 0 when provided")


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if str(file_path) in {"", "."} or not file_path.exists() or file_path.is_dir():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _safe_read_csv(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def _metric_series(frame: pd.DataFrame) -> tuple[pd.Series | None, str | None]:
    for column in ["spearman_ic", "test_sharpe_ratio", "test_sharpe", "mean_fold_sharpe"]:
        if column in frame.columns:
            series = pd.to_numeric(frame[column], errors="coerce").dropna()
            if not series.empty:
                return series, column
    return None, None


def _fold_metrics(manifest: dict[str, Any]) -> dict[str, Any]:
    artifact_paths = manifest.get("artifact_paths", {})
    folds = _safe_read_csv(artifact_paths.get("fold_results_path"))
    if folds.empty:
        return {
            "number_of_folds": int(manifest.get("folds_tested") or 0),
            "mean_metric": None,
            "metric_std": None,
            "positive_fold_ratio": None,
            "consistency_score": None,
            "metric_name": None,
        }
    series, metric_name = _metric_series(folds)
    number_of_folds = int(folds["fold_id"].nunique()) if "fold_id" in folds.columns else int(len(folds.index))
    if series is None or series.empty:
        return {
            "number_of_folds": number_of_folds,
            "mean_metric": None,
            "metric_std": None,
            "positive_fold_ratio": None,
            "consistency_score": None,
            "metric_name": metric_name,
        }
    positive_fold_ratio = float((series > 0).mean())
    metric_std = float(series.std(ddof=0)) if len(series) > 1 else 0.0
    consistency_score = float((series.mean() / (series.std(ddof=0) + 1e-9))) if len(series) > 1 else float(series.mean())
    return {
        "number_of_folds": number_of_folds,
        "mean_metric": float(series.mean()),
        "metric_std": metric_std,
        "positive_fold_ratio": positive_fold_ratio,
        "consistency_score": consistency_score,
        "metric_name": metric_name,
    }


def _validation_status(
    *,
    fold_metrics: dict[str, Any],
    out_of_sample_sharpe: float | None,
    mean_spearman_ic: float | None,
    policy: StrategyValidationPolicyConfig,
) -> tuple[str, list[str], float]:
    reasons: list[str] = []
    number_of_folds = int(fold_metrics.get("number_of_folds") or 0)
    positive_fold_ratio = _safe_float(fold_metrics.get("positive_fold_ratio"))
    metric_std = _safe_float(fold_metrics.get("metric_std"))
    consistency_score = _safe_float(fold_metrics.get("consistency_score"))
    proxy_confidence_score = 0.0

    fold_score = min(1.0, number_of_folds / max(policy.min_folds, 1))
    sharpe_score = max(min(((out_of_sample_sharpe or 0.0) / max(policy.min_out_of_sample_sharpe, 1e-9)), 1.0), 0.0)
    ic_score = max(min(((mean_spearman_ic or 0.0) / max(policy.min_mean_spearman_ic, 1e-9)), 1.0), 0.0)
    positive_score = positive_fold_ratio if positive_fold_ratio is not None else 0.0
    stability_score = 1.0
    if metric_std is not None and policy.max_metric_std is not None:
        stability_score = max(0.0, 1.0 - (metric_std / max(policy.max_metric_std, 1e-9)))
    proxy_confidence_score = round((0.35 * fold_score) + (0.25 * sharpe_score) + (0.20 * ic_score) + (0.10 * positive_score) + (0.10 * stability_score), 6)

    pass_checks = []
    weak_checks = []
    pass_checks.append(number_of_folds >= policy.min_folds)
    weak_checks.append(number_of_folds >= max(1, policy.min_folds - 1))
    pass_checks.append((out_of_sample_sharpe or float("-inf")) >= policy.min_out_of_sample_sharpe)
    weak_checks.append((out_of_sample_sharpe or float("-inf")) >= policy.weak_out_of_sample_sharpe)
    pass_checks.append((mean_spearman_ic or float("-inf")) >= policy.min_mean_spearman_ic)
    weak_checks.append((mean_spearman_ic or float("-inf")) >= policy.weak_mean_spearman_ic)
    pass_checks.append((positive_fold_ratio or float("-inf")) >= policy.min_positive_fold_ratio)
    weak_checks.append((positive_fold_ratio or float("-inf")) >= policy.weak_positive_fold_ratio)
    pass_checks.append(proxy_confidence_score >= policy.min_proxy_confidence_score)
    weak_checks.append(proxy_confidence_score >= max(policy.min_proxy_confidence_score * 0.7, 0.0))
    if policy.max_metric_std is not None and metric_std is not None:
        pass_checks.append(metric_std <= policy.max_metric_std)
        weak_checks.append(metric_std <= policy.max_metric_std * 1.5)

    if not pass_checks[0]:
        reasons.append(f"number_of_folds {number_of_folds} < {policy.min_folds}")
    if out_of_sample_sharpe is None or out_of_sample_sharpe < policy.min_out_of_sample_sharpe:
        reasons.append(
            f"out_of_sample_sharpe {out_of_sample_sharpe if out_of_sample_sharpe is not None else 'missing'} < {policy.min_out_of_sample_sharpe}"
        )
    if mean_spearman_ic is None or mean_spearman_ic < policy.min_mean_spearman_ic:
        reasons.append(
            f"mean_spearman_ic {mean_spearman_ic if mean_spearman_ic is not None else 'missing'} < {policy.min_mean_spearman_ic}"
        )
    if positive_fold_ratio is None or positive_fold_ratio < policy.min_positive_fold_ratio:
        reasons.append(
            f"positive_fold_ratio {positive_fold_ratio if positive_fold_ratio is not None else 'missing'} < {policy.min_positive_fold_ratio}"
        )
    if metric_std is not None and policy.max_metric_std is not None and metric_std > policy.max_metric_std:
        reasons.append(f"metric_std {metric_std} > {policy.max_metric_std}")
    if proxy_confidence_score < policy.min_proxy_confidence_score:
        reasons.append(f"proxy_confidence_score {proxy_confidence_score} < {policy.min_proxy_confidence_score}")
    if consistency_score is not None and consistency_score < 0:
        reasons.append("negative_consistency_score")

    if all(pass_checks):
        return "pass", ["validation_pass"], proxy_confidence_score
    if all(weak_checks):
        return "weak", reasons or ["weak_validation"], proxy_confidence_score
    return "fail", reasons or ["validation_fail"], proxy_confidence_score


def build_strategy_validation(
    *,
    artifacts_root: str | Path,
    output_dir: str | Path,
    policy: StrategyValidationPolicyConfig,
) -> dict[str, Any]:
    manifests = load_research_manifests(artifacts_root)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for manifest in manifests:
        top_metrics = manifest.get("top_metrics", {})
        fold_metrics = _fold_metrics(manifest)
        out_of_sample_sharpe = _safe_float(
            _safe_read_json(manifest.get("artifact_paths", {}).get("walk_forward_summary_path")).get("summary", {}).get("mean_sharpe_ratio")
        )
        if out_of_sample_sharpe is None:
            out_of_sample_sharpe = _safe_float(top_metrics.get("portfolio_sharpe")) or _safe_float(fold_metrics.get("mean_metric"))
        mean_spearman_ic = _safe_float(top_metrics.get("mean_spearman_ic"))
        status, reasons, proxy_confidence_score = _validation_status(
            fold_metrics=fold_metrics,
            out_of_sample_sharpe=out_of_sample_sharpe,
            mean_spearman_ic=mean_spearman_ic,
            policy=policy,
        )
        rows.append(
            {
                "run_id": manifest.get("run_id"),
                "timestamp": manifest.get("timestamp"),
                "signal_family": manifest.get("signal_family"),
                "universe": manifest.get("universe"),
                "in_sample_metrics": {
                    "mean_spearman_ic": mean_spearman_ic,
                    "portfolio_sharpe": _safe_float(top_metrics.get("portfolio_sharpe")),
                    "portfolio_total_return": _safe_float(top_metrics.get("portfolio_total_return")),
                },
                "out_of_sample_metrics": {
                    "mean_metric": fold_metrics.get("mean_metric"),
                    "out_of_sample_sharpe": out_of_sample_sharpe,
                    "positive_fold_ratio": fold_metrics.get("positive_fold_ratio"),
                },
                "walk_forward_metrics": {
                    "number_of_folds": fold_metrics.get("number_of_folds"),
                    "metric_name": fold_metrics.get("metric_name"),
                    "metric_std": fold_metrics.get("metric_std"),
                    "consistency_score": fold_metrics.get("consistency_score"),
                },
                "number_of_folds": fold_metrics.get("number_of_folds"),
                "stability_metric_std": fold_metrics.get("metric_std"),
                "stability_consistency_score": fold_metrics.get("consistency_score"),
                "proxy_confidence_score": proxy_confidence_score,
                "validation_status": status,
                "validation_reason": "; ".join(reasons),
            }
        )

    rows = sorted(
        rows,
        key=lambda row: (
            {"pass": 0, "weak": 1, "fail": 2}.get(str(row.get("validation_status")), 3),
            -(row.get("proxy_confidence_score") or 0.0),
            str(row.get("run_id") or ""),
        ),
    )
    payload = {
        "schema_version": STRATEGY_VALIDATION_SCHEMA_VERSION,
        "generated_at": _now_utc(),
        "artifacts_root": str(Path(artifacts_root)),
        "policy": asdict(policy),
        "summary": {
            "run_count": len(rows),
            "pass_count": sum(1 for row in rows if row["validation_status"] == "pass"),
            "weak_count": sum(1 for row in rows if row["validation_status"] == "weak"),
            "fail_count": sum(1 for row in rows if row["validation_status"] == "fail"),
        },
        "rows": rows,
    }
    json_path = _write_json(output_path / "strategy_validation.json", payload)
    csv_path = output_path / "strategy_validation.csv"
    pd.DataFrame(
        [
            {
                "run_id": row["run_id"],
                "timestamp": row["timestamp"],
                "signal_family": row["signal_family"],
                "universe": row["universe"],
                "number_of_folds": row["number_of_folds"],
                "mean_spearman_ic": row["in_sample_metrics"].get("mean_spearman_ic"),
                "portfolio_sharpe": row["in_sample_metrics"].get("portfolio_sharpe"),
                "out_of_sample_sharpe": row["out_of_sample_metrics"].get("out_of_sample_sharpe"),
                "positive_fold_ratio": row["out_of_sample_metrics"].get("positive_fold_ratio"),
                "stability_metric_std": row["stability_metric_std"],
                "stability_consistency_score": row["stability_consistency_score"],
                "proxy_confidence_score": row["proxy_confidence_score"],
                "validation_status": row["validation_status"],
                "validation_reason": row["validation_reason"],
            }
            for row in rows
        ]
    ).to_csv(csv_path, index=False)
    return {
        "strategy_validation_json_path": str(json_path),
        "strategy_validation_csv_path": str(csv_path),
        "pass_count": payload["summary"]["pass_count"],
        "weak_count": payload["summary"]["weak_count"],
        "fail_count": payload["summary"]["fail_count"],
    }


def load_strategy_validation(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "strategy_validation.json"
    payload = _safe_read_json(path)
    if not payload:
        raise FileNotFoundError(f"Strategy validation artifact not found or invalid: {path}")
    return payload
