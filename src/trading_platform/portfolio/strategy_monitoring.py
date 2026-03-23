from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.portfolio.strategy_portfolio import load_strategy_portfolio


STRATEGY_MONITORING_SCHEMA_VERSION = 1
RECOMMENDATION_PRIORITY = {"keep": 0, "review": 1, "reduce": 2, "deactivate": 3}


@dataclass(frozen=True)
class StrategyMonitoringPolicyConfig:
    schema_version: int = STRATEGY_MONITORING_SCHEMA_VERSION
    min_observations: int = 5
    warning_drawdown: float = 0.08
    deactivate_drawdown: float = 0.15
    warning_realized_sharpe: float = 0.5
    deactivate_realized_sharpe: float = 0.0
    max_drift_from_expected: float | None = 1.0
    max_underperformance_streak: int | None = 5
    max_missing_data_days: int | None = 3
    include_inactive_strategies: bool = True
    kill_switch_mode: str = "recommendation_only"
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.schema_version != STRATEGY_MONITORING_SCHEMA_VERSION:
            raise ValueError(f"Unsupported strategy monitoring schema_version: {self.schema_version}")
        if self.min_observations < 0:
            raise ValueError("min_observations must be >= 0")
        if self.warning_drawdown < 0 or self.deactivate_drawdown < 0:
            raise ValueError("drawdown thresholds must be >= 0")
        if self.deactivate_drawdown < self.warning_drawdown:
            raise ValueError("deactivate_drawdown must be >= warning_drawdown")
        if self.max_underperformance_streak is not None and self.max_underperformance_streak < 0:
            raise ValueError("max_underperformance_streak must be >= 0 when provided")
        if self.max_missing_data_days is not None and self.max_missing_data_days < 0:
            raise ValueError("max_missing_data_days must be >= 0 when provided")
        if self.max_drift_from_expected is not None and self.max_drift_from_expected < 0:
            raise ValueError("max_drift_from_expected must be >= 0 when provided")
        if self.warning_realized_sharpe < self.deactivate_realized_sharpe:
            raise ValueError("warning_realized_sharpe must be >= deactivate_realized_sharpe")
        if self.kill_switch_mode not in {"recommendation_only", "status_artifact"}:
            raise ValueError("kill_switch_mode must be one of: recommendation_only, status_artifact")


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists():
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


def _safe_int(value: Any) -> int | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    serialized = dict(row)
    for key in ["warning_flags", "recommendation_reasons"]:
        serialized[key] = "|".join(serialized.get(key, []))
    return serialized


def _timestamp_series(frame: pd.DataFrame) -> pd.Series | None:
    for column in ["rebalance_timestamp", "timestamp", "as_of"]:
        if column in frame.columns:
            return pd.to_datetime(frame[column], errors="coerce", utc=True)
    return None


def _equity_metrics(equity_curve: pd.DataFrame) -> dict[str, Any]:
    if equity_curve.empty or "equity" not in equity_curve.columns:
        return {
            "realized_return": None,
            "realized_volatility": None,
            "realized_sharpe": None,
            "drawdown": None,
            "observation_count": 0,
            "underperformance_streak": 0,
            "monitoring_window": "",
            "latest_timestamp": None,
        }
    frame = equity_curve.copy()
    timestamps = _timestamp_series(frame)
    if timestamps is not None:
        frame = frame.assign(_timestamp=timestamps).dropna(subset=["_timestamp"]).sort_values("_timestamp", kind="stable")
    else:
        frame = frame.reset_index(drop=True)

    if frame.empty:
        return {
            "realized_return": None,
            "realized_volatility": None,
            "realized_sharpe": None,
            "drawdown": None,
            "observation_count": 0,
            "underperformance_streak": 0,
            "monitoring_window": "",
            "latest_timestamp": None,
        }

    equity = frame["equity"].astype(float)
    first = float(equity.iloc[0])
    last = float(equity.iloc[-1])
    realized_return = ((last / first) - 1.0) if first > 0 else None

    returns = equity.pct_change().dropna()
    if len(returns) >= 2 and float(returns.std()) > 0:
        realized_volatility = float(returns.std() * (252**0.5))
        realized_sharpe = float((returns.mean() / returns.std()) * (252**0.5))
    else:
        realized_volatility = None
        realized_sharpe = None

    running_max = equity.cummax()
    drawdown = float(abs(((equity / running_max) - 1.0).min()))
    underperformance_streak = 0
    for value in reversed(list(returns)):
        if float(value) < 0:
            underperformance_streak += 1
        else:
            break

    if "_timestamp" in frame.columns:
        start = frame["_timestamp"].iloc[0].isoformat()
        end = frame["_timestamp"].iloc[-1].isoformat()
        latest_timestamp = frame["_timestamp"].iloc[-1]
    else:
        start = ""
        end = ""
        latest_timestamp = None

    return {
        "realized_return": float(realized_return) if realized_return is not None else None,
        "realized_volatility": realized_volatility,
        "realized_sharpe": realized_sharpe,
        "drawdown": drawdown,
        "observation_count": int(len(frame)),
        "underperformance_streak": int(underperformance_streak),
        "monitoring_window": f"{start}..{end}" if start or end else "",
        "latest_timestamp": latest_timestamp.isoformat() if latest_timestamp is not None else None,
    }


def _latest_metric(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    return _safe_float(frame.iloc[-1][column])


def _latest_timestamp(frame: pd.DataFrame) -> datetime | None:
    timestamps = _timestamp_series(frame)
    if timestamps is None:
        return None
    valid = timestamps.dropna()
    if valid.empty:
        return None
    return valid.max().to_pydatetime()


def _missing_data_days(paper_summary: pd.DataFrame, equity_curve: pd.DataFrame) -> int | None:
    latest = _latest_timestamp(paper_summary) or _latest_timestamp(equity_curve)
    if latest is None:
        return None
    return int((datetime.now(UTC).date() - latest.astimezone(UTC).date()).days)


def _portfolio_allocation_lookup(allocation_dir: str | Path | None) -> tuple[dict[str, float], dict[str, int], dict[str, float]]:
    if allocation_dir is None:
        return {}, {}, {}
    sleeve_attr = _safe_read_csv(Path(allocation_dir) / "sleeve_attribution.csv")
    sleeve_targets = _safe_read_csv(Path(allocation_dir) / "sleeve_target_weights.csv")
    gross_lookup: dict[str, float] = {}
    position_lookup: dict[str, int] = {}
    overlap_lookup: dict[str, float] = {}

    if not sleeve_attr.empty and "sleeve_name" in sleeve_attr.columns:
        gross_col = "gross_contribution" if "gross_contribution" in sleeve_attr.columns else None
        if gross_col is not None:
            gross_lookup = {
                str(row["sleeve_name"]): float(row[gross_col])
                for row in sleeve_attr.to_dict(orient="records")
                if _safe_float(row.get(gross_col)) is not None
            }
    if not sleeve_targets.empty and "sleeve_name" in sleeve_targets.columns:
        grouped = sleeve_targets.groupby("sleeve_name", as_index=False)
        position_lookup = {
            str(row["sleeve_name"]): int(row["symbol"])
            for row in grouped["symbol"].nunique().to_dict(orient="records")
        }
        if "scaled_target_weight" in sleeve_targets.columns:
            overlap_frame = sleeve_targets.groupby("sleeve_name", as_index=False)["scaled_target_weight"].apply(
                lambda series: float(series.abs().sum())
            )
            overlap_lookup = {
                str(row["sleeve_name"]): float(row["scaled_target_weight"])
                for row in overlap_frame.to_dict(orient="records")
            }
    return gross_lookup, position_lookup, overlap_lookup


def _recommendation_from_signals(
    *,
    current_status: str,
    observation_count: int,
    drawdown: float | None,
    realized_sharpe: float | None,
    drift_from_expected: float | None,
    underperformance_streak: int,
    missing_data_days: int | None,
    policy: StrategyMonitoringPolicyConfig,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    recommendation = "keep"

    def escalate(target: str, reason: str) -> None:
        nonlocal recommendation
        reasons.append(reason)
        if RECOMMENDATION_PRIORITY[target] > RECOMMENDATION_PRIORITY[recommendation]:
            recommendation = target

    if observation_count < policy.min_observations:
        escalate("review", "insufficient_observations")
    if missing_data_days is not None and policy.max_missing_data_days is not None and missing_data_days > policy.max_missing_data_days:
        escalate("deactivate", "missing_data_days_exceeded")
    if drawdown is not None:
        if drawdown >= policy.deactivate_drawdown:
            escalate("deactivate", "drawdown_breach")
        elif drawdown >= policy.warning_drawdown:
            escalate("reduce", "drawdown_warning")
    if realized_sharpe is not None and observation_count >= policy.min_observations:
        if realized_sharpe <= policy.deactivate_realized_sharpe:
            escalate("deactivate", "realized_sharpe_breach")
        elif realized_sharpe <= policy.warning_realized_sharpe:
            escalate("review", "realized_sharpe_warning")
    if (
        drift_from_expected is not None
        and policy.max_drift_from_expected is not None
        and drift_from_expected > policy.max_drift_from_expected
    ):
        escalate("review", "drift_from_expected")
    if (
        policy.max_underperformance_streak is not None
        and underperformance_streak > policy.max_underperformance_streak
    ):
        escalate("reduce", "underperformance_streak")
    if current_status != "active" and recommendation == "keep":
        escalate("review", "status_not_active")
    return recommendation, reasons


def build_strategy_monitoring_snapshot(
    *,
    strategy_portfolio_path: str | Path,
    paper_dir: str | Path,
    execution_dir: str | Path | None,
    allocation_dir: str | Path | None,
    output_dir: str | Path,
    policy: StrategyMonitoringPolicyConfig,
) -> dict[str, Any]:
    portfolio = load_strategy_portfolio(strategy_portfolio_path)
    selected_rows = list(portfolio.get("selected_strategies", []))
    if not selected_rows:
        raise ValueError("Strategy portfolio contains no selected strategies")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    paper_path = Path(paper_dir)
    execution_path = Path(execution_dir) if execution_dir is not None else None
    allocation_path = Path(allocation_dir) if allocation_dir is not None else None

    equity_curve = _safe_read_csv(paper_path / "paper_equity_curve.csv")
    paper_summary = _safe_read_csv(paper_path / "paper_run_summary.csv")
    paper_health_checks = _safe_read_csv(paper_path / "paper_health_checks.csv")
    paper_latest = _safe_read_json(paper_path / "paper_run_summary_latest.json")
    execution_summary = _safe_read_json(execution_path / "execution_summary.json") if execution_path is not None else {}

    portfolio_metrics = _equity_metrics(equity_curve)
    total_return = portfolio_metrics["realized_return"]
    total_drawdown = portfolio_metrics["drawdown"]
    total_sharpe = portfolio_metrics["realized_sharpe"]
    observation_count = int(portfolio_metrics["observation_count"])
    missing_data_days = _missing_data_days(paper_summary, equity_curve)
    latest_turnover = _latest_metric(paper_summary, "turnover_estimate")
    health_failures = int((paper_health_checks["status"] == "fail").sum()) if not paper_health_checks.empty and "status" in paper_health_checks.columns else 0
    gross_lookup, position_lookup, overlap_lookup = _portfolio_allocation_lookup(allocation_path)

    strategy_rows: list[dict[str, Any]] = []
    recommendations: list[dict[str, Any]] = []
    inactive_skipped = 0

    for row in selected_rows:
        current_status = str(row.get("promotion_status") or "active")
        if current_status != "active" and not policy.include_inactive_strategies:
            inactive_skipped += 1
            continue

        preset_name = str(row.get("preset_name") or "")
        weight = float(row.get("target_capital_fraction", row.get("allocation_weight", 0.0)) or 0.0)
        expected_metric = _safe_float(row.get("selection_metric_value"))
        realized_sharpe = total_sharpe
        drift_from_expected = (
            abs(expected_metric - realized_sharpe)
            if expected_metric is not None and realized_sharpe is not None
            else None
        )
        recommendation, reasons = _recommendation_from_signals(
            current_status=current_status,
            observation_count=observation_count,
            drawdown=total_drawdown,
            realized_sharpe=realized_sharpe,
            drift_from_expected=drift_from_expected,
            underperformance_streak=int(portfolio_metrics["underperformance_streak"]),
            missing_data_days=missing_data_days,
            policy=policy,
        )

        warning_flags = sorted(set(reasons))
        proxy_return_contribution = float(total_return * weight) if total_return is not None else None
        proxy_drawdown_contribution = float(total_drawdown * weight) if total_drawdown is not None else None
        strategy_row = {
            "preset_name": preset_name,
            "source_run_id": row.get("source_run_id"),
            "signal_family": row.get("signal_family"),
            "universe": row.get("universe"),
            "current_status": current_status,
            "portfolio_weight": weight,
            "monitoring_window": portfolio_metrics["monitoring_window"],
            "realized_return": None,
            "realized_volatility": None,
            "realized_sharpe": realized_sharpe,
            "drawdown": total_drawdown,
            "turnover": latest_turnover,
            "paper_observation_count": observation_count,
            "live_observation_count": 0,
            "drift_from_expected": drift_from_expected,
            "missing_data_days": missing_data_days,
            "health_failure_count": health_failures,
            "warning_flags": warning_flags,
            "recommendation": recommendation,
            "recommendation_reasons": reasons,
            "attribution_method": "proxy_weight_scaled",
            "attribution_confidence": "low",
            "proxy_return_contribution": proxy_return_contribution,
            "proxy_drawdown_contribution": proxy_drawdown_contribution,
            "allocation_gross_contribution": gross_lookup.get(preset_name, weight),
            "allocation_position_count": position_lookup.get(preset_name),
            "allocation_abs_weight_sum": overlap_lookup.get(preset_name),
        }
        strategy_rows.append(strategy_row)
        if recommendation != "keep":
            recommendations.append(
                {
                    "preset_name": preset_name,
                    "source_run_id": row.get("source_run_id"),
                    "signal_family": row.get("signal_family"),
                    "universe": row.get("universe"),
                    "current_status": current_status,
                    "recommendation": recommendation,
                    "reasons": reasons,
                    "portfolio_weight": weight,
                    "drawdown": total_drawdown,
                    "realized_sharpe": realized_sharpe,
                    "paper_observation_count": observation_count,
                }
            )

    warning_strategy_count = sum(1 for row in strategy_rows if row["recommendation"] in {"review", "reduce", "deactivate"})
    deactivate_count = sum(1 for row in strategy_rows if row["recommendation"] == "deactivate")
    max_weight = max((float(row["portfolio_weight"]) for row in strategy_rows), default=0.0)
    weight_hhi = float(sum(float(row["portfolio_weight"]) ** 2 for row in strategy_rows))
    monitoring_payload = {
        "schema_version": STRATEGY_MONITORING_SCHEMA_VERSION,
        "generated_at": _now_utc(),
        "strategy_portfolio_path": str(Path(strategy_portfolio_path)),
        "paper_dir": str(paper_path),
        "execution_dir": str(execution_path) if execution_path is not None else None,
        "allocation_dir": str(allocation_path) if allocation_path is not None else None,
        "policy": asdict(policy),
        "summary": {
            "aggregate_return": total_return,
            "aggregate_drawdown": total_drawdown,
            "aggregate_realized_sharpe": total_sharpe,
            "aggregate_turnover": latest_turnover,
            "monitoring_window": portfolio_metrics["monitoring_window"],
            "observation_count": observation_count,
            "warning_strategy_count": warning_strategy_count,
            "deactivation_candidate_count": deactivate_count,
            "selected_strategy_count": len(strategy_rows),
            "inactive_skipped_count": inactive_skipped,
            "max_strategy_weight": max_weight,
            "strategy_weight_hhi": weight_hhi,
            "estimated_execution_cost": _safe_float(execution_summary.get("expected_total_cost")),
            "estimated_rejected_order_count": _safe_int(execution_summary.get("rejected_order_count")),
            "status": "warning" if warning_strategy_count > 0 else "healthy",
        },
        "strategies": strategy_rows,
        "attribution_summary": {
            "method": "proxy_weight_scaled",
            "confidence": "low",
            "explanation": "Per-strategy paper PnL was not available, so aggregate paper metrics were scaled by strategy portfolio weights.",
        },
        "kill_switch_recommendations": recommendations,
        "warnings": list(portfolio.get("warnings", [])),
        "source_summary": paper_latest.get("summary", paper_latest),
    }

    monitoring_json_path = output_path / "strategy_monitoring.json"
    monitoring_json_path.write_text(json.dumps(monitoring_payload, indent=2, default=str), encoding="utf-8")
    monitoring_csv_path = output_path / "strategy_monitoring.csv"
    pd.DataFrame([_serialize_row(row) for row in strategy_rows]).to_csv(monitoring_csv_path, index=False)

    attribution_csv_path = output_path / "strategy_attribution.csv"
    pd.DataFrame(
        [
            {
                "preset_name": row["preset_name"],
                "portfolio_weight": row["portfolio_weight"],
                "attribution_method": row["attribution_method"],
                "attribution_confidence": row["attribution_confidence"],
                "proxy_return_contribution": row["proxy_return_contribution"],
                "proxy_drawdown_contribution": row["proxy_drawdown_contribution"],
                "allocation_gross_contribution": row["allocation_gross_contribution"],
                "allocation_position_count": row["allocation_position_count"],
            }
            for row in strategy_rows
        ]
    ).to_csv(attribution_csv_path, index=False)

    kill_payload = {
        "generated_at": _now_utc(),
        "mode": policy.kill_switch_mode,
        "summary": {
            "recommendation_count": len(recommendations),
            "deactivate_count": sum(1 for item in recommendations if item["recommendation"] == "deactivate"),
            "reduce_count": sum(1 for item in recommendations if item["recommendation"] == "reduce"),
            "review_count": sum(1 for item in recommendations if item["recommendation"] == "review"),
        },
        "recommendations": recommendations,
    }
    kill_json_path = output_path / "kill_switch_recommendations.json"
    kill_json_path.write_text(json.dumps(kill_payload, indent=2, default=str), encoding="utf-8")
    kill_csv_path = output_path / "kill_switch_recommendations.csv"
    pd.DataFrame(
        [
            {**item, "reasons": "|".join(item.get("reasons", []))}
            for item in recommendations
        ]
    ).to_csv(kill_csv_path, index=False)

    return {
        "strategy_monitoring_json_path": str(monitoring_json_path),
        "strategy_monitoring_csv_path": str(monitoring_csv_path),
        "strategy_attribution_csv_path": str(attribution_csv_path),
        "kill_switch_recommendations_json_path": str(kill_json_path),
        "kill_switch_recommendations_csv_path": str(kill_csv_path),
        "warning_strategy_count": warning_strategy_count,
        "deactivation_candidate_count": deactivate_count,
    }


def load_strategy_monitoring(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "strategy_monitoring.json"
    payload = _safe_read_json(path)
    if not payload:
        raise FileNotFoundError(f"Strategy monitoring artifact not found or invalid: {path}")
    return payload


def recommend_kill_switch_actions(
    *,
    strategy_monitoring_path: str | Path,
    output_dir: str | Path | None = None,
    include_review: bool = False,
) -> dict[str, Any]:
    monitoring_path = Path(strategy_monitoring_path)
    payload = load_strategy_monitoring(monitoring_path)
    recommendations = [
        item
        for item in payload.get("kill_switch_recommendations", [])
        if include_review or item.get("recommendation") in {"reduce", "deactivate"}
    ]
    recommendation_payload = {
        "generated_at": _now_utc(),
        "source_strategy_monitoring_path": str(monitoring_path),
        "summary": {
            "recommendation_count": len(recommendations),
            "deactivate_count": sum(1 for item in recommendations if item.get("recommendation") == "deactivate"),
            "reduce_count": sum(1 for item in recommendations if item.get("recommendation") == "reduce"),
            "review_count": sum(1 for item in recommendations if item.get("recommendation") == "review"),
        },
        "recommendations": recommendations,
    }
    output_path = Path(output_dir) if output_dir is not None else (monitoring_path if monitoring_path.is_dir() else monitoring_path.parent)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "kill_switch_recommendations.json"
    csv_path = output_path / "kill_switch_recommendations.csv"
    json_path.write_text(json.dumps(recommendation_payload, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(
        [
            {**item, "reasons": "|".join(item.get("reasons", []))}
            for item in recommendations
        ]
    ).to_csv(csv_path, index=False)
    return {
        "kill_switch_recommendations_json_path": str(json_path),
        "kill_switch_recommendations_csv_path": str(csv_path),
        "recommendation_count": len(recommendations),
    }
