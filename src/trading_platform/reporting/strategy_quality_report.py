from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


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


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _read_promoted_rows(promoted_dir: Path) -> list[dict[str, Any]]:
    return list(_read_json(promoted_dir / "promoted_strategies.json").get("strategies", []))


def _read_selected_rows(portfolio_dir: Path) -> list[dict[str, Any]]:
    payload = _read_json(portfolio_dir / "strategy_portfolio.json")
    return list(payload.get("selected_strategies", []))


def _read_activated_rows(activated_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    payload = _read_json(activated_dir / "activated_strategy_portfolio.json")
    return (
        list(payload.get("strategies", [])),
        list(payload.get("active_strategies", [])),
        dict(payload.get("summary") or {}),
    )


def _promotion_candidate_maps(
    promoted_dir: Path,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]]]:
    promoted_payload = _read_json(promoted_dir / "promoted_strategies.json")
    candidates_path = promoted_payload.get("promotion_candidates_path")
    payload = _read_json(candidates_path)
    baseline_rows = {
        (str(row.get("run_id") or ""), str(row.get("signal_family") or "")): row for row in payload.get("rows", [])
    }
    conditional_rows = {
        (str(row.get("run_id") or ""), str(row.get("signal_family") or ""), str(row.get("condition_id") or "")): row
        for row in payload.get("conditional_rows", [])
    }
    return baseline_rows, conditional_rows


def _paper_metrics(paper_output_dir: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    return (
        _read_json(paper_output_dir / "paper_run_summary_latest.json"),
        _read_json(paper_output_dir / "portfolio_performance_summary.json"),
        _read_json(paper_output_dir / "strategy_contribution_summary.json"),
    )


def _build_strategy_rows(
    *,
    promoted_dir: Path,
    portfolio_dir: Path,
    activated_dir: Path,
    paper_output_dir: Path,
    run_name: str,
) -> list[dict[str, Any]]:
    promoted_rows = _read_promoted_rows(promoted_dir)
    selected_rows = _read_selected_rows(portfolio_dir)
    activated_rows, active_rows, activation_summary = _read_activated_rows(activated_dir)
    paper_summary, performance_summary, contribution_summary = _paper_metrics(paper_output_dir)
    baseline_map, conditional_map = _promotion_candidate_maps(promoted_dir)

    promoted_by_name = {str(row.get("preset_name") or ""): row for row in promoted_rows}
    activated_by_name = {str(row.get("preset_name") or ""): row for row in activated_rows}
    active_names = {str(row.get("preset_name") or "") for row in active_rows}
    capital_weights = dict(contribution_summary.get("normalized_capital_weights") or {})
    sleeve_contribution = dict(contribution_summary.get("sleeve_contribution") or {})
    as_of = str(_coalesce(paper_summary.get("timestamp"), contribution_summary.get("as_of"), _now_utc()))

    rows: list[dict[str, Any]] = []
    for row in selected_rows:
        preset_name = str(row.get("preset_name") or "")
        promoted = promoted_by_name.get(preset_name, {})
        activated = activated_by_name.get(preset_name, row)
        signal_family = str(_coalesce(row.get("signal_family"), promoted.get("signal_family"), "unknown"))
        condition_id = str(_coalesce(row.get("condition_id"), promoted.get("condition_id"), "")) or None
        source_run_id = str(_coalesce(row.get("source_run_id"), promoted.get("source_run_id"), ""))
        baseline_candidate = baseline_map.get((source_run_id, signal_family), {})
        conditional_candidate = conditional_map.get((source_run_id, signal_family, condition_id or ""), {})

        mean_spearman_ic = _coalesce(
            promoted.get("mean_spearman_ic"),
            (
                conditional_candidate.get("metric_value")
                if str(promoted.get("ranking_metric") or "") == "mean_spearman_ic"
                else None
            ),
            baseline_candidate.get("mean_spearman_ic"),
            row.get("selection_metric_value") if str(row.get("ranking_metric") or "") == "mean_spearman_ic" else None,
        )
        portfolio_sharpe = _coalesce(
            promoted.get("portfolio_sharpe"),
            baseline_candidate.get("portfolio_sharpe"),
            promoted.get("ranking_value") if str(promoted.get("ranking_metric") or "") == "portfolio_sharpe" else None,
            row.get("ranking_value") if str(row.get("ranking_metric") or "") == "portfolio_sharpe" else None,
        )
        overall_turnover = _safe_float(
            _coalesce(paper_summary.get("turnover_estimate"), performance_summary.get("turnover"))
        )
        weight = (
            _safe_float(
                _coalesce(
                    capital_weights.get(preset_name), row.get("allocation_weight"), row.get("target_capital_fraction")
                )
            )
            or 0.0
        )
        max_drawdown = _safe_float(_coalesce(promoted.get("max_drawdown"), performance_summary.get("max_drawdown")))

        rows.append(
            {
                "as_of": as_of,
                "run_name": run_name,
                "strategy_id": preset_name,
                "source_run_id": source_run_id or None,
                "signal_family": signal_family,
                "promotion_variant": _coalesce(
                    row.get("promotion_variant"), promoted.get("promotion_variant"), "unconditional"
                ),
                "condition_id": condition_id,
                "condition_type": _coalesce(row.get("condition_type"), promoted.get("condition_type")),
                "selection_rank": row.get("selection_rank"),
                "is_selected": True,
                "is_active": preset_name in active_names,
                "activation_state": activated.get("activation_state"),
                "activation_reason": activated.get("activation_reason"),
                "portfolio_bucket": _coalesce(activated.get("portfolio_bucket"), row.get("portfolio_bucket")),
                "allocation_weight": _safe_float(row.get("allocation_weight")) or 0.0,
                "target_capital_fraction": _safe_float(row.get("target_capital_fraction")) or 0.0,
                "normalized_capital_weight": weight,
                "sleeve_contribution": _safe_float(sleeve_contribution.get(preset_name)) or 0.0,
                "strategy_weight_metric": _coalesce(
                    row.get("selection_metric"), row.get("ranking_metric"), promoted.get("ranking_metric")
                ),
                "ranking_metric": _coalesce(row.get("ranking_metric"), promoted.get("ranking_metric")),
                "ranking_value": _safe_float(_coalesce(row.get("ranking_value"), promoted.get("ranking_value"))),
                "mean_spearman_ic": _safe_float(mean_spearman_ic),
                "portfolio_sharpe": _safe_float(portfolio_sharpe),
                "max_drawdown": max_drawdown,
                "turnover": (overall_turnover * weight) if overall_turnover is not None else None,
                "hit_rate": None,
                "runtime_computability_pass": bool(
                    _coalesce(
                        promoted.get("runtime_score_validation_pass"),
                        baseline_candidate.get("runtime_computability_pass"),
                        False,
                    )
                ),
                "runtime_computability_reason": _coalesce(
                    promoted.get("runtime_score_validation_reason"),
                    baseline_candidate.get("runtime_computability_reason"),
                ),
                "runtime_computable_symbol_count": _safe_float(
                    _coalesce(
                        promoted.get("runtime_computable_symbol_count"),
                        conditional_candidate.get("runtime_computable_symbol_count"),
                        baseline_candidate.get("runtime_computable_symbol_count"),
                    )
                ),
                "execution_ready": bool(promoted.get("execution_ready", True)),
                "shadow_only": bool(promoted.get("shadow_only", False)),
                "generated_preset_path": promoted.get("generated_preset_path"),
                "signal_artifact_path": promoted.get("signal_artifact_path"),
                "daily_pnl_proxy": (_safe_float(performance_summary.get("total_pnl")) or 0.0) * weight,
                "daily_return_proxy": (_safe_float(performance_summary.get("total_return")) or 0.0) * weight,
                "observation_source": "daily_trading_snapshot",
                "drawdown_source": "paper_portfolio_proxy" if max_drawdown is not None else "unavailable",
            }
        )
    return rows


def _read_history_rows(output_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in output_root.rglob("strategy_performance_history.csv"):
        try:
            frame = pd.read_csv(path)
        except (pd.errors.EmptyDataError, pd.errors.ParserError):
            continue
        rows.extend(frame.to_dict(orient="records"))
    return rows


def _dedupe_history(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in ("as_of", "run_name", "strategy_id", "signal_family"):
        if column not in frame.columns:
            frame[column] = None
    frame = frame.drop_duplicates(subset=["as_of", "run_name", "strategy_id"], keep="last")
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce")
    frame = frame.sort_values(["strategy_id", "as_of", "run_name"], kind="stable")
    return frame


def _rolling_sharpe_rows(history: pd.DataFrame) -> list[dict[str, Any]]:
    if history.empty:
        return []
    rows: list[dict[str, Any]] = []
    for strategy_id, group in history.groupby("strategy_id", dropna=False):
        series = pd.to_numeric(group.get("daily_return_proxy"), errors="coerce")
        timestamps = group.get("as_of")
        for idx in range(len(group)):
            window = series.iloc[max(0, idx - 19) : idx + 1].dropna()
            if window.empty:
                rolling_sharpe = None
            elif len(window) == 1 or float(window.std(ddof=0) or 0.0) == 0.0:
                rolling_sharpe = float(window.iloc[-1])
            else:
                rolling_sharpe = float((window.mean() / window.std(ddof=0)) * (252**0.5))
            rows.append(
                {
                    "as_of": timestamps.iloc[idx].strftime("%Y-%m-%d") if not pd.isna(timestamps.iloc[idx]) else None,
                    "strategy_id": strategy_id,
                    "signal_family": group.iloc[idx].get("signal_family"),
                    "observation_count": int(idx + 1),
                    "rolling_sharpe": rolling_sharpe,
                    "hit_rate": float((window > 0).mean()) if not window.empty else None,
                    "turnover": _safe_float(group.iloc[idx].get("turnover")),
                }
            )
    return rows


def _rolling_ic_rows(history: pd.DataFrame) -> list[dict[str, Any]]:
    if history.empty:
        return []
    rows: list[dict[str, Any]] = []
    for signal_family, group in history.groupby("signal_family", dropna=False):
        series = pd.to_numeric(group.get("mean_spearman_ic"), errors="coerce")
        timestamps = group.get("as_of")
        for idx in range(len(group)):
            window = series.iloc[max(0, idx - 19) : idx + 1].dropna()
            rows.append(
                {
                    "as_of": timestamps.iloc[idx].strftime("%Y-%m-%d") if not pd.isna(timestamps.iloc[idx]) else None,
                    "signal_family": signal_family,
                    "observation_count": int(idx + 1),
                    "rolling_ic": float(window.mean()) if not window.empty else None,
                    "ic_stability_std": (
                        float(window.std(ddof=0)) if len(window) > 1 else 0.0 if len(window) == 1 else None
                    ),
                }
            )
    return rows


def build_strategy_quality_report(
    *,
    promoted_dir: str | Path,
    portfolio_dir: str | Path,
    activated_dir: str | Path,
    paper_output_dir: str | Path,
    output_root: str | Path,
    run_name: str,
) -> dict[str, Any]:
    current_rows = _build_strategy_rows(
        promoted_dir=Path(promoted_dir),
        portfolio_dir=Path(portfolio_dir),
        activated_dir=Path(activated_dir),
        paper_output_dir=Path(paper_output_dir),
        run_name=run_name,
    )
    historical_rows = _read_history_rows(Path(output_root))
    history = _dedupe_history(historical_rows + current_rows)
    comparison_rows = current_rows
    drawdown_rows = [
        {
            "as_of": row.get("as_of"),
            "strategy_id": row.get("strategy_id"),
            "signal_family": row.get("signal_family"),
            "max_drawdown": row.get("max_drawdown"),
            "drawdown_source": row.get("drawdown_source"),
            "runtime_computability_pass": row.get("runtime_computability_pass"),
        }
        for row in current_rows
    ]
    return {
        "generated_at": _now_utc(),
        "summary": {
            "strategy_count": len(comparison_rows),
            "active_strategy_count": sum(1 for row in comparison_rows if row.get("is_active")),
            "signal_family_count": len({str(row.get("signal_family") or "") for row in comparison_rows}),
            "history_row_count": int(len(history)),
        },
        "strategy_comparison_rows": comparison_rows,
        "strategy_performance_history_rows": history.to_dict(orient="records") if not history.empty else [],
        "rolling_sharpe_rows": _rolling_sharpe_rows(history),
        "rolling_ic_rows": _rolling_ic_rows(history),
        "drawdown_rows": drawdown_rows,
    }


def write_strategy_quality_report(
    *,
    report: dict[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    comparison_path = output_path / "strategy_comparison_summary.csv"
    history_path = output_path / "strategy_performance_history.csv"
    sharpe_path = output_path / "rolling_sharpe_by_strategy.csv"
    ic_path = output_path / "rolling_ic_by_signal.csv"
    drawdown_path = output_path / "drawdown_by_strategy.csv"
    summary_path = output_path / "strategy_quality_summary.json"

    pd.DataFrame(report.get("strategy_comparison_rows", [])).to_csv(comparison_path, index=False)
    pd.DataFrame(report.get("strategy_performance_history_rows", [])).to_csv(history_path, index=False)
    pd.DataFrame(report.get("rolling_sharpe_rows", [])).to_csv(sharpe_path, index=False)
    pd.DataFrame(report.get("rolling_ic_rows", [])).to_csv(ic_path, index=False)
    pd.DataFrame(report.get("drawdown_rows", [])).to_csv(drawdown_path, index=False)
    summary_path.write_text(
        json.dumps(
            {
                "generated_at": report.get("generated_at"),
                "summary": report.get("summary", {}),
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    return {
        "strategy_comparison_summary_path": comparison_path,
        "strategy_performance_history_path": history_path,
        "rolling_sharpe_by_strategy_path": sharpe_path,
        "rolling_ic_by_signal_path": ic_path,
        "drawdown_by_strategy_path": drawdown_path,
        "strategy_quality_summary_path": summary_path,
    }
