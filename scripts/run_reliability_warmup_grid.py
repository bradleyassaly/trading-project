from __future__ import annotations

import argparse
import json
from itertools import product
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.research.trade_ev_reliability import run_replay_trade_ev_reliability


DEFAULT_BOOTSTRAP_VALUES = [5, 8, 10, 12, 15]
DEFAULT_HISTORY_ROW_VALUES = [5, 10, 15, 20]
DEFAULT_FIT_DAY_VALUES = [0, 1, 3]
DEFAULT_COLD_START_BEHAVIORS = ["disabled_passthrough", "neutral_score"]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _summarize_fit_day_metrics(training_audit_rows: list[dict[str, Any]]) -> dict[str, float]:
    fit_rows = [row for row in training_audit_rows if bool(row.get("did_fit_model", False))]
    if not fit_rows:
        return {
            "fit_days": 0,
            "avg_training_row_count_on_fit_days": 0.0,
            "min_training_row_count_on_fit_days": 0,
            "avg_positive_label_rate_on_fit_days": 0.0,
        }
    training_counts = [_safe_int(row.get("training_row_count", 0)) for row in fit_rows]
    positive_rates = [_safe_float(row.get("positive_label_rate", 0.0)) for row in fit_rows]
    return {
        "fit_days": len(fit_rows),
        "avg_training_row_count_on_fit_days": sum(training_counts) / len(training_counts),
        "min_training_row_count_on_fit_days": min(training_counts),
        "avg_positive_label_rate_on_fit_days": sum(positive_rates) / len(positive_rates),
    }


def _build_row(
    *,
    replay_summary: dict[str, Any],
    result: dict[str, Any],
    bootstrap_min_training_samples: int,
    enabled_after_min_history_rows: int,
    enabled_after_min_fit_days: int,
    cold_start_behavior: str,
) -> dict[str, Any]:
    summary = dict(result.get("summary") or {})
    training_audit_rows = list(result.get("training_audit_rows") or [])
    fit_metrics = _summarize_fit_day_metrics(training_audit_rows)
    return {
        "bootstrap_min_training_samples": bootstrap_min_training_samples,
        "enabled_after_min_history_rows": enabled_after_min_history_rows,
        "enabled_after_min_fit_days": enabled_after_min_fit_days,
        "cold_start_behavior": cold_start_behavior,
        "days_reliability_active": _safe_int(summary.get("days_reliability_active")),
        "days_reliability_inactive_cold_start": _safe_int(summary.get("days_reliability_inactive_cold_start")),
        "fit_days": _safe_int(fit_metrics.get("fit_days")),
        "training_fallback_reason_counts": json.dumps(summary.get("training_fallback_reason_counts", {}), sort_keys=True),
        "scoring_fallback_reason_counts": json.dumps(summary.get("scoring_fallback_reason_counts", {}), sort_keys=True),
        "total_rows_dropped_missing_predicted_return": _safe_int(
            summary.get("total_rows_dropped_missing_predicted_return")
        ),
        "total_rows_dropped_missing_ev_score": _safe_int(summary.get("total_rows_dropped_missing_ev_fields")),
        "avg_training_row_count_on_fit_days": _safe_float(fit_metrics.get("avg_training_row_count_on_fit_days")),
        "min_training_row_count_on_fit_days": _safe_int(fit_metrics.get("min_training_row_count_on_fit_days")),
        "avg_positive_label_rate_on_fit_days": _safe_float(fit_metrics.get("avg_positive_label_rate_on_fit_days")),
        "reliability_score_std": _safe_float(summary.get("reliability_score_std")),
        "reliability_unique_value_count": _safe_int(summary.get("reliability_unique_value_count")),
        "reliability_rank_ic": _safe_float(summary.get("reliability_rank_ic")),
        "combined_rank_ic": _safe_float(summary.get("combined_rank_ic")),
        "reliability_top_vs_bottom_after_cost_spread": _safe_float(
            summary.get("reliability_top_vs_bottom_after_cost_spread")
        ),
        "net_total_pnl": replay_summary.get("net_total_pnl"),
        "total_execution_cost": replay_summary.get("total_execution_cost"),
        "cost_drag_pct": replay_summary.get("cost_drag_pct"),
        "total_order_count": replay_summary.get("total_order_count"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a post-hoc reliability warm-up threshold grid.")
    parser.add_argument("--replay-root", required=True, help="Replay root to evaluate.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for grid artifacts. Defaults to <replay-root>/reliability_warmup_grid.",
    )
    args = parser.parse_args()

    replay_root = Path(args.replay_root).resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else replay_root / "reliability_warmup_grid"
    output_dir.mkdir(parents=True, exist_ok=True)

    replay_summary = _read_json(replay_root / "replay_summary.json")
    rows: list[dict[str, Any]] = []

    for bootstrap, history_rows, fit_days, cold_start_behavior in product(
        DEFAULT_BOOTSTRAP_VALUES,
        DEFAULT_HISTORY_ROW_VALUES,
        DEFAULT_FIT_DAY_VALUES,
        DEFAULT_COLD_START_BEHAVIORS,
    ):
        run_name = f"b{bootstrap}_h{history_rows}_f{fit_days}_{cold_start_behavior}"
        run_output_dir = output_dir / run_name
        result = run_replay_trade_ev_reliability(
            replay_root=replay_root,
            output_root=run_output_dir,
            ev_config_overrides={
                "ev_gate_reliability_model_type": "gradient_boosting",
                "ev_gate_reliability_target_type": "top_bucket_realized_return",
                "ev_gate_reliability_usage_mode": "reranking_only",
                "ev_gate_reliability_bootstrap_min_training_samples": bootstrap,
                "ev_gate_reliability_enabled_after_min_history_rows": history_rows,
                "ev_gate_reliability_enabled_after_min_fit_days": fit_days,
                "ev_gate_reliability_cold_start_behavior": cold_start_behavior,
            },
        )
        row = _build_row(
            replay_summary=replay_summary,
            result=result,
            bootstrap_min_training_samples=bootstrap,
            enabled_after_min_history_rows=history_rows,
            enabled_after_min_fit_days=fit_days,
            cold_start_behavior=cold_start_behavior,
        )
        row["run_name"] = run_name
        rows.append(row)

    summary_frame = pd.DataFrame(rows)
    summary_frame.sort_values(
        by=[
            "days_reliability_active",
            "reliability_unique_value_count",
            "reliability_score_std",
            "reliability_rank_ic",
            "reliability_top_vs_bottom_after_cost_spread",
        ],
        ascending=[False, False, False, False, False],
        inplace=True,
    )
    summary_path = output_dir / "replay_ev_reliability_warmup_grid_summary.csv"
    summary_frame.to_csv(summary_path, index=False)
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
