from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.cli.common import compound_return_pct
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.research.service import run_vectorized_research_on_df
from trading_platform.signals.loaders import load_feature_frame


STATUS_ORDER = {
    "pass": 0,
    "review": 1,
    "fail": 2,
}


@dataclass(frozen=True)
class SignalValidationConfig:
    symbols: list[str]
    strategy: str
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None
    fast_values: list[int] | None = None
    slow_values: list[int] | None = None
    lookback_values: list[int] | None = None
    cash: float = 10_000.0
    commission: float = 0.001
    rebalance_frequency: str = "daily"
    select_by: str = "Sharpe Ratio"
    train_years: int = 5
    test_years: int = 1
    min_train_rows: int = 252
    min_test_rows: int = 126
    output_dir: Path = Path("artifacts/validate_signal")


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(result):
        return None
    return result


def _prepare_price_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    working = df.copy()

    if "Date" in working.columns:
        working["Date"] = pd.to_datetime(working["Date"])
        date_col = "Date"
    elif "timestamp" in working.columns:
        working["timestamp"] = pd.to_datetime(working["timestamp"])
        date_col = "timestamp"
    else:
        working.index = pd.to_datetime(working.index)
        working = working.reset_index().rename(columns={"index": "Date"})
        date_col = "Date"

    return working.sort_values(date_col).reset_index(drop=True), date_col


def build_validation_param_grid(config: SignalValidationConfig) -> list[dict[str, int | None]]:
    if config.strategy == "sma_cross":
        fast_values = list(dict.fromkeys(config.fast_values or [config.fast]))
        slow_values = list(dict.fromkeys(config.slow_values or [config.slow]))
        grid = [
            {"fast": fast, "slow": slow, "lookback": None}
            for fast in fast_values
            for slow in slow_values
            if fast is not None and slow is not None and fast < slow
        ]
        if not grid:
            raise ValueError("sma_cross validation requires valid fast/slow params or sweep values")
        return grid

    if config.strategy == "momentum_hold":
        lookback_values = list(dict.fromkeys(config.lookback_values or [config.lookback]))
        grid = [
            {"fast": None, "slow": None, "lookback": lookback}
            for lookback in lookback_values
            if lookback is not None and lookback > 0
        ]
        if not grid:
            raise ValueError("momentum_hold validation requires lookback or lookback sweep values")
        return grid

    raise ValueError(f"Unsupported validation strategy: {config.strategy}")


def _run_vectorized(
    df: pd.DataFrame,
    *,
    symbol: str,
    strategy: str,
    fast: int | None,
    slow: int | None,
    lookback: int | None,
    cash: float,
    commission: float,
    rebalance_frequency: str,
):
    policy = ExecutionPolicy(rebalance_frequency=rebalance_frequency)
    return run_vectorized_research_on_df(
        df=df,
        symbol=symbol,
        strategy=strategy,
        fast=fast or 20,
        slow=slow or 100,
        lookback=lookback or 20,
        cost_per_turnover=commission,
        initial_equity=cash,
        execution_policy=policy,
    )


def _extract_trade_count(timeseries: pd.DataFrame) -> int | None:
    if "effective_position" not in timeseries.columns:
        return None
    position = timeseries["effective_position"].fillna(0.0).astype(float)
    changes = position.diff().fillna(position)
    return int((changes.abs() > 1e-12).sum())


def _extract_in_sample_metrics(
    *,
    symbol: str,
    strategy: str,
    result,
) -> dict[str, object]:
    summary = result.simulation.summary
    return {
        "symbol": symbol,
        "strategy": strategy,
        "in_sample_return_pct": _safe_float(summary.get("total_return")) * 100.0
        if _safe_float(summary.get("total_return")) is not None
        else None,
        "in_sample_sharpe": _safe_float(summary.get("sharpe")),
        "in_sample_max_drawdown_pct": _safe_float(summary.get("max_drawdown")) * 100.0
        if _safe_float(summary.get("max_drawdown")) is not None
        else None,
        "trade_count": _extract_trade_count(result.simulation.timeseries),
    }


def _score_value(metric: object) -> float:
    value = _safe_float(metric)
    if value is None:
        return float("-inf")
    return value


def _build_sweep_leaderboard(
    df: pd.DataFrame,
    *,
    symbol: str,
    config: SignalValidationConfig,
    param_grid: list[dict[str, int | None]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for params in param_grid:
        result = _run_vectorized(
            df,
            symbol=symbol,
            strategy=config.strategy,
            fast=params["fast"],
            slow=params["slow"],
            lookback=params["lookback"],
            cash=config.cash,
            commission=config.commission,
            rebalance_frequency=config.rebalance_frequency,
        )
        metrics = _extract_in_sample_metrics(symbol=symbol, strategy=config.strategy, result=result)
        rows.append(
            {
                "symbol": symbol,
                "strategy": config.strategy,
                "fast": params["fast"],
                "slow": params["slow"],
                "lookback": params["lookback"],
                "return_pct": metrics["in_sample_return_pct"],
                "sharpe": metrics["in_sample_sharpe"],
                "max_drawdown_pct": metrics["in_sample_max_drawdown_pct"],
                "trade_count": metrics["trade_count"],
            }
        )

    leaderboard = pd.DataFrame(rows)
    if leaderboard.empty:
        return leaderboard

    sort_col = "sharpe" if config.select_by == "Sharpe Ratio" else "return_pct"
    return leaderboard.sort_values(
        by=[sort_col, "return_pct"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)


def _walk_forward_windows(
    df: pd.DataFrame,
    *,
    symbol: str,
    date_col: str,
    config: SignalValidationConfig,
    param_grid: list[dict[str, int | None]],
) -> pd.DataFrame:
    start_date = df[date_col].min()
    train_offset = pd.DateOffset(years=config.train_years)
    test_offset = pd.DateOffset(years=config.test_years)
    current_train_start = start_date
    rows: list[dict[str, object]] = []

    while True:
        train_end = current_train_start + train_offset
        test_end = train_end + test_offset

        train_df = df[(df[date_col] >= current_train_start) & (df[date_col] < train_end)]
        test_df = df[(df[date_col] >= train_end) & (df[date_col] < test_end)]

        if len(train_df) < config.min_train_rows or len(test_df) < config.min_test_rows:
            break

        best_params: dict[str, int | None] | None = None
        best_score = float("-inf")

        for params in param_grid:
            train_result = _run_vectorized(
                train_df,
                symbol=symbol,
                strategy=config.strategy,
                fast=params["fast"],
                slow=params["slow"],
                lookback=params["lookback"],
                cash=config.cash,
                commission=config.commission,
                rebalance_frequency=config.rebalance_frequency,
            )
            train_metrics = _extract_in_sample_metrics(
                symbol=symbol,
                strategy=config.strategy,
                result=train_result,
            )
            metric_name = "in_sample_sharpe" if config.select_by == "Sharpe Ratio" else "in_sample_return_pct"
            score = _score_value(train_metrics.get(metric_name))
            if score > best_score:
                best_score = score
                best_params = params

        if best_params is None:
            current_train_start = current_train_start + test_offset
            continue

        test_result = _run_vectorized(
            test_df,
            symbol=symbol,
            strategy=config.strategy,
            fast=best_params["fast"],
            slow=best_params["slow"],
            lookback=best_params["lookback"],
            cash=config.cash,
            commission=config.commission,
            rebalance_frequency=config.rebalance_frequency,
        )
        test_metrics = _extract_in_sample_metrics(
            symbol=symbol,
            strategy=config.strategy,
            result=test_result,
        )
        rows.append(
            {
                "symbol": symbol,
                "train_start": str(pd.Timestamp(current_train_start).date()),
                "train_end": str(pd.Timestamp(train_end).date()),
                "test_start": str(pd.Timestamp(train_end).date()),
                "test_end": str(pd.Timestamp(test_end).date()),
                "fast": best_params["fast"],
                "slow": best_params["slow"],
                "lookback": best_params["lookback"],
                "test_return_pct": test_metrics["in_sample_return_pct"],
                "test_sharpe": test_metrics["in_sample_sharpe"],
                "test_max_drawdown_pct": test_metrics["in_sample_max_drawdown_pct"],
                "trade_count": test_metrics["trade_count"],
            }
        )
        current_train_start = current_train_start + test_offset

    return pd.DataFrame(rows)


def _summarize_walk_forward(walkforward_df: pd.DataFrame) -> dict[str, object]:
    if walkforward_df.empty:
        return {
            "walkforward_window_count": 0,
            "walkforward_mean_return_pct": None,
            "walkforward_mean_sharpe": None,
            "walkforward_compounded_return_pct": None,
            "walkforward_worst_drawdown_pct": None,
            "walkforward_trade_count": None,
        }

    return {
        "walkforward_window_count": int(len(walkforward_df)),
        "walkforward_mean_return_pct": _safe_float(walkforward_df["test_return_pct"].mean()),
        "walkforward_mean_sharpe": _safe_float(walkforward_df["test_sharpe"].mean()),
        "walkforward_compounded_return_pct": compound_return_pct(walkforward_df["test_return_pct"]),
        "walkforward_worst_drawdown_pct": _safe_float(walkforward_df["test_max_drawdown_pct"].min()),
        "walkforward_trade_count": int(walkforward_df["trade_count"].fillna(0).sum()),
    }


def _status_for_record(record: dict[str, object]) -> tuple[str, str]:
    if record.get("error"):
        return "fail", str(record["error"])

    windows = int(record.get("walkforward_window_count") or 0)
    if windows <= 0:
        return "fail", "No valid walk-forward windows"

    in_sample_return = _safe_float(record.get("in_sample_return_pct"))
    wf_return = _safe_float(record.get("walkforward_mean_return_pct"))
    wf_sharpe = _safe_float(record.get("walkforward_mean_sharpe"))
    worst_drawdown = _safe_float(record.get("worst_drawdown_pct"))

    if (
        in_sample_return is not None
        and wf_return is not None
        and wf_sharpe is not None
        and worst_drawdown is not None
        and in_sample_return > 0.0
        and wf_return > 0.0
        and wf_sharpe > 0.0
        and worst_drawdown > -25.0
    ):
        return "pass", "Positive in-sample and walk-forward metrics"

    if (
        in_sample_return is not None
        and wf_return is not None
        and worst_drawdown is not None
        and (in_sample_return <= -5.0 or wf_return <= -2.0 or worst_drawdown <= -35.0)
    ):
        return "fail", "Weak returns or drawdown outside threshold"

    return "review", "Mixed validation metrics"


def build_validation_leaderboard(records: list[dict[str, object]]) -> pd.DataFrame:
    leaderboard = pd.DataFrame(records)
    if leaderboard.empty:
        return leaderboard

    leaderboard["_status_order"] = leaderboard["status"].map(STATUS_ORDER).fillna(99)
    leaderboard = leaderboard.sort_values(
        by=["_status_order", "walkforward_mean_return_pct", "in_sample_return_pct"],
        ascending=[True, False, False],
        na_position="last",
    ).drop(columns="_status_order")
    return leaderboard.reset_index(drop=True)


def _write_json_report(
    *,
    config: SignalValidationConfig,
    records: list[dict[str, object]],
    output_dir: Path,
) -> Path:
    summary = {
        "pass_count": sum(1 for row in records if row.get("status") == "pass"),
        "review_count": sum(1 for row in records if row.get("status") == "review"),
        "fail_count": sum(1 for row in records if row.get("status") == "fail"),
    }
    path = output_dir / "validation_report.json"
    path.write_text(
        json.dumps(
            {
                "strategy": config.strategy,
                "symbols": config.symbols,
                "summary": summary,
                "reports": records,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def run_signal_validation(config: SignalValidationConfig) -> dict[str, Any]:
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    per_symbol_dir = output_dir / "per_symbol"
    per_symbol_dir.mkdir(parents=True, exist_ok=True)

    param_grid = build_validation_param_grid(config)
    records: list[dict[str, object]] = []

    for symbol in config.symbols:
        summary_row: dict[str, object] = {
            "symbol": symbol,
            "strategy": config.strategy,
            "selected_fast": None,
            "selected_slow": None,
            "selected_lookback": None,
            "in_sample_return_pct": None,
            "walkforward_mean_return_pct": None,
            "walkforward_mean_sharpe": None,
            "worst_drawdown_pct": None,
            "trade_count": None,
            "status": "fail",
            "reason": None,
            "error": None,
        }

        try:
            feature_df = load_feature_frame(symbol)
            prepared_df, date_col = _prepare_price_frame(feature_df)
            if len(prepared_df) < max(config.min_train_rows + config.min_test_rows, 2):
                raise ValueError(
                    f"Insufficient data for validation: have {len(prepared_df)} rows, "
                    f"need at least {config.min_train_rows + config.min_test_rows}"
                )

            baseline_result = _run_vectorized(
                prepared_df,
                symbol=symbol,
                strategy=config.strategy,
                fast=config.fast,
                slow=config.slow,
                lookback=config.lookback,
                cash=config.cash,
                commission=config.commission,
                rebalance_frequency=config.rebalance_frequency,
            )
            baseline_metrics = _extract_in_sample_metrics(
                symbol=symbol,
                strategy=config.strategy,
                result=baseline_result,
            )

            sweep_df = _build_sweep_leaderboard(
                prepared_df,
                symbol=symbol,
                config=config,
                param_grid=param_grid,
            )
            walkforward_df = _walk_forward_windows(
                prepared_df,
                symbol=symbol,
                date_col=date_col,
                config=config,
                param_grid=param_grid,
            )
            walkforward_summary = _summarize_walk_forward(walkforward_df)

            summary_row.update(baseline_metrics)
            summary_row.update(walkforward_summary)
            if not sweep_df.empty:
                best_row = sweep_df.iloc[0]
                summary_row["selected_fast"] = best_row.get("fast")
                summary_row["selected_slow"] = best_row.get("slow")
                summary_row["selected_lookback"] = best_row.get("lookback")

            drawdown_values = [
                value
                for value in [
                    _safe_float(summary_row.get("in_sample_max_drawdown_pct")),
                    _safe_float(summary_row.get("walkforward_worst_drawdown_pct")),
                ]
                if value is not None
            ]
            summary_row["worst_drawdown_pct"] = min(drawdown_values) if drawdown_values else None
            if summary_row.get("trade_count") is None:
                summary_row["trade_count"] = walkforward_summary.get("walkforward_trade_count")

            status, reason = _status_for_record(summary_row)
            summary_row["status"] = status
            summary_row["reason"] = reason

            sweep_path = per_symbol_dir / f"{symbol}_sweep.csv"
            sweep_df.to_csv(sweep_path, index=False)

            windows_path = per_symbol_dir / f"{symbol}_walkforward_windows.csv"
            walkforward_df.to_csv(windows_path, index=False)

        except Exception as exc:
            summary_row["error"] = f"{type(exc).__name__}: {exc}"
            status, reason = _status_for_record(summary_row)
            summary_row["status"] = status
            summary_row["reason"] = reason

        symbol_summary_df = pd.DataFrame([summary_row])
        symbol_summary_path = per_symbol_dir / f"{symbol}_summary.csv"
        symbol_summary_df.to_csv(symbol_summary_path, index=False)
        summary_row["summary_csv"] = str(symbol_summary_path)
        records.append(summary_row)

    leaderboard = build_validation_leaderboard(records)
    leaderboard_path = output_dir / "validation_leaderboard.csv"
    leaderboard.to_csv(leaderboard_path, index=False)

    report_path = _write_json_report(
        config=config,
        records=leaderboard.to_dict(orient="records"),
        output_dir=output_dir,
    )

    return {
        "records": records,
        "leaderboard": leaderboard,
        "leaderboard_path": leaderboard_path,
        "report_path": report_path,
        "output_dir": output_dir,
    }
