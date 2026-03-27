from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.config.models import MultiStrategyPortfolioConfig
from trading_platform.execution.realism import ExecutionConfig
from trading_platform.paper.models import PaperTradingConfig, PaperTradingRunResult
from trading_platform.paper.persistence import persist_paper_run_outputs
from trading_platform.paper.service import JsonPaperStateStore, run_paper_trading_cycle_for_targets, write_paper_trading_artifacts
from trading_platform.portfolio.multi_strategy import allocate_multi_strategy_portfolio, write_multi_strategy_artifacts
from trading_platform.simulation.metrics import summarize_equity_curve


@dataclass(frozen=True)
class MultiStrategyReplayStep:
    requested_date: str
    processed_date: str
    step_output_dir: Path
    result: PaperTradingRunResult
    allocation_summary: dict[str, Any]


@dataclass(frozen=True)
class MultiStrategyReplayResult:
    steps: list[MultiStrategyReplayStep]
    skipped_dates: list[dict[str, Any]]
    requested_dates: list[str]
    output_dir: Path
    state_path: Path
    artifact_paths: dict[str, Path]
    summary: dict[str, Any]


def build_requested_replay_dates(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    explicit_dates: list[str] | None = None,
    max_steps: int | None = None,
) -> list[str]:
    if explicit_dates:
        ordered = [str(pd.Timestamp(item).date()) for item in explicit_dates]
    elif start_date and end_date:
        ordered = [
            str(timestamp.date())
            for timestamp in pd.bdate_range(start=str(pd.Timestamp(start_date).date()), end=str(pd.Timestamp(end_date).date()))
        ]
    else:
        raise ValueError("Replay requires either explicit_dates or both start_date and end_date")

    deduped = list(dict.fromkeys(ordered))
    if max_steps is not None:
        return deduped[: max(int(max_steps), 0)]
    return deduped


def _build_multi_strategy_paper_config(result, reserve_cash_pct: float, *, as_of_date: str) -> PaperTradingConfig:
    symbols = sorted(result.combined_target_weights)
    return PaperTradingConfig(
        symbols=symbols,
        preset_name="multi_strategy",
        universe_name=f"{result.summary['enabled_sleeve_count']}_sleeves",
        strategy="multi_strategy",
        signal_source="legacy",
        reserve_cash_pct=reserve_cash_pct,
        replay_as_of_date=as_of_date,
    )


def _build_target_diagnostics(allocation_result, handoff_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "portfolio_construction_mode": "multi_strategy",
        "rebalance_timestamp": allocation_result.as_of,
        "selected_symbols": ",".join(sorted(set(row["symbol"] for row in allocation_result.sleeve_rows))),
        "target_selected_symbols": ",".join(sorted(allocation_result.combined_target_weights)),
        "requested_active_strategy_count": allocation_result.summary.get("requested_active_strategy_count"),
        "requested_symbol_count": allocation_result.summary.get("requested_symbol_count"),
        "pre_validation_target_symbol_count": allocation_result.summary.get("pre_validation_target_symbol_count"),
        "post_validation_target_symbol_count": len(allocation_result.combined_target_weights),
        "usable_symbol_count": allocation_result.summary.get("usable_symbol_count"),
        "skipped_symbol_count": allocation_result.summary.get("skipped_symbol_count"),
        "target_drop_stage": allocation_result.summary.get("target_drop_stage"),
        "zero_target_reason": allocation_result.summary.get("zero_target_reason"),
        "target_drop_reason": allocation_result.summary.get("target_drop_reason"),
        "latest_price_source_summary": allocation_result.summary.get("latest_price_source_summary", {}),
        "generated_preset_path": allocation_result.summary.get("generated_preset_path"),
        "signal_artifact_path": allocation_result.summary.get("signal_artifact_path"),
        "realized_holdings_count": len(allocation_result.combined_target_weights),
        "realized_holdings_minus_top_n": 0,
        "average_gross_exposure": allocation_result.summary["gross_exposure_after_constraints"],
        "liquidity_excluded_count": sum(
            int(bundle.diagnostics.get("liquidity_excluded_count") or 0)
            for bundle in allocation_result.sleeve_bundles
        ),
        "sector_cap_excluded_count": sum(
            1
            for row in allocation_result.summary["symbols_removed_or_clipped"]
            if row["constraint_name"] == "sector_cap"
        ),
        "turnover_cap_binding_count": int(allocation_result.summary["turnover_cap_binding"]),
        "turnover_buffer_blocked_replacements": sum(
            int(bundle.diagnostics.get("turnover_buffer_blocked_replacements") or 0)
            for bundle in allocation_result.sleeve_bundles
        ),
        "semantic_warning": "portfolio_constraints_applied"
        if allocation_result.summary["symbols_removed_or_clipped"]
        else "",
        "target_selected_count": len(allocation_result.combined_target_weights),
        "summary": {
            "mean_turnover": allocation_result.summary["turnover_estimate"],
        },
        "multi_strategy_allocation": allocation_result.summary,
        "strategy_execution_handoff": handoff_summary,
    }


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists():
        path.unlink()


def _append_rows(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    if path.exists():
        existing = pd.read_csv(path)
        frame = pd.concat([existing, frame], ignore_index=True)
    frame.to_csv(path, index=False)
    return path


def _write_replay_artifacts(
    *,
    output_dir: Path,
    steps: list[MultiStrategyReplayStep],
    skipped_dates: list[dict[str, Any]],
    requested_dates: list[str],
    state_path: Path,
) -> tuple[dict[str, Path], dict[str, Any]]:
    equity_path = output_dir / "paper_equity_curve.csv"
    equity_frame = pd.read_csv(equity_path) if equity_path.exists() else pd.DataFrame()
    if not equity_frame.empty:
        working = equity_frame.copy()
        timestamp_col = "timestamp" if "timestamp" in working.columns else "as_of"
        working[timestamp_col] = pd.to_datetime(working[timestamp_col], errors="coerce")
        working = working.dropna(subset=[timestamp_col]).sort_values(timestamp_col)
        working["daily_return"] = working["equity"].astype(float).pct_change().fillna(0.0)
        rolling_max = working["equity"].astype(float).cummax().replace(0.0, pd.NA)
        working["drawdown"] = ((working["equity"].astype(float) / rolling_max) - 1.0).fillna(0.0)
        daily_returns_path = output_dir / "portfolio_daily_returns.csv"
        working[[col for col in [timestamp_col, "equity", "daily_return", "drawdown"] if col in working.columns]].rename(
            columns={timestamp_col: "as_of"}
        ).to_csv(daily_returns_path, index=False)
        metrics = summarize_equity_curve(
            returns=working["daily_return"],
            equity=working["equity"].astype(float),
        )
    else:
        daily_returns_path = output_dir / "portfolio_daily_returns.csv"
        pd.DataFrame(columns=["as_of", "equity", "daily_return", "drawdown"]).to_csv(daily_returns_path, index=False)
        metrics = {}

    execution_rows: list[dict[str, Any]] = []
    target_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []
    for step_index, step in enumerate(steps, start=1):
        accounting = dict(step.result.diagnostics.get("accounting", {}))
        execution_rows.append(
            {
                "step_index": step_index,
                "requested_date": step.requested_date,
                "processed_date": step.processed_date,
                "fill_application_status": accounting.get("fill_application_status"),
                "requested_order_count": len(step.result.orders),
                "fill_count": len(step.result.fills),
                "starting_cash": accounting.get("starting_cash"),
                "ending_cash": accounting.get("ending_cash"),
                "starting_equity": accounting.get("starting_equity"),
                "ending_equity": accounting.get("ending_equity"),
                "realized_pnl_delta": accounting.get("realized_pnl_delta"),
                "unrealized_pnl": accounting.get("unrealized_pnl"),
                "total_pnl": accounting.get("total_pnl"),
                "turnover_estimate": step.allocation_summary.get("turnover_estimate"),
                "active_strategy_count": step.allocation_summary.get("active_strategy_count"),
                "zero_target_reason": step.allocation_summary.get("zero_target_reason"),
            }
        )
        for symbol, weight in sorted(step.result.latest_target_weights.items()):
            target_rows.append(
                {
                    "step_index": step_index,
                    "requested_date": step.requested_date,
                    "processed_date": step.processed_date,
                    "symbol": symbol,
                    "effective_target_weight": float(weight),
                    "scheduled_target_weight": float(step.result.scheduled_target_weights.get(symbol, 0.0)),
                    "latest_price": step.result.latest_prices.get(symbol),
                    "latest_score": step.result.latest_scores.get(symbol),
                }
            )
        for fill in step.result.fills:
            fill_rows.append(
                {
                    "step_index": step_index,
                    "requested_date": step.requested_date,
                    "processed_date": step.processed_date,
                    "symbol": fill.symbol,
                    "side": fill.side,
                    "quantity": int(fill.quantity),
                    "fill_price": float(fill.fill_price),
                    "notional": float(fill.notional),
                    "commission": float(fill.commission),
                    "slippage_bps": float(fill.slippage_bps),
                    "realized_pnl": float(fill.realized_pnl),
                }
            )

    rolling_execution_log_path = _append_rows(output_dir / "rolling_execution_log.csv", execution_rows) if execution_rows else _append_rows(output_dir / "rolling_execution_log.csv", [])
    rolling_target_history_path = _append_rows(output_dir / "rolling_target_history.csv", target_rows) if target_rows else _append_rows(output_dir / "rolling_target_history.csv", [])
    rolling_fill_history_path = _append_rows(output_dir / "rolling_fill_history.csv", fill_rows) if fill_rows else _append_rows(output_dir / "rolling_fill_history.csv", [])

    rolling_paper_run_summary_path = output_dir / "rolling_paper_run_summary.csv"
    if (output_dir / "paper_run_summary.csv").exists():
        shutil.copyfile(output_dir / "paper_run_summary.csv", rolling_paper_run_summary_path)
    else:
        pd.DataFrame().to_csv(rolling_paper_run_summary_path, index=False)

    rolling_position_history_path = output_dir / "rolling_position_history.csv"
    if (output_dir / "paper_positions_history.csv").exists():
        shutil.copyfile(output_dir / "paper_positions_history.csv", rolling_position_history_path)
    else:
        pd.DataFrame().to_csv(rolling_position_history_path, index=False)

    rolling_order_history_path = output_dir / "rolling_order_history.csv"
    if (output_dir / "paper_orders_history.csv").exists():
        shutil.copyfile(output_dir / "paper_orders_history.csv", rolling_order_history_path)
    else:
        pd.DataFrame().to_csv(rolling_order_history_path, index=False)

    latest_execution_summary_path = output_dir / "execution_summary.json"
    latest_portfolio_performance_summary_path = output_dir / "portfolio_performance_summary.json"
    latest_strategy_contribution_summary_path = output_dir / "strategy_contribution_summary.json"
    if steps:
        final_step_dir = steps[-1].step_output_dir
        for source_name, destination in (
            ("execution_summary.json", latest_execution_summary_path),
            ("portfolio_performance_summary.json", latest_portfolio_performance_summary_path),
            ("strategy_contribution_summary.json", latest_strategy_contribution_summary_path),
        ):
            source_path = final_step_dir / source_name
            if source_path.exists():
                shutil.copyfile(source_path, destination)

    final_equity = float(steps[-1].result.state.equity) if steps else 0.0
    final_state = steps[-1].result.state if steps else None
    summary = {
        "requested_date_count": len(requested_dates),
        "processed_date_count": len(steps),
        "skipped_date_count": len(skipped_dates),
        "requested_dates": requested_dates,
        "processed_dates": [step.processed_date for step in steps],
        "skipped_dates": skipped_dates,
        "state_path": str(state_path),
        "final_as_of": steps[-1].processed_date if steps else None,
        "final_equity": final_equity,
        "final_cash": float(final_state.cash) if final_state is not None else 0.0,
        "final_position_count": len(final_state.positions) if final_state is not None else 0,
        "cumulative_realized_pnl": float(final_state.cumulative_realized_pnl) if final_state is not None else 0.0,
        "cumulative_fees": float(final_state.cumulative_fees) if final_state is not None else 0.0,
        "performance_metrics": metrics,
    }
    rolling_performance_summary_path = output_dir / "rolling_performance_summary.json"
    rolling_performance_summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return {
        "portfolio_daily_returns_path": daily_returns_path,
        "rolling_paper_run_summary_path": rolling_paper_run_summary_path,
        "rolling_execution_log_path": rolling_execution_log_path,
        "rolling_position_history_path": rolling_position_history_path,
        "rolling_target_history_path": rolling_target_history_path,
        "rolling_order_history_path": rolling_order_history_path,
        "rolling_fill_history_path": rolling_fill_history_path,
        "rolling_performance_summary_path": rolling_performance_summary_path,
        "execution_summary_json_path": latest_execution_summary_path,
        "portfolio_performance_summary_path": latest_portfolio_performance_summary_path,
        "strategy_contribution_summary_path": latest_strategy_contribution_summary_path,
    }, summary


def run_multi_strategy_paper_replay(
    *,
    portfolio_config: MultiStrategyPortfolioConfig,
    handoff_summary: dict[str, Any],
    requested_dates: list[str],
    state_path: str | Path,
    output_dir: str | Path,
    execution_config: ExecutionConfig | None = None,
    auto_apply_fills: bool = True,
    reset_state: bool = False,
) -> MultiStrategyReplayResult:
    root_output_dir = Path(output_dir)
    state_file = Path(state_path)

    if reset_state:
        _remove_path(state_file)
        _remove_path(root_output_dir)

    root_output_dir.mkdir(parents=True, exist_ok=True)
    steps_dir = root_output_dir / "steps"
    steps_dir.mkdir(parents=True, exist_ok=True)

    state_store = JsonPaperStateStore(state_file)
    steps: list[MultiStrategyReplayStep] = []
    skipped_dates: list[dict[str, Any]] = []

    for requested_date in requested_dates:
        try:
            allocation_result = allocate_multi_strategy_portfolio(
                portfolio_config,
                as_of_date=requested_date,
            )
        except Exception as exc:
            skipped_dates.append(
                {
                    "requested_date": requested_date,
                    "status": "skipped",
                    "reason": f"allocation_failed:{exc}",
                }
            )
            continue

        if str(allocation_result.as_of) != str(requested_date):
            skipped_dates.append(
                {
                    "requested_date": requested_date,
                    "status": "skipped",
                    "reason": f"no_exact_data_for_requested_date:resolved_as_of={allocation_result.as_of}",
                }
            )
            continue

        step_output_dir = steps_dir / requested_date
        allocation_paths = write_multi_strategy_artifacts(allocation_result, step_output_dir)
        paper_config = _build_multi_strategy_paper_config(
            allocation_result,
            reserve_cash_pct=portfolio_config.cash_reserve_pct,
            as_of_date=requested_date,
        )
        state_file_preexisting = state_file.exists()
        result = run_paper_trading_cycle_for_targets(
            config=paper_config,
            state_store=state_store,
            as_of=allocation_result.as_of,
            latest_prices=allocation_result.latest_prices,
            latest_scores={},
            latest_scheduled_weights=allocation_result.combined_target_weights,
            latest_effective_weights=allocation_result.combined_target_weights,
            target_diagnostics=_build_target_diagnostics(allocation_result, handoff_summary),
            skipped_symbols=sorted(
                {
                    str(row["symbol"])
                    for row in getattr(allocation_result, "execution_symbol_coverage_rows", [])
                    if str(row.get("skip_reason") or "")
                }
            ),
            extra_diagnostics={
                "multi_strategy_allocation": allocation_result.summary,
                "strategy_execution_handoff": handoff_summary,
            },
            execution_config=execution_config,
            auto_apply_fills=auto_apply_fills,
        )
        paper_paths = write_paper_trading_artifacts(result=result, output_dir=step_output_dir)
        persistence_paths, _health_checks, _latest_summary = persist_paper_run_outputs(
            result=result,
            config=paper_config,
            output_dir=root_output_dir,
            state_file_preexisting=state_file_preexisting,
        )
        steps.append(
            MultiStrategyReplayStep(
                requested_date=requested_date,
                processed_date=result.as_of,
                step_output_dir=step_output_dir,
                result=result,
                allocation_summary=allocation_result.summary,
            )
        )

        manifest_path = step_output_dir / "step_artifact_manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "requested_date": requested_date,
                    "processed_date": result.as_of,
                    "allocation_artifacts": {name: str(path) for name, path in allocation_paths.items()},
                    "paper_artifacts": {name: str(path) for name, path in paper_paths.items()},
                    "persistence_artifacts": {name: str(path) for name, path in persistence_paths.items()},
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    replay_paths, summary = _write_replay_artifacts(
        output_dir=root_output_dir,
        steps=steps,
        skipped_dates=skipped_dates,
        requested_dates=requested_dates,
        state_path=state_file,
    )
    return MultiStrategyReplayResult(
        steps=steps,
        skipped_dates=skipped_dates,
        requested_dates=requested_dates,
        output_dir=root_output_dir,
        state_path=state_file,
        artifact_paths=replay_paths,
        summary=summary,
    )
