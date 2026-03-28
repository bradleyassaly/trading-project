from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.artifacts.summary_utils import (
    add_standard_summary_fields,
    warnings_and_errors_from_checks,
    workflow_status_from_checks,
)
from trading_platform.paper.models import PaperTradingConfig, PaperTradingRunResult


def _upsert_csv(
    *,
    path: str | Path,
    rows: list[dict[str, Any]],
    key_columns: list[str],
    columns: list[str],
) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame(rows, columns=columns)
    if output_path.exists():
        existing_df = pd.read_csv(output_path)
    else:
        existing_df = pd.DataFrame(columns=columns)
    existing_df = existing_df.reindex(columns=columns)
    new_df = new_df.reindex(columns=columns)
    if existing_df.empty:
        combined = new_df.copy()
    elif new_df.empty:
        combined = existing_df.copy()
    else:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    if combined.empty:
        combined = pd.DataFrame(columns=columns)
    else:
        combined = combined.drop_duplicates(subset=key_columns, keep="last")
        sort_columns = [
            column
            for column in ["rebalance_timestamp", "timestamp", "as_of", "symbol", "check_name", "order_index"]
            if column in combined.columns
        ]
        if sort_columns:
            combined = combined.sort_values(sort_columns, kind="stable")
    combined.to_csv(output_path, index=False)
    return output_path


def _summary_markdown(summary: dict[str, Any], health_checks: list[dict[str, Any]]) -> str:
    status_counts = {
        "pass": sum(1 for item in health_checks if item["status"] == "pass"),
        "warn": sum(1 for item in health_checks if item["status"] == "warn"),
        "fail": sum(1 for item in health_checks if item["status"] == "fail"),
    }
    lines = [
        f"# Paper Run Summary: {summary['preset_name'] or summary['strategy']}",
        "",
        f"- Timestamp: `{summary['timestamp']}`",
        f"- Rebalance timestamp: `{summary['rebalance_timestamp']}`",
        f"- Preset: `{summary['preset_name']}`",
        f"- Strategy: `{summary['strategy']}`",
        f"- Universe: `{summary['universe']}`",
        f"- Construction mode: `{summary['portfolio_construction_mode']}`",
        f"- Benchmark: `{summary['benchmark']}`",
        f"- Initial equity: `{summary['initial_equity']}`",
        f"- Current paper equity: `{summary['current_equity']}`",
        f"- Starting cash: `{summary.get('starting_cash')}`",
        f"- Ending cash: `{summary.get('ending_cash')}`",
        f"- Starting equity: `{summary.get('starting_equity')}`",
        f"- Ending equity: `{summary.get('ending_equity')}`",
        f"- Gross exposure: `{summary['gross_exposure']}`",
        f"- Fill count: `{summary.get('fill_count', 0)}`",
        f"- Buy fill count: `{summary.get('buy_fill_count', 0)}`",
        f"- Sell fill count: `{summary.get('sell_fill_count', 0)}`",
        f"- Fill application status: `{summary.get('fill_application_status')}`",
        f"- Realized PnL delta: `{summary.get('realized_pnl_delta')}`",
        f"- Gross realized PnL delta: `{summary.get('gross_realized_pnl_delta')}`",
        f"- Cumulative realized PnL: `{summary.get('cumulative_realized_pnl')}`",
        f"- Gross realized PnL: `{summary.get('gross_realized_pnl')}`",
        f"- Gross unrealized PnL: `{summary.get('gross_unrealized_pnl')}`",
        f"- Unrealized PnL: `{summary.get('unrealized_pnl')}`",
        f"- Net unrealized PnL: `{summary.get('net_unrealized_pnl')}`",
        f"- Gross total PnL: `{summary.get('gross_total_pnl')}`",
        f"- Net total PnL: `{summary.get('net_total_pnl')}`",
        f"- Total PnL: `{summary.get('total_pnl')}`",
        f"- Total PnL delta: `{summary.get('total_pnl_delta')}`",
        f"- Fees paid delta: `{summary.get('fees_paid_delta')}`",
        f"- Execution cost delta: `{summary.get('execution_cost_delta')}`",
        f"- Total execution cost: `{summary.get('total_execution_cost')}`",
        f"- Total slippage cost: `{summary.get('total_slippage_cost')}`",
        f"- Total spread cost: `{summary.get('total_spread_cost')}`",
        f"- Total commission cost: `{summary.get('total_commission_cost')}`",
        f"- Realized holdings: `{summary['realized_holdings_count']}`",
        f"- Target selected count: `{summary['target_selected_count']}`",
        f"- Realized holdings minus top_n: `{summary['realized_holdings_minus_top_n']}`",
        f"- Selected names: `{summary['selected_names']}`",
        f"- Target names: `{summary['target_names']}`",
        f"- Activation applied: `{summary.get('activation_applied')}`",
        f"- Active strategy count: `{summary.get('active_strategy_count', 0)}`",
        f"- Active unconditional count: `{summary.get('active_unconditional_count', 0)}`",
        f"- Active conditional count: `{summary.get('active_conditional_count', 0)}`",
        f"- Inactive conditional count: `{summary.get('inactive_conditional_count', 0)}`",
        f"- Requested active strategy count: `{summary.get('requested_active_strategy_count', 0)}`",
        f"- Requested symbol count: `{summary.get('requested_symbol_count', 0)}`",
        f"- Pre-validation target count: `{summary.get('pre_validation_target_symbol_count', 0)}`",
        f"- Post-validation target count: `{summary.get('post_validation_target_symbol_count', 0)}`",
        f"- Usable symbol count: `{summary.get('usable_symbol_count', 0)}`",
        f"- Skipped symbol count: `{summary.get('skipped_symbol_count', 0)}`",
        f"- Target drop stage: `{summary.get('target_drop_stage')}`",
        f"- Zero target reason: `{summary.get('zero_target_reason')}`",
        f"- Target drop reason: `{summary.get('target_drop_reason')}`",
        f"- Generated preset path: `{summary.get('generated_preset_path')}`",
        f"- Signal artifact path: `{summary.get('signal_artifact_path')}`",
        f"- Latest price source summary: `{summary.get('latest_price_source_summary')}`",
        f"- Source portfolio path: `{summary.get('source_portfolio_path')}`",
        f"- Turnover estimate: `{summary['turnover_estimate']}`",
        f"- Requested order count: `{summary.get('requested_order_count', 0)}`",
        f"- Executable order count: `{summary.get('executable_order_count', 0)}`",
        f"- Skipped trades count: `{summary.get('skipped_trades_count', 0)}`",
        f"- Skipped turnover: `{summary.get('skipped_turnover', 0.0)}`",
        f"- Effective turnover reduction: `{summary.get('effective_turnover_reduction', 0.0)}`",
        f"- Score band enabled: `{summary.get('score_band_enabled', False)}`",
        f"- Entry threshold used: `{summary.get('entry_threshold_used')}`",
        f"- Exit threshold used: `{summary.get('exit_threshold_used')}`",
        f"- Blocked entries count: `{summary.get('blocked_entries_count', 0)}`",
        f"- Held in hold zone count: `{summary.get('held_in_hold_zone_count', 0)}`",
        f"- Forced exit count: `{summary.get('forced_exit_count', 0)}`",
        f"- Estimated execution cost: `{summary.get('estimated_execution_cost', 0.0)}`",
        f"- Estimated slippage cost: `{summary.get('estimated_slippage_cost', 0.0)}`",
        f"- Latest data source: `{summary.get('latest_data_source')}`",
        f"- Latest data fallback used: `{summary.get('latest_data_fallback_used')}`",
        f"- Latest data stale: `{summary.get('latest_data_stale')}`",
        f"- Ensemble enabled: `{summary.get('ensemble_enabled')}`",
        f"- Ensemble mode: `{summary.get('ensemble_mode')}`",
        f"- Slippage model: `{summary.get('slippage_model')}`",
        f"- Slippage buy bps: `{summary.get('slippage_buy_bps')}`",
        f"- Slippage sell bps: `{summary.get('slippage_sell_bps')}`",
        f"- Rejected order count: `{summary.get('rejected_order_count', 0)}`",
        f"- Turnover before execution constraints: `{summary.get('turnover_before_execution_constraints', 0.0)}`",
        f"- Turnover after execution constraints: `{summary.get('turnover_after_execution_constraints', 0.0)}`",
        f"- Turnover cap binding count: `{summary['turnover_cap_binding_count']}`",
        f"- Liquidity excluded count: `{summary['liquidity_excluded_count']}`",
        f"- Sector cap excluded count: `{summary['sector_cap_excluded_count']}`",
        f"- Semantic warning: `{summary['semantic_warning'] or 'none'}`",
        "",
        "## Health Check Summary",
        f"- pass: `{status_counts['pass']}`",
        f"- warn: `{status_counts['warn']}`",
        f"- fail: `{status_counts['fail']}`",
        "",
    ]

    notable = [item for item in health_checks if item["status"] != "pass"]
    if notable:
        lines.append("## Warnings / Failures")
        lines.extend([f"- `{item['status']}` `{item['check_name']}`: {item['message']}" for item in notable])
        lines.append("")
    return "\n".join(lines)


def _run_key(config: PaperTradingConfig, result: PaperTradingRunResult) -> str:
    universe_name = config.universe_name or f"{len(config.symbols)}_symbols"
    preset_name = config.preset_name or "manual"
    return "|".join(
        [
            preset_name,
            config.strategy,
            universe_name,
            config.portfolio_construction_mode,
            result.as_of,
        ]
    )


def _health_checks(
    *,
    summary_row: dict[str, Any],
    config: PaperTradingConfig,
    result: PaperTradingRunResult,
    state_file_preexisting: bool,
) -> list[dict[str, Any]]:
    run_key = summary_row["run_key"]
    timestamp = summary_row["timestamp"]
    common = {
        "run_key": run_key,
        "timestamp": timestamp,
        "preset": summary_row["preset_name"],
        "strategy": summary_row["strategy"],
        "universe": summary_row["universe"],
    }
    checks: list[dict[str, Any]] = []

    def add(check_name: str, status: str, message: str) -> None:
        checks.append({**common, "check_name": check_name, "status": status, "message": message})

    available_symbols = len(result.latest_prices)
    target_selected = int(summary_row["target_selected_count"] or 0)
    realized_holdings = int(summary_row["realized_holdings_count"] or 0)
    top_n = max(int(config.top_n), 1)
    gross_exposure = float(summary_row["gross_exposure"] or 0.0)
    liquidity_excluded = int(summary_row["liquidity_excluded_count"] or 0)
    turnover_cap_bindings = int(summary_row["turnover_cap_binding_count"] or 0)

    add(
        "data_loaded",
        "pass" if available_symbols > 0 else "fail",
        f"loaded latest prices for {available_symbols} symbol(s)",
    )
    if available_symbols >= top_n:
        add("available_symbols", "pass", f"{available_symbols} symbol(s) available for top_n={top_n}")
    elif available_symbols > 0:
        add("available_symbols", "warn", f"only {available_symbols} symbol(s) available for top_n={top_n}")
    else:
        add("available_symbols", "fail", "no symbols available")

    add(
        "selected_set",
        "pass" if target_selected > 0 else "fail",
        f"target_selected_count={target_selected}",
    )
    if not target_selected and summary_row.get("zero_target_reason"):
        add("zero_target_reason", "fail", f"zero_target_reason={summary_row['zero_target_reason']}")
    if 0.0 < gross_exposure <= 1.05:
        add("gross_exposure", "pass", f"gross_exposure={gross_exposure:.4f}")
    elif gross_exposure == 0.0:
        add("gross_exposure", "warn", "gross_exposure is zero")
    else:
        add("gross_exposure", "warn", f"gross_exposure={gross_exposure:.4f} is outside expected range")

    if config.portfolio_construction_mode == "pure_topn":
        if realized_holdings <= top_n:
            add("holdings_vs_top_n", "pass", f"realized_holdings_count={realized_holdings} within top_n={top_n}")
        elif realized_holdings > top_n * 2:
            add(
                "holdings_vs_top_n",
                "fail",
                f"realized_holdings_count={realized_holdings} materially exceeds top_n={top_n}",
            )
        else:
            add("holdings_vs_top_n", "warn", f"realized_holdings_count={realized_holdings} exceeds top_n={top_n}")
    else:
        if realized_holdings <= top_n * 5:
            add("holdings_vs_top_n", "pass", f"transition realized_holdings_count={realized_holdings}")
        else:
            add(
                "holdings_vs_top_n",
                "warn",
                f"transition realized_holdings_count={realized_holdings} materially exceeds top_n={top_n}",
            )

    liquidity_status = "pass" if liquidity_excluded <= max(5, top_n * 2) else "warn"
    add("liquidity_exclusions", liquidity_status, f"liquidity_excluded_count={liquidity_excluded}")

    turnover_status = "pass" if turnover_cap_bindings <= 25 else "warn"
    add("turnover_cap_bindings", turnover_status, f"turnover_cap_binding_count={turnover_cap_bindings}")

    sector_warning = str(summary_row.get("data_quality_warnings", "") or "")
    if config.max_names_per_sector is not None and "sector_metadata_unavailable" in sector_warning:
        add("sector_metadata", "warn", "sector cap requested but sector metadata is unavailable")
    else:
        add("sector_metadata", "pass", "sector metadata status acceptable")

    add(
        "state_file",
        "pass" if state_file_preexisting else "warn",
        "loaded existing paper state" if state_file_preexisting else "bootstrapped a new paper state file",
    )
    add("output_files", "pass", "paper artifacts and ledgers were written successfully")
    return checks


def persist_paper_run_outputs(
    *,
    result: PaperTradingRunResult,
    config: PaperTradingConfig,
    output_dir: str | Path,
    state_file_preexisting: bool,
) -> tuple[dict[str, Path], list[dict[str, Any]], dict[str, Any]]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    run_key = _run_key(config, result)
    target_diag = dict(result.diagnostics.get("target_construction", {}))
    order_diag = dict(result.diagnostics.get("order_generation", {}))
    skip_reasons = target_diag.get("skip_reasons", {})

    universe_name = config.universe_name or f"{len(config.symbols)}_symbols"
    selected_names = str(target_diag.get("selected_symbols", "") or "").strip()
    target_names = str(target_diag.get("target_selected_symbols", "") or "").strip()
    summary_stats = dict(target_diag.get("summary", {}))
    handoff = dict(target_diag.get("strategy_execution_handoff", {}))
    execution_diag = dict(result.diagnostics.get("execution", {}))
    execution_summary = dict(execution_diag.get("execution_summary", {}))
    paper_execution_diag = dict(result.diagnostics.get("paper_execution", {}))
    accounting_diag = dict(result.diagnostics.get("accounting", {}))
    attribution_summary = dict((result.attribution or {}).get("summary", {}))
    timestamp = result.as_of
    rebalance_timestamp = str(target_diag.get("rebalance_timestamp", timestamp))
    gross_exposure = target_diag.get("average_gross_exposure")
    if gross_exposure is None:
        gross_exposure = float(sum(abs(weight) for weight in result.latest_target_weights.values()))

    summary_row: dict[str, Any] = {
        "run_key": run_key,
        "timestamp": timestamp,
        "rebalance_timestamp": rebalance_timestamp,
        "preset_name": config.preset_name,
        "strategy": config.strategy,
        "universe": universe_name,
        "benchmark": config.benchmark,
        "portfolio_construction_mode": config.portfolio_construction_mode,
        "initial_equity": float(order_diag.get("equity", result.state.equity)),
        "current_equity": float(result.state.equity),
        "final_gross_equity": float(result.state.initial_cash_basis + accounting_diag.get("gross_total_pnl", 0.0)),
        "final_net_equity": float(result.state.equity),
        "cash": float(result.state.cash),
        "gross_market_value": float(result.state.gross_market_value),
        "gross_exposure": float(gross_exposure),
        "starting_cash": float(accounting_diag.get("starting_cash", result.state.cash)),
        "ending_cash": float(accounting_diag.get("ending_cash", result.state.cash)),
        "starting_gross_market_value": float(accounting_diag.get("starting_gross_market_value", 0.0)),
        "ending_gross_market_value": float(
            accounting_diag.get("ending_gross_market_value", result.state.gross_market_value)
        ),
        "starting_equity": float(accounting_diag.get("starting_equity", result.state.equity)),
        "ending_equity": float(accounting_diag.get("ending_equity", result.state.equity)),
        "gross_realized_pnl_delta": float(accounting_diag.get("gross_realized_pnl_delta", 0.0)),
        "realized_pnl_delta": float(accounting_diag.get("realized_pnl_delta", 0.0)),
        "net_realized_pnl_delta": float(accounting_diag.get("net_realized_pnl_delta", 0.0)),
        "gross_realized_pnl": float(
            accounting_diag.get("gross_realized_pnl", getattr(result.state, "cumulative_gross_realized_pnl", 0.0))
        ),
        "cumulative_realized_pnl": float(
            accounting_diag.get("cumulative_realized_pnl", result.state.cumulative_realized_pnl)
        ),
        "net_realized_pnl": float(accounting_diag.get("net_realized_pnl", result.state.cumulative_realized_pnl)),
        "gross_unrealized_pnl": float(accounting_diag.get("gross_unrealized_pnl", 0.0)),
        "unrealized_pnl": float(accounting_diag.get("unrealized_pnl", result.state.unrealized_pnl)),
        "net_unrealized_pnl": float(accounting_diag.get("net_unrealized_pnl", result.state.unrealized_pnl)),
        "gross_total_pnl": float(accounting_diag.get("gross_total_pnl", 0.0)),
        "net_total_pnl": float(accounting_diag.get("net_total_pnl", result.state.total_pnl)),
        "total_pnl": float(accounting_diag.get("total_pnl", result.state.total_pnl)),
        "total_pnl_delta": float(accounting_diag.get("total_pnl_delta", 0.0)),
        "fees_paid_delta": float(accounting_diag.get("fees_paid_delta", 0.0)),
        "cumulative_fees": float(accounting_diag.get("cumulative_fees", result.state.cumulative_fees)),
        "commission_cost_delta": float(accounting_diag.get("commission_cost_delta", 0.0)),
        "slippage_cost_delta": float(accounting_diag.get("slippage_cost_delta", 0.0)),
        "spread_cost_delta": float(accounting_diag.get("spread_cost_delta", 0.0)),
        "execution_cost_delta": float(accounting_diag.get("execution_cost_delta", 0.0)),
        "total_commission_cost": float(accounting_diag.get("total_commission_cost", result.state.cumulative_fees)),
        "total_slippage_cost": float(accounting_diag.get("total_slippage_cost", 0.0)),
        "total_spread_cost": float(accounting_diag.get("total_spread_cost", 0.0)),
        "total_execution_cost": float(accounting_diag.get("total_execution_cost", 0.0)),
        "cost_drag_pct": float(accounting_diag.get("cost_drag_pct", 0.0)),
        "realized_holdings_count": int(target_diag.get("realized_holdings_count", len(result.latest_target_weights))),
        "target_selected_count": int(
            target_diag.get("target_selected_count", len([name for name in target_names.split(",") if name]))
        ),
        "realized_holdings_minus_top_n": int(target_diag.get("realized_holdings_minus_top_n", 0)),
        "selected_names": selected_names,
        "target_names": target_names,
        "turnover_estimate": float(summary_stats.get("mean_turnover", 0.0) or 0.0),
        "turnover_cap_binding_count": int(target_diag.get("turnover_cap_binding_count", 0) or 0),
        "turnover_buffer_blocked_replacements": int(target_diag.get("turnover_buffer_blocked_replacements", 0) or 0),
        "liquidity_excluded_count": int(target_diag.get("liquidity_excluded_count", 0) or 0),
        "sector_cap_excluded_count": int(target_diag.get("sector_cap_excluded_count", 0) or 0),
        "semantic_warning": str(target_diag.get("semantic_warning", "") or ""),
        "data_quality_warnings": json.dumps(skip_reasons, sort_keys=True) if skip_reasons else "",
        "skipped_symbol_count": int(len(result.skipped_symbols)),
        "estimated_execution_cost": float(execution_summary.get("expected_total_cost", 0.0) or 0.0),
        "estimated_slippage_cost": float(execution_summary.get("expected_slippage_cost_total", 0.0) or 0.0),
        "latest_data_source": str(
            paper_execution_diag.get(
                "latest_data_source",
                target_diag.get("latest_data_source", target_diag.get("latest_price_source", "yfinance")),
            )
            or "yfinance"
        ),
        "latest_data_fallback_used": bool(
            paper_execution_diag.get(
                "latest_data_fallback_used",
                target_diag.get("latest_data_fallback_used", target_diag.get("latest_price_fallback_used", False)),
            )
        ),
        "latest_bar_timestamp": paper_execution_diag.get(
            "latest_bar_timestamp", target_diag.get("latest_bar_timestamp")
        ),
        "latest_bar_age_seconds": paper_execution_diag.get(
            "latest_bar_age_seconds", target_diag.get("latest_bar_age_seconds")
        ),
        "latest_data_stale": paper_execution_diag.get("latest_data_stale", target_diag.get("latest_data_stale")),
        "ensemble_enabled": bool(paper_execution_diag.get("ensemble_enabled", False)),
        "ensemble_mode": paper_execution_diag.get("ensemble_mode", "disabled"),
        "slippage_enabled": bool(paper_execution_diag.get("slippage_enabled", False)),
        "slippage_model": str(paper_execution_diag.get("slippage_model", "none") or "none"),
        "slippage_buy_bps": float(paper_execution_diag.get("slippage_buy_bps", 0.0) or 0.0),
        "slippage_sell_bps": float(paper_execution_diag.get("slippage_sell_bps", 0.0) or 0.0),
        "cost_model_enabled": bool(paper_execution_diag.get("cost_model_enabled", False)),
        "cost_model": str(paper_execution_diag.get("cost_model", "disabled") or "disabled"),
        "commission_bps": float(paper_execution_diag.get("commission_bps", 0.0) or 0.0),
        "minimum_commission": float(paper_execution_diag.get("minimum_commission", 0.0) or 0.0),
        "spread_bps": float(paper_execution_diag.get("spread_bps", 0.0) or 0.0),
        "min_weight_change_to_trade": float(paper_execution_diag.get("min_weight_change_to_trade", 0.0) or 0.0),
        "score_band_enabled": bool(paper_execution_diag.get("score_band_enabled", False)),
        "entry_threshold_used": paper_execution_diag.get("entry_threshold_used"),
        "exit_threshold_used": paper_execution_diag.get("exit_threshold_used"),
        "score_band_mode": str(paper_execution_diag.get("score_band_mode", "raw_score") or "raw_score"),
        "blocked_entries_count": int(paper_execution_diag.get("blocked_entries_count", 0) or 0),
        "held_in_hold_zone_count": int(paper_execution_diag.get("held_in_hold_zone_count", 0) or 0),
        "forced_exit_count": int(paper_execution_diag.get("forced_exit_count", 0) or 0),
        "skipped_due_to_entry_band_count": int(
            paper_execution_diag.get("skipped_due_to_entry_band_count", 0) or 0
        ),
        "skipped_due_to_hold_zone_count": int(
            paper_execution_diag.get("skipped_due_to_hold_zone_count", 0) or 0
        ),
        "rejected_order_count": int(execution_summary.get("rejected_order_count", 0) or 0),
        "requested_order_count": int(execution_summary.get("requested_order_count", 0) or len(result.orders)),
        "executable_order_count": int(execution_summary.get("executable_order_count", 0) or len(result.orders)),
        "skipped_trades_count": int(paper_execution_diag.get("skipped_trades_count", 0) or 0),
        "skipped_turnover": float(paper_execution_diag.get("skipped_turnover", 0.0) or 0.0),
        "effective_turnover_reduction": float(
            paper_execution_diag.get("effective_turnover_reduction", 0.0) or 0.0
        ),
        "fill_count": int(accounting_diag.get("fill_count", len(result.fills))),
        "buy_fill_count": int(accounting_diag.get("buy_fill_count", 0)),
        "sell_fill_count": int(accounting_diag.get("sell_fill_count", 0)),
        "fill_notional": float(accounting_diag.get("fill_notional", 0.0)),
        "auto_apply_fills": bool(accounting_diag.get("auto_apply_fills", False)),
        "fill_application_status": str(accounting_diag.get("fill_application_status", "unknown") or "unknown"),
        "turnover_before_execution_constraints": float(
            execution_summary.get("turnover_before_constraints", 0.0) or 0.0
        ),
        "turnover_after_execution_constraints": float(execution_summary.get("turnover_after_constraints", 0.0) or 0.0),
        "active_strategy_count": int(handoff.get("active_strategy_count", 0) or 0),
        "active_unconditional_count": int(handoff.get("active_unconditional_count", 0) or 0),
        "active_conditional_count": int(handoff.get("active_conditional_count", 0) or 0),
        "inactive_conditional_count": int(handoff.get("inactive_conditional_count", 0) or 0),
        "requested_active_strategy_count": int(
            target_diag.get("requested_active_strategy_count", handoff.get("active_strategy_count", 0)) or 0
        ),
        "requested_symbol_count": int(target_diag.get("requested_symbol_count", 0) or 0),
        "pre_validation_target_symbol_count": int(target_diag.get("pre_validation_target_symbol_count", 0) or 0),
        "post_validation_target_symbol_count": int(
            target_diag.get("post_validation_target_symbol_count", len(result.latest_target_weights)) or 0
        ),
        "usable_symbol_count": int(target_diag.get("usable_symbol_count", len(result.latest_prices)) or 0),
        "zero_target_reason": str(target_diag.get("zero_target_reason", "") or ""),
        "target_drop_stage": str(target_diag.get("target_drop_stage", "") or ""),
        "target_drop_reason": str(target_diag.get("target_drop_reason", "") or ""),
        "generated_preset_path": str(target_diag.get("generated_preset_path", "") or ""),
        "signal_artifact_path": str(target_diag.get("signal_artifact_path", "") or ""),
        "latest_price_source_summary": json.dumps(target_diag.get("latest_price_source_summary", {}), sort_keys=True),
        "source_portfolio_path": str(handoff.get("source_portfolio_path") or ""),
        "activation_applied": bool(handoff.get("activation_applied", False)),
        "attribution_total_gross_realized_pnl": float(attribution_summary.get("total_gross_realized_pnl", 0.0) or 0.0),
        "attribution_total_net_realized_pnl": float(attribution_summary.get("total_net_realized_pnl", 0.0) or 0.0),
        "attribution_total_realized_pnl": float(attribution_summary.get("total_realized_pnl", 0.0) or 0.0),
        "attribution_total_gross_unrealized_pnl": float(
            attribution_summary.get("total_gross_unrealized_pnl", 0.0) or 0.0
        ),
        "attribution_total_net_unrealized_pnl": float(
            attribution_summary.get("total_net_unrealized_pnl", 0.0) or 0.0
        ),
        "attribution_total_unrealized_pnl": float(attribution_summary.get("total_unrealized_pnl", 0.0) or 0.0),
        "attribution_total_execution_cost": float(attribution_summary.get("total_execution_cost", 0.0) or 0.0),
        "attribution_total_gross_pnl": float(attribution_summary.get("total_gross_pnl", 0.0) or 0.0),
        "attribution_total_net_pnl": float(attribution_summary.get("total_net_pnl", 0.0) or 0.0),
        "attribution_total_pnl": float(attribution_summary.get("total_pnl", 0.0) or 0.0),
    }

    health_checks = _health_checks(
        summary_row=summary_row,
        config=config,
        result=result,
        state_file_preexisting=state_file_preexisting,
    )

    equity_row = {
        "run_key": run_key,
        "timestamp": timestamp,
        "rebalance_timestamp": rebalance_timestamp,
        "preset_name": config.preset_name,
        "strategy": config.strategy,
        "universe": universe_name,
        "cash": float(result.state.cash),
        "gross_market_value": float(result.state.gross_market_value),
        "equity": float(result.state.equity),
        "cost_basis": float(result.state.cost_basis),
        "unrealized_pnl": float(result.state.unrealized_pnl),
        "cumulative_realized_pnl": float(result.state.cumulative_realized_pnl),
        "total_pnl": float(result.state.total_pnl),
        "position_count": int(len(result.state.positions)),
        "gross_exposure": float(gross_exposure),
    }
    position_rows = [
        {
            "run_key": run_key,
            "timestamp": timestamp,
            "rebalance_timestamp": rebalance_timestamp,
            "preset_name": config.preset_name,
            "strategy": config.strategy,
            "universe": universe_name,
            "symbol": position.symbol,
            "quantity": int(position.quantity),
            "avg_price": float(position.avg_price),
            "last_price": float(position.last_price),
            "market_value": float(position.market_value),
            "cost_basis": float(position.cost_basis),
            "unrealized_pnl": float(position.unrealized_pnl),
            "portfolio_weight": float(position.market_value / result.state.equity) if result.state.equity > 0 else 0.0,
        }
        for position in result.state.positions.values()
    ]
    order_rows = [
        {
            "run_key": run_key,
            "timestamp": timestamp,
            "rebalance_timestamp": rebalance_timestamp,
            "preset_name": config.preset_name,
            "strategy": config.strategy,
            "universe": universe_name,
            "order_index": order_index,
            "symbol": order.symbol,
            "side": order.side,
            "quantity": int(order.quantity),
            "reference_price": float(order.reference_price),
            "target_weight": float(order.target_weight),
            "current_quantity": int(order.current_quantity),
            "target_quantity": int(order.target_quantity),
            "notional": float(order.notional),
            "reason": order.reason,
        }
        for order_index, order in enumerate(result.orders)
    ]

    paths = {
        "paper_equity_curve_path": _upsert_csv(
            path=output_path / "paper_equity_curve.csv",
            rows=[equity_row],
            key_columns=["run_key"],
            columns=list(equity_row.keys()),
        ),
        "portfolio_equity_curve_path": _upsert_csv(
            path=output_path / "portfolio_equity_curve.csv",
            rows=[equity_row],
            key_columns=["run_key"],
            columns=list(equity_row.keys()),
        ),
        "paper_positions_history_path": _upsert_csv(
            path=output_path / "paper_positions_history.csv",
            rows=position_rows,
            key_columns=["run_key", "symbol"],
            columns=[
                "run_key",
                "timestamp",
                "rebalance_timestamp",
                "preset_name",
                "strategy",
                "universe",
                "symbol",
                "quantity",
                "avg_price",
                "last_price",
                "market_value",
                "cost_basis",
                "unrealized_pnl",
                "portfolio_weight",
            ],
        ),
        "paper_orders_history_path": _upsert_csv(
            path=output_path / "paper_orders_history.csv",
            rows=order_rows,
            key_columns=["run_key", "order_index"],
            columns=[
                "run_key",
                "timestamp",
                "rebalance_timestamp",
                "preset_name",
                "strategy",
                "universe",
                "order_index",
                "symbol",
                "side",
                "quantity",
                "reference_price",
                "target_weight",
                "current_quantity",
                "target_quantity",
                "notional",
                "reason",
            ],
        ),
        "paper_run_summary_path": _upsert_csv(
            path=output_path / "paper_run_summary.csv",
            rows=[summary_row],
            key_columns=["run_key"],
            columns=list(summary_row.keys()),
        ),
        "paper_health_checks_path": _upsert_csv(
            path=output_path / "paper_health_checks.csv",
            rows=health_checks,
            key_columns=["run_key", "check_name"],
            columns=["run_key", "timestamp", "preset", "strategy", "universe", "check_name", "status", "message"],
        ),
    }

    latest_json_path = output_path / "paper_run_summary_latest.json"
    latest_md_path = output_path / "paper_run_summary_latest.md"
    summary_payload = {
        "summary": summary_row,
        "health_checks": health_checks,
        "pnl_attribution_summary": attribution_summary,
    }
    warnings, errors = warnings_and_errors_from_checks(health_checks)
    summary_payload = add_standard_summary_fields(
        summary_payload,
        summary_type="paper_run",
        timestamp=str(summary_row["timestamp"]),
        status=workflow_status_from_checks(health_checks),
        key_counts={
            "realized_holdings_count": summary_row["realized_holdings_count"],
            "target_selected_count": summary_row["target_selected_count"],
            "active_strategy_count": summary_row["active_strategy_count"],
            "active_unconditional_count": summary_row["active_unconditional_count"],
            "active_conditional_count": summary_row["active_conditional_count"],
            "inactive_conditional_count": summary_row["inactive_conditional_count"],
            "requested_active_strategy_count": summary_row["requested_active_strategy_count"],
            "requested_symbol_count": summary_row["requested_symbol_count"],
            "pre_validation_target_symbol_count": summary_row["pre_validation_target_symbol_count"],
            "post_validation_target_symbol_count": summary_row["post_validation_target_symbol_count"],
            "usable_symbol_count": summary_row["usable_symbol_count"],
            "skipped_symbol_count": summary_row["skipped_symbol_count"],
            "requested_order_count": summary_row["requested_order_count"],
            "executable_order_count": summary_row["executable_order_count"],
            "rejected_order_count": summary_row["rejected_order_count"],
            "fill_count": summary_row["fill_count"],
            "buy_fill_count": summary_row["buy_fill_count"],
            "sell_fill_count": summary_row["sell_fill_count"],
        },
        key_metrics={
            "current_equity": summary_row["current_equity"],
            "gross_total_pnl": summary_row["gross_total_pnl"],
            "net_total_pnl": summary_row["net_total_pnl"],
            "cash": summary_row["cash"],
            "total_pnl": summary_row["total_pnl"],
            "unrealized_pnl": summary_row["unrealized_pnl"],
            "cumulative_realized_pnl": summary_row["cumulative_realized_pnl"],
            "gross_exposure": summary_row["gross_exposure"],
            "turnover_estimate": summary_row["turnover_estimate"],
            "estimated_execution_cost": summary_row["estimated_execution_cost"],
            "total_execution_cost": summary_row["total_execution_cost"],
        },
        warnings=warnings,
        errors=errors,
    )
    latest_json_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    latest_md_path.write_text(_summary_markdown(summary_row, health_checks), encoding="utf-8")
    paths["paper_run_summary_latest_json_path"] = latest_json_path
    paths["paper_run_summary_latest_md_path"] = latest_md_path
    summary_payload["artifact_paths"] = {name: str(path) for name, path in paths.items()}
    latest_json_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    return paths, health_checks, summary_row
