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

    combined = pd.concat([existing_df, new_df], ignore_index=True)
    if combined.empty:
        combined = pd.DataFrame(columns=columns)
    else:
        combined = combined.drop_duplicates(subset=key_columns, keep="last")
        sort_columns = [column for column in ["rebalance_timestamp", "timestamp", "as_of", "symbol", "check_name", "order_index"] if column in combined.columns]
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
        f"- Gross exposure: `{summary['gross_exposure']}`",
        f"- Realized holdings: `{summary['realized_holdings_count']}`",
        f"- Target selected count: `{summary['target_selected_count']}`",
        f"- Realized holdings minus top_n: `{summary['realized_holdings_minus_top_n']}`",
        f"- Selected names: `{summary['selected_names']}`",
        f"- Target names: `{summary['target_names']}`",
        f"- Turnover estimate: `{summary['turnover_estimate']}`",
        f"- Requested order count: `{summary.get('requested_order_count', 0)}`",
        f"- Executable order count: `{summary.get('executable_order_count', 0)}`",
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

    add("data_loaded", "pass" if available_symbols > 0 else "fail", f"loaded latest prices for {available_symbols} symbol(s)")
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
            add("holdings_vs_top_n", "fail", f"realized_holdings_count={realized_holdings} materially exceeds top_n={top_n}")
        else:
            add("holdings_vs_top_n", "warn", f"realized_holdings_count={realized_holdings} exceeds top_n={top_n}")
    else:
        if realized_holdings <= top_n * 5:
            add("holdings_vs_top_n", "pass", f"transition realized_holdings_count={realized_holdings}")
        else:
            add("holdings_vs_top_n", "warn", f"transition realized_holdings_count={realized_holdings} materially exceeds top_n={top_n}")

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
    execution_diag = dict(result.diagnostics.get("execution", {}))
    execution_summary = dict(execution_diag.get("execution_summary", {}))
    paper_execution_diag = dict(result.diagnostics.get("paper_execution", {}))
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
        "cash": float(result.state.cash),
        "gross_market_value": float(result.state.gross_market_value),
        "gross_exposure": float(gross_exposure),
        "realized_holdings_count": int(target_diag.get("realized_holdings_count", len(result.latest_target_weights))),
        "target_selected_count": int(target_diag.get("target_selected_count", len([name for name in target_names.split(",") if name]))),
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
        "latest_data_source": str(paper_execution_diag.get("latest_data_source", target_diag.get("latest_data_source", target_diag.get("latest_price_source", "yfinance"))) or "yfinance"),
        "latest_data_fallback_used": bool(paper_execution_diag.get("latest_data_fallback_used", target_diag.get("latest_data_fallback_used", target_diag.get("latest_price_fallback_used", False)))),
        "latest_bar_timestamp": paper_execution_diag.get("latest_bar_timestamp", target_diag.get("latest_bar_timestamp")),
        "latest_bar_age_seconds": paper_execution_diag.get("latest_bar_age_seconds", target_diag.get("latest_bar_age_seconds")),
        "latest_data_stale": paper_execution_diag.get("latest_data_stale", target_diag.get("latest_data_stale")),
        "ensemble_enabled": bool(paper_execution_diag.get("ensemble_enabled", False)),
        "ensemble_mode": paper_execution_diag.get("ensemble_mode", "disabled"),
        "slippage_enabled": bool(paper_execution_diag.get("slippage_enabled", False)),
        "slippage_model": str(paper_execution_diag.get("slippage_model", "none") or "none"),
        "slippage_buy_bps": float(paper_execution_diag.get("slippage_buy_bps", 0.0) or 0.0),
        "slippage_sell_bps": float(paper_execution_diag.get("slippage_sell_bps", 0.0) or 0.0),
        "rejected_order_count": int(execution_summary.get("rejected_order_count", 0) or 0),
        "requested_order_count": int(execution_summary.get("requested_order_count", 0) or len(result.orders)),
        "executable_order_count": int(execution_summary.get("executable_order_count", 0) or len(result.orders)),
        "turnover_before_execution_constraints": float(execution_summary.get("turnover_before_constraints", 0.0) or 0.0),
        "turnover_after_execution_constraints": float(execution_summary.get("turnover_after_constraints", 0.0) or 0.0),
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
    summary_payload = {"summary": summary_row, "health_checks": health_checks}
    warnings, errors = warnings_and_errors_from_checks(health_checks)
    summary_payload = add_standard_summary_fields(
        summary_payload,
        summary_type="paper_run",
        timestamp=str(summary_row["timestamp"]),
        status=workflow_status_from_checks(health_checks),
        key_counts={
            "realized_holdings_count": summary_row["realized_holdings_count"],
            "target_selected_count": summary_row["target_selected_count"],
            "requested_order_count": summary_row["requested_order_count"],
            "executable_order_count": summary_row["executable_order_count"],
            "rejected_order_count": summary_row["rejected_order_count"],
        },
        key_metrics={
            "current_equity": summary_row["current_equity"],
            "cash": summary_row["cash"],
            "gross_exposure": summary_row["gross_exposure"],
            "turnover_estimate": summary_row["turnover_estimate"],
            "estimated_execution_cost": summary_row["estimated_execution_cost"],
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
