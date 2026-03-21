from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.live.preview import LivePreviewResult


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
        sort_columns = [column for column in ["timestamp", "rebalance_timestamp", "symbol", "check_name", "order_index"] if column in combined.columns]
        if sort_columns:
            combined = combined.sort_values(sort_columns, kind="stable")
    combined.to_csv(output_path, index=False)
    return output_path


def _run_key(result: LivePreviewResult) -> str:
    config = result.config
    return "|".join(
        [
            config.preset_name or "manual",
            config.strategy,
            config.universe_name or f"{len(config.symbols)}_symbols",
            config.broker,
            result.as_of,
        ]
    )


def _semantic_warning_check(result: LivePreviewResult) -> dict[str, Any] | None:
    message = str(result.target_diagnostics.get("semantic_warning") or "").strip()
    if not message or message.lower() == "none":
        return None
    return {
        "check_name": "semantic_warning",
        "status": "warn",
        "message": message,
    }


def compute_readiness(health_checks: list[dict[str, Any]]) -> str:
    if any(item["status"] == "fail" for item in health_checks):
        return "blocked"

    degraded_warn_checks = {
        "market_data",
        "selected_set",
        "holdings_reasonableness",
        "single_position_change",
        "cash_residual",
        "semantic_warning",
    }
    if any(item["status"] == "warn" and item["check_name"] in degraded_warn_checks for item in health_checks):
        return "degraded"
    return "ready_for_manual_review"


def _summary_markdown(summary: dict[str, Any], health_checks: list[dict[str, Any]]) -> str:
    notable = [item for item in health_checks if item["status"] != "pass"]
    lines = [
        f"# Scheduled Live Dry-Run Summary: {summary['preset_name'] or summary['strategy']}",
        "",
        f"- Timestamp: `{summary['timestamp']}`",
        f"- Rebalance timestamp: `{summary['rebalance_timestamp']}`",
        f"- Preset: `{summary['preset_name']}`",
        f"- Strategy: `{summary['strategy']}`",
        f"- Universe: `{summary['universe']}`",
        f"- Broker: `{summary['broker']}`",
        f"- Readiness: `{summary['readiness']}`",
        f"- Proposed orders: `{summary['proposed_order_count']}`",
        f"- Gross exposure: `{summary['gross_exposure']}`",
        f"- Target holdings count: `{summary['target_holdings_count']}`",
        f"- Realized holdings count: `{summary['realized_holdings_count']}`",
        f"- Turnover estimate: `{summary['turnover_estimate']}`",
        "",
    ]
    if notable:
        lines.append("## Top Warnings / Failures")
        for item in notable[:10]:
            lines.append(f"- `{item['status']}` `{item['check_name']}`: {item['message']}")
        lines.append("")
    return "\n".join(lines)


def persist_live_scheduled_outputs(
    *,
    result: LivePreviewResult,
    output_dir: str | Path,
) -> tuple[dict[str, Path], list[dict[str, Any]], dict[str, Any]]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    run_key = _run_key(result)
    timestamp = result.as_of
    config = result.config
    target_diag = dict(result.target_diagnostics)
    health_checks = [
        {
            "run_key": run_key,
            "timestamp": item.timestamp,
            "preset": item.preset,
            "strategy": item.strategy,
            "universe": item.universe,
            "check_name": item.check_name,
            "status": item.status,
            "message": item.message,
        }
        for item in result.health_checks
    ]
    semantic_warning_check = _semantic_warning_check(result)
    if semantic_warning_check is not None:
        health_checks.append(
            {
                "run_key": run_key,
                "timestamp": timestamp,
                "preset": config.preset_name,
                "strategy": config.strategy,
                "universe": config.universe_name,
                **semantic_warning_check,
            }
        )

    readiness = compute_readiness(health_checks)
    top_failures = [item["message"] for item in health_checks if item["status"] == "fail"][:5]
    top_warnings = [item["message"] for item in health_checks if item["status"] == "warn"][:5]
    target_names = [name for name in str(target_diag.get("target_selected_symbols") or "").split(",") if name]
    selected_names = [name for name in str(target_diag.get("selected_symbols") or "").split(",") if name]
    turnover_estimate = float(target_diag.get("summary", {}).get("mean_turnover", 0.0) or 0.0)
    rebalance_timestamp = str(target_diag.get("rebalance_timestamp", timestamp))

    summary_row = {
        "run_key": run_key,
        "timestamp": timestamp,
        "rebalance_timestamp": rebalance_timestamp,
        "preset_name": config.preset_name,
        "strategy": config.strategy,
        "universe": config.universe_name or f"{len(config.symbols)}_symbols",
        "broker": config.broker,
        "portfolio_construction_mode": config.portfolio_construction_mode,
        "benchmark": config.benchmark,
        "readiness": readiness,
        "cash": float(result.account.cash),
        "equity": float(result.account.equity),
        "gross_exposure": float(target_diag.get("average_gross_exposure") or 0.0),
        "target_holdings_count": len(target_names) if target_names else int(target_diag.get("target_selected_count") or 0),
        "realized_holdings_count": int(target_diag.get("realized_holdings_count") or 0),
        "realized_holdings_minus_top_n": int(target_diag.get("realized_holdings_minus_top_n") or 0),
        "turnover_estimate": turnover_estimate,
        "proposed_order_count": int(len(result.adjusted_orders)),
        "raw_order_count": int(len(result.reconciliation.orders)),
        "open_order_count": int(len(result.open_orders)),
        "selected_names": ",".join(selected_names),
        "target_names": ",".join(target_names),
        "liquidity_excluded_count": int(target_diag.get("liquidity_excluded_count") or 0),
        "sector_cap_excluded_count": int(target_diag.get("sector_cap_excluded_count") or 0),
        "turnover_cap_binding_count": int(target_diag.get("turnover_cap_binding_count") or 0),
        "turnover_buffer_blocked_replacements": int(target_diag.get("turnover_buffer_blocked_replacements") or 0),
        "semantic_warning": str(target_diag.get("semantic_warning") or ""),
        "top_failures": json.dumps(top_failures),
        "top_warnings": json.dumps(top_warnings),
    }

    order_rows = []
    for order_index, order in enumerate(result.adjusted_orders):
        order_rows.append(
            {
                "run_key": run_key,
                "timestamp": timestamp,
                "rebalance_timestamp": rebalance_timestamp,
                "preset_name": config.preset_name,
                "strategy": config.strategy,
                "universe": summary_row["universe"],
                "order_index": order_index,
                "symbol": order.symbol,
                "side": order.side,
                "quantity": int(order.quantity),
                "order_type": order.order_type,
                "time_in_force": order.time_in_force,
                "reason": order.reason,
            }
        )

    reconciliation_rows = [
        {
            "run_key": run_key,
            "timestamp": timestamp,
            "rebalance_timestamp": rebalance_timestamp,
            "preset_name": config.preset_name,
            "strategy": config.strategy,
            "universe": summary_row["universe"],
            **row,
        }
        for row in result.reconciliation_rows
    ]

    notification_payload = {
        "run_key": run_key,
        "timestamp": timestamp,
        "preset_name": config.preset_name,
        "broker": config.broker,
        "readiness": readiness,
        "top_warnings": top_warnings,
        "top_failures": top_failures,
        "target_names": target_names,
        "top_proposed_orders": order_rows[:5],
        "gross_exposure": summary_row["gross_exposure"],
        "realized_holdings_count": summary_row["realized_holdings_count"],
    }

    run_dir = output_path / "runs" / timestamp.replace(":", "-")
    run_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "live_run_summary_path": _upsert_csv(
            path=output_path / "live_run_summary.csv",
            rows=[summary_row],
            key_columns=["run_key"],
            columns=list(summary_row.keys()),
        ),
        "live_health_checks_path": _upsert_csv(
            path=output_path / "live_health_checks.csv",
            rows=health_checks,
            key_columns=["run_key", "check_name"],
            columns=["run_key", "timestamp", "preset", "strategy", "universe", "check_name", "status", "message"],
        ),
        "live_proposed_orders_history_path": _upsert_csv(
            path=output_path / "live_proposed_orders_history.csv",
            rows=order_rows,
            key_columns=["run_key", "order_index"],
            columns=["run_key", "timestamp", "rebalance_timestamp", "preset_name", "strategy", "universe", "order_index", "symbol", "side", "quantity", "order_type", "time_in_force", "reason"],
        ),
        "live_reconciliation_history_path": _upsert_csv(
            path=output_path / "live_reconciliation_history.csv",
            rows=reconciliation_rows,
            key_columns=["run_key", "symbol"],
            columns=[
                "run_key",
                "timestamp",
                "rebalance_timestamp",
                "preset_name",
                "strategy",
                "universe",
                "symbol",
                "current_qty",
                "current_weight",
                "target_weight",
                "target_notional",
                "delta_notional",
                "current_price",
                "target_qty",
                "delta_qty",
                "proposed_side",
                "proposed_qty",
                "pending_open_order_qty",
                "reason",
                "blocked_flag",
                "warning_flag",
            ],
        ),
    }

    latest_json_path = output_path / "live_run_summary_latest.json"
    latest_md_path = output_path / "live_run_summary_latest.md"
    notification_path = output_path / "live_notification_payload.json"
    latest_json_path.write_text(json.dumps({"summary": summary_row, "health_checks": health_checks}, indent=2), encoding="utf-8")
    latest_md_path.write_text(_summary_markdown(summary_row, health_checks), encoding="utf-8")
    notification_path.write_text(json.dumps(notification_payload, indent=2), encoding="utf-8")
    paths["live_run_summary_latest_json_path"] = latest_json_path
    paths["live_run_summary_latest_md_path"] = latest_md_path
    paths["live_notification_payload_path"] = notification_path

    run_summary_path = run_dir / "live_run_summary.json"
    run_summary_path.write_text(json.dumps({"summary": summary_row, "health_checks": health_checks}, indent=2), encoding="utf-8")
    paths["run_summary_path"] = run_summary_path

    return paths, health_checks, summary_row
