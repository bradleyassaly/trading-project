from __future__ import annotations

import json
from pathlib import Path

from trading_platform.config.loader import load_multi_strategy_portfolio_config
from trading_platform.live.preview import (
    LivePreviewConfig,
    run_live_dry_run_preview_for_targets,
    write_live_dry_run_artifacts,
)
from trading_platform.portfolio.multi_strategy import (
    allocate_multi_strategy_portfolio,
    write_multi_strategy_artifacts,
)


def cmd_live_dry_run_multi_strategy(args) -> None:
    portfolio_config = load_multi_strategy_portfolio_config(args.config)
    allocation_result = allocate_multi_strategy_portfolio(portfolio_config)
    allocation_paths = write_multi_strategy_artifacts(allocation_result, Path(args.output_dir))

    target_diagnostics = {
        "portfolio_construction_mode": "multi_strategy",
        "rebalance_timestamp": allocation_result.as_of,
        "selected_symbols": ",".join(sorted(set(row["symbol"] for row in allocation_result.sleeve_rows))),
        "target_selected_symbols": ",".join(sorted(allocation_result.combined_target_weights)),
        "target_selected_count": len(allocation_result.combined_target_weights),
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
        "multi_strategy_allocation": allocation_result.summary,
    }
    preview_config = LivePreviewConfig(
        symbols=sorted(allocation_result.combined_target_weights),
        preset_name="multi_strategy",
        universe_name=f"{allocation_result.summary['enabled_sleeve_count']}_sleeves",
        strategy="multi_strategy",
        reserve_cash_pct=portfolio_config.cash_reserve_pct,
        broker=args.broker,
        output_dir=Path(args.output_dir),
    )
    result = run_live_dry_run_preview_for_targets(
        config=preview_config,
        as_of=allocation_result.as_of,
        target_weights=allocation_result.combined_target_weights,
        latest_prices=allocation_result.latest_prices,
        target_diagnostics=target_diagnostics,
    )
    live_paths = write_live_dry_run_artifacts(result)

    summary_payload = json.loads((Path(args.output_dir) / "live_dry_run_summary.json").read_text(encoding="utf-8"))
    print(f"As of: {result.as_of}")
    print(f"Broker: {preview_config.broker}")
    print(f"Enabled sleeves: {allocation_result.summary['enabled_sleeve_count']}")
    print(f"Adjusted proposed orders: {summary_payload['adjusted_order_count']}")
    print(f"Gross exposure: {allocation_result.summary['gross_exposure_after_constraints']:.6f}")
    print("Artifacts:")
    combined_paths = {**allocation_paths, **live_paths}
    for name, path in sorted(combined_paths.items()):
        print(f"  {name}: {path}")
