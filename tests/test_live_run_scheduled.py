from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd

from trading_platform.broker.live_models import BrokerAccount, LiveBrokerOrderRequest
from trading_platform.execution.reconciliation import ReconciliationResult
from trading_platform.live.preview import (
    LivePreviewConfig,
    LivePreviewHealthCheck,
    LivePreviewResult,
)
from trading_platform.cli.commands.live_run_scheduled import cmd_live_run_scheduled


def _args(tmp_path, **overrides):
    args = SimpleNamespace(
        preset="xsec_nasdaq100_momentum_v1_deploy",
        symbols=["MU", "WDC"],
        universe=None,
        strategy="xsec_momentum_topn",
        fast=None,
        slow=None,
        lookback=None,
        lookback_bars=84,
        skip_bars=21,
        top_n=2,
        weighting_scheme="inv_vol",
        vol_lookback_bars=20,
        rebalance_bars=21,
        portfolio_construction_mode="transition",
        max_position_weight=0.5,
        min_avg_dollar_volume=50_000_000.0,
        max_names_per_sector=None,
        turnover_buffer_bps=0.0,
        max_turnover_per_rebalance=0.5,
        benchmark="equal_weight",
        initial_cash=100_000.0,
        min_trade_dollars=25.0,
        lot_size=1,
        reserve_cash_pct=0.0,
        order_type="market",
        time_in_force="day",
        broker="mock",
        mock_equity=100_000.0,
        mock_cash=100_000.0,
        mock_positions_path=None,
        output_dir=str(tmp_path),
        _cli_argv=[],
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _result(tmp_path, *, statuses: list[tuple[str, str, str]]) -> LivePreviewResult:
    config = LivePreviewConfig(
        symbols=["MU", "WDC"],
        preset_name="xsec_nasdaq100_momentum_v1_deploy",
        universe_name="nasdaq100",
        strategy="xsec_momentum_topn",
        top_n=2,
        weighting_scheme="inv_vol",
        lookback_bars=84,
        skip_bars=21,
        rebalance_bars=21,
        portfolio_construction_mode="transition",
        max_position_weight=0.5,
        min_avg_dollar_volume=50_000_000.0,
        max_turnover_per_rebalance=0.5,
        benchmark="equal_weight",
        broker="mock",
        output_dir=tmp_path,
    )
    checks = [
        LivePreviewHealthCheck(
            check_name=name,
            status=status,
            message=message,
            timestamp="2025-01-04",
            preset=config.preset_name,
            strategy=config.strategy,
            universe=config.universe_name,
        )
        for name, status, message in statuses
    ]
    orders = [
        LiveBrokerOrderRequest(symbol="MU", side="BUY", quantity=100, reason="rebalance_to_target"),
        LiveBrokerOrderRequest(symbol="WDC", side="BUY", quantity=80, reason="rebalance_to_target"),
    ]
    reconciliation = ReconciliationResult(
        orders=orders,
        target_quantities={"MU": 100, "WDC": 80},
        current_quantities={},
        diagnostics={
            "investable_equity": 100_000.0,
            "target_weight_sum": 1.0,
            "order_count": 2,
        },
    )
    return LivePreviewResult(
        run_id="xsec_nasdaq100_momentum_v1_deploy|xsec_momentum_topn|nasdaq100|mock|2025-01-04",
        as_of="2025-01-04",
        config=config,
        account=BrokerAccount(account_id="acct-1", cash=100_000.0, equity=100_000.0, buying_power=100_000.0),
        positions={},
        open_orders=[],
        latest_prices={"MU": 100.0, "WDC": 50.0},
        target_weights={"MU": 0.5, "WDC": 0.5},
        target_diagnostics={
            "selected_symbols": "ALNY,APP,MU,WDC",
            "target_selected_symbols": "MU,WDC",
            "target_selected_count": 2,
            "realized_holdings_count": 24,
            "realized_holdings_minus_top_n": 22,
            "average_gross_exposure": 0.91,
            "liquidity_excluded_count": 0,
            "sector_cap_excluded_count": 0,
            "turnover_cap_binding_count": 70,
            "turnover_buffer_blocked_replacements": 0,
            "semantic_warning": "none",
            "rebalance_timestamp": "2025-01-03",
            "summary": {"mean_turnover": 0.0238},
        },
        reconciliation=reconciliation,
        adjusted_orders=orders,
        order_adjustment_diagnostics={"open_order_count": 0},
        reconciliation_rows=[
            {
                "symbol": "MU",
                "current_qty": 0,
                "current_weight": 0.0,
                "target_weight": 0.5,
                "target_notional": 50_000.0,
                "delta_notional": 50_000.0,
                "current_price": 100.0,
                "target_qty": 100,
                "delta_qty": 100,
                "proposed_side": "BUY",
                "proposed_qty": 100,
                "pending_open_order_qty": 0,
                "reason": "rebalance_to_target",
                "blocked_flag": False,
                "warning_flag": False,
            },
            {
                "symbol": "WDC",
                "current_qty": 0,
                "current_weight": 0.0,
                "target_weight": 0.5,
                "target_notional": 50_000.0,
                "delta_notional": 50_000.0,
                "current_price": 50.0,
                "target_qty": 80,
                "delta_qty": 80,
                "proposed_side": "BUY",
                "proposed_qty": 80,
                "pending_open_order_qty": 0,
                "reason": "rebalance_to_target",
                "blocked_flag": False,
                "warning_flag": False,
            },
        ],
        health_checks=checks,
    )


def test_scheduled_live_run_writes_latest_and_history(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("trading_platform.cli.commands.live_run_scheduled.apply_cli_preset", lambda args: None)
    monkeypatch.setattr("trading_platform.cli.commands.live_run_scheduled.run_live_dry_run_preview", lambda config: _result(tmp_path, statuses=[("broker_connectivity", "pass", "ok"), ("turnover_cap_bindings", "warn", "bindings=70")]))

    cmd_live_run_scheduled(_args(tmp_path))

    assert (tmp_path / "live_run_summary.csv").exists()
    assert (tmp_path / "live_run_summary_latest.json").exists()
    assert (tmp_path / "live_run_summary_latest.md").exists()
    assert (tmp_path / "live_health_checks.csv").exists()
    assert (tmp_path / "live_proposed_orders_history.csv").exists()
    assert (tmp_path / "live_reconciliation_history.csv").exists()
    assert (tmp_path / "live_notification_payload.json").exists()

    summary_df = pd.read_csv(tmp_path / "live_run_summary.csv")
    assert len(summary_df) == 1
    assert summary_df.loc[0, "readiness"] == "ready_for_manual_review"


def test_scheduled_live_run_is_idempotent_for_same_run_key(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("trading_platform.cli.commands.live_run_scheduled.apply_cli_preset", lambda args: None)
    monkeypatch.setattr("trading_platform.cli.commands.live_run_scheduled.run_live_dry_run_preview", lambda config: _result(tmp_path, statuses=[("broker_connectivity", "pass", "ok")]))

    args = _args(tmp_path)
    cmd_live_run_scheduled(args)
    cmd_live_run_scheduled(args)

    summary_df = pd.read_csv(tmp_path / "live_run_summary.csv")
    orders_df = pd.read_csv(tmp_path / "live_proposed_orders_history.csv")
    assert len(summary_df) == 1
    assert len(orders_df) == 2


def test_scheduled_live_run_blocked_exits_nonzero(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("trading_platform.cli.commands.live_run_scheduled.apply_cli_preset", lambda args: None)
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_run_scheduled.run_live_dry_run_preview",
        lambda config: _result(tmp_path, statuses=[("market_data", "fail", "missing prices")]),
    )

    try:
        cmd_live_run_scheduled(_args(tmp_path))
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("expected SystemExit(1)")

    latest = json.loads((tmp_path / "live_run_summary_latest.json").read_text(encoding="utf-8"))
    assert latest["summary"]["readiness"] == "blocked"


def test_scheduled_live_run_warning_only_is_reviewable(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("trading_platform.cli.commands.live_run_scheduled.apply_cli_preset", lambda args: None)
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_run_scheduled.run_live_dry_run_preview",
        lambda config: _result(tmp_path, statuses=[("turnover_cap_bindings", "warn", "bindings=70")]),
    )

    cmd_live_run_scheduled(_args(tmp_path))
    latest = json.loads((tmp_path / "live_run_summary_latest.json").read_text(encoding="utf-8"))
    assert latest["summary"]["readiness"] == "ready_for_manual_review"


def test_scheduled_live_run_propagates_xsec_diagnostics(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("trading_platform.cli.commands.live_run_scheduled.apply_cli_preset", lambda args: None)
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_run_scheduled.run_live_dry_run_preview",
        lambda config: _result(tmp_path, statuses=[("broker_connectivity", "pass", "ok")]),
    )

    cmd_live_run_scheduled(_args(tmp_path))
    latest = json.loads((tmp_path / "live_run_summary_latest.json").read_text(encoding="utf-8"))
    summary = latest["summary"]
    assert summary["portfolio_construction_mode"] == "transition"
    assert summary["turnover_cap_binding_count"] == 70
    assert summary["realized_holdings_count"] == 24
    notification = json.loads((tmp_path / "live_notification_payload.json").read_text(encoding="utf-8"))
    assert notification["target_names"] == ["MU", "WDC"]
