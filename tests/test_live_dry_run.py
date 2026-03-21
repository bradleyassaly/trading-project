from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd

from trading_platform.broker.live_models import BrokerAccount, LiveBrokerPosition
from trading_platform.cli.commands.live_dry_run import cmd_live_dry_run


def _base_args(tmp_path, **overrides):
    args = SimpleNamespace(
        preset=None,
        symbols=["AAPL", "MSFT"],
        universe=None,
        strategy="sma_cross",
        fast=None,
        slow=None,
        lookback=None,
        lookback_bars=84,
        skip_bars=21,
        top_n=1,
        weighting_scheme="equal",
        vol_window=20,
        vol_lookback_bars=20,
        min_score=None,
        max_weight=None,
        max_names_per_group=None,
        max_group_weight=None,
        group_map_path=None,
        rebalance_frequency="daily",
        timing="next_bar",
        initial_cash=100_000.0,
        min_trade_dollars=1.0,
        lot_size=1,
        reserve_cash_pct=0.0,
        order_type="market",
        time_in_force="day",
        broker="mock",
        mock_equity=100_000.0,
        mock_cash=100_000.0,
        mock_positions_path=None,
        output_dir=str(tmp_path),
        portfolio_construction_mode="pure_topn",
        max_position_weight=None,
        min_avg_dollar_volume=None,
        max_names_per_sector=None,
        turnover_buffer_bps=0.0,
        max_turnover_per_rebalance=None,
        benchmark=None,
        _cli_argv=[],
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_cmd_live_dry_run_writes_reconciliation_artifacts(monkeypatch, capsys, tmp_path) -> None:
    snapshot_index = pd.to_datetime(["2025-01-03", "2025-01-04"])
    closes = pd.DataFrame(
        {"AAPL": [100.0, 101.0], "MSFT": [200.0, 202.0]},
        index=snapshot_index,
    )
    scores = pd.DataFrame(
        {"AAPL": [1.0, 2.0], "MSFT": [1.5, 2.5]},
        index=snapshot_index,
    )
    asset_returns = pd.DataFrame(
        {"AAPL": [0.0, 0.01], "MSFT": [0.0, 0.02]},
        index=snapshot_index,
    )

    class DummySnapshot:
        def __init__(self):
            self.closes = closes
            self.scores = scores
            self.asset_returns = asset_returns
            self.skipped_symbols = []

    monkeypatch.setattr(
        "trading_platform.live.preview.load_signal_snapshot",
        lambda **kwargs: DummySnapshot(),
    )
    monkeypatch.setattr(
        "trading_platform.live.preview.compute_latest_target_weights",
        lambda **kwargs: (
            "2025-01-04",
            {"MSFT": 1.0},
            {"MSFT": 1.0},
            {"selected_symbols": ["MSFT"], "target_selected_symbols": "MSFT", "target_selected_count": 1, "realized_holdings_count": 1, "realized_holdings_minus_top_n": 0, "average_gross_exposure": 1.0, "liquidity_excluded_count": 0, "sector_cap_excluded_count": 0, "turnover_cap_binding_count": 0, "turnover_buffer_blocked_replacements": 0, "semantic_warning": ""},
        ),
    )

    args = _base_args(tmp_path)
    cmd_live_dry_run(args)

    stdout = capsys.readouterr().out
    assert "Running live dry-run for 2 symbol(s): AAPL, MSFT" in stdout
    assert "Adjusted proposed orders:" in stdout
    assert (tmp_path / "live_dry_run_summary.json").exists()
    assert (tmp_path / "live_dry_run_summary.md").exists()
    assert (tmp_path / "live_dry_run_target_positions.csv").exists()
    assert (tmp_path / "live_dry_run_current_positions.csv").exists()
    assert (tmp_path / "live_dry_run_proposed_orders.csv").exists()
    assert (tmp_path / "live_dry_run_reconciliation.csv").exists()
    assert (tmp_path / "live_dry_run_health_checks.csv").exists()

    payload = json.loads((tmp_path / "live_dry_run_summary.json").read_text(encoding="utf-8"))
    assert payload["adjusted_order_count"] >= 1
    assert payload["health_checks"]

    reconciliation_df = pd.read_csv(tmp_path / "live_dry_run_reconciliation.csv")
    assert {"symbol", "current_qty", "target_weight", "proposed_side", "reason", "blocked_flag", "warning_flag"}.issubset(reconciliation_df.columns)


def test_cmd_live_dry_run_no_orders_when_already_matched(monkeypatch, tmp_path) -> None:
    snapshot_index = pd.to_datetime(["2025-01-03", "2025-01-04"])

    class DummySnapshot:
        def __init__(self):
            self.closes = pd.DataFrame({"AAPL": [100.0, 100.0]}, index=snapshot_index)
            self.scores = pd.DataFrame({"AAPL": [1.0, 1.0]}, index=snapshot_index)
            self.asset_returns = pd.DataFrame({"AAPL": [0.0, 0.0]}, index=snapshot_index)
            self.skipped_symbols = []

    monkeypatch.setattr(
        "trading_platform.live.preview.load_signal_snapshot",
        lambda **kwargs: DummySnapshot(),
    )
    monkeypatch.setattr(
        "trading_platform.live.preview.compute_latest_target_weights",
        lambda **kwargs: (
            "2025-01-04",
            {"AAPL": 1.0},
            {"AAPL": 1.0},
            {"selected_symbols": ["AAPL"], "target_selected_symbols": "AAPL", "target_selected_count": 1, "realized_holdings_count": 1, "realized_holdings_minus_top_n": 0, "average_gross_exposure": 1.0, "liquidity_excluded_count": 0, "sector_cap_excluded_count": 0, "turnover_cap_binding_count": 0, "turnover_buffer_blocked_replacements": 0, "semantic_warning": ""},
        ),
    )

    positions_path = tmp_path / "positions.csv"
    pd.DataFrame(
        [{"symbol": "AAPL", "quantity": 1000, "avg_price": 90.0, "market_price": 100.0}]
    ).to_csv(positions_path, index=False)

    args = _base_args(
        tmp_path,
        symbols=["AAPL"],
        mock_positions_path=str(positions_path),
    )
    cmd_live_dry_run(args)

    payload = json.loads((tmp_path / "live_dry_run_summary.json").read_text(encoding="utf-8"))
    assert payload["adjusted_order_count"] == 0
    reconciliation_df = pd.read_csv(tmp_path / "live_dry_run_reconciliation.csv")
    assert "already_at_target" in reconciliation_df["reason"].tolist()


def test_cmd_live_dry_run_uses_xsec_preset_diagnostics(monkeypatch, capsys, tmp_path) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run.apply_cli_preset",
        lambda args: setattr(args, "_resolved_preset", "xsec_nasdaq100_momentum_v1_deploy"),
    )
    monkeypatch.setattr(
        "trading_platform.live.preview._compute_latest_xsec_target_weights",
        lambda config: (
            "2025-01-04",
            {"MU": 0.5, "WDC": 0.5},
            {"MU": 0.5, "WDC": 0.5},
            {"MU": 100.0, "WDC": 50.0},
            {"MU": 1.0, "WDC": 0.9},
            {
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
            },
            [],
        ),
    )

    args = _base_args(
        tmp_path,
        preset="xsec_nasdaq100_momentum_v1_deploy",
        universe="nasdaq100",
        symbols=None,
        strategy="xsec_momentum_topn",
        top_n=2,
        portfolio_construction_mode="transition",
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run.resolve_symbols",
        lambda _args: ["MU", "WDC", "ALNY", "APP"],
    )
    cmd_live_dry_run(args)

    stdout = capsys.readouterr().out
    assert "Preset: xsec_nasdaq100_momentum_v1_deploy" in stdout
    assert "portfolio_construction_mode: transition" in stdout
    assert "realized_holdings_count: 24" in stdout
    payload = json.loads((tmp_path / "live_dry_run_summary.json").read_text(encoding="utf-8"))
    assert payload["preset_name"] == "xsec_nasdaq100_momentum_v1_deploy"
    assert payload["portfolio_construction_mode"] == "transition"
    assert payload["turnover_cap_binding_count"] == 70


def test_cmd_live_dry_run_with_mock_broker_never_submits(monkeypatch, tmp_path) -> None:
    snapshot_index = pd.to_datetime(["2025-01-03", "2025-01-04"])

    class DummySnapshot:
        def __init__(self):
            self.closes = pd.DataFrame({"AAPL": [100.0, 101.0]}, index=snapshot_index)
            self.scores = pd.DataFrame({"AAPL": [1.0, 2.0]}, index=snapshot_index)
            self.asset_returns = pd.DataFrame({"AAPL": [0.0, 0.01]}, index=snapshot_index)
            self.skipped_symbols = []

    monkeypatch.setattr(
        "trading_platform.live.preview.load_signal_snapshot",
        lambda **kwargs: DummySnapshot(),
    )
    monkeypatch.setattr(
        "trading_platform.live.preview.compute_latest_target_weights",
        lambda **kwargs: (
            "2025-01-04",
            {"AAPL": 1.0},
            {"AAPL": 1.0},
            {"selected_symbols": ["AAPL"], "target_selected_symbols": "AAPL", "target_selected_count": 1, "realized_holdings_count": 1, "realized_holdings_minus_top_n": 0, "average_gross_exposure": 1.0, "liquidity_excluded_count": 0, "sector_cap_excluded_count": 0, "turnover_cap_binding_count": 0, "turnover_buffer_blocked_replacements": 0, "semantic_warning": ""},
        ),
    )

    class SubmitTrapBroker:
        def get_account(self):
            return BrokerAccount(account_id="acct-1", cash=100_000.0, equity=100_000.0, buying_power=100_000.0)

        def get_positions(self):
            return {}

        def list_open_orders(self):
            return []

        def submit_orders(self, orders):
            raise AssertionError("dry-run must not submit orders")

    monkeypatch.setattr("trading_platform.live.preview._resolve_broker", lambda config: SubmitTrapBroker())
    args = _base_args(tmp_path, symbols=["AAPL"])
    cmd_live_dry_run(args)

    payload = json.loads((tmp_path / "live_dry_run_summary.json").read_text(encoding="utf-8"))
    assert payload["adjusted_order_count"] >= 0
    assert payload["broker"] == "mock"
