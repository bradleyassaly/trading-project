from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from trading_platform.broker.live_models import BrokerAccount, LiveBrokerPosition
from trading_platform.cli.commands.live_dry_run import cmd_live_dry_run


def test_cmd_live_dry_run_prints_reconciled_orders(monkeypatch, capsys) -> None:
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
        "trading_platform.cli.commands.live_dry_run.load_signal_snapshot",
        lambda **kwargs: DummySnapshot(),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run.compute_latest_target_weights",
        lambda **kwargs: (
            "2025-01-04",
            {"MSFT": 1.0},
            {"MSFT": 1.0},
            {"selected_symbols": ["MSFT"]},
        ),
    )

    class FakeBroker:
        def __init__(self, config):
            self.config = config

        def get_account(self):
            return BrokerAccount(
                account_id="acct-1",
                cash=10_000.0,
                equity=10_000.0,
                buying_power=10_000.0,
            )

        def get_positions(self):
            return {
                "AAPL": LiveBrokerPosition(
                    symbol="AAPL",
                    quantity=10,
                    avg_price=100.0,
                    market_price=101.0,
                    market_value=1_010.0,
                )
            }

    class FakeBrokerConfig:
        @classmethod
        def from_env(cls):
            return cls()

    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run.AlpacaBroker",
        FakeBroker,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run.AlpacaBrokerConfig",
        FakeBrokerConfig,
    )

    args = SimpleNamespace(
        symbols=["AAPL", "MSFT"],
        universe=None,
        strategy="sma_cross",
        fast=None,
        slow=None,
        lookback=None,
        top_n=1,
        weighting_scheme="equal",
        vol_window=20,
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
    )

    cmd_live_dry_run(args)

    stdout = capsys.readouterr().out
    assert "Running live dry-run for 2 symbol(s): AAPL, MSFT" in stdout
    assert "As of: 2025-01-04" in stdout
    assert "Computed orders:" in stdout
    assert "BUY" in stdout or "SELL" in stdout