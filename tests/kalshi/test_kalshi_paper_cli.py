from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.kalshi_paper_run import cmd_kalshi_paper_run
from trading_platform.kalshi.auth import KalshiConfig


def test_kalshi_paper_cli_builds_config_and_runs(monkeypatch, tmp_path: Path, capsys) -> None:
    market_config = tmp_path / "kalshi.yaml"
    market_config.write_text(
        """
environment:
  demo: false
ingestion:
  tracked_tickers:
    - KTEST-1
  orderbook_depth: 12
  orderbook_poll_interval_sec: 7
risk:
  max_drawdown_pct: 0.25
  max_single_trade_contracts: 8
""".strip(),
        encoding="utf-8",
    )
    research_config = tmp_path / "kalshi_research.yaml"
    research_config.write_text(
        """
signals:
  families:
    - kalshi_taker_imbalance
paper:
  state_path: artifacts/kalshi_paper/custom_state.json
  output_dir: artifacts/kalshi_paper/custom_output
  initial_cash: 1500
  max_iterations: 2
  execution:
    min_recent_trades: 12
    max_spread: 0.05
    entry_threshold: 0.7
  risk:
    max_exposure_per_market: 75
""".strip(),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    class FakeTrader:
        def __init__(self, *, client, config, signal_families):
            captured["config"] = config
            captured["signal_families"] = signal_families

        def run(self):
            return SimpleNamespace(
                markets_polled=1,
                candidate_signals=1,
                executed_entries=0,
                executed_exits=0,
                open_positions=0,
                cash=1500.0,
                equity=1500.0,
                current_drawdown_pct=0.0,
                halt_reason=None,
            )

    class FakeClient:
        def __init__(self, config):
            captured["client_config"] = config

    monkeypatch.setattr(
        "trading_platform.cli.commands.kalshi_paper_run.KalshiPaperTrader",
        FakeTrader,
    )
    monkeypatch.setattr(
        "trading_platform.kalshi.client.KalshiClient",
        FakeClient,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.kalshi_paper_run.KalshiConfig.from_env",
        classmethod(lambda cls: KalshiConfig(api_key_id="key", private_key_pem="pem", demo=False)),
    )

    args = SimpleNamespace(
        config=str(market_config),
        research_config=str(research_config),
        state_path=None,
        output_dir=None,
        tracked_series=None,
        tracked_tickers=None,
        entry_threshold=None,
        orderbook_depth=None,
        poll_interval_seconds=None,
        max_iterations=None,
    )

    cmd_kalshi_paper_run(args)

    config = captured["config"]
    assert config.signal_family_names == ("kalshi_taker_imbalance",)
    assert config.tracked_tickers == ("KTEST-1",)
    assert config.execution.orderbook_depth == 12
    assert config.execution.entry_threshold == 0.7
    assert config.risk.max_drawdown_pct == 0.25
    assert config.risk.max_exposure_per_market == 75.0
    assert Path(config.state_path).name == "custom_state.json"
    assert Path(config.output_dir).name == "custom_output"

    stdout = capsys.readouterr().out
    assert "Kalshi Paper Trading" in stdout
    assert "Markets polled" in stdout
