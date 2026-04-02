from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.cross_market_monitor import cmd_cross_market_monitor
from trading_platform.kalshi.auth import KalshiConfig


def test_cross_market_monitor_cli_builds_config_and_runs(monkeypatch, tmp_path: Path, capsys) -> None:
    market_config = tmp_path / "kalshi.yaml"
    market_config.write_text(
        """
environment:
  demo: false
""".strip(),
        encoding="utf-8",
    )
    research_config = tmp_path / "kalshi_research.yaml"
    research_config.write_text(
        """
cross_market_monitor:
  output_dir: artifacts/cross_market/custom
  kalshi_max_markets: 40
  polymarket_max_markets: 55
  min_probability_spread: 0.04
  match_threshold: 0.88
  ambiguity_margin: 0.02
  append_history: false
""".strip(),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    class FakeMonitor:
        def __init__(self, *, kalshi_adapter, polymarket_adapter, config):
            captured["config"] = config

        def run(self):
            output_dir = Path(str(captured["config"].output_dir))
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "cross_market_summary.json").write_text(
                '{"category_breakdown": [{"category": "politics", "opportunity_count": 1}]}',
                encoding="utf-8",
            )
            return SimpleNamespace(
                total_kalshi_markets=10,
                total_polymarket_markets=12,
                total_candidate_matches=4,
                total_accepted_matches=2,
                total_opportunities=1,
                average_spread=0.05,
                max_spread=0.08,
            )

    class FakeKalshiClient:
        def __init__(self, config):
            captured["kalshi_client_config"] = config

    class FakePolymarketClient:
        def __init__(self, config):
            captured["polymarket_config"] = config

    monkeypatch.setattr(
        "trading_platform.cli.commands.cross_market_monitor.CrossMarketMonitor",
        FakeMonitor,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.cross_market_monitor.KalshiClient",
        FakeKalshiClient,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.cross_market_monitor.PolymarketClient",
        FakePolymarketClient,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.cross_market_monitor.KalshiConfig.from_env",
        classmethod(lambda cls: KalshiConfig(api_key_id="key", private_key_pem="pem", demo=False)),
    )

    args = SimpleNamespace(
        config=str(research_config),
        kalshi_config=str(market_config),
        output_dir=None,
        kalshi_max_markets=None,
        polymarket_max_markets=None,
        min_probability_spread=None,
        match_threshold=None,
        ambiguity_margin=None,
        max_expiration_diff_hours=None,
        min_title_similarity=None,
        min_token_overlap=None,
        snapshot_tag=None,
        append_history=None,
    )

    cmd_cross_market_monitor(args)

    config = captured["config"]
    assert config.kalshi_max_markets == 40
    assert config.polymarket_max_markets == 55
    assert config.min_probability_spread == 0.04
    assert config.match_threshold == 0.88
    assert config.append_history is False

    stdout = capsys.readouterr().out
    assert "Cross-Market Monitor" in stdout
    assert "Candidate matches" in stdout
