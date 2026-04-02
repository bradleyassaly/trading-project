from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from trading_platform.prediction_markets.cross_market import (
    CrossMarketMonitor,
    CrossMarketMonitorConfig,
    NormalizedPredictionMarket,
    PredictionMarketVenue,
    normalize_prediction_text,
)


class _StaticAdapter:
    def __init__(self, markets: list[NormalizedPredictionMarket]) -> None:
        self.markets = markets

    def fetch_markets(self, *, max_markets: int | None = None) -> list[NormalizedPredictionMarket]:
        if max_markets is None:
            return list(self.markets)
        return list(self.markets[:max_markets])


def _market(
    *,
    venue: PredictionMarketVenue,
    market_id: str,
    title: str,
    category: str = "politics",
    expiration: str = "2026-11-04T00:00:00+00:00",
    probability: float = 0.55,
    liquidity: float = 1000.0,
    volume: float = 500.0,
    settlement_rules: str | None = None,
) -> NormalizedPredictionMarket:
    return NormalizedPredictionMarket(
        venue=venue,
        source_market_id=market_id,
        title=title,
        category=category,
        expiration_time=expiration,
        yes_probability=probability,
        liquidity_proxy=liquidity,
        volume_proxy=volume,
        equivalence_key=f"{normalize_prediction_text(title)}|{datetime.fromisoformat(expiration).date().isoformat()}",
        normalized_title=normalize_prediction_text(title),
        matchable_tokens=tuple(normalize_prediction_text(f"{title} {settlement_rules or ''}").split()),
        settlement_rules=settlement_rules,
        raw={},
    )


def test_normalize_prediction_text_strips_punctuation_and_case() -> None:
    assert normalize_prediction_text("Will Trump win in 2028?") == "will trump win in 2028"


def test_cross_market_monitor_accepts_obvious_true_match(tmp_path: Path) -> None:
    kalshi = _market(
        venue=PredictionMarketVenue.KALSHI,
        market_id="K-1",
        title="Will Trump win the 2028 presidential election?",
        probability=0.61,
        settlement_rules="Resolves to Yes if Donald Trump wins the 2028 U.S. presidential election.",
    )
    polymarket = _market(
        venue=PredictionMarketVenue.POLYMARKET,
        market_id="P-1",
        title="Will Trump win the 2028 presidential election",
        probability=0.54,
        settlement_rules="This market resolves to Yes if Donald Trump wins the 2028 US presidential election.",
    )
    monitor = CrossMarketMonitor(
        kalshi_adapter=_StaticAdapter([kalshi]),
        polymarket_adapter=_StaticAdapter([polymarket]),
        config=CrossMarketMonitorConfig(output_dir=str(tmp_path), min_probability_spread=0.02),
    )

    summary = monitor.run()

    assert summary.total_candidate_matches == 1
    assert summary.total_accepted_matches == 1
    assert summary.total_opportunities == 1
    opportunity_rows = (tmp_path / "cross_market_opportunities.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(opportunity_rows) == 1
    payload = json.loads(opportunity_rows[0])
    assert payload["kalshi_market_id"] == "K-1"
    assert payload["polymarket_market_id"] == "P-1"


def test_cross_market_monitor_rejects_obvious_false_match(tmp_path: Path) -> None:
    kalshi = _market(
        venue=PredictionMarketVenue.KALSHI,
        market_id="K-2",
        title="Will inflation exceed 4 percent in 2026?",
        category="economics",
    )
    polymarket = _market(
        venue=PredictionMarketVenue.POLYMARKET,
        market_id="P-2",
        title="Will Bitcoin reach $150k in 2026?",
        category="crypto",
    )
    monitor = CrossMarketMonitor(
        kalshi_adapter=_StaticAdapter([kalshi]),
        polymarket_adapter=_StaticAdapter([polymarket]),
        config=CrossMarketMonitorConfig(output_dir=str(tmp_path)),
    )

    summary = monitor.run()

    assert summary.total_candidate_matches == 1
    assert summary.total_accepted_matches == 0
    assert summary.total_opportunities == 0
    match_payload = json.loads((tmp_path / "cross_market_matches.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert match_payload["accepted"] is False
    assert "match_score_below_threshold" in match_payload["rejection_reasons"]


def test_cross_market_monitor_rejects_settlement_mismatch(tmp_path: Path) -> None:
    kalshi = _market(
        venue=PredictionMarketVenue.KALSHI,
        market_id="K-3",
        title="Will CPI exceed 3 percent by June 2026?",
        expiration="2026-06-30T00:00:00+00:00",
        settlement_rules="Resolves to Yes if CPI exceeds 3 percent by June 30, 2026.",
    )
    polymarket = _market(
        venue=PredictionMarketVenue.POLYMARKET,
        market_id="P-3",
        title="Will CPI exceed 3 percent by July 2026?",
        expiration="2026-07-31T00:00:00+00:00",
        settlement_rules="Resolves to Yes if CPI exceeds 3 percent by July 31, 2026.",
    )
    monitor = CrossMarketMonitor(
        kalshi_adapter=_StaticAdapter([kalshi]),
        polymarket_adapter=_StaticAdapter([polymarket]),
        config=CrossMarketMonitorConfig(output_dir=str(tmp_path), max_expiration_diff_hours=24.0),
    )

    summary = monitor.run()

    assert summary.total_accepted_matches == 0
    match_payload = json.loads((tmp_path / "cross_market_matches.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert "numeric_token_mismatch" in match_payload["settlement_mismatch_flags"] or "expiration_mismatch" in match_payload["settlement_mismatch_flags"]


def test_cross_market_monitor_persists_spread_history_across_runs(tmp_path: Path) -> None:
    kalshi = _market(
        venue=PredictionMarketVenue.KALSHI,
        market_id="K-4",
        title="Will Democrats win the Senate in 2026?",
        probability=0.58,
    )
    polymarket = _market(
        venue=PredictionMarketVenue.POLYMARKET,
        market_id="P-4",
        title="Will Democrats win the Senate in 2026?",
        probability=0.50,
    )
    monitor = CrossMarketMonitor(
        kalshi_adapter=_StaticAdapter([kalshi]),
        polymarket_adapter=_StaticAdapter([polymarket]),
        config=CrossMarketMonitorConfig(output_dir=str(tmp_path), min_probability_spread=0.01, append_history=True),
    )

    first_summary = monitor.run()
    second_summary = monitor.run()

    assert first_summary.total_opportunities == 1
    assert second_summary.persistence_summary[0]["observation_count"] == 2
    opportunity_rows = (tmp_path / "cross_market_opportunities.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(opportunity_rows) == 2


def test_cross_market_monitor_generates_report_artifacts(tmp_path: Path) -> None:
    kalshi = _market(
        venue=PredictionMarketVenue.KALSHI,
        market_id="K-5",
        title="Will the Fed cut rates in September 2026?",
        category="economics",
        probability=0.44,
    )
    polymarket = _market(
        venue=PredictionMarketVenue.POLYMARKET,
        market_id="P-5",
        title="Will the Fed cut rates in September 2026?",
        category="economics",
        probability=0.51,
    )
    monitor = CrossMarketMonitor(
        kalshi_adapter=_StaticAdapter([kalshi]),
        polymarket_adapter=_StaticAdapter([polymarket]),
        config=CrossMarketMonitorConfig(output_dir=str(tmp_path), min_probability_spread=0.01, snapshot_tag="daily-check"),
    )

    summary = monitor.run()

    assert summary.snapshot_tag == "daily-check"
    assert (tmp_path / "cross_market_summary.json").exists()
    assert (tmp_path / "cross_market_report.md").exists()
    report_text = (tmp_path / "cross_market_report.md").read_text(encoding="utf-8")
    assert "Cross-Market Monitor Report" in report_text
    assert "Strongest Opportunities" in report_text
