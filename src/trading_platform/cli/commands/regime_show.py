from __future__ import annotations

from pathlib import Path

from trading_platform.regime.service import load_market_regime


def cmd_regime_show(args) -> None:
    payload = load_market_regime(Path(args.regime))
    latest = payload.get("latest", {})
    print(f"Regime: {latest.get('regime_label')}")
    print(f"Confidence: {latest.get('confidence_score')}")
    print(f"Volatility: {latest.get('realized_volatility')}")
    print(f"Long return: {latest.get('long_return')}")
    print(f"Input path: {payload.get('input_path')}")
