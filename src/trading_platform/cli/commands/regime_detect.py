from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_market_regime_policy_config
from trading_platform.regime.service import MarketRegimePolicyConfig, detect_market_regime


def cmd_regime_detect(args) -> None:
    policy = (
        load_market_regime_policy_config(args.policy_config)
        if getattr(args, "policy_config", None)
        else MarketRegimePolicyConfig()
    )
    result = detect_market_regime(
        input_path=Path(args.input),
        output_dir=Path(args.output_dir),
        policy=policy,
    )
    latest = result["latest"]
    print(f"Regime: {latest.get('regime_label')}")
    print(f"Confidence: {latest.get('confidence_score')}")
    print(f"Regime JSON: {result['market_regime_json_path']}")
    print(f"Regime CSV: {result['market_regime_csv_path']}")
