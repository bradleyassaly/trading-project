from __future__ import annotations

import argparse

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.config.models import FeatureConfig
from trading_platform.services.feature_service import run_feature_build


def cmd_features(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Building features for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        config = FeatureConfig(
            symbol=symbol,
            feature_groups=args.feature_groups,
        )

        path = run_feature_build(config=config)

        print(f"[OK] {symbol}: saved features to {path}")