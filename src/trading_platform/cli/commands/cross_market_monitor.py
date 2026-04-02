from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.kalshi.client import KalshiClient
from trading_platform.polymarket.client import PolymarketClient, PolymarketConfig
from trading_platform.prediction_markets.cross_market import (
    CrossMarketMonitor,
    CrossMarketMonitorConfig,
    KalshiMarketAdapter,
    PolymarketMarketAdapter,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _load_yaml(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    config_path = Path(path)
    if not config_path.exists():
        return {}
    with config_path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _project_relative_path(path_str: str | None, default: str) -> Path:
    raw = path_str or default
    path = Path(raw)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _build_kalshi_config(config_path: str | None) -> KalshiConfig:
    yaml_config = _load_yaml(config_path)
    demo = bool(yaml_config.get("environment", {}).get("demo", False))
    auth_cfg = yaml_config.get("auth", {})
    if auth_cfg:
        config = KalshiConfig.from_mapping(
            auth_cfg,
            env_fallback=True,
            demo=demo,
            source_label=f"Kalshi auth ({config_path or 'config'})",
        )
        if config is None:
            raise ValueError("Kalshi auth is required for cross-market monitoring.")
        return config
    env_config = KalshiConfig.from_env()
    return KalshiConfig(
        api_key_id=env_config.api_key_id,
        private_key_pem=env_config.private_key_pem,
        demo=demo,
        private_key_path=env_config.private_key_path,
    )


def cmd_cross_market_monitor(args: argparse.Namespace) -> None:
    research_cfg = _load_yaml(getattr(args, "config", None))
    monitor_cfg = research_cfg.get("cross_market_monitor", {})

    output_dir = _project_relative_path(
        getattr(args, "output_dir", None) or monitor_cfg.get("output_dir"),
        "artifacts/cross_market",
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    config = CrossMarketMonitorConfig(
        output_dir=str(output_dir),
        min_probability_spread=float(
            getattr(args, "min_probability_spread", None)
            or monitor_cfg.get("min_probability_spread", 0.03)
        ),
        match_threshold=float(
            getattr(args, "match_threshold", None)
            or monitor_cfg.get("match_threshold", 0.84)
        ),
        ambiguity_margin=float(
            getattr(args, "ambiguity_margin", None)
            or monitor_cfg.get("ambiguity_margin", 0.03)
        ),
        max_expiration_diff_hours=float(
            getattr(args, "max_expiration_diff_hours", None)
            or monitor_cfg.get("max_expiration_diff_hours", 24.0)
        ),
        min_title_similarity=float(
            getattr(args, "min_title_similarity", None)
            or monitor_cfg.get("min_title_similarity", 0.72)
        ),
        min_token_overlap=float(
            getattr(args, "min_token_overlap", None)
            or monitor_cfg.get("min_token_overlap", 0.60)
        ),
        kalshi_max_markets=getattr(args, "kalshi_max_markets", None)
        or monitor_cfg.get("kalshi_max_markets", 250),
        polymarket_max_markets=getattr(args, "polymarket_max_markets", None)
        or monitor_cfg.get("polymarket_max_markets", 250),
        append_history=bool(
            monitor_cfg.get("append_history", True)
            if getattr(args, "append_history", None) is None
            else args.append_history
        ),
        snapshot_tag=getattr(args, "snapshot_tag", None) or monitor_cfg.get("snapshot_tag"),
    )

    polymarket_client = PolymarketClient(
        PolymarketConfig(
            request_sleep_sec=float(monitor_cfg.get("request_sleep_sec", 0.05)),
        )
    )
    kalshi_client = KalshiClient(_build_kalshi_config(getattr(args, "kalshi_config", None)))
    monitor = CrossMarketMonitor(
        kalshi_adapter=KalshiMarketAdapter(kalshi_client),
        polymarket_adapter=PolymarketMarketAdapter(polymarket_client),
        config=config,
    )

    print("Cross-Market Monitor")
    print(f"  output dir             : {config.output_dir}")
    print(f"  Kalshi max markets     : {config.kalshi_max_markets}")
    print(f"  Polymarket max markets : {config.polymarket_max_markets}")
    print(f"  match threshold        : {config.match_threshold}")
    print(f"  min spread             : {config.min_probability_spread}")
    if config.snapshot_tag:
        print(f"  snapshot tag           : {config.snapshot_tag}")

    summary = monitor.run()

    print("\nCross-market scan complete.")
    print(f"  Kalshi scanned    : {summary.total_kalshi_markets}")
    print(f"  Polymarket scanned: {summary.total_polymarket_markets}")
    print(f"  Candidate matches : {summary.total_candidate_matches}")
    print(f"  Accepted matches  : {summary.total_accepted_matches}")
    print(f"  Opportunities     : {summary.total_opportunities}")
    print(f"  Avg spread        : {summary.average_spread:.4f}")
    print(f"  Max spread        : {summary.max_spread:.4f}")

    summary_path = Path(config.output_dir) / "cross_market_summary.json"
    if summary_path.exists():
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        print("\nArtifacts written:")
        print(f"  Matches      : {Path(config.output_dir) / 'cross_market_matches.jsonl'}")
        print(f"  Opportunities: {Path(config.output_dir) / 'cross_market_opportunities.jsonl'}")
        print(f"  Summary      : {summary_path}")
        print(f"  Report       : {Path(config.output_dir) / 'cross_market_report.md'}")
        print(f"  Categories   : {payload.get('category_breakdown', [])}")
