from __future__ import annotations

import json
from pathlib import Path

from trading_platform.binance.health import evaluate_binance_alerts, evaluate_binance_health_check
from trading_platform.binance.models import BinanceAlertsConfig, BinanceHealthCheckConfig


def test_evaluate_binance_alerts_detects_stale_and_missing_scope(tmp_path: Path) -> None:
    latest_manifest = tmp_path / "sync" / "latest_sync_manifest.json"
    latest_manifest.parent.mkdir(parents=True, exist_ok=True)
    latest_manifest.write_text(
        json.dumps(
            {
                "sync_id": "sync-1",
                "status": "completed",
                "completed_at": "2024-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    status_summary = tmp_path / "status" / "binance_status.json"
    status_summary.parent.mkdir(parents=True, exist_ok=True)
    status_summary.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "dataset_name": "crypto_market_features",
                        "dataset_family": "feature",
                        "symbol": "BTCUSDT",
                        "interval": "1m",
                        "stale": True,
                        "freshness_age_seconds": 1000,
                        "staleness_threshold_seconds": 900,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = evaluate_binance_alerts(
        BinanceAlertsConfig(
            latest_sync_manifest_path=str(latest_manifest),
            status_summary_path=str(status_summary),
            output_root=str(tmp_path / "alerts"),
            summary_path=str(tmp_path / "alerts" / "summary.json"),
            symbols=("BTCUSDT", "ETHUSDT"),
            intervals=("1m",),
            latest_sync_max_age_sec=60,
        )
    )

    assert result.status == "critical"
    payload = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert payload["alert_count"] >= 2


def test_evaluate_binance_health_check_passes_for_recent_healthy_scope(tmp_path: Path) -> None:
    latest_manifest = tmp_path / "sync" / "latest_sync_manifest.json"
    latest_manifest.parent.mkdir(parents=True, exist_ok=True)
    latest_manifest.write_text(
        json.dumps(
            {
                "sync_id": "sync-2",
                "status": "completed",
                "completed_at": "2999-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    status_summary = tmp_path / "status" / "binance_status.json"
    status_summary.parent.mkdir(parents=True, exist_ok=True)
    status_summary.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "dataset_name": "crypto_ohlcv_bars",
                        "dataset_family": "projection",
                        "symbol": "BTCUSDT",
                        "interval": "1m",
                        "stale": False,
                    },
                    {
                        "dataset_name": "crypto_market_features",
                        "dataset_family": "feature",
                        "symbol": "BTCUSDT",
                        "interval": "1m",
                        "stale": False,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = evaluate_binance_health_check(
        BinanceHealthCheckConfig(
            latest_sync_manifest_path=str(latest_manifest),
            status_summary_path=str(status_summary),
            output_root=str(tmp_path / "health"),
            summary_path=str(tmp_path / "health" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
        )
    )

    assert result.status == "healthy"
    payload = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert payload["status"] == "healthy"
