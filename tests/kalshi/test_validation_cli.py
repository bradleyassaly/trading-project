from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.kalshi_validate_dataset import cmd_kalshi_validate_dataset


def test_kalshi_validate_dataset_cli_runs_and_writes_artifacts(tmp_path: Path, capsys) -> None:
    normalized_dir = tmp_path / "data" / "kalshi" / "normalized"
    trades_dir = normalized_dir / "trades"
    candles_dir = normalized_dir / "candles"
    raw_dir = tmp_path / "data" / "kalshi" / "raw"
    features_dir = tmp_path / "data" / "kalshi" / "features" / "real"
    for path in (trades_dir, candles_dir, raw_dir, features_dir):
        path.mkdir(parents=True, exist_ok=True)

    (tmp_path / "kalshi.yaml").write_text(
        f"""
data_validation:
  normalized_markets_path: "{(normalized_dir / 'markets.parquet').as_posix()}"
  normalized_trades_path: "{trades_dir.as_posix()}"
  normalized_candles_path: "{candles_dir.as_posix()}"
  resolution_csv_path: "{(normalized_dir / 'resolution.csv').as_posix()}"
  ingest_summary_path: "{(raw_dir / 'ingest_summary.json').as_posix()}"
  ingest_manifest_path: "{(raw_dir / 'ingest_manifest.json').as_posix()}"
  ingest_checkpoint_path: "{(raw_dir / 'ingest_checkpoint.json').as_posix()}"
  features_dir: "{features_dir.as_posix()}"
  output_dir: "{(tmp_path / 'validation_out').as_posix()}"
""".strip(),
        encoding="utf-8",
    )

    import json
    import polars as pl

    pl.DataFrame(
        {
            "ticker": ["REAL-001"],
            "market_id": ["m-1"],
            "category": ["Economics"],
            "close_time": ["2026-01-01T00:00:00Z"],
            "result": ["yes"],
            "source_tier": ["historical"],
        }
    ).write_parquet(normalized_dir / "markets.parquet")
    pl.DataFrame(
        {
            "ticker": ["REAL-001"],
            "resolution_price": [100.0],
            "result": ["yes"],
            "close_time": ["2026-01-01T00:00:00Z"],
            "source_tier": ["historical"],
        }
    ).write_csv(normalized_dir / "resolution.csv")
    pl.DataFrame(
        {
            "trade_id": ["t-1"],
            "ticker": ["REAL-001"],
            "traded_at": ["2026-01-01T00:00:00Z"],
            "yes_price": [55.0],
            "count": [1.0],
        }
    ).write_parquet(trades_dir / "REAL-001.parquet")
    pl.DataFrame(
        {
            "timestamp": ["2026-01-01T00:00:00Z"],
            "open": [50.0],
            "high": [55.0],
            "low": [49.0],
            "close": [54.0],
            "volume": [10.0],
        }
    ).write_parquet(candles_dir / "REAL-001.parquet")

    ingest_payload = {
        "markets_downloaded": 1,
        "markets_after_filters": 1,
        "markets_excluded_by_filters": 0,
        "filter_diagnostics": {
            "total_markets_before_filters": 1,
            "retained_markets": 1,
            "excluded_markets_total": 0,
            "excluded_by_category": 0,
            "excluded_by_series_pattern": 0,
            "excluded_by_min_volume": 0,
            "excluded_by_bracket": 0,
            "effective_filter_config": {},
        },
    }
    (raw_dir / "ingest_summary.json").write_text(json.dumps(ingest_payload), encoding="utf-8")
    (raw_dir / "ingest_manifest.json").write_text(json.dumps(ingest_payload), encoding="utf-8")
    (raw_dir / "ingest_checkpoint.json").write_text(json.dumps({"processed_tickers": ["REAL-001"]}), encoding="utf-8")

    args = SimpleNamespace(
        config=str(tmp_path / "kalshi.yaml"),
        markets_path=None,
        trades_path=None,
        candles_path=None,
        resolution_path=None,
        ingest_summary_path=None,
        ingest_manifest_path=None,
        ingest_checkpoint_path=None,
        features_dir=None,
        output_dir=None,
    )
    cmd_kalshi_validate_dataset(args)

    stdout = capsys.readouterr().out
    assert "Kalshi Dataset Validation" in stdout
    assert "Validation complete." in stdout
    assert (tmp_path / "validation_out" / "kalshi_data_validation_summary.json").exists()
