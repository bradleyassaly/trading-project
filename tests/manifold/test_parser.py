"""Tests for Manifold Markets data dump parser."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.manifold.parser import (
    ManifoldParser,
    ManifoldParseResult,
    _load_json_or_jsonl,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _market(
    market_id: str = "mkt-1",
    question: str = "Will X happen?",
    outcome_type: str = "BINARY",
    resolution: str = "YES",
    close_time_ms: int = 1700000000000,
    volume: float = 5000.0,
) -> dict:
    return {
        "id": market_id,
        "question": question,
        "outcomeType": outcome_type,
        "resolution": resolution,
        "closeTime": close_time_ms,
        "resolutionTime": close_time_ms + 3600000,
        "volume": volume,
        "probability": 0.85,
    }


def _bet(
    contract_id: str = "mkt-1",
    created_time_ms: int = 1700000000000,
    prob_before: float = 0.5,
    prob_after: float = 0.55,
    amount: float = 100.0,
    outcome: str = "YES",
) -> dict:
    return {
        "id": f"bet-{created_time_ms}",
        "contractId": contract_id,
        "createdTime": created_time_ms,
        "probBefore": prob_before,
        "probAfter": prob_after,
        "amount": amount,
        "outcome": outcome,
        "shares": amount * 1.5,
    }


def _write_dump(tmp_path: Path, markets: list[dict], bets: list[dict]) -> Path:
    """Write a minimal dump directory with markets.json and bets.json."""
    dump_dir = tmp_path / "dump"
    dump_dir.mkdir()
    (dump_dir / "markets.json").write_text(json.dumps(markets), encoding="utf-8")
    (dump_dir / "bets.json").write_text(json.dumps(bets), encoding="utf-8")
    return dump_dir


def _make_bets(market_id: str, n: int = 20, start_ms: int = 1700000000000) -> list[dict]:
    """Generate N bets with rising probability."""
    return [
        _bet(
            contract_id=market_id,
            created_time_ms=start_ms + i * 3600000,
            prob_before=0.5 + i * 0.02,
            prob_after=0.5 + (i + 1) * 0.02,
        )
        for i in range(n)
    ]


# ── JSON loading ─────────────────────────────────────────────────────────────


class TestLoadJsonOrJsonl:
    def test_loads_json_array(self, tmp_path: Path) -> None:
        path = tmp_path / "data.json"
        path.write_text(json.dumps([{"a": 1}, {"b": 2}]))
        result = _load_json_or_jsonl(path)
        assert len(result) == 2

    def test_loads_jsonl(self, tmp_path: Path) -> None:
        path = tmp_path / "data.jsonl"
        path.write_text('{"a": 1}\n{"b": 2}\n')
        result = _load_json_or_jsonl(path)
        assert len(result) == 2

    def test_skips_blank_lines(self, tmp_path: Path) -> None:
        path = tmp_path / "data.jsonl"
        path.write_text('{"a": 1}\n\n{"b": 2}\n\n')
        result = _load_json_or_jsonl(path)
        assert len(result) == 2


# ── Parser ───────────────────────────────────────────────────────────────────


class TestManifoldParser:
    def test_happy_path(self, tmp_path: Path) -> None:
        markets = [_market("m1")]
        bets = _make_bets("m1", n=20)
        dump_dir = _write_dump(tmp_path, markets, bets)
        output_dir = tmp_path / "output"

        result = ManifoldParser().parse(dump_dir, output_dir)

        assert result.markets_loaded == 1
        assert result.markets_processed == 1
        assert result.feature_files_written == 1
        assert result.resolution_records == 1

        # Feature parquet exists
        parquet = output_dir / "features" / "m1.parquet"
        assert parquet.exists()
        df = pd.read_parquet(parquet)
        assert len(df) > 0
        assert "close" in df.columns
        assert "timestamp" in df.columns

        # Resolution CSV exists
        res_path = output_dir / "resolution.csv"
        assert res_path.exists()
        with res_path.open() as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1
        assert rows[0]["ticker"] == "m1"
        assert float(rows[0]["resolution_price"]) == 100.0

    def test_filters_non_binary(self, tmp_path: Path) -> None:
        markets = [
            _market("m1", outcome_type="BINARY"),
            _market("m2", outcome_type="MULTIPLE_CHOICE"),
            _market("m3", outcome_type="FREE_RESPONSE"),
        ]
        bets = _make_bets("m1", n=20)
        dump_dir = _write_dump(tmp_path, markets, bets)

        result = ManifoldParser().parse(dump_dir, tmp_path / "out")

        assert result.markets_skipped_type == 2
        assert result.markets_processed == 1

    def test_filters_bad_resolution(self, tmp_path: Path) -> None:
        markets = [
            _market("m1", resolution="YES"),
            _market("m2", resolution="CANCEL"),
            _market("m3", resolution="MKT"),
            _market("m4", resolution="N/A"),
        ]
        bets = _make_bets("m1", n=20)
        dump_dir = _write_dump(tmp_path, markets, bets)

        result = ManifoldParser().parse(dump_dir, tmp_path / "out")

        assert result.markets_skipped_resolution == 3
        assert result.markets_processed == 1

    def test_skips_few_bets(self, tmp_path: Path) -> None:
        markets = [_market("m1")]
        bets = _make_bets("m1", n=5)
        dump_dir = _write_dump(tmp_path, markets, bets)

        result = ManifoldParser().parse(dump_dir, tmp_path / "out", min_bets=10)

        assert result.markets_skipped_few_bets == 1
        assert result.markets_processed == 0

    def test_limit_flag(self, tmp_path: Path) -> None:
        markets = [_market(f"m{i}") for i in range(5)]
        bets = []
        for i in range(5):
            bets.extend(_make_bets(f"m{i}", n=15))
        dump_dir = _write_dump(tmp_path, markets, bets)

        result = ManifoldParser().parse(dump_dir, tmp_path / "out", limit=2)

        # Only first 2 qualifying markets should be processed
        assert result.markets_processed <= 2

    def test_resolution_no_means_price_zero(self, tmp_path: Path) -> None:
        markets = [_market("m1", resolution="NO")]
        bets = _make_bets("m1", n=20)
        dump_dir = _write_dump(tmp_path, markets, bets)

        ManifoldParser().parse(dump_dir, tmp_path / "out")

        res_path = tmp_path / "out" / "resolution.csv"
        with res_path.open() as f:
            rows = list(csv.DictReader(f))
        assert float(rows[0]["resolution_price"]) == 0.0
        assert rows[0]["resolves_yes"] == "False"

    def test_multiple_markets(self, tmp_path: Path) -> None:
        markets = [_market("m1"), _market("m2", resolution="NO")]
        bets = _make_bets("m1", n=20) + _make_bets("m2", n=15)
        dump_dir = _write_dump(tmp_path, markets, bets)

        result = ManifoldParser().parse(dump_dir, tmp_path / "out")

        assert result.markets_processed == 2
        assert result.feature_files_written == 2
        assert result.resolution_records == 2

    def test_missing_markets_file(self, tmp_path: Path) -> None:
        dump_dir = tmp_path / "empty_dump"
        dump_dir.mkdir()

        result = ManifoldParser().parse(dump_dir, tmp_path / "out")

        assert result.markets_loaded == 0
        assert result.markets_processed == 0

    def test_jsonl_format(self, tmp_path: Path) -> None:
        """Parser should handle JSONL format (one JSON per line)."""
        dump_dir = tmp_path / "dump"
        dump_dir.mkdir()

        market = _market("m1")
        bets = _make_bets("m1", n=15)

        (dump_dir / "markets.jsonl").write_text(
            "\n".join(json.dumps(m) for m in [market]),
            encoding="utf-8",
        )
        (dump_dir / "bets.jsonl").write_text(
            "\n".join(json.dumps(b) for b in bets),
            encoding="utf-8",
        )

        result = ManifoldParser().parse(dump_dir, tmp_path / "out")
        assert result.markets_processed == 1

    def test_sharded_bets_directory(self, tmp_path: Path) -> None:
        """Parser should handle bets/ directory with shard files."""
        dump_dir = tmp_path / "dump"
        dump_dir.mkdir()

        markets = [_market("m1")]
        (dump_dir / "markets.json").write_text(json.dumps(markets), encoding="utf-8")

        bets_dir = dump_dir / "bets"
        bets_dir.mkdir()
        bets = _make_bets("m1", n=20)
        half = len(bets) // 2
        (bets_dir / "shard_0.json").write_text(json.dumps(bets[:half]), encoding="utf-8")
        (bets_dir / "shard_1.json").write_text(json.dumps(bets[half:]), encoding="utf-8")

        result = ManifoldParser().parse(dump_dir, tmp_path / "out")
        assert result.markets_processed == 1
