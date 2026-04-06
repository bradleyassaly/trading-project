"""Tests for historical trade backfill script."""
from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _write_resolution_csv(path: Path, markets: list[dict]) -> None:
    fieldnames = ["ticker", "condition_id", "resolution_price", "resolves_yes",
                   "question", "close_time", "volume"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(markets)


def _resolved_market(ticker: str = "tok1", volume: float = 10000, rp: float = 100.0) -> dict:
    return {
        "ticker": ticker, "condition_id": "0xabc", "resolution_price": str(rp),
        "resolves_yes": str(rp >= 99), "question": "Q?",
        "close_time": "2026-04-01", "volume": str(volume),
    }


class TestBackfill:
    def test_filters_low_volume(self, tmp_path: Path) -> None:
        res_csv = tmp_path / "resolution.csv"
        _write_resolution_csv(res_csv, [
            _resolved_market("t1", volume=100),    # below threshold
            _resolved_market("t2", volume=10000),  # above
        ])

        with patch("scripts.backfill_historical_trades.requests") as mock_req:
            session = MagicMock()
            resp = MagicMock()
            resp.json.return_value = []
            resp.raise_for_status = MagicMock()
            session.get.return_value = resp
            session.headers = MagicMock()
            mock_req.Session.return_value = session

            from scripts.backfill_historical_trades import backfill
            result = backfill(
                resolution_csv=res_csv, output_dir=tmp_path / "trades",
                limit=100, min_volume=5000,
            )
            # Only 1 market should be fetched (t2)
            assert result["markets_fetched"] == 1

    def test_pagination_stops_on_empty(self, tmp_path: Path) -> None:
        res_csv = tmp_path / "resolution.csv"
        _write_resolution_csv(res_csv, [_resolved_market("t1")])

        call_count = [0]
        with patch("scripts.backfill_historical_trades.requests") as mock_req:
            session = MagicMock()
            def _get(*args, **kwargs):
                call_count[0] += 1
                resp = MagicMock()
                resp.raise_for_status = MagicMock()
                if call_count[0] == 1:
                    resp.json.return_value = [{"price": "0.5", "size": "10", "timestamp": "123"}]
                else:
                    resp.json.return_value = []
                return resp
            session.get.side_effect = _get
            session.headers = MagicMock()
            mock_req.Session.return_value = session

            from scripts.backfill_historical_trades import backfill
            with patch("scripts.backfill_historical_trades.time.sleep"):
                result = backfill(
                    resolution_csv=res_csv, output_dir=tmp_path / "trades",
                    limit=1, min_volume=0,
                )
            assert result["total_trades"] == 1
            # 1 trade < 500 limit → loop breaks after first page (no second fetch needed)
            assert call_count[0] == 1

    def test_sleep_called(self, tmp_path: Path) -> None:
        res_csv = tmp_path / "resolution.csv"
        _write_resolution_csv(res_csv, [_resolved_market("t1")])

        with patch("scripts.backfill_historical_trades.requests") as mock_req:
            session = MagicMock()
            resp = MagicMock()
            resp.json.return_value = []
            resp.raise_for_status = MagicMock()
            session.get.return_value = resp
            session.headers = MagicMock()
            mock_req.Session.return_value = session

            with patch("scripts.backfill_historical_trades.time.sleep") as mock_sleep:
                from scripts.backfill_historical_trades import backfill
                backfill(
                    resolution_csv=res_csv, output_dir=tmp_path / "trades",
                    limit=1, min_volume=0,
                )
                assert mock_sleep.call_count >= 1
