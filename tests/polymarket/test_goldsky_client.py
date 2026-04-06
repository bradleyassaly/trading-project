"""Tests for GoldskyClient."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trading_platform.polymarket.goldsky_client import GoldskyClient


def _fill_event(ts: int = 1712000000, maker: str = "0xm1", taker: str = "0xt1",
                maker_amt: str = "100000000", taker_amt: str = "65000000") -> dict:
    return {
        "id": f"fill-{ts}",
        "timestamp": str(ts),
        "maker": maker,
        "taker": taker,
        "makerAmountFilled": maker_amt,
        "takerAmountFilled": taker_amt,
        "makerAssetId": "tok1",
        "takerAssetId": "tok2",
        "fee": "500000",
    }


class TestGoldskyClient:
    def _mock_client(self, responses: list[dict]) -> GoldskyClient:
        client = GoldskyClient()
        mock = MagicMock()
        side_effects = []
        for r in responses:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = r
            resp.raise_for_status = MagicMock()
            side_effects.append(resp)
        mock.post.side_effect = side_effects
        mock.headers = MagicMock()
        client._session = mock
        return client

    def test_fetch_fills_returns_dataframe(self) -> None:
        fills = [_fill_event(1712000000 + i) for i in range(5)]
        client = self._mock_client([
            {"data": {"orderFilledEvents": fills}},
        ])
        df = client.fetch_fills("tok1", since_hours=24)
        assert len(df) == 5
        assert "timestamp" in df.columns
        assert "maker_wallet" in df.columns
        assert "maker_amount" in df.columns
        assert df.iloc[0]["maker_amount"] == 100.0  # 100000000 / 1e6

    def test_pagination_two_pages(self) -> None:
        page1 = [_fill_event(i) for i in range(1000)]
        page2 = [_fill_event(2000 + i) for i in range(50)]
        client = self._mock_client([
            {"data": {"orderFilledEvents": page1}},
            {"data": {"orderFilledEvents": page2}},
        ])
        df = client.fetch_fills("tok1", since_hours=720)
        assert len(df) == 1050

    def test_empty_market(self) -> None:
        client = self._mock_client([{"data": {"orderFilledEvents": []}}])
        df = client.fetch_fills("tok_empty", since_hours=24)
        assert df.empty
        assert "timestamp" in df.columns

    def test_429_triggers_retry(self) -> None:
        client = GoldskyClient()
        mock = MagicMock()
        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.raise_for_status.side_effect = Exception("429")
        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.json.return_value = {"data": {"orderFilledEvents": [_fill_event()]}}
        resp_ok.raise_for_status = MagicMock()
        mock.post.side_effect = [resp_429, resp_ok]
        mock.headers = MagicMock()
        client._session = mock

        with patch("trading_platform.polymarket.goldsky_client.time.sleep"):
            df = client.fetch_fills("tok1", since_hours=1)
        assert len(df) == 1

    def test_graphql_errors_raises(self) -> None:
        client = self._mock_client([{"errors": [{"message": "bad query"}]}])
        with pytest.raises(ValueError, match="GraphQL errors"):
            client.query("{ bad }")

    def test_timestamp_utc_aware(self) -> None:
        client = self._mock_client([
            {"data": {"orderFilledEvents": [_fill_event(1712000000)]}},
        ])
        df = client.fetch_fills("tok1", since_hours=720)
        assert df.iloc[0]["timestamp"].tzinfo is not None
