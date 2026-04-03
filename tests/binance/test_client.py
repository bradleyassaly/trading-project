from __future__ import annotations

from unittest.mock import patch

import requests

from trading_platform.binance.client import BinanceClient, BinanceClientConfig


class FakeResponse:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses

    def get(self, url: str, params=None, timeout=None):  # noqa: ANN001
        return self.responses.pop(0)


def test_client_retries_transient_failure() -> None:
    client = BinanceClient(
        BinanceClientConfig(request_sleep_sec=0.0, max_retries=2, backoff_base_sec=0.0, backoff_max_sec=0.0),
        session=FakeSession(
            [
                FakeResponse(500, {"code": -1}),
                FakeResponse(200, {"symbols": []}),
            ]
        ),
    )
    with patch("trading_platform.binance.client.time.sleep", return_value=None):
        payload = client.get_exchange_info()

    assert payload == {"symbols": []}
    assert client.stats.request_count == 2
    assert client.stats.retry_count == 1
