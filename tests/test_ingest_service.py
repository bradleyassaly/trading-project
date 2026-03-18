from __future__ import annotations

from pathlib import Path

from trading_platform.config.models import IngestConfig
from trading_platform.services.ingest_service import run_ingest


def test_run_ingest_delegates_to_ingest_symbol(monkeypatch) -> None:
    expected = Path("/tmp/fake-output.parquet")
    captured: dict[str, object] = {}

    def fake_ingest_symbol(**kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(
        "trading_platform.services.ingest_service.ingest_symbol",
        fake_ingest_symbol,
    )

    config = IngestConfig(
        symbol="AAPL",
        start="2024-01-01",
        end="2024-02-01",
        interval="1d",
    )

    out = run_ingest(config=config, provider=None)

    assert out == expected
    assert captured["symbol"] == "AAPL"
    assert captured["start"] == "2024-01-01"
    assert captured["end"] == "2024-02-01"
    assert captured["interval"] == "1d"