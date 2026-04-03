"""Tests for PolymarketIngestPipeline."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from trading_platform.polymarket.historical_ingest import (
    PolymarketIngestConfig,
    PolymarketIngestPipeline,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_config(tmp_path: Path, **overrides) -> PolymarketIngestConfig:
    cfg = PolymarketIngestConfig(
        raw_markets_dir=str(tmp_path / "raw/markets"),
        raw_prices_dir=str(tmp_path / "raw/prices"),
        features_dir=str(tmp_path / "features"),
        resolution_csv_path=str(tmp_path / "resolution.csv"),
        manifest_path=str(tmp_path / "raw/ingest_manifest.json"),
        lookback_days=0,  # disable cutoff so all resolved markets pass
        min_volume=0.0,
        sort_newest_first=False,
    )
    for k, v in overrides.items():
        object.__setattr__(cfg, k, v)
    return cfg


def _raw_market(
    market_id: str = "mkt-1",
    resolved: bool = True,
    winner_outcome: str = "Yes",
    volume: float = 5000.0,
    end_date_iso: str = "2025-01-10T00:00:00Z",
    condition_id: str = "0xabc",
    clob_token_ids: list[str] | None = None,
) -> dict:
    import json as _json
    if clob_token_ids is None:
        clob_token_ids = ["111111111111", "222222222222"]
    return {
        "id": market_id,
        "question": f"Question for {market_id}?",
        "endDateIso": end_date_iso,
        "closed": True,
        "resolved": resolved,
        "winnerOutcome": winner_outcome,
        "volume": volume,
        "outcomes": _json.dumps(["Yes", "No"]),
        "outcomePrices": _json.dumps(["0.9", "0.1"]),
        "tags": [{"slug": "politics"}],
        "conditionId": condition_id,
        "clobTokenIds": _json.dumps(clob_token_ids),
    }


def _price_history(n: int = 30) -> list[dict]:
    """30 hourly price points starting at t=0."""
    base_ts = 1700000000
    return [{"t": base_ts + i * 3600, "p": 0.5 + i * 0.01} for i in range(n)]


class _FakeClient:
    def __init__(
        self,
        markets_by_tag: dict[str, list[dict]] | None = None,
        price_history: list[dict] | None = None,
    ) -> None:
        self._markets_by_tag = markets_by_tag or {}
        self._price_history = price_history if price_history is not None else _price_history()
        self.price_history_calls: list[str] = []

    def get_all_markets(self, *, tag_slug: str | None = None, closed: bool = True,
                        order: str | None = None, ascending: bool | None = None) -> list[dict]:
        return list(self._markets_by_tag.get(tag_slug or "", []))

    def get_markets(self, *, tag_slug: str | None = None, closed: bool = True,
                    limit: int = 100, offset: int = 0,
                    order: str | None = None, ascending: bool | None = None,
                    end_date_min: str | None = None,
                    ) -> tuple[list[dict], int | None]:
        all_m = self._markets_by_tag.get(tag_slug or "", [])
        page = all_m[offset:offset + limit]
        next_offset = offset + len(page) if len(page) == limit else None
        return page, next_offset

    def get_price_history(self, token_id: str, **_) -> list[dict]:
        self.price_history_calls.append(token_id)
        return list(self._price_history)


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestPolymarketIngestPipeline:
    def test_happy_path_writes_artifacts(self, tmp_path: Path) -> None:
        """Full run: one market → feature parquet + resolution.csv + manifest."""
        client = _FakeClient(
            markets_by_tag={"politics": [_raw_market()]},
            price_history=_price_history(30),
        )
        config = _make_config(tmp_path, tag_slugs=["politics"])
        result = PolymarketIngestPipeline(client, config).run()

        assert result.markets_fetched == 1
        assert result.markets_processed == 1
        assert result.feature_files_written == 1
        assert result.resolution_records == 1
        assert result.markets_failed == 0

        # resolution.csv exists and has correct row
        res_path = Path(config.resolution_csv_path)
        assert res_path.exists()
        with res_path.open(newline="") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 1
        assert rows[0]["ticker"] == "mkt-1"
        assert float(rows[0]["resolution_price"]) == 100.0
        assert rows[0]["resolves_yes"] == "True"

        # feature parquet exists
        feature_path = Path(config.features_dir) / "mkt-1.parquet"
        assert feature_path.exists()

        # manifest exists
        manifest_path = Path(config.manifest_path)
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        assert manifest["markets_fetched"] == 1
        assert manifest["markets_processed"] == 1

    def test_raw_json_written(self, tmp_path: Path) -> None:
        """Raw market + price JSON files should be saved."""
        market = _raw_market()
        client = _FakeClient(
            markets_by_tag={"economics": [market]},
            price_history=_price_history(20),
        )
        config = _make_config(tmp_path, tag_slugs=["economics"])
        PolymarketIngestPipeline(client, config).run()

        raw_market_path = Path(config.raw_markets_dir) / "mkt-1.json"
        assert raw_market_path.exists()
        loaded = json.loads(raw_market_path.read_text())
        assert loaded["id"] == "mkt-1"

        raw_prices_path = Path(config.raw_prices_dir) / "mkt-1.json"
        assert raw_prices_path.exists()

    def test_volume_filter_skips_low_volume(self, tmp_path: Path) -> None:
        markets = [
            _raw_market("low-vol", volume=50.0),
            _raw_market("high-vol", volume=5000.0),
        ]
        client = _FakeClient(markets_by_tag={"politics": markets})
        config = _make_config(tmp_path, tag_slugs=["politics"], min_volume=1000.0)
        result = PolymarketIngestPipeline(client, config).run()

        assert result.markets_skipped_volume == 1
        assert result.markets_processed == 1

    def test_unresolved_market_skipped(self, tmp_path: Path) -> None:
        market = _raw_market(resolved=False, winner_outcome=None)
        client = _FakeClient(markets_by_tag={"politics": [market]})
        config = _make_config(tmp_path, tag_slugs=["politics"])
        result = PolymarketIngestPipeline(client, config).run()

        assert result.markets_processed == 0
        assert result.resolution_records == 0

    def test_missing_clob_token_ids_skipped(self, tmp_path: Path) -> None:
        raw = _raw_market(clob_token_ids=[])
        client = _FakeClient(markets_by_tag={"politics": [raw]})
        config = _make_config(tmp_path, tag_slugs=["politics"])
        result = PolymarketIngestPipeline(client, config).run()

        assert result.markets_skipped_no_condition_id == 1
        assert result.markets_processed == 0

    def test_empty_price_history_skipped(self, tmp_path: Path) -> None:
        client = _FakeClient(
            markets_by_tag={"politics": [_raw_market()]},
            price_history=[],
        )
        config = _make_config(tmp_path, tag_slugs=["politics"])
        result = PolymarketIngestPipeline(client, config).run()

        assert result.markets_skipped_no_prices == 1
        assert result.markets_processed == 0

    def test_deduplication_across_tags(self, tmp_path: Path) -> None:
        """The same market appearing in two tags should only be processed once."""
        market = _raw_market("dup-id")
        client = _FakeClient(
            markets_by_tag={"politics": [market], "world": [market]},
            price_history=_price_history(20),
        )
        config = _make_config(tmp_path, tag_slugs=["politics", "world"])
        result = PolymarketIngestPipeline(client, config).run()

        assert result.markets_fetched == 1  # deduped
        assert result.markets_processed == 1

    def test_tag_breakdown_counts(self, tmp_path: Path) -> None:
        client = _FakeClient(
            markets_by_tag={
                "politics": [_raw_market("a"), _raw_market("b")],
                "economics": [_raw_market("c")],
            },
            price_history=_price_history(20),
        )
        config = _make_config(tmp_path, tag_slugs=["politics", "economics"])
        result = PolymarketIngestPipeline(client, config).run()

        assert result.tag_breakdown["politics"] == 2
        assert result.tag_breakdown["economics"] == 1

    def test_resolution_no_winner_returns_none(self, tmp_path: Path) -> None:
        """Markets with ambiguous winner (not Yes/No) have no resolution_price → skipped."""
        market = _raw_market(winner_outcome="Draw")
        client = _FakeClient(markets_by_tag={"politics": [market]})
        config = _make_config(tmp_path, tag_slugs=["politics"])
        result = PolymarketIngestPipeline(client, config).run()
        assert result.markets_processed == 0

    def test_client_error_on_price_history_increments_skipped(self, tmp_path: Path) -> None:
        class ErrClient(_FakeClient):
            def get_price_history(self, *a, **kw):
                raise RuntimeError("network error")

        client = ErrClient(markets_by_tag={"politics": [_raw_market()]})
        config = _make_config(tmp_path, tag_slugs=["politics"])
        result = PolymarketIngestPipeline(client, config).run()
        assert result.markets_skipped_no_prices == 1

    def test_no_resolution_csv_written_when_no_markets(self, tmp_path: Path) -> None:
        client = _FakeClient(markets_by_tag={"politics": []})
        config = _make_config(tmp_path, tag_slugs=["politics"])
        PolymarketIngestPipeline(client, config).run()
        assert not Path(config.resolution_csv_path).exists()

    def test_sort_newest_first_passes_end_date_min(self, tmp_path: Path) -> None:
        """When sort_newest_first=True + lookback_days, get_markets receives end_date_min."""
        call_log: list[dict] = []

        class SpyClient(_FakeClient):
            def get_markets(self, **kwargs):
                call_log.append(kwargs)
                return super().get_markets(**kwargs)

        client = SpyClient(markets_by_tag={"politics": [_raw_market()]})
        config = _make_config(
            tmp_path, tag_slugs=["politics"],
            sort_newest_first=True, lookback_days=30,
        )
        PolymarketIngestPipeline(client, config).run()

        assert len(call_log) >= 1
        assert call_log[0]["end_date_min"] is not None
        # Should NOT send order/ascending (breaks tag_slug on Gamma API)
        assert "order" not in call_log[0] or call_log[0].get("order") is None

    def test_sort_newest_first_disabled_no_end_date_min(self, tmp_path: Path) -> None:
        """Without sort_newest_first, end_date_min is not sent."""
        call_log: list[dict] = []

        class SpyClient(_FakeClient):
            def get_markets(self, **kwargs):
                call_log.append(kwargs)
                return super().get_markets(**kwargs)

        client = SpyClient(markets_by_tag={"politics": [_raw_market()]})
        config = _make_config(
            tmp_path, tag_slugs=["politics"],
            sort_newest_first=False, lookback_days=30,
        )
        PolymarketIngestPipeline(client, config).run()

        assert len(call_log) >= 1
        assert call_log[0].get("end_date_min") is None

    def test_max_markets_per_tag_caps_pagination(self, tmp_path: Path) -> None:
        """Pipeline stops fetching for a tag after max_markets_per_tag."""
        # 200 markets forces multiple pages (limit=100 per page).
        # With max_markets_per_tag=150, pipeline should stop after page 2.
        markets = [_raw_market(f"m-{i}") for i in range(200)]
        call_count = 0

        class CountingClient(_FakeClient):
            def get_markets(self, **kwargs):
                nonlocal call_count
                call_count += 1
                return super().get_markets(**kwargs)

        client = CountingClient(
            markets_by_tag={"politics": markets},
            price_history=_price_history(20),
        )
        config = _make_config(
            tmp_path, tag_slugs=["politics"], max_markets_per_tag=150,
        )
        result = PolymarketIngestPipeline(client, config).run()

        # Should stop after page 2 (200 markets, 100/page, cap at 150)
        assert call_count == 2
        assert result.markets_fetched == 200  # all 200 from 2 full pages
        assert result.tag_breakdown["politics"] == 200
