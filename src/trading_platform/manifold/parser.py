"""
Manifold Markets data dump parser.

Reads the free Manifold database dump (markets.json + bets.json) and
produces Kalshi-compatible feature parquets and a resolution CSV, so the
existing KalshiBacktester can run signals against Manifold's 500k+
historical prediction markets (binary YES/NO only).

Manifold uses play money (Mana) not real USD — volume figures are in Mana.

Usage::

    from trading_platform.manifold.parser import ManifoldParser
    result = ManifoldParser().parse("~/Downloads/manifold_dump", "data/manifold")
"""
from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

_VALID_RESOLUTIONS = {"YES", "NO"}


@dataclass
class ManifoldParseResult:
    markets_loaded: int = 0
    markets_filtered: int = 0
    markets_skipped_type: int = 0
    markets_skipped_resolution: int = 0
    markets_skipped_few_bets: int = 0
    markets_skipped_feature_error: int = 0
    markets_processed: int = 0
    feature_files_written: int = 0
    resolution_records: int = 0
    date_range_start: str | None = None
    date_range_end: str | None = None


class ManifoldParser:
    """Parse Manifold Markets data dumps into feature parquets."""

    def parse(
        self,
        dump_dir: str | Path,
        output_dir: str | Path,
        *,
        min_bets: int = 10,
        limit: int | None = None,
    ) -> ManifoldParseResult:
        dump_dir = Path(dump_dir)
        output_dir = Path(output_dir)
        features_dir = output_dir / "features"
        features_dir.mkdir(parents=True, exist_ok=True)
        resolution_csv_path = output_dir / "resolution.csv"

        result = ManifoldParseResult()

        # ── Load markets ──────────────────────────────────────────────────
        markets_path = _find_file(dump_dir, "markets")
        if not markets_path:
            logger.error("No markets.json or markets.jsonl found in %s", dump_dir)
            return result

        logger.info("Loading markets from %s", markets_path)
        raw_markets = _load_json_or_jsonl(markets_path)
        result.markets_loaded = len(raw_markets)
        logger.info("Loaded %d markets", result.markets_loaded)

        # Filter to binary, resolved YES/NO
        qualifying: list[dict[str, Any]] = []
        for m in raw_markets:
            if m.get("outcomeType") != "BINARY":
                result.markets_skipped_type += 1
                continue
            resolution = m.get("resolution")
            if resolution not in _VALID_RESOLUTIONS:
                result.markets_skipped_resolution += 1
                continue
            qualifying.append(m)
        result.markets_filtered = len(qualifying)
        logger.info("Qualifying binary resolved markets: %d", result.markets_filtered)

        if limit is not None:
            qualifying = qualifying[:limit]

        # ── Load bets ─────────────────────────────────────────────────────
        bets_path = _find_file(dump_dir, "bets")
        if not bets_path:
            logger.error("No bets.json or bets.jsonl found in %s", dump_dir)
            return result

        # Build set of qualifying market IDs for fast lookup
        qualifying_ids = {m["id"] for m in qualifying}

        logger.info("Loading bets from %s (filtering to %d markets)", bets_path, len(qualifying_ids))
        bets_by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)

        if bets_path.is_dir():
            # Sharded bets directory
            for shard in sorted(bets_path.glob("*.json*")):
                for bet in _load_json_or_jsonl(shard):
                    cid = bet.get("contractId", "")
                    if cid in qualifying_ids:
                        bets_by_market[cid].append(bet)
        else:
            for bet in _load_json_or_jsonl(bets_path):
                cid = bet.get("contractId", "")
                if cid in qualifying_ids:
                    bets_by_market[cid].append(bet)

        logger.info("Loaded bets for %d markets", len(bets_by_market))

        # ── Process each market ───────────────────────────────────────────
        from trading_platform.kalshi.features import build_kalshi_features

        resolution_rows: list[dict[str, Any]] = []
        close_times: list[datetime] = []

        for market in qualifying:
            market_id = market["id"]
            bets = bets_by_market.get(market_id, [])

            if len(bets) < min_bets:
                result.markets_skipped_few_bets += 1
                continue

            # Sort bets by createdTime ascending
            bets.sort(key=lambda b: b.get("createdTime", 0))

            # Convert to trades DataFrame
            rows = []
            for bet in bets:
                ct = bet.get("createdTime")
                prob_after = bet.get("probAfter")
                if ct is None or prob_after is None:
                    continue
                try:
                    rows.append({
                        "traded_at": datetime.fromtimestamp(float(ct) / 1000, tz=timezone.utc),
                        "yes_price": float(prob_after),
                        "count": 1,
                    })
                except (TypeError, ValueError, OSError):
                    continue

            if len(rows) < min_bets:
                result.markets_skipped_few_bets += 1
                continue

            trades_df = pl.DataFrame(rows)

            # Parse close time
            close_time: datetime | None = None
            ct_ms = market.get("closeTime") or market.get("resolutionTime")
            if ct_ms:
                try:
                    close_time = datetime.fromtimestamp(float(ct_ms) / 1000, tz=timezone.utc)
                    close_times.append(close_time)
                except (TypeError, ValueError, OSError):
                    pass

            # Build features
            try:
                features_df = build_kalshi_features(
                    trades_df,
                    ticker=market_id,
                    period="1h",
                    close_time=close_time,
                    feature_groups=["probability_calibration", "volume_activity", "time_decay"],
                )
            except Exception as exc:
                logger.debug("Feature generation failed for %s: %s", market_id, exc)
                result.markets_skipped_feature_error += 1
                continue

            if features_df.is_empty():
                result.markets_skipped_feature_error += 1
                continue

            # Write parquet
            parquet_path = features_dir / f"{market_id}.parquet"
            features_df.write_parquet(parquet_path)
            result.feature_files_written += 1
            result.markets_processed += 1

            # Collect resolution row
            resolution = market.get("resolution", "")
            resolution_price = 100.0 if resolution == "YES" else 0.0
            total_volume = float(market.get("volume") or 0)

            resolution_rows.append({
                "ticker": market_id,
                "resolution_price": resolution_price,
                "resolves_yes": resolution == "YES",
                "question": market.get("question", ""),
                "volume": total_volume,
                "close_time": close_time.isoformat() if close_time else "",
                "bet_count": len(bets),
            })

        result.resolution_records = len(resolution_rows)

        # ── Write resolution CSV ──────────────────────────────────────────
        if resolution_rows:
            fieldnames = ["ticker", "resolution_price", "resolves_yes", "question",
                          "volume", "close_time", "bet_count"]
            with resolution_csv_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(resolution_rows)

        if close_times:
            result.date_range_start = min(close_times).isoformat()
            result.date_range_end = max(close_times).isoformat()

        return result


# ── Helpers ───────────────────────────────────────────────────────────────────


def _find_file(dump_dir: Path, name: str) -> Path | None:
    """Find markets.json, markets.jsonl, bets.json, bets.jsonl, or bets/ dir."""
    for ext in (".json", ".jsonl"):
        p = dump_dir / f"{name}{ext}"
        if p.exists():
            return p
    # Sharded directory (e.g. bets/)
    d = dump_dir / name
    if d.is_dir():
        return d
    return None


def _load_json_or_jsonl(path: Path) -> list[dict[str, Any]]:
    """Load a JSON array or JSONL file into a list of dicts."""
    text = path.read_text(encoding="utf-8")
    # Try JSON array first
    stripped = text.lstrip()
    if stripped.startswith("["):
        return json.loads(text)
    # JSONL: one JSON object per line
    results = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            results.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return results
