"""
Polymarket blockchain trade ingestion pipeline.

Reads on-chain trade CSV from poly-trade-scan and converts to
Kalshi-compatible feature parquets using the same pipeline as
Polymarket historical ingest.

poly-trade-scan CSV schema::

    block_number, timestamp, tx_hash, wallet, token_id, side,
    tokens, price, total_usdc

Where ``token_id`` is the clobTokenId (same as ``yes_token_id``
from the Gamma API).

Usage::

    from trading_platform.polymarket.blockchain_ingest import PolymarketBlockchainIngest
    result = PolymarketBlockchainIngest().run("trades.csv", "data/polymarket/blockchain")
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


@dataclass
class BlockchainIngestResult:
    rows_loaded: int = 0
    token_ids_found: int = 0
    markets_matched: int = 0
    markets_skipped_few_trades: int = 0
    markets_skipped_feature_error: int = 0
    markets_processed: int = 0
    feature_files_written: int = 0
    resolution_records: int = 0
    date_range_start: str | None = None
    date_range_end: str | None = None


class PolymarketBlockchainIngest:
    """Convert poly-trade-scan CSV into feature parquets."""

    def run(
        self,
        csv_path: str | Path,
        output_dir: str | Path,
        *,
        metadata_db_path: str | Path | None = None,
        min_trades: int = 10,
        limit: int | None = None,
    ) -> BlockchainIngestResult:
        csv_path = Path(csv_path)
        output_dir = Path(output_dir)
        features_dir = output_dir / "features"
        features_dir.mkdir(parents=True, exist_ok=True)
        resolution_csv_path = output_dir / "resolution.csv"

        result = BlockchainIngestResult()

        if not csv_path.exists():
            logger.error("Trades CSV not found: %s", csv_path)
            return result

        # ── Load market metadata (token_id → market_id, question) ─────────
        token_to_market: dict[str, dict[str, Any]] = {}
        if metadata_db_path:
            token_to_market = self._load_metadata_from_db(Path(metadata_db_path))
        if not token_to_market:
            # Try default live DB
            default_db = Path("data/polymarket/live/prices.db")
            if default_db.exists():
                token_to_market = self._load_metadata_from_db(default_db)

        # ── Load trades CSV ───────────────────────────────────────────────
        logger.info("Loading trades from %s", csv_path)
        trades_by_token: dict[str, list[dict[str, Any]]] = defaultdict(list)

        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                result.rows_loaded += 1
                token_id = row.get("token_id", "").strip()
                if not token_id:
                    continue
                trades_by_token[token_id].append(row)

        result.token_ids_found = len(trades_by_token)
        logger.info("Loaded %d rows across %d token IDs", result.rows_loaded, result.token_ids_found)

        # ── Match token IDs to known markets ──────────────────────────────
        processable: list[tuple[str, str, str, list[dict]]] = []
        for token_id, trades in trades_by_token.items():
            meta = token_to_market.get(token_id)
            if meta:
                market_id = meta["market_id"]
                question = meta.get("question", "")
            else:
                # Use token_id as market_id when no metadata available
                market_id = token_id[:16]
                question = ""
            processable.append((token_id, market_id, question, trades))

        result.markets_matched = len(processable)
        if limit is not None:
            processable = processable[:limit]

        # ── Process each market ───────────────────────────────────────────
        from trading_platform.kalshi.features import build_kalshi_features

        resolution_rows: list[dict[str, Any]] = []
        all_dates: list[datetime] = []

        for token_id, market_id, question, trades in processable:
            if len(trades) < min_trades:
                result.markets_skipped_few_trades += 1
                continue

            # Sort by timestamp ascending
            trades.sort(key=lambda t: t.get("timestamp", ""))

            # Convert to trades DataFrame
            rows = []
            for trade in trades:
                ts_str = trade.get("timestamp", "")
                price_str = trade.get("price", "")
                usdc_str = trade.get("total_usdc", "0")
                try:
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    try:
                        dt = datetime.fromtimestamp(float(ts_str), tz=timezone.utc)
                    except (ValueError, TypeError, OSError):
                        continue
                try:
                    price = float(price_str)
                except (TypeError, ValueError):
                    continue
                rows.append({
                    "traded_at": dt,
                    "yes_price": price,  # 0-1 fraction, auto-scaled by feature builder
                    "count": 1,
                })
                all_dates.append(dt)

            if len(rows) < min_trades:
                result.markets_skipped_few_trades += 1
                continue

            trades_df = pl.DataFrame(rows)

            try:
                features_df = build_kalshi_features(
                    trades_df,
                    ticker=market_id,
                    period="1h",
                    feature_groups=["probability_calibration", "volume_activity", "time_decay"],
                )
            except Exception as exc:
                logger.debug("Feature generation failed for %s: %s", market_id, exc)
                result.markets_skipped_feature_error += 1
                continue

            if features_df.is_empty():
                result.markets_skipped_feature_error += 1
                continue

            parquet_path = features_dir / f"{market_id}.parquet"
            features_df.write_parquet(parquet_path)
            result.feature_files_written += 1
            result.markets_processed += 1

            # Resolution: final price determines outcome
            last_price = rows[-1]["yes_price"]
            if last_price >= 0.99:
                resolution_price = 100.0
            elif last_price <= 0.01:
                resolution_price = 0.0
            else:
                resolution_price = None

            total_usdc = sum(
                float(t.get("total_usdc") or 0) for t in trades
            )
            resolution_rows.append({
                "ticker": market_id,
                "resolution_price": resolution_price if resolution_price is not None else "",
                "resolves_yes": "True" if resolution_price == 100.0 else ("False" if resolution_price == 0.0 else ""),
                "question": question,
                "volume": total_usdc,
                "trade_count": len(trades),
            })

        result.resolution_records = len(resolution_rows)

        # ── Write resolution CSV ──────────────────────────────────────────
        if resolution_rows:
            fieldnames = ["ticker", "resolution_price", "resolves_yes", "question",
                          "volume", "trade_count"]
            with resolution_csv_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(resolution_rows)

        if all_dates:
            result.date_range_start = min(all_dates).isoformat()
            result.date_range_end = max(all_dates).isoformat()

        return result

    @staticmethod
    def _load_metadata_from_db(db_path: Path) -> dict[str, dict[str, Any]]:
        """Load token_id → {market_id, question} from live collector SQLite."""
        import sqlite3
        try:
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
            rows = conn.execute(
                "SELECT yes_token_id, market_id, question FROM markets WHERE yes_token_id IS NOT NULL"
            ).fetchall()
            conn.close()
            return {
                r[0]: {"market_id": r[1], "question": r[2]}
                for r in rows if r[0]
            }
        except Exception as exc:
            logger.warning("Failed to load metadata from %s: %s", db_path, exc)
            return {}
