"""
PredictIt historical CSV parser.

Reads a PredictIt daily market data CSV (downloadable from
predictit.org/reports/MARKET_DATA) and converts it into
Kalshi-compatible feature parquets and a resolution CSV.

PredictIt uses real USD (capped at $850 per contract), unlike
Manifold's play money.

Expected CSV schema (PredictIt daily market data export)::

    ContractID,Date,ContractName,MarketName,OpenSharePrice,
    CloseSharePrice,LowSharePrice,HighSharePrice,Volume

Prices are 0.01–0.99 (USD fractions). We scale to 0–100.

Usage::

    from trading_platform.predictit.parser import PredictItParser
    result = PredictItParser().parse("data.csv", "data/predictit")
"""
from __future__ import annotations

import csv
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class PredictItParseResult:
    rows_loaded: int = 0
    contracts_found: int = 0
    contracts_skipped_few_bars: int = 0
    contracts_skipped_feature_error: int = 0
    contracts_processed: int = 0
    feature_files_written: int = 0
    resolution_records: int = 0
    date_range_start: str | None = None
    date_range_end: str | None = None


class PredictItParser:
    """Parse PredictIt historical CSV into feature parquets."""

    def parse(
        self,
        csv_path: str | Path,
        output_dir: str | Path,
        *,
        min_bars: int = 10,
        limit: int | None = None,
    ) -> PredictItParseResult:
        csv_path = Path(csv_path)
        output_dir = Path(output_dir)
        features_dir = output_dir / "features"
        features_dir.mkdir(parents=True, exist_ok=True)
        resolution_csv_path = output_dir / "resolution.csv"

        result = PredictItParseResult()

        if not csv_path.exists():
            logger.error("CSV file not found: %s", csv_path)
            return result

        # ── Load CSV ──────────────────────────────────────────────────────
        logger.info("Loading PredictIt CSV from %s", csv_path)
        rows_by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
        contract_meta: dict[str, dict[str, str]] = {}

        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                result.rows_loaded += 1
                contract_id = row.get("ContractID") or row.get("contractid") or ""
                if not contract_id:
                    continue
                rows_by_contract[contract_id].append(row)
                if contract_id not in contract_meta:
                    contract_meta[contract_id] = {
                        "contract_name": row.get("ContractName") or row.get("contractname") or "",
                        "market_name": row.get("MarketName") or row.get("marketname") or "",
                    }

        result.contracts_found = len(rows_by_contract)
        logger.info("Loaded %d rows across %d contracts", result.rows_loaded, result.contracts_found)

        if limit is not None:
            contract_ids = list(rows_by_contract.keys())[:limit]
            rows_by_contract = {k: rows_by_contract[k] for k in contract_ids}

        # ── Process each contract ─────────────────────────────────────────
        from trading_platform.kalshi.features import build_kalshi_features

        resolution_rows: list[dict[str, Any]] = []
        all_dates: list[datetime] = []

        for contract_id, rows in rows_by_contract.items():
            if len(rows) < min_bars:
                result.contracts_skipped_few_bars += 1
                continue

            # Sort by date ascending
            rows.sort(key=lambda r: r.get("Date") or r.get("date") or "")

            # Convert to trades DataFrame
            trade_rows = []
            for row in rows:
                date_str = row.get("Date") or row.get("date") or ""
                close_price = row.get("CloseSharePrice") or row.get("closeshareprice") or ""
                open_price = row.get("OpenSharePrice") or row.get("openshareprice") or ""
                high_price = row.get("HighSharePrice") or row.get("highshareprice") or ""
                low_price = row.get("LowSharePrice") or row.get("lowshareprice") or ""
                volume = row.get("Volume") or row.get("volume") or "0"

                try:
                    dt = datetime.strptime(date_str.strip(), "%m/%d/%Y").replace(tzinfo=timezone.utc)
                except (ValueError, AttributeError):
                    try:
                        dt = datetime.fromisoformat(date_str.strip()).replace(tzinfo=timezone.utc)
                    except (ValueError, AttributeError):
                        continue

                try:
                    cp = float(close_price)
                except (TypeError, ValueError):
                    continue

                trade_rows.append({
                    "traded_at": dt,
                    "yes_price": cp,  # 0.01-0.99 fraction, auto-scaled by feature builder
                    "count": max(int(float(volume)), 1),
                })
                all_dates.append(dt)

            if len(trade_rows) < min_bars:
                result.contracts_skipped_few_bars += 1
                continue

            trades_df = pl.DataFrame(trade_rows)

            # Build features — use daily period since PredictIt data is daily
            try:
                features_df = build_kalshi_features(
                    trades_df,
                    ticker=contract_id,
                    period="1d",
                    feature_groups=["probability_calibration", "volume_activity", "time_decay"],
                )
            except Exception as exc:
                logger.debug("Feature generation failed for %s: %s", contract_id, exc)
                result.contracts_skipped_feature_error += 1
                continue

            if features_df.is_empty():
                result.contracts_skipped_feature_error += 1
                continue

            parquet_path = features_dir / f"{contract_id}.parquet"
            features_df.write_parquet(parquet_path)
            result.feature_files_written += 1
            result.contracts_processed += 1

            # Resolution: last close price determines likely resolution
            # PredictIt contracts resolve at $1.00 (YES) or $0.00 (NO)
            last_price = trade_rows[-1]["yes_price"]
            resolution_price = 100.0 if last_price >= 0.90 else (0.0 if last_price <= 0.10 else None)

            meta = contract_meta.get(contract_id, {})
            resolution_rows.append({
                "ticker": contract_id,
                "resolution_price": resolution_price if resolution_price is not None else "",
                "resolves_yes": "True" if resolution_price == 100.0 else ("False" if resolution_price == 0.0 else ""),
                "question": meta.get("contract_name", ""),
                "market_name": meta.get("market_name", ""),
                "volume": sum(int(float(r.get("Volume") or r.get("volume") or 0)) for r in rows),
                "bar_count": len(trade_rows),
            })

        result.resolution_records = len(resolution_rows)

        # ── Write resolution CSV ──────────────────────────────────────────
        if resolution_rows:
            fieldnames = ["ticker", "resolution_price", "resolves_yes", "question",
                          "market_name", "volume", "bar_count"]
            with resolution_csv_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(resolution_rows)

        if all_dates:
            result.date_range_start = min(all_dates).isoformat()
            result.date_range_end = max(all_dates).isoformat()

        return result
