"""
Metaculus resolved question parser.

Fetches resolved binary questions from the Metaculus public API,
converts forecast history into Kalshi-compatible feature parquets.

Metaculus has serious forecasters (less noise than play-money platforms),
covers the same events as Kalshi (CPI, elections, geopolitics), and
provides high-quality probability calibration data.

No authentication required for public questions.

Usage::

    from trading_platform.metaculus.parser import MetaculusParser
    result = MetaculusParser().fetch_resolved("data/metaculus", limit=2000)
"""
from __future__ import annotations

import csv
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.metaculus.com/api2"
_REQUEST_SLEEP = 0.5


@dataclass
class MetaculusFetchResult:
    questions_fetched: int = 0
    questions_skipped_ambiguous: int = 0
    questions_skipped_no_history: int = 0
    questions_skipped_few_forecasts: int = 0
    questions_skipped_feature_error: int = 0
    questions_processed: int = 0
    feature_files_written: int = 0
    resolution_records: int = 0
    date_range_start: str | None = None
    date_range_end: str | None = None


class MetaculusParser:
    """Fetch and parse Metaculus resolved binary questions."""

    def __init__(self, *, base_url: str = _BASE_URL, sleep_sec: float = _REQUEST_SLEEP) -> None:
        self._base_url = base_url.rstrip("/")
        self._sleep_sec = sleep_sec
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    def fetch_resolved(
        self,
        output_dir: str | Path,
        *,
        limit: int = 2000,
        min_forecasts: int = 5,
    ) -> MetaculusFetchResult:
        output_dir = Path(output_dir)
        features_dir = output_dir / "features"
        features_dir.mkdir(parents=True, exist_ok=True)
        resolution_csv_path = output_dir / "resolution.csv"

        result = MetaculusFetchResult()

        # ── Paginate through resolved binary questions ────────────────────
        questions: list[dict[str, Any]] = []
        offset = 0
        page_size = 100

        while len(questions) < limit:
            logger.info("Fetching questions offset=%d...", offset)
            try:
                resp = self._session.get(
                    f"{self._base_url}/questions/",
                    params={
                        "status": "resolved",
                        "type": "binary",
                        "limit": min(page_size, limit - len(questions)),
                        "offset": offset,
                        "order_by": "-resolved_at",
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.warning("Failed to fetch questions at offset=%d: %s", offset, exc)
                break

            page = data.get("results", [])
            if not page:
                break
            questions.extend(page)
            offset += len(page)
            time.sleep(self._sleep_sec)

            if not data.get("next"):
                break

        result.questions_fetched = len(questions)
        logger.info("Fetched %d resolved binary questions", result.questions_fetched)

        # ── Process each question ─────────────────────────────────────────
        from trading_platform.kalshi.features import build_kalshi_features

        resolution_rows: list[dict[str, Any]] = []
        all_dates: list[datetime] = []

        for q in questions:
            qid = str(q.get("id", ""))
            title = q.get("title", "")
            resolution = q.get("resolution")

            # Skip ambiguous/null resolutions
            if resolution not in (0, 0.0, 1, 1.0, "yes", "no", True, False):
                result.questions_skipped_ambiguous += 1
                continue

            # Extract forecast history
            history = self._get_forecast_history(q)
            if not history:
                result.questions_skipped_no_history += 1
                continue

            if len(history) < min_forecasts:
                result.questions_skipped_few_forecasts += 1
                continue

            # Convert to trades DataFrame
            rows = []
            for point in history:
                ts = point.get("t") or point.get("time")
                prob = point.get("x") or point.get("community_prediction")
                if ts is None or prob is None:
                    continue
                try:
                    if isinstance(ts, (int, float)):
                        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
                    else:
                        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    rows.append({
                        "traded_at": dt,
                        "yes_price": float(prob),
                        "count": 1,
                    })
                    all_dates.append(dt)
                except (TypeError, ValueError, OSError):
                    continue

            if len(rows) < min_forecasts:
                result.questions_skipped_few_forecasts += 1
                continue

            trades_df = pl.DataFrame(rows)

            # Parse close time
            close_time: datetime | None = None
            for ct_key in ("resolve_time", "close_time", "scheduled_close_time"):
                ct_raw = q.get(ct_key)
                if ct_raw:
                    try:
                        close_time = datetime.fromisoformat(str(ct_raw).replace("Z", "+00:00"))
                        break
                    except (ValueError, AttributeError):
                        pass

            # Build features
            try:
                feat_df = build_kalshi_features(
                    trades_df,
                    ticker=qid,
                    period="1h",
                    close_time=close_time,
                    feature_groups=["probability_calibration", "volume_activity", "time_decay"],
                )
            except Exception as exc:
                logger.debug("Feature build failed for %s: %s", qid, exc)
                result.questions_skipped_feature_error += 1
                continue

            if feat_df.is_empty():
                result.questions_skipped_feature_error += 1
                continue

            feat_df.write_parquet(features_dir / f"{qid}.parquet")
            result.feature_files_written += 1
            result.questions_processed += 1

            # Resolution
            if resolution in (1, 1.0, "yes", True):
                resolution_price = 100.0
            else:
                resolution_price = 0.0

            resolution_rows.append({
                "ticker": qid,
                "resolution_price": resolution_price,
                "resolves_yes": resolution_price == 100.0,
                "question": title,
                "close_time": close_time.isoformat() if close_time else "",
            })

        result.resolution_records = len(resolution_rows)

        # ── Write resolution CSV ──────────────────────────────────────────
        if resolution_rows:
            fieldnames = ["ticker", "resolution_price", "resolves_yes", "question", "close_time"]
            with resolution_csv_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(resolution_rows)

        if all_dates:
            result.date_range_start = min(all_dates).isoformat()
            result.date_range_end = max(all_dates).isoformat()

        return result

    def _get_forecast_history(self, question: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract forecast history from a question object.

        Tries inline ``community_prediction.history`` first, then fetches
        the full question detail if needed.
        """
        # Try inline history
        cp = question.get("community_prediction")
        if isinstance(cp, dict):
            history = cp.get("history")
            if isinstance(history, list) and history:
                return history

        # Fetch full question detail
        qid = question.get("id")
        if not qid:
            return []

        try:
            time.sleep(self._sleep_sec)
            resp = self._session.get(
                f"{self._base_url}/questions/{qid}/",
                timeout=30,
            )
            resp.raise_for_status()
            detail = resp.json()
        except Exception as exc:
            logger.debug("Failed to fetch detail for question %s: %s", qid, exc)
            return []

        cp = detail.get("community_prediction")
        if isinstance(cp, dict):
            history = cp.get("history")
            if isinstance(history, list):
                return history

        return []
