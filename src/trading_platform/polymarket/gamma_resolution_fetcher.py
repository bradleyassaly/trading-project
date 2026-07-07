"""
Fetch market resolutions from the Polymarket Gamma API.

The Gamma API (gamma-api.polymarket.com) returns resolved markets
with their outcome. No authentication required.

Usage::

    from trading_platform.polymarket.gamma_resolution_fetcher import GammaResolutionFetcher
    fetcher = GammaResolutionFetcher()
    resolutions = fetcher.fetch_resolved(token_ids=["tok1", "tok2"])
"""
from __future__ import annotations

import csv
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from trading_platform.polymarket.resolution_resolver import normalize_token_id

logger = logging.getLogger(__name__)

_GAMMA_BASE = "https://gamma-api.polymarket.com"


class GammaResolutionFetcher:
    """Fetch resolved market outcomes from the Gamma API."""

    def __init__(self, *, base_url: str = _GAMMA_BASE, sleep_sec: float = 0.2) -> None:
        self._base = base_url.rstrip("/")
        self._sleep = sleep_sec
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        # Highest endDate (YYYY-MM-DD) seen while paging — the watermark used
        # to advance the end_date_min window past Gamma's ~2000 offset cap.
        self._last_end_date_seen: str = ""

    def fetch_resolved(
        self,
        token_ids: list[str] | None = None,
        *,
        since_days: int = 90,
    ) -> dict[str, float]:
        """Fetch resolutions for token IDs or all recent resolved markets.

        Returns ``{canonical_token_id: resolution_price}`` (100.0=YES, 0.0=NO).
        """
        if token_ids:
            return self._fetch_by_token_ids(token_ids)
        return self._fetch_all_resolved(since_days=since_days)

    def fetch_all_resolved(
        self,
        output_path: str | Path,
        *,
        since_days: int = 90,
    ) -> int:
        """Fetch all resolved markets and save to CSV. Returns count."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        _now = datetime.now(tz=timezone.utc)
        cutoff = (_now - timedelta(days=since_days)).strftime("%Y-%m-%d")
        page_size = 100
        max_pages = 300
        # 2026-07-05: Gamma rejects offset > ~2000 with 422. Page in END-DATE
        # WINDOWS to cover result sets larger than the cap.
        # 2026-07-07 (audit C6): page NEWEST-FIRST (endDate DESCENDING). The
        # ascending version exhausted its 30k-market budget ~15 days past the
        # 90-day cutoff — with ~2000 closed markets/day, the CSV's newest
        # close_time stayed frozen at ~cutoff (verified: rewritten daily but
        # newest entry 2026-04-21) so every consumer was starved of RECENT
        # resolutions. Descending spends the budget on the recent markets we
        # actually need to book. Window advances by LOWERING end_date_max to
        # the oldest end date seen; stop once we cross below the cutoff.
        GAMMA_OFFSET_CAP = 2000
        window_max = (_now + timedelta(days=1)).strftime("%Y-%m-%d")
        oldest_seen = ""  # lowest endDate seen in the current window
        offset = 0
        total_fetched = 0
        seen_cids: set[str] = set()
        any_page_ok = False

        # Write incrementally — don't buffer in memory
        fieldnames = ["ticker", "condition_id", "resolution_price", "resolves_yes", "question", "close_time", "volume"]
        rows_written = 0

        from trading_platform.polymarket import api_health

        with output_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()

            for page_num in range(max_pages):
                if offset + page_size > GAMMA_OFFSET_CAP:
                    # Advance the window DOWNWARD past the offset cap.
                    if oldest_seen and oldest_seen < window_max:
                        window_max = oldest_seen
                    else:
                        # Can't advance (2000+ markets share one end date) —
                        # step back a day rather than loop forever.
                        try:
                            _d = datetime.strptime(window_max, "%Y-%m-%d")
                            window_max = (_d - timedelta(days=1)).strftime("%Y-%m-%d")
                            logger.warning(
                                "Gamma window could not advance by data; stepping to %s",
                                window_max,
                            )
                        except ValueError:
                            break
                    offset = 0
                    print(f"  [window] lowering end_date_max → {window_max}, offset reset")
                    if window_max <= cutoff:
                        break
                try:
                    time.sleep(self._sleep)
                    resp = self._session.get(
                        f"{self._base}/markets",
                        params={
                            "closed": "true",
                            "end_date_min": cutoff,
                            "end_date_max": window_max,
                            "limit": page_size,
                            "offset": offset,
                            "order": "endDate",
                            "ascending": "false",
                        },
                        timeout=15,
                    )
                    resp.raise_for_status()
                    raw = resp.json()
                    any_page_ok = True
                except Exception as exc:
                    logger.warning("Gamma fetch failed at offset %d: %s", offset, exc)
                    print(f"  [ERROR] Page {page_num}: {exc}")
                    api_health.record_error("gamma", str(exc))
                    break

                if isinstance(raw, list):
                    batch = raw
                elif isinstance(raw, dict):
                    batch = raw.get("markets") or raw.get("data") or []
                else:
                    batch = []

                if not batch:
                    print(f"  Page {page_num}: empty response → done")
                    break

                total_fetched += len(batch)
                page_resolved = 0

                for m in batch:
                    # Track the OLDEST endDate in this window (descending order)
                    # so the window can advance downward past the offset cap.
                    _end_iso = str(m.get("endDateIso") or m.get("endDate") or "")[:10]
                    if _end_iso and (oldest_seen == "" or _end_iso < oldest_seen):
                        oldest_seen = _end_iso
                    _cid = m.get("conditionId") or ""
                    if _cid:
                        if _cid in seen_cids:
                            continue
                        seen_cids.add(_cid)
                    rp = self._extract_resolution(m)
                    if rp is None:
                        continue
                    import json as _json
                    clob_raw = m.get("clobTokenIds", "[]")
                    if isinstance(clob_raw, str):
                        try:
                            clob_ids = [str(t) for t in _json.loads(clob_raw)]
                        except (_json.JSONDecodeError, ValueError):
                            clob_ids = []
                    else:
                        clob_ids = [str(t) for t in (clob_raw or [])]

                    cond_id = m.get("conditionId") or ""
                    question = m.get("question", "")
                    close_time = m.get("endDateIso") or m.get("closedTime") or ""
                    volume = m.get("volume") or 0

                    # `resolves_yes` is a per-MARKET fact: did the YES outcome win?
                    # The previous version set it from `rp >= 99.0` per row, which
                    # accidentally collapsed both NO-won and YES-won markets to the
                    # same True value (the second row of a NO-won market has rp=100
                    # because it's the NO token). The result was every market in the
                    # CSV reading as "YES won". See reports/data_audit_2026-04-12.md.
                    yes_won = rp >= 50.0
                    # Write row for YES token (index 0)
                    if clob_ids:
                        writer.writerow({
                            "ticker": clob_ids[0],
                            "condition_id": cond_id,
                            "resolution_price": rp,            # YES token's payout
                            "resolves_yes": yes_won,
                            "question": question,
                            "close_time": close_time,
                            "volume": volume,
                        })
                        rows_written += 1
                    # Write row for NO token (index 1)
                    if len(clob_ids) > 1:
                        writer.writerow({
                            "ticker": clob_ids[1],
                            "condition_id": cond_id,
                            "resolution_price": 100.0 - rp,    # NO token's payout
                            "resolves_yes": yes_won,           # same per-market fact
                            "question": question,
                            "close_time": close_time,
                            "volume": volume,
                        })
                        rows_written += 1
                    page_resolved += 1

                print(f"  Page {page_num}: {len(batch)} markets, {page_resolved} resolved (total: {total_fetched} fetched, {rows_written} resolved)")

                if len(batch) < page_size:
                    break
                offset += len(batch)
            else:
                print(f"  [WARN] Hit max page cap ({max_pages})")

        if any_page_ok:
            api_health.record_success("gamma")
        logger.info("Wrote %d resolutions to %s", rows_written, output_path)
        return rows_written

    def _fetch_by_token_ids(self, token_ids: list[str]) -> dict[str, float]:
        """Fetch resolutions by individual token ID lookups."""
        results: dict[str, float] = {}
        for tid in token_ids:
            try:
                time.sleep(self._sleep)
                resp = self._session.get(
                    f"{self._base}/markets",
                    params={"clob_token_ids": tid, "limit": 5},
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
                markets = data if isinstance(data, list) else data.get("markets", data.get("data", []))
                for m in markets:
                    rp = self._extract_resolution(m)
                    if rp is not None:
                        results[normalize_token_id(tid)] = rp
                        break
            except Exception as exc:
                logger.debug("Gamma lookup failed for %s: %s", tid[:16], exc)
        return results

    def _fetch_all_resolved(self, *, since_days: int) -> dict[str, float]:
        """Fetch all resolved markets from last N days."""
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(days=since_days)).strftime("%Y-%m-%d")
        results: dict[str, float] = {}
        offset = 0
        while True:
            try:
                time.sleep(self._sleep)
                resp = self._session.get(
                    f"{self._base}/markets",
                    params={"closed": "true", "end_date_min": cutoff, "limit": 100, "offset": offset},
                    timeout=15,
                )
                resp.raise_for_status()
                page = resp.json()
            except Exception:
                break
            batch = page if isinstance(page, list) else page.get("markets", page.get("data", []))
            if not batch:
                break
            for m in batch:
                rp = self._extract_resolution(m)
                if rp is None:
                    continue
                import json
                clob_raw = m.get("clobTokenIds", "[]")
                if isinstance(clob_raw, str):
                    try:
                        clob_ids = json.loads(clob_raw)
                    except Exception:
                        clob_ids = []
                else:
                    clob_ids = list(clob_raw or [])
                if clob_ids:
                    results[normalize_token_id(str(clob_ids[0]))] = rp
            offset += len(batch)
            if len(batch) < 100:
                break
        return results

    @staticmethod
    def _extract_resolution(market: dict[str, Any]) -> float | None:
        """Extract resolution price from a Gamma API market object."""
        import json
        # Check outcomePrices for resolution
        prices_raw = market.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try:
                prices = [float(p) for p in json.loads(prices_raw)]
            except (json.JSONDecodeError, ValueError):
                prices = []
        else:
            prices = [float(p) for p in (prices_raw or [])]

        outcomes_raw = market.get("outcomes", "[]")
        if isinstance(outcomes_raw, str):
            try:
                outcomes = json.loads(outcomes_raw)
            except (json.JSONDecodeError, ValueError):
                outcomes = []
        else:
            outcomes = list(outcomes_raw or [])

        if prices and outcomes:
            max_price = max(prices)
            if max_price >= 0.95:
                idx = prices.index(max_price)
                if idx < len(outcomes):
                    winning = outcomes[idx].strip().lower()
                    if winning == "yes":
                        return 100.0
                    if winning == "no":
                        return 0.0
        return None
