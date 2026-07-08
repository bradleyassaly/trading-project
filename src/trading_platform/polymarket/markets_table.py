"""Persistent per-market metadata table.

Currently the only authoritative market info we keep is `market_categories`
(slug+category). Every other piece of market info — endDate, volume,
outcomes, tags, resolution status — gets re-fetched from Gamma each time a
signal engine or exit checker needs it.

This module defines a `markets` table that caches the full Gamma
snapshot for every condition_id we care about, keyed so the rest of the
pipeline can JOIN on it rather than hitting the API.

Usage:
  - Run ``refresh()`` on a schedule (6–24h) to pull fresh snapshots
  - ``get(cid)`` reads the cached row
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from trading_platform.polymarket.db_connection import db, get_connection

logger = logging.getLogger(__name__)
_GAMMA_URL = "https://gamma-api.polymarket.com/markets"
_PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Rate-limit Gamma fetches
_SLEEP = 0.35
_MAX_PER_RUN = 500


_SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
    condition_id      TEXT PRIMARY KEY,
    slug              TEXT,
    question          TEXT,
    end_date_iso      TEXT,
    close_time_iso    TEXT,
    volume            REAL,
    volume_24h        REAL,
    liquidity         REAL,
    outcomes_json     TEXT,
    outcome_prices    TEXT,
    tags_json         TEXT,
    uma_status        TEXT,
    closed            INTEGER,
    active            INTEGER,
    archived          INTEGER,
    last_fetched_at   INTEGER NOT NULL,
    created_at_iso    TEXT,
    yes_token_id      TEXT,
    no_token_id       TEXT,
    event_slug        TEXT,
    neg_risk          INTEGER
);
CREATE INDEX IF NOT EXISTS idx_markets_end_date ON markets(end_date_iso);
CREATE INDEX IF NOT EXISTS idx_markets_closed   ON markets(closed);
CREATE INDEX IF NOT EXISTS idx_markets_event    ON markets(event_slug);
CREATE INDEX IF NOT EXISTS idx_markets_yes_token ON markets(yes_token_id);
CREATE INDEX IF NOT EXISTS idx_markets_no_token  ON markets(no_token_id);
"""


def ensure_schema() -> None:
    # 2026-07-06: event_slug added for the per-event exposure cap — one
    # football match carried 6 correlated positions because the
    # topic-stem cap can't see that "Spread: Belgium (-1.5)" and "Will
    # Belgium win..." are the same event. The ALTER must run BEFORE the
    # index statements in _SCHEMA (CREATE TABLE IF NOT EXISTS is a no-op
    # on existing deployments, so idx_markets_event would otherwise
    # reference a missing column). Separate transaction: a failed ALTER
    # ("already exists") poisons a shared Postgres transaction.
    try:
        with db() as c:
            c.execute("ALTER TABLE markets ADD COLUMN event_slug TEXT")
    except Exception:
        pass  # already present (or table doesn't exist yet — created below)
    # 2026-07-07 (P4): neg_risk cache — NULL=unknown, 0=false, 1=true.
    # neg_risk selects which exchange contract an order is signed against;
    # a market's tokens are minted under one exchange and never move, so
    # this is immutable and safe to cache forever.
    try:
        with db() as c:
            c.execute("ALTER TABLE markets ADD COLUMN neg_risk INTEGER")
    except Exception:
        pass
    with db() as c:
        for stmt in _SCHEMA.split(";"):
            if stmt.strip():
                c.execute(stmt)


# Sentinel for a definitive "Gamma does not index this market" answer
# (HTTP 200 with an empty list) — distinct from a transient fetch error.
_NOT_IN_GAMMA = "not_in_gamma"


def _fetch_one(condition_id: str, session: Any = None) -> dict | str | None:
    """Return the market dict, _NOT_IN_GAMMA for a definitive miss, or
    None for a transient error (bad status / network / parse)."""
    import requests
    s = session or requests
    try:
        r = s.get(_GAMMA_URL, params={"condition_ids": condition_id}, timeout=8)
    except Exception as exc:
        logger.debug("gamma fetch error for %s: %s", condition_id[:14], exc)
        return None
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    if isinstance(data, list) and not data:
        return _NOT_IN_GAMMA
    m = data[0] if isinstance(data, list) and data else None
    if not isinstance(m, dict):
        return None
    return m


def _extract_row(cid: str, m: dict) -> tuple:
    tags = m.get("tags")
    if isinstance(tags, list):
        tags_json = json.dumps(tags)
    elif isinstance(tags, str):
        tags_json = tags
    else:
        tags_json = None

    # Parse clob token IDs. Gamma returns '["yes_id", "no_id"]'.
    yes_tid, no_tid = None, None
    tids_raw = m.get("clobTokenIds")
    try:
        tids = json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
        if isinstance(tids, list) and len(tids) >= 2:
            yes_tid = str(tids[0]) if tids[0] else None
            no_tid = str(tids[1]) if tids[1] else None
    except Exception:
        pass

    # Event slug: Gamma nests the parent event under "events". All markets
    # of one real-world event (a match, an election night) share it — the
    # unit the per-event exposure cap groups by.
    event_slug = None
    try:
        evs = m.get("events")
        if isinstance(evs, list) and evs and isinstance(evs[0], dict):
            event_slug = evs[0].get("slug") or evs[0].get("ticker")
    except Exception:
        pass

    return (
        cid,
        m.get("slug"),
        m.get("question"),
        m.get("endDate") or m.get("endDateIso"),
        m.get("umaEndDate"),
        _safe_float(m.get("volume")),
        _safe_float(m.get("volume24hr") or m.get("volume24Hr")),
        _safe_float(m.get("liquidity")),
        m.get("outcomes") if isinstance(m.get("outcomes"), str) else json.dumps(m.get("outcomes")) if m.get("outcomes") else None,
        m.get("outcomePrices") if isinstance(m.get("outcomePrices"), str) else json.dumps(m.get("outcomePrices")) if m.get("outcomePrices") else None,
        tags_json,
        m.get("umaResolutionStatus"),
        1 if m.get("closed") else 0,
        1 if m.get("active") else 0,
        1 if m.get("archived") else 0,
        int(time.time()),
        m.get("createdAt") or m.get("startDate"),
        yes_tid,
        no_tid,
        event_slug,
        # neg_risk: None (NULL) when Gamma omits the key so unknown stays
        # unknown; verified Gamma's negRisk agrees with CLOB /neg-risk.
        (1 if m.get("negRisk") else 0) if "negRisk" in m else None,
    )


def _safe_float(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def refresh(
    *,
    max_per_run: int = _MAX_PER_RUN,
    force_all: bool = False,
    staleness_hours: float = 24.0,
) -> dict:
    """Pull Gamma snapshots for markets we care about.

    Prioritises: markets with open paper positions → pending signal
    outcomes → recently-active wallet_trades. Stops after max_per_run
    API calls. Refreshes rows older than staleness_hours unless
    force_all is set.
    """
    ensure_schema()
    cutoff = int(time.time() - staleness_hours * 3600)

    # Priority order: open positions first, then recently-EXITED live
    # positions (2026-07-06: their markets stopped refreshing the moment
    # we sold, so no local source ever learned the resolution — the
    # exit-policy counterfactual found 0 of 69 exits resolvable), then
    # pending signals, then recent trades.
    priority_sql = f"""
    SELECT cid, min(priority) FROM (
      SELECT condition_id AS cid, 1 AS priority
      FROM polymarket_paper_trades WHERE exit_ts IS NULL AND archived = 0
      UNION ALL
      SELECT condition_id AS cid, 2 AS priority
      FROM live_trades
      WHERE dry_run = 0 AND exit_ts IS NOT NULL
        AND exit_ts > strftime('%s','now','-60 days')
      UNION ALL
      SELECT condition_id AS cid, 3 AS priority
      FROM signal_outcomes WHERE resolution_price IS NULL AND signal_type != 'price_velocity'
      UNION ALL
      SELECT condition_id AS cid, 4 AS priority
      FROM wallet_trades WHERE timestamp > strftime('%s','now','-7 days')
    ) GROUP BY cid
    """

    with db() as c:
        rows = c.execute(priority_sql).fetchall()
        known = {}
        if not force_all:
            for r in c.execute(
                "SELECT condition_id, last_fetched_at FROM markets"
            ).fetchall():
                known[r[0]] = r[1]

    candidates: list[str] = []
    for cid, _priority in rows:
        if not cid:
            continue
        fetched = known.get(cid, 0)
        if force_all or fetched < cutoff:
            candidates.append(cid)
        if len(candidates) >= max_per_run:
            break

    if not candidates:
        return {"fetched": 0, "updated": 0, "skipped": len(rows), "stale_threshold_h": staleness_hours}

    try:
        import requests
        session = requests.Session()
    except ImportError:
        session = None

    updated = 0
    failed = 0
    tombstoned = 0
    any_ok = False
    updates: list[tuple] = []
    for i, cid in enumerate(candidates):
        m = _fetch_one(cid, session=session)
        if m is None:
            failed += 1
        elif m == _NOT_IN_GAMMA:
            # 2026-07-05: Gamma answers 200/[] for delisted markets. These
            # were counted as failures and never got a row, so the same
            # dead condition_ids re-consumed the whole per-run budget every
            # cycle (observed 490/500 "failed" per run). Write a tombstone
            # so they only recheck after the staleness window. Tombstones
            # only touch last_fetched_at/uma_status — a previously-cached
            # snapshot keeps its endDate/outcome fields.
            any_ok = True
            tombstoned += 1
            _persist_tombstone(cid)
        else:
            any_ok = True
            updates.append(_extract_row(cid, m))
        # Batch commit every 50 to minimise lock time
        if len(updates) >= 50:
            _persist(updates)
            updated += len(updates)
            updates = []
        time.sleep(_SLEEP)

    if updates:
        _persist(updates)
        updated += len(updates)

    try:
        from trading_platform.polymarket import api_health
        if any_ok:
            api_health.record_success("gamma")
        elif candidates and failed:
            api_health.record_error("gamma", f"all {failed} single-market fetches failed")
    except Exception:
        pass

    return {
        "fetched": len(candidates),
        "updated": updated,
        "failed": failed,
        "tombstoned": tombstoned,
        "skipped_fresh": len(rows) - len(candidates),
    }


def refresh_active_universe(*, max_pages: int = 60, page_size: int = 100) -> dict:
    """Bulk-index ALL active Gamma markets so token→market lookups hit.

    2026-07-02: refresh() is reactive — it only fetches markets we already
    touched via positions/signals/wallet_trades, which capped the tracked
    universe at ~225 markets. Chain-direct dispatch (wallet_stream) can
    only copy fills on markets whose token ids exist locally, so every
    whale fill outside that set was watched but not copyable. This pages
    the full active market list (thousands of rows, one call per 100)
    and persists the same row shape. Run daily from the scheduler.
    """
    ensure_schema()
    try:
        import requests
        session = requests.Session()
    except ImportError:
        return {"error": "requests unavailable"}

    total = 0
    failed_pages = 0
    updates: list[tuple] = []
    # 2026-07-05: Gamma 422s any offset > ~2000, so a straight
    # volume-ordered walk dies at page 21 with everything below the top
    # 2000 markets unindexed. Page in END-DATE WINDOWS instead: order by
    # endDate ascending and, when the offset nears the cap, advance
    # end_date_min to the highest end date seen and reset the offset.
    # Duplicates across window edges are skipped via seen-cid tracking.
    GAMMA_OFFSET_CAP = 2000
    window_min = ""  # first window: no end_date_min bound
    offset = 0
    watermark = ""
    seen_cids: set[str] = set()
    any_ok = False
    for page in range(max_pages):
        if offset + page_size > GAMMA_OFFSET_CAP:
            if watermark and watermark > window_min:
                window_min = watermark
                offset = 0
                logger.info("universe window advance: end_date_min=%s", window_min)
            else:
                logger.warning("universe window cannot advance past %s — stopping", window_min)
                break
        params = {
            "active": "true", "closed": "false",
            "limit": page_size, "offset": offset,
            "order": "endDate", "ascending": "true",
        }
        if window_min:
            params["end_date_min"] = window_min
        try:
            r = session.get(_GAMMA_URL, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            any_ok = True
        except Exception as exc:
            logger.warning("universe page %d (offset %d) failed: %s", page, offset, exc)
            failed_pages += 1
            if failed_pages >= 3:
                break
            time.sleep(2)
            offset += page_size
            continue
        if not isinstance(data, list) or not data:
            break
        for m in data:
            cid = m.get("conditionId") or m.get("condition_id")
            if not cid or str(cid) in seen_cids:
                continue
            seen_cids.add(str(cid))
            _end = str(m.get("endDateIso") or m.get("endDate") or "")[:10]
            if _end > watermark:
                watermark = _end
            updates.append(_extract_row(str(cid), m))
        if len(updates) >= 200:
            _persist(updates)
            total += len(updates)
            updates = []
        if len(data) < page_size:
            break
        offset += page_size
        time.sleep(_SLEEP)
    if updates:
        _persist(updates)
        total += len(updates)
    try:
        from trading_platform.polymarket import api_health
        if any_ok:
            api_health.record_success("gamma")
        elif failed_pages:
            api_health.record_error("gamma", f"universe refresh: {failed_pages} pages failed")
    except Exception:
        pass
    logger.info("universe refresh: %d active markets indexed", total)
    return {"indexed": total, "failed_pages": failed_pages}


def get_by_token_id(token_id: str) -> dict | None:
    """Resolve a market by CLOB token id — local table first, then Gamma.

    The Gamma fallback (one ~200ms call) exists for chain-direct dispatch:
    a whale fill on a market we haven't indexed is still worth copying,
    and 200ms is 2,500× faster than waiting for data-api indexing. The
    fetched row is persisted so the next lookup is local.
    """
    ensure_schema()
    tid = str(token_id)
    with db() as c:
        row = c.execute(
            """SELECT condition_id, question,
                      CASE WHEN yes_token_id = ? THEN 'YES'
                           WHEN no_token_id = ? THEN 'NO' END
                 FROM markets
                WHERE yes_token_id = ? OR no_token_id = ?
                LIMIT 1""",
            (tid, tid, tid, tid),
        ).fetchone()
    if row and row[0]:
        return {"condition_id": row[0], "question": row[1] or "",
                "outcome": row[2] or "YES"}
    # Gamma by-token fallback.
    try:
        import requests
        r = requests.get(_GAMMA_URL, params={"clob_token_ids": tid}, timeout=5)
        r.raise_for_status()
        data = r.json()
        m = data[0] if isinstance(data, list) and data else None
        cid = (m or {}).get("conditionId") or (m or {}).get("condition_id")
        if m and cid:
            _persist([_extract_row(str(cid), m)])
            yes_tid, no_tid = get_token_ids(str(cid))
            outcome = "YES" if str(yes_tid) == tid else ("NO" if str(no_tid) == tid else "YES")
            return {"condition_id": str(cid),
                    "question": m.get("question") or "", "outcome": outcome}
    except Exception as exc:
        logger.debug("gamma by-token lookup failed for %s: %s", tid[:16], exc)
    return None


def _persist_tombstone(condition_id: str) -> None:
    """Record 'Gamma does not index this market' without disturbing any
    previously-cached snapshot fields."""
    with db() as c:
        c.execute(
            """INSERT INTO markets (condition_id, uma_status, last_fetched_at)
               VALUES (?, ?, ?)
               ON CONFLICT (condition_id) DO UPDATE SET
                 uma_status = EXCLUDED.uma_status,
                 last_fetched_at = EXCLUDED.last_fetched_at""",
            (condition_id, _NOT_IN_GAMMA, int(time.time())),
        )


def _persist(rows: list[tuple]) -> None:
    with db() as c:
        c.executemany(
            "INSERT OR REPLACE INTO markets ("
            " condition_id, slug, question, end_date_iso, close_time_iso,"
            " volume, volume_24h, liquidity, outcomes_json, outcome_prices,"
            " tags_json, uma_status, closed, active, archived, last_fetched_at,"
            " created_at_iso, yes_token_id, no_token_id, event_slug, neg_risk"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def warm_watched_markets(days: int = 7, max_fetches: int = 200) -> dict:
    """P3: pre-index markets that WATCHED wallets trade.

    The chain-direct fast lane dispatches instantly only for tokens the
    local markets table knows; recon measured 51% of condition_ids traded
    by watched wallets in the last 7d had NO markets row (+4% with NULL
    token ids) — every one of those fills fell back to the slow REST poll.
    This warms the gap from wallet_trades, capped per run. Scheduled 30min.
    """
    import requests
    cutoff = int(time.time()) - days * 86400
    with db() as c:
        # Most-recently-traded first: Gamma delists resolved markets, so an
        # unordered sweep burns the whole budget on tombstones (first run:
        # 200/200 fetched were delisted). Fresh trades ⇒ market still live.
        missing = [r[0] for r in c.execute(
            """SELECT wt.condition_id
                 FROM wallet_trades wt
                 LEFT JOIN markets m ON m.condition_id = wt.condition_id
                WHERE wt.timestamp > ?
                  AND wt.condition_id IS NOT NULL
                  AND (m.condition_id IS NULL OR m.yes_token_id IS NULL)
                  -- tombstones (Gamma doesn't index) have NULL token ids
                  -- by design; without this they were re-fetched every run
                  AND COALESCE(m.uma_status, '') != ?
                GROUP BY wt.condition_id
                ORDER BY MAX(wt.timestamp) DESC""",
            (cutoff, _NOT_IN_GAMMA),
        ).fetchall()]
    if not missing:
        return {"missing": 0, "fetched": 0, "persisted": 0, "tombstoned": 0}
    session = requests.Session()
    fetched = persisted = tombstoned = 0
    rows: list[tuple] = []
    for cid in missing[:max_fetches]:
        m = _fetch_one(cid, session=session)
        fetched += 1
        if m == _NOT_IN_GAMMA:
            _persist_tombstone(cid)
            tombstoned += 1
        elif isinstance(m, dict):
            rows.append(_extract_row(cid, m))
            persisted += 1
        time.sleep(0.15)
    if rows:
        _persist(rows)
    out = {"missing": len(missing), "fetched": fetched,
           "persisted": persisted, "tombstoned": tombstoned,
           "skipped": max(0, len(missing) - max_fetches)}
    logger.info("[warm_watched_markets] %s", out)
    return out


def get_neg_risk_cached(token_id: str) -> bool | None:
    """P4: read-through neg_risk cache. neg_risk is immutable per market (it
    selects which exchange contract orders are EIP-712-signed against), yet
    the hot lane paid a REST round-trip for it on every order.

    Local markets row first; on NULL/miss, one unauthenticated CLOB GET with
    write-back (UPDATE only — never create skeleton rows). Returns None when
    both sources fail; callers keep their existing fallback.
    """
    tid = str(token_id)
    try:
        with db() as c:
            row = c.execute(
                "SELECT neg_risk FROM markets "
                "WHERE yes_token_id = ? OR no_token_id = ? LIMIT 1",
                (tid, tid),
            ).fetchone()
    except Exception:
        row = None
    if row is not None and row[0] is not None:
        return bool(row[0])
    try:
        import requests
        r = requests.get("https://clob.polymarket.com/neg-risk",
                         params={"token_id": tid}, timeout=5)
        if r.status_code != 200:
            return None
        val = bool(r.json().get("neg_risk"))
    except Exception:
        return None
    try:
        with db() as c:
            c.execute(
                "UPDATE markets SET neg_risk = ? "
                "WHERE yes_token_id = ? OR no_token_id = ?",
                (1 if val else 0, tid, tid),
            )
    except Exception:
        pass
    return val


def get_token_ids(condition_id: str) -> tuple[str | None, str | None]:
    """Fast lookup of (yes_token_id, no_token_id) from the cached markets table.

    Returns (None, None) if the market isn't cached yet. Caller should fall
    back to a live Gamma fetch in that case. This is the preferred path for
    signal-time token resolution — one local read instead of an API call.
    """
    try:
        with db() as c:
            row = c.execute(
                "SELECT yes_token_id, no_token_id FROM markets WHERE condition_id = ?",
                (condition_id,),
            ).fetchone()
    except Exception:
        return (None, None)
    if not row:
        return (None, None)
    return (row[0], row[1])


def get(condition_id: str) -> dict | None:
    with db() as c:
        row = c.execute(
            "SELECT condition_id, slug, question, end_date_iso, close_time_iso,"
            " volume, volume_24h, liquidity, outcomes_json, outcome_prices,"
            " tags_json, uma_status, closed, active, archived, last_fetched_at, created_at_iso"
            " FROM markets WHERE condition_id = ?",
            (condition_id,),
        ).fetchone()
    if not row:
        return None
    cols = [
        "condition_id", "slug", "question", "end_date_iso", "close_time_iso",
        "volume", "volume_24h", "liquidity", "outcomes_json", "outcome_prices",
        "tags_json", "uma_status", "closed", "active", "archived",
        "last_fetched_at", "created_at_iso",
    ]
    return dict(zip(cols, row))


def main() -> int:
    import sys
    if "--warm-watched" in sys.argv:
        print(f"warm: {warm_watched_markets()}")
        return 0
    force = "--force" in sys.argv
    result = refresh(force_all=force)
    print(f"markets refresh: {result}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
