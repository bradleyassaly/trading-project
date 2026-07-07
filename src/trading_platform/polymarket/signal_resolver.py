"""
Signal outcome resolver.

Reads unresolved rows from ``signal_outcomes`` and fills in
``resolution_price``, ``resolved_at``, ``outcome_delta``, ``is_win``,
and ``hold_days`` by looking up each ``condition_id`` in
``data/polymarket/gamma_resolution.csv`` (refreshed by
``trading-cli data polymarket fetch-resolutions``). Also updates the
linked ``polymarket_paper_trades`` row if present.

Designed to be called from:
  * the scheduler every 30 min
  * ``run_daily_intelligence.py`` as Step 0
  * the API endpoint ``POST /api/paper/check-resolutions``
  * CLI: ``trading-cli data polymarket check-resolutions``
"""
from __future__ import annotations

import csv
import json
import logging
import time
from pathlib import Path
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"
_GAMMA_CSV = _PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv"
_GAMMA_URL = "https://gamma-api.polymarket.com/markets"

# Per-run ceiling on Gamma API calls — protects against runaway and rate limit.
# 2026-05-03: raised 300→1000. At 0.35s/call that's ~350s (6 min) per run,
# well within the 4h interval. Old 300-call ceiling created a permanent
# equilibrium with ~8K backlogged markets — we added ~300 new markets per
# run while resolving only 300, so the queue never drained.
_MAX_GAMMA_CALLS_PER_RUN = 1000
# Sleep between Gamma calls; public API tolerates ~3 req/s comfortably.
_GAMMA_SLEEP_SEC = 0.35


def _load_gamma_resolutions(csv_path: Path) -> dict[str, float]:
    """Return {condition_id_lower: yes_resolution_price_0_to_1}.

    `resolves_yes` is a per-market boolean: True if the YES outcome won,
    False if the NO outcome won. We translate it directly to the YES-token
    final price (1.0 if YES won, 0.0 if NO won) so the rest of the
    pipeline can compute outcome_delta in the YES-token reference frame
    without inspecting which row of the CSV it came from.
    """
    out: dict[str, float] = {}
    if not csv_path.exists():
        logger.warning("gamma_resolution.csv missing at %s", csv_path)
        return out

    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cid = (row.get("condition_id") or "").strip().lower()
            if not cid:
                continue
            ry_raw = (row.get("resolves_yes") or "").strip().lower()
            if ry_raw not in ("true", "false"):
                continue
            yes_won = ry_raw == "true"
            out[cid] = 1.0 if yes_won else 0.0
    logger.info("Loaded %d condition_id resolutions from gamma CSV", len(out))
    return out


def _fetch_gamma_resolution(condition_id: str, session: Any = None) -> float | None:
    """Live-fetch a single market's YES-token resolution price from Gamma.

    Returns ``1.0`` if YES won, ``0.0`` if NO won, ``None`` if the market is
    not yet resolved or the response is malformed. Accepts an optional
    ``requests.Session`` for connection reuse across a batch.
    """
    try:
        import requests
    except ImportError:
        return None
    s = session or requests
    try:
        r = s.get(_GAMMA_URL, params={"condition_ids": condition_id}, timeout=10)
    except Exception as exc:
        logger.debug("gamma fetch network error: %s", exc)
        return None
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    m = data[0] if isinstance(data, list) and data else None
    if not isinstance(m, dict):
        return None
    if not (m.get("closed") and m.get("umaResolutionStatus") == "resolved"):
        return None
    # outcomePrices is a JSON-stringified list like '["0", "1"]' paired
    # with outcomes '["Yes", "No"]'. Extract the YES-token price.
    outcomes_raw = m.get("outcomes")
    prices_raw = m.get("outcomePrices")
    if not outcomes_raw or not prices_raw:
        return None
    try:
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    except Exception:
        return None
    if not isinstance(outcomes, list) or not isinstance(prices, list):
        return None
    if len(outcomes) != len(prices):
        return None
    # Find the YES index (case-insensitive)
    yes_idx = None
    for i, name in enumerate(outcomes):
        if isinstance(name, str) and name.strip().lower() == "yes":
            yes_idx = i
            break
    if yes_idx is None:
        return None
    try:
        yes_price = float(prices[yes_idx])
    except (TypeError, ValueError):
        return None
    # Resolved markets settle to exactly 0 or 1; clamp tiny drift.
    if yes_price >= 0.99:
        return 1.0
    if yes_price <= 0.01:
        return 0.0
    # Non-binary result shouldn't happen for resolved binary markets;
    # refuse to commit an ambiguous value.
    return None


def _append_to_csv(csv_path: Path, condition_id: str, yes_price: float) -> None:
    """Append a Gamma-sourced resolution to gamma_resolution.csv so the
    cheap CSV pass picks it up on future runs."""
    exists = csv_path.exists()
    try:
        with csv_path.open("a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if not exists:
                w.writerow(["condition_id", "resolves_yes", "source"])
            w.writerow([condition_id, "true" if yes_price >= 0.5 else "false", "gamma_fallback"])
    except Exception as exc:
        logger.debug("csv append failed: %s", exc)


class SignalResolver:
    """Resolve pending signal_outcomes rows from gamma_resolution.csv,
    with a live Gamma API fallback for markets not yet in the CSV."""

    def __init__(
        self,
        db_path: str | Path | None = None,
        gamma_csv: str | Path | None = None,
        gamma_fallback: bool = True,
        max_gamma_calls: int = _MAX_GAMMA_CALLS_PER_RUN,
    ) -> None:
        self._db_path = str(db_path or _DEFAULT_DB)
        self._gamma_csv = Path(gamma_csv or _GAMMA_CSV)
        self._gamma_fallback = gamma_fallback
        self._max_gamma_calls = max_gamma_calls

    def _apply_resolution(
        self,
        conn: Any,
        sid: int,
        cid: str,
        direction: str | None,
        entry_price: float | None,
        fired_at: int | None,
        ptid: int | None,
        res_price: float,
    ) -> tuple[bool, bool]:
        """Write one resolution to signal_outcomes + mirror to paper trade.

        Returns ``(resolved, paper_updated)`` booleans so the caller can
        tally totals.
        """
        if entry_price is None:
            outcome_delta = None
        elif (direction or "").upper() in ("BUY", "BUY_YES"):
            outcome_delta = res_price - float(entry_price)
        elif (direction or "").upper() in ("SELL", "BUY_NO"):
            outcome_delta = (1.0 - res_price) - (1.0 - float(entry_price))
        else:
            outcome_delta = None

        is_win = 1 if (outcome_delta is not None and outcome_delta > 0) else 0

        # 2026-07-07 (audit C4): realized_pnl was booked side-blind as
        # (res_price - entry)*(size/entry) using the YES-frame for ALL
        # trades — a NO-side win (res_price=0) booked as -100% loss while
        # `outcome` said 'win'. Compute PnL-per-dollar in the HELD token's
        # own space (NO for SELL/BUY_NO) so SQL can scale by size_usd.
        _pnl_per_dollar = None
        if entry_price is not None:
            _ep = float(entry_price)
            if (direction or "").upper() in ("BUY", "BUY_YES"):
                _held_cost = max(_ep, 0.001)
                _held_payout = res_price
            else:
                _held_cost = max(1.0 - _ep, 0.001)
                _held_payout = 1.0 - res_price
            _pnl_per_dollar = (_held_payout / _held_cost) - 1.0

        conn.execute(
            """UPDATE signal_outcomes
               SET resolution_price = ?,
                   resolved_at = COALESCE(resolved_at, unixepoch()),
                   outcome_delta = ?,
                   is_win = ?,
                   hold_days = CASE
                     WHEN resolved_at IS NOT NULL
                       THEN ROUND((resolved_at - ?) / 86400.0, 3)
                     ELSE NULL END
               WHERE id = ?""",
            (res_price, outcome_delta, is_win, fired_at, sid),
        )

        paper_updated = False
        if ptid:
            cur = conn.execute(
                """UPDATE polymarket_paper_trades
                   SET exit_price = ?,
                       exit_ts = COALESCE(exit_ts, unixepoch()),
                       outcome = CASE WHEN ? > 0 THEN 'win' ELSE 'loss' END,
                       exit_reason = COALESCE(exit_reason, 'resolution'),
                       realized_pnl = COALESCE(realized_pnl,
                         size_usd * ?)
                   WHERE id = ? AND exit_ts IS NULL""",
                (res_price, outcome_delta, _pnl_per_dollar, ptid),
            )
            paper_updated = bool(cur.rowcount)
            # Propagate MAE + paper_pnl + paper_stake_usd back from the
            # paper trade so signal_outcomes is the one-stop calibration
            # source. paper_pnl=NULL was blocking every signal from
            # graduating to live (calibration.ev_per_trade can't compute
            # without a $PnL value).
            try:
                conn.execute(
                    """UPDATE signal_outcomes
                       SET mae = COALESCE(
                             signal_outcomes.mae,
                             (SELECT mae FROM polymarket_paper_trades WHERE id = ?)),
                           paper_pnl = COALESCE(
                             signal_outcomes.paper_pnl,
                             (SELECT realized_pnl FROM polymarket_paper_trades WHERE id = ?)),
                           paper_stake_usd = COALESCE(
                             signal_outcomes.paper_stake_usd,
                             (SELECT size_usd FROM polymarket_paper_trades WHERE id = ?))
                       WHERE id = ?""",
                    (ptid, ptid, ptid, sid),
                )
            except Exception:
                pass  # mae column may not exist on older DBs

        return True, paper_updated

    def run(self) -> dict[str, Any]:
        """Resolve all pending signals. Returns a summary dict."""
        resolutions = _load_gamma_resolutions(self._gamma_csv)

        conn = get_connection(self._db_path)
        try:
            pending = conn.execute(
                """SELECT id, condition_id, direction, entry_price, fired_at,
                          paper_trade_id
                   FROM signal_outcomes
                   WHERE resolution_price IS NULL"""
            ).fetchall()
            checked = len(pending)
            resolved = 0
            paper_updates = 0

            # ── Pass 1: cheap CSV lookup ──────────────────────────────────
            still_pending: list[tuple] = []
            for sid, cid, direction, entry_price, fired_at, ptid in pending:
                if not cid:
                    continue
                res_price = resolutions.get(cid.lower())
                if res_price is None:
                    still_pending.append((sid, cid, direction, entry_price, fired_at, ptid))
                    continue
                did_resolve, pu = self._apply_resolution(
                    conn, sid, cid, direction, entry_price, fired_at, ptid, res_price,
                )
                if did_resolve:
                    resolved += 1
                if pu:
                    paper_updates += 1

            conn.commit()

            # ── Pass 2: live Gamma API fallback for the rest ─────────────
            # Many markets resolve on Polymarket before being backfilled
            # into gamma_resolution.csv. Query Gamma directly for
            # condition_ids the CSV didn't cover — cap per-run to respect
            # rate limits. Any resolution we fetch is written back to the
            # CSV so future runs hit the cheap path.
            gamma_resolved = 0
            gamma_calls = 0
            gamma_skipped = 0
            gamma_cache: dict[str, float | None] = {}
            if self._gamma_fallback and still_pending:
                try:
                    import requests
                    session = requests.Session()
                except ImportError:
                    session = None

                # Deduplicate condition_ids so we don't re-query the same market
                unique_cids = []
                seen = set()
                for row in still_pending:
                    cid = row[1]
                    low = cid.lower() if isinstance(cid, str) else None
                    if low and low not in seen:
                        seen.add(low)
                        unique_cids.append(cid)

                logger.info(
                    "gamma fallback: %d unresolved rows across %d unique markets",
                    len(still_pending), len(unique_cids),
                )

                # Fetch, respecting the per-run ceiling
                for cid in unique_cids:
                    if gamma_calls >= self._max_gamma_calls:
                        gamma_skipped = len(unique_cids) - gamma_calls
                        logger.info(
                            "gamma fallback hit %d-call ceiling, %d markets skipped",
                            self._max_gamma_calls, gamma_skipped,
                        )
                        break
                    yes_price = _fetch_gamma_resolution(cid, session=session)
                    gamma_calls += 1
                    gamma_cache[cid.lower()] = yes_price
                    if yes_price is not None:
                        _append_to_csv(self._gamma_csv, cid, yes_price)
                    time.sleep(_GAMMA_SLEEP_SEC)

                # Apply resolutions to every row whose cid got a live answer
                for sid, cid, direction, entry_price, fired_at, ptid in still_pending:
                    if not cid:
                        continue
                    res_price = gamma_cache.get(cid.lower())
                    if res_price is None:
                        continue
                    did_resolve, pu = self._apply_resolution(
                        conn, sid, cid, direction, entry_price, fired_at, ptid, res_price,
                    )
                    if did_resolve:
                        gamma_resolved += 1
                        resolved += 1
                    if pu:
                        paper_updates += 1

                conn.commit()

            # Sanity check: resolution YES rate should be 15-25%, not 100%.
            # If it's higher, the gamma_resolution_fetcher bug may have
            # regressed. See reports/data_audit_2026-04-12.md.
            if resolved > 0 and resolutions:
                yes_count = sum(1 for r in resolutions.values() if r >= 0.5)
                yes_rate = yes_count / len(resolutions)
                if yes_rate > 0.40:
                    logger.error(
                        "RESOLUTION ANOMALY: gamma CSV YES rate %.1f%% "
                        "(expected ~18%%). Possible regression of the "
                        "resolves_yes bug.",
                        yes_rate * 100,
                    )

            return {
                "checked": checked,
                "resolved": resolved,
                "paper_trades_updated": paper_updates,
                "gamma_rows": len(resolutions),
                "gamma_calls": gamma_calls,
                "gamma_resolved": gamma_resolved,
                "gamma_skipped": gamma_skipped,
            }
        finally:
            conn.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = SignalResolver().run()
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
