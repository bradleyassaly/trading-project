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
import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"
_GAMMA_CSV = _PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv"


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


class SignalResolver:
    """Resolve pending signal_outcomes rows from gamma_resolution.csv."""

    def __init__(
        self,
        db_path: str | Path | None = None,
        gamma_csv: str | Path | None = None,
    ) -> None:
        self._db_path = str(db_path or _DEFAULT_DB)
        self._gamma_csv = Path(gamma_csv or _GAMMA_CSV)

    def run(self) -> dict[str, Any]:
        """Resolve all pending signals. Returns a summary dict."""
        resolutions = _load_gamma_resolutions(self._gamma_csv)
        if not resolutions:
            return {
                "checked": 0, "resolved": 0, "paper_trades_updated": 0,
                "error": "no gamma resolutions available",
            }

        conn = sqlite3.connect(self._db_path, timeout=60)
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

            for sid, cid, direction, entry_price, fired_at, ptid in pending:
                if not cid:
                    continue
                res_price = resolutions.get(cid.lower())
                if res_price is None:
                    continue

                # Determine the outcome delta for whatever side the signal
                # recommended. We store delta in the YES-token reference
                # frame: BUY / BUY_YES gains `yes_price - entry`; SELL / BUY_NO
                # gains `(1 - yes_price) - (1 - entry)`.
                if entry_price is None:
                    outcome_delta = None
                elif (direction or "").upper() in ("BUY", "BUY_YES"):
                    outcome_delta = res_price - float(entry_price)
                elif (direction or "").upper() in ("SELL", "BUY_NO"):
                    outcome_delta = (1.0 - res_price) - (1.0 - float(entry_price))
                else:
                    outcome_delta = None

                is_win = 1 if (outcome_delta is not None and outcome_delta > 0) else 0

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
                resolved += 1

                # Mirror to polymarket_paper_trades if linked and still open
                if ptid:
                    cur = conn.execute(
                        """UPDATE polymarket_paper_trades
                           SET exit_price = ?,
                               exit_ts = COALESCE(exit_ts, unixepoch()),
                               outcome = CASE WHEN ? > 0 THEN 'win' ELSE 'loss' END,
                               exit_reason = COALESCE(exit_reason, 'resolution'),
                               realized_pnl = COALESCE(realized_pnl,
                                 (? - entry_price) * (size_usd / entry_price))
                           WHERE id = ? AND exit_ts IS NULL""",
                        (res_price, outcome_delta, res_price, ptid),
                    )
                    if cur.rowcount:
                        paper_updates += 1

            conn.commit()

            # Sanity check: resolution YES rate should be 15-25%, not 100%.
            # If it's higher, the gamma_resolution_fetcher bug may have
            # regressed. See reports/data_audit_2026-04-12.md.
            if resolved > 0:
                yes_count = sum(
                    1 for r in resolutions.values() if r >= 0.5
                )
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
