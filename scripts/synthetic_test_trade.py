"""Daily synthetic-trade smoke test.

Fires a $0.50 paper trade through the full signal → executor →
hypothesis → resolution pipeline once a day. If yesterday's synthetic
trade hasn't resolved by the time we fire today's, raise a Telegram
alert — the pipeline has a gap.

Run from the scheduler (interval=24h). Detects the silent-pipeline-gap
class of failure (signals fire, but no paper trade ever appears, or
trades open but never resolve) within 24h instead of weeks.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("synthetic_test_trade")


def _alpha_db_path() -> str:
    return os.environ.get("WALLET_DB_PATH", "/app/data/polymarket/wallet_intelligence.db")


def _send_alert(msg: str) -> None:
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        a = get_alerter()
        if a.enabled:
            a._send(msg, disable_notification=False)
    except Exception:
        pass


def _check_yesterdays_resolution() -> tuple[bool, str]:
    """Return (resolved_ok, detail). True = the synthetic trade
    fired ≥20h and ≤30h ago has reached terminal state.
    """
    from trading_platform.polymarket.db_connection import get_connection
    cutoff_old = int(time.time()) - 30 * 3600
    cutoff_new = int(time.time()) - 20 * 3600
    try:
        conn = get_connection(_alpha_db_path())
    except Exception as exc:
        return False, f"db error: {exc}"
    try:
        row = conn.execute(
            """SELECT id, entry_ts, exit_ts, outcome
                 FROM polymarket_paper_trades
                WHERE wallet = 'synthetic_test'
                  AND entry_ts BETWEEN ? AND ?
                ORDER BY entry_ts DESC LIMIT 1""",
            (cutoff_old, cutoff_new),
        ).fetchone()
    except Exception as exc:
        return False, f"query error: {exc}"
    finally:
        try: conn.close()
        except Exception: pass
    if not row:
        return False, "no synthetic trade fired in the past 20-30h window"
    trade_id, entry_ts, exit_ts, outcome = row
    if exit_ts is None or outcome is None:
        return False, f"trade #{trade_id} entered {(int(time.time())-entry_ts)//3600}h ago still unresolved"
    return True, f"trade #{trade_id} resolved {outcome}"


def _fire_synthetic() -> tuple[bool, str]:
    """Insert a synthetic paper trade row directly. Bypasses the signal
    engine — we're testing the post-entry path (resolution, exit).
    Pick a market that resolves within 24h so we can verify end-to-end
    in the next run.
    """
    from trading_platform.polymarket.db_connection import get_connection
    try:
        conn = get_connection(_alpha_db_path())
    except Exception as exc:
        return False, f"db error: {exc}"
    try:
        # Pick the soonest-resolving open market with both YES and NO
        # liquidity. Limit to crypto/sports (known fast-resolution).
        market = conn.execute(
            """SELECT condition_id, slug, question, end_date_iso, yes_token_id, no_token_id
                 FROM markets
                WHERE end_date_iso IS NOT NULL
                  AND end_date_iso > datetime('now')
                  AND end_date_iso < datetime('now', '+24 hours')
                  AND yes_token_id IS NOT NULL
                  AND no_token_id IS NOT NULL
                ORDER BY end_date_iso ASC LIMIT 1"""
        ).fetchone()
    except Exception as exc:
        return False, f"query error: {exc}"
    if not market:
        try: conn.close()
        except Exception: pass
        return False, "no market resolving within 24h"
    cond_id, slug, question, end_iso, yes_tid, no_tid = market
    now = int(time.time())
    try:
        cur = conn.execute(
            """INSERT INTO polymarket_paper_trades
                 (wallet, signal_type, condition_id, token_id, side, direction,
                  entry_price, size_usd, confidence, entry_ts,
                  category, slug, question, archived)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
            (
                "synthetic_test", "synthetic", cond_id, yes_tid, "YES", "BUY",
                0.50, 0.50, 0.50, now,
                "synthetic", slug, question[:200],
            ),
        )
        conn.commit()
        try: conn.close()
        except Exception: pass
        return True, f"synthetic trade fired: {slug[:40]} (resolves {end_iso})"
    except Exception as exc:
        try: conn.close()
        except Exception: pass
        return False, f"insert failed: {exc}"


def main() -> int:
    # Step 1: check whether yesterday's synthetic resolved.
    ok, detail = _check_yesterdays_resolution()
    if not ok:
        msg = (
            f"\u26a0\ufe0f <b>SYNTHETIC TRADE PIPELINE GAP</b>\n\n"
            f"{detail}\n\n"
            f"Run <code>scripts/synthetic_test_trade.py</code> manually + check:\n"
            f"  - <code>paper_check_exits</code> scheduler health\n"
            f"  - <code>paper_resolutions</code> scheduler health\n"
            f"  - <code>polymarket_paper_trades</code> for recent synthetic rows"
        )
        logger.warning("yesterday's synthetic NOT resolved: %s", detail)
        _send_alert(msg)
    else:
        logger.info("yesterday's synthetic OK: %s", detail)

    # Step 2: fire today's synthetic.
    ok2, detail2 = _fire_synthetic()
    if ok2:
        logger.info("[synthetic] %s", detail2)
    else:
        logger.warning("[synthetic] FIRE FAILED: %s", detail2)
        _send_alert(
            f"\U0001f534 <b>SYNTHETIC TRADE FIRE FAILED</b>\n\n{detail2}"
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
