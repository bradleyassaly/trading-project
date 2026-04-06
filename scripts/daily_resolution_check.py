"""
Daily resolution check for paper trades.

Checks all open paper trades against Kalshi API for settlements,
records outcomes, and logs results.

Usage:
    python scripts/daily_resolution_check.py

Schedule via Windows Task Scheduler at 9am daily, or run manually.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "resolution_log.txt"


def main() -> None:
    from dotenv import load_dotenv
    load_dotenv()

    from trading_platform.kalshi.paper_executor import KalshiPaperExecutor
    from trading_platform.kalshi.client import KalshiClient
    from trading_platform.kalshi.auth import KalshiConfig

    now = datetime.now(tz=timezone.utc)
    ts = now.strftime("%Y-%m-%d %H:%M UTC")

    # Get pre-check state
    executor = KalshiPaperExecutor()
    summary_before = executor.get_summary()
    open_before = summary_before["open_trades"]

    print(f"[{ts}] Resolution check starting")
    print(f"  Open trades: {open_before}")
    print(f"  Cash: ${summary_before['cash_usd']:.2f}")
    print()

    if open_before == 0:
        msg = f"[{ts}] No open trades to check.\n"
        print(msg)
        _append_log(msg)
        return

    # Connect to Kalshi
    try:
        auth = KalshiConfig.from_env()
        client = KalshiClient(auth)
    except Exception as exc:
        msg = f"[{ts}] ERROR: Could not connect to Kalshi: {exc}\n"
        print(msg)
        _append_log(msg)
        return

    # Check resolutions
    resolved = executor.check_resolutions(client)
    summary_after = executor.get_summary()

    print(f"  Resolved: {resolved} trades")
    print(f"  Open remaining: {summary_after['open_trades']}")
    print(f"  Cash after: ${summary_after['cash_usd']:.2f}")
    print(f"  Realized P&L: ${summary_after['realized_pnl']:.2f}")
    print(f"  Win rate: {summary_after['win_rate']:.1%} ({summary_after['wins']}/{summary_after['closed_trades']})")

    # Show newly resolved trades
    if resolved > 0:
        recent = executor.get_recent_trades(limit=resolved + 5)
        closed = [t for t in recent if t["status"] == "closed"][:resolved]
        print(f"\n  Newly resolved:")
        for t in closed:
            ret = t.get("return_pct", 0) or 0
            print(f"    {t['ticker']:30s} {t['side']:3s} {t['outcome']:4s} "
                  f"return={ret*100:+.1f}% signal={t.get('signal_family', '?')}")

    # Signal attribution
    attr = summary_after.get("signal_attribution", [])
    if any(a["closed"] > 0 for a in attr):
        print(f"\n  Signal Attribution:")
        print(f"  {'Signal':<25s} {'Trades':>6s} {'Wins':>5s} {'WR':>7s} {'Avg $':>8s}")
        for a in attr:
            wr = f"{a['win_rate']:.1%}" if a["closed"] > 0 else "n/a"
            print(f"  {a['signal_type']:<25s} {a['total_trades']:>6d} {a['wins']:>5d} "
                  f"{wr:>7s} ${a['avg_stake']:>7.2f}")

    # Log to file
    log_lines = [
        f"[{ts}] Checked {open_before} open trades, resolved {resolved}",
        f"  Cash: ${summary_after['cash_usd']:.2f} | P&L: ${summary_after['realized_pnl']:.2f} | "
        f"WR: {summary_after['win_rate']:.1%} ({summary_after['wins']}/{summary_after['closed_trades']})",
    ]
    if resolved > 0:
        for t in closed:
            ret = t.get("return_pct", 0) or 0
            log_lines.append(f"  -> {t['ticker']} {t['side']} {t['outcome']} "
                             f"ret={ret*100:+.1f}% sig={t.get('signal_family', '?')}")
    _append_log("\n".join(log_lines) + "\n")

    executor.close()
    print(f"\n  Log appended to {LOG_FILE}")


def _append_log(text: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(text + "\n")


if __name__ == "__main__":
    main()
