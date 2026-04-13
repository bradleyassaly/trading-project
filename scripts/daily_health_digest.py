"""
Daily health digest — sends a Telegram summary of the trading system.

Run manually or via Windows Task Scheduler:
  schtasks /create /tn "TradingPlatform_DailyDigest" ^
    /tr "cmd /c cd C:\\Users\\bradl\\PycharmProjects\\trading_platform && .venv\\Scripts\\python.exe scripts\\daily_health_digest.py" ^
    /sc daily /st 08:00 /f
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Ensure the project is on the path
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))
os.chdir(str(_ROOT))

from dotenv import load_dotenv
load_dotenv()


def send_telegram(msg: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat:
        print("Telegram not configured — writing to file")
        reports = _ROOT / "reports"
        reports.mkdir(exist_ok=True)
        with open(reports / "daily_digest.txt", "a") as f:
            f.write(f"\n--- {datetime.now()} ---\n{msg}\n")
        return False
    try:
        import requests
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
        return r.ok
    except Exception as exc:
        print(f"Telegram send failed: {exc}")
        return False


def build_digest() -> str:
    from trading_platform.polymarket.db import connect_wallet_db

    conn = connect_wallet_db()
    now = int(time.time())
    yesterday = now - 86400

    # Paper trades
    row = conn.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN exit_ts IS NULL AND archived=0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN outcome IS NOT NULL AND archived=0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN outcome='win' AND archived=0 THEN 1 ELSE 0 END),
               COALESCE(SUM(CASE WHEN outcome IS NOT NULL AND archived=0 THEN realized_pnl ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN exit_ts IS NULL AND archived=0 THEN size_usd ELSE 0 END), 0)
        FROM polymarket_paper_trades WHERE archived=0
    """).fetchone()
    total, open_n, resolved, wins, pnl, deployed = row
    wr = f"{wins/resolved:.0%}" if resolved and resolved > 0 else "N/A"

    # Last 24h activity
    new_trades = conn.execute(
        "SELECT COUNT(*) FROM polymarket_paper_trades WHERE archived=0 AND entry_ts > ?",
        (yesterday,),
    ).fetchone()[0]
    exit_row = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(realized_pnl),0) FROM polymarket_paper_trades WHERE exit_ts > ? AND archived=0",
        (yesterday,),
    ).fetchone()

    # Category exposure
    cats = conn.execute("""
        SELECT category, COUNT(*), COALESCE(SUM(size_usd),0)
        FROM polymarket_paper_trades
        WHERE archived=0 AND exit_ts IS NULL
        GROUP BY category ORDER BY SUM(size_usd) DESC
    """).fetchall()
    cat_lines = "\n".join(f"  {c[0]}: {c[1]} trades, ${c[2]:.0f}" for c in cats) or "  (none)"

    # Copyable wallets
    copy_n = conn.execute("SELECT COUNT(DISTINCT wallet) FROM wallet_alpha_scores WHERE is_copyable=1").fetchone()[0]

    conn.close()

    return (
        f"*Daily Trading Digest*\n"
        f"\n"
        f"*Portfolio*\n"
        f"Open: {open_n} | Resolved: {resolved}/50 | WR: {wr}\n"
        f"Deployed: ${deployed:,.0f} | P&L: ${pnl:,.2f}\n"
        f"\n"
        f"*Last 24h*\n"
        f"New trades: {new_trades}\n"
        f"Exits: {exit_row[0]} (P&L: ${exit_row[1]:,.2f})\n"
        f"\n"
        f"*Exposure*\n"
        f"{cat_lines}\n"
        f"\n"
        f"*System*\n"
        f"Copyable wallets: {copy_n}\n"
        f"Go-live: {resolved}/50 resolved"
    )


if __name__ == "__main__":
    msg = build_digest()
    print(msg)
    ok = send_telegram(msg)
    print(f"\nTelegram: {'sent' if ok else 'not sent (see file)'}")
