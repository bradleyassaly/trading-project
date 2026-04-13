"""Full system state snapshot."""
import sqlite3, os
import pandas as pd
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

conn = sqlite3.connect("data/polymarket/wallet_intelligence.db")
print("=== DB ===")
print("journal:", conn.execute("PRAGMA journal_mode").fetchone()[0])
print("wal:", os.path.exists("data/polymarket/wallet_intelligence.db-wal"))

print("\n=== FRESHNESS ===")
for t, c in [("wallet_trades","timestamp"),("market_signals","fired_at"),
             ("signal_outcomes","fired_at"),("wallet_archetypes","classified_at")]:
    try:
        r = conn.execute(f"SELECT COUNT(*), datetime(MAX({c}),'unixepoch') FROM {t}").fetchone()
        print(f"  {t}: n={r[0]} latest={r[1]}")
    except: pass

print("\n=== SIGNAL CONFIG ===")
from trading_platform.polymarket.whale_signal_engine import DISABLED_SIGNAL_TYPES
from trading_platform.polymarket.polymarket_paper_executor import SIGNAL_BANKROLL
for st in sorted(set(list(SIGNAL_BANKROLL) + list(DISABLED_SIGNAL_TYPES))):
    d = "DIS" if st in DISABLED_SIGNAL_TYPES else "ACT"
    b = SIGNAL_BANKROLL.get(st, "-")
    print(f"  {st:>25} {d:>3} bankroll={b}")

print("\n=== SIGNAL OUTCOMES ===")
df = pd.read_sql("SELECT signal_type, COUNT(*) f, SUM(CASE WHEN resolution_price IS NOT NULL THEN 1 ELSE 0 END) r, SUM(is_win) w, ROUND(AVG(outcome_delta),4) ev FROM signal_outcomes GROUP BY signal_type ORDER BY r DESC", conn)
print(df.to_string(index=False))

yes = conn.execute("SELECT COUNT(*) FROM signal_outcomes WHERE resolution_price=1.0").fetchone()[0]
tot = conn.execute("SELECT COUNT(*) FROM signal_outcomes WHERE resolution_price IS NOT NULL").fetchone()[0]
print(f"\nres YES rate: {yes}/{tot} = {yes/tot*100:.1f}%")

print("\n=== ARCHETYPES ===")
print(pd.read_sql("SELECT archetype, copyable, COUNT(*) n FROM wallet_archetypes GROUP BY archetype, copyable ORDER BY copyable DESC, n DESC", conn).to_string(index=False))

print("\n=== PAPER TRADES ===")
print(pd.read_sql("SELECT signal_type, SUM(CASE WHEN exit_ts IS NULL THEN 1 ELSE 0 END) open_, SUM(CASE WHEN exit_ts IS NOT NULL THEN 1 ELSE 0 END) closed, ROUND(SUM(realized_pnl),2) pnl FROM polymarket_paper_trades WHERE archived=0 OR archived IS NULL GROUP BY signal_type", conn).to_string(index=False))

print("\n=== KILL SWITCH ===")
from trading_platform.polymarket.kill_switch import KillSwitch
ks = KillSwitch("data/polymarket/wallet_intelligence.db", bankroll=500)
print(f"  bankroll=${ks.BANKROLL} max_loss=${ks.BANKROLL*ks.MAX_DAILY_LOSS_PCT:.0f} max_trade=${ks.MAX_TRADE_USD}")
s, r = ks.is_emergency_stopped()
print(f"  emergency_stop: {s}")
conn.close()
