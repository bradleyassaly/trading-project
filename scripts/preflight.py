"""Pre-flight checklist for live trading."""
import sqlite3, os, sys
sys.path.insert(0, "src")
from dotenv import load_dotenv
load_dotenv()

checks = {}
conn = sqlite3.connect("data/polymarket/wallet_intelligence.db", timeout=30)

# 1. DB health
mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
checks["db_journal_delete"] = mode == "delete"
import glob
checks["no_wal_files"] = len(glob.glob("data/polymarket/wallet_intelligence.db-*")) == 0

# 2. Resolution sanity
yes = conn.execute("SELECT COUNT(*) FROM signal_outcomes WHERE resolution_price=1.0").fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM signal_outcomes WHERE resolution_price IS NOT NULL").fetchone()[0]
yes_rate = yes / total if total else 0
checks["res_yes_rate_sane"] = yes_rate < 0.30

# 3. Accumulation positive EV
from scipy import stats
import pandas as pd
acc = pd.read_sql("SELECT outcome_delta FROM signal_outcomes WHERE signal_type='accumulation' AND resolution_price IS NOT NULL AND entry_price>0 AND entry_price<1", conn)
if len(acc) >= 10:
    _, p = stats.ttest_1samp(acc["outcome_delta"], 0)
    checks["accumulation_ev_positive"] = acc["outcome_delta"].mean() > 0 and p / 2 < 0.15
else:
    checks["accumulation_ev_positive"] = False

# 4. Signals firing
recent = conn.execute("SELECT COUNT(*) FROM market_signals WHERE fired_at > unixepoch('now','-48 hours')").fetchone()[0]
checks["signals_firing_48h"] = recent > 0

# 5. Archetypes populated
wa = conn.execute("SELECT COUNT(*) FROM wallet_archetypes").fetchone()[0]
checks["archetypes_populated"] = wa > 50

# 6. Kill switch clear
from trading_platform.polymarket.kill_switch import KillSwitch
ks = KillSwitch("data/polymarket/wallet_intelligence.db", bankroll=500)
stopped, _ = ks.is_emergency_stopped()
checks["kill_switch_clear"] = not stopped

# 7. CLOB credentials
checks["clob_private_key"] = bool(os.getenv("POLYMARKET_PRIVATE_KEY") or os.getenv("PRIVATE_KEY"))
checks["clob_api_key"] = bool(os.getenv("POLYMARKET_API_KEY") or os.getenv("CLOB_API_KEY"))

# 8. CLOB connectivity
try:
    from trading_platform.polymarket.clob_client import ClobClient
    clob = ClobClient()
    test = clob.test_connection()
    checks["clob_public_endpoint"] = test.get("public_endpoint", False)
    checks["clob_configured"] = test.get("credentials_configured", False)
    checks["py_clob_client_installed"] = test.get("py_clob_client", False)
except Exception:
    checks["clob_public_endpoint"] = False
    checks["clob_configured"] = False

# 9. Live executor exists + importable
try:
    from trading_platform.polymarket.polymarket_live_executor import PolymarketLiveExecutor
    checks["live_executor_importable"] = True
except Exception:
    checks["live_executor_importable"] = False

# 10. Token ID coverage
token_ok = conn.execute("SELECT COUNT(*) FROM signal_outcomes WHERE token_id IS NOT NULL AND token_id!='' AND resolution_price IS NULL").fetchone()[0]
token_miss = conn.execute("SELECT COUNT(*) FROM signal_outcomes WHERE (token_id IS NULL OR token_id='') AND resolution_price IS NULL").fetchone()[0]
checks["token_id_coverage"] = token_ok > 0

# 11. Signal config correct
from trading_platform.polymarket.whale_signal_engine import DISABLED_SIGNAL_TYPES
checks["accumulation_not_disabled"] = "accumulation" not in DISABLED_SIGNAL_TYPES
checks["whale_entry_filtered_not_disabled"] = "whale_entry_filtered" not in DISABLED_SIGNAL_TYPES
checks["whale_entry_raw_disabled"] = "whale_entry" in DISABLED_SIGNAL_TYPES

# 12. KillSwitch bankroll
checks["killswitch_bankroll_500"] = ks.BANKROLL == 500

conn.close()

# VERDICT
print("=" * 60)
print("PRE-FLIGHT CHECKLIST")
print("=" * 60)
failed = []
for check, passed in checks.items():
    status = "PASS" if passed else "FAIL"
    if not passed:
        failed.append(check)
    print(f"  [{status}] {check}")

print(f"\n{'=' * 60}")
if not failed:
    print("ALL CHECKS PASS -- CLEARED FOR LIVE")
else:
    print(f"{len(failed)} CHECK(S) FAILED:")
    for f in failed:
        print(f"  -> {f}")

print(f"\nInfo:")
print(f"  res YES rate: {yes_rate:.1%}")
print(f"  accumulation EV: {acc['outcome_delta'].mean():+.4f} (n={len(acc)})")
print(f"  signals 48h: {recent}")
print(f"  archetypes: {wa}")
print(f"  token_id: {token_ok} have / {token_miss} missing")
