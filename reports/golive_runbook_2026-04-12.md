# Go-Live Runbook — 2026-04-12

## Pre-Flight: 18/18 PASS

All gates cleared on fully rebuilt, corrected data:

| Gate | Status |
|---|---|
| DB integrity | ok |
| Journal mode | delete |
| No WAL sidecars | none |
| Resolution YES rate | 10.6% (< 30%) |
| accumulation EV positive + significant | +0.280 (p < 0.001) |
| Signals firing (48h) | 1,145 |
| Archetypes populated | 199 |
| Kill switch clear | yes |
| CLOB private key | set |
| CLOB API key | set |
| **CLOB configured** | **pass** |
| py-clob-client installed | yes |
| Live executor importable | yes |
| Token ID coverage | 7,219 / 0 missing |
| accumulation not disabled | correct |
| whale_entry_filtered not disabled | correct |
| whale_entry raw disabled | correct |
| KillSwitch bankroll | $500 |

## Config Changes Applied This Session

1. **Politics confidence boost removed**: specialist/conviction wallets in politics no longer get +0.15 boost (only geopolitics does). Politics has no positive EV on any signal type.
2. **Crypto/entertainment/sports excluded from accumulation**: paper and live executors now skip accumulation signals in these categories. Crypto accumulation: 7 signals, 0 wins, EV=-0.50.
3. **pol_geo virtual tier**: 141 wallets with combined politics+geopolitics tier (24 S, 17 A, 25 B, 75 C). Wallets that trade both categories get credit for the combined sample.
4. **ClobClient passphrase env var**: now reads both `POLYMARKET_API_PASSPHRASE` and `POLYMARKET_PASSPHRASE` (the latter is what's in `.env`).

## Signal Configuration

| Signal | Status | Bankroll | Category restrictions |
|---|---|---:|---|
| **accumulation** | ACTIVE | $20,000 | Excluded: crypto, entertainment, sports |
| **whale_entry_filtered** | ACTIVE | $15,000 | Geo boost: +0.15 for specialist/conviction |
| whale_entry (raw) | DISABLED | — | Recording only |
| Everything else | DISABLED | — | — |

## Go-Live Steps

```bash
# 1. Verify wallet has USDC + MATIC on Polygon
#    Need: ~$500 USDC + ~$5-10 MATIC for gas

# 2. Verify CLOB client works end-to-end
python -c "
from trading_platform.polymarket.clob_client import ClobClient
c = ClobClient()
print(c.test_connection())
# Should show: public_endpoint=True, credentials_configured=True
"

# 3. Start Docker services with named volume
docker compose up -d
# Verify all 5 containers start:
docker compose ps

# 4. Verify DB accessible from inside Docker
docker exec polymarket-api python -c "
import sqlite3
c = sqlite3.connect('/app/data/polymarket/wallet_intelligence.db')
print('integrity:', c.execute('PRAGMA integrity_check').fetchone()[0][:5])
print('trades:', c.execute('SELECT COUNT(*) FROM wallet_trades').fetchone()[0])
c.close()
"

# 5. Enable live trading
# Add to .env:
echo 'POLYMARKET_LIVE_ENABLED=1' >> .env

# 6. Restart to pick up the env change
docker compose restart

# 7. Monitor first signal
docker compose logs -f live-collect 2>&1 | grep -i "SIGNAL\|TRADE\|LIVE\|accumulation"
```

## Week 1 Configuration

| Parameter | Value |
|---|---|
| Bankroll | $500 |
| Max per trade | $25 |
| Max daily loss | $50 |
| Max open positions | 10 |
| Order type | LIMIT (market order via py-clob-client) |
| Stale signal cutoff | 15 minutes |
| Price freshness | 5% max movement |
| Liquidity floor | 2x trade size in ask depth |
| Signals enabled for live | accumulation, whale_entry_filtered |
| Category boost | geopolitics +0.15 (specialist/conviction only) |
| Categories excluded | crypto, entertainment, sports (for accumulation) |

## Daily Monitoring Checklist

- [ ] Check Telegram for trade alerts (every 2-4 hours)
- [ ] Run `python scripts/preflight.py` — all 18 gates still passing
- [ ] Resolution YES rate < 30%
- [ ] Kill switch not tripped (`GET /api/system/kill-switch`)
- [ ] Compare any live fills to paper fills (same signal, different execution)
- [ ] Review signal fire rate (accumulation: 4-6/day, whale_filtered: 2-5/week)

## Week 1 Decision Points

| Observation | Action |
|---|---|
| 5+ live trades, net positive, fills clean | Raise cap to $50/trade |
| Execution friction visible (slippage > 3%) | Tighten liquidity filter |
| < 5 trades after 7 days | Extend to week 2, no changes |
| Kill switch trips | Evaluate cause, don't auto-resume |
| accumulation EV drops below +0.10 | Pause live, review |

## Emergency Procedures

| Event | Response |
|---|---|
| Kill switch trips | Telegram alert fires. Check open positions. Don't auto-resume. |
| CLOB API error | Live executor logs error, returns None. Paper trade still placed. No manual action needed. |
| DB corruption | Should never happen on Docker named volume. If it does: `docker compose stop`, inspect volume, rebuild if needed. |
| Resolution bug regression | YES-rate > 40% triggers ERROR log in SignalResolver. Stop live, investigate. |
| Total wipeout scenario | $50 daily limit catches it. Maximum loss: $500 (entire bankroll). |

## Scaling Plan

| Milestone | Action |
|---|---|
| Week 1 positive, 5+ trades | $50/trade cap |
| Week 2, accumulation EV > +0.15 at 70+ resolved | $100/trade cap |
| whale_entry_filtered has 10+ resolved with positive EV | Add to live at $25/trade |
| Consistent profitability over 4 weeks | Consider $1,000 bankroll |
| Never | Exceed $250/trade or 50% of bankroll in open positions |

## Key Numbers (for reference)

| Metric | Value |
|---|---|
| accumulation: EV | +0.280 |
| accumulation: WR | 79.7% (47/59) |
| accumulation: p-value | < 0.001 |
| accumulation x geopolitics: EV | +0.456 |
| accumulation x geopolitics: WR | 95.7% (44/46) |
| Resolution YES rate | 10.6% |
| DB integrity | ok |
| Wallet archetypes | 199 (85 conviction, 35 diversified, 17 specialist) |
| pol_geo S/A wallets | 41 (24 S + 17 A) |

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\golive_runbook_2026-04-12.md`
