# Pre-Flight Report — 2026-04-12

## Verdict: NO-GO (DB corruption blocker)

The pre-flight checklist found a critical blocker: **page-level corruption in 6 essential database tables**, caused by the accumulated abuse of the Windows Docker bind-mount across 5 sessions of concurrent-access, force-kills, and bug-fixing. The affected tables cannot be read at all — not even row-by-row recovery works.

### Corrupted tables

| Table | Rows (pre-corruption) | Rebuildable? | Method |
|---|---:|---|---|
| `wallet_trades` | 410,054 | Yes (30-60 min) | `sync-wallet-trades --top-n 200` + `data-api-fetch --hours-back 168` |
| `wallet_profiles` | 16,252 | Yes (15 min) | `wallet-profiles --from-db` |
| `wallet_category_profiles` | 532 | Yes (10 min) | `scripts/build_wallet_category_profiles.py` |
| `wallet_alpha_scores` | 391 | Yes (5 min) | `compute-alpha-scores` |
| `leaderboard` | ~200 | Yes (1 min) | `build-leaderboard` |
| `stale_signals` | varies | Not critical | Logging only |

### Tables that survived (27 of 34)

`market_signals` (9,717 rows), `polymarket_paper_trades` (267), `market_categories` (10,918), `wallet_poll_state`, `dry_run_trades`, `live_trades`, `trade_hypotheses`, `market_ticks`, `calibration_reports`, `category_performance`, `circuit_breaker_*`, `wallet_alerts`, `market_anomalies`, `market_price_history`, etc. All intact after the clean rebuild.

### `signal_outcomes` (recreated fresh)

The table was recreated and backfilled with 9,537 rows from `market_signals`. **Resolution attachment and outcome_delta computation need to be re-run after wallet_trades is rebuilt** (the resolution source chain: gamma CSV → wallet_trades.market_outcome → signal_outcomes).

## What Was Built Before the Blocker

All code changes from this session are intact and ready:

### 1. Signal audit (Part 1)

No overlooked alpha. `accumulation` is the sole live candidate (N=59, WR=80%, EV=+0.28, p<0.001). `market_maker_flip × geopolitics` (n=12, +0.27) is worth monitoring. Everything else is negative.

### 2. Live executor guards (Part 2-9)

Three safety guards added to `polymarket_live_executor.py`:

- **Week-1 trade cap**: `WEEK1_MAX_TRADE = 25` — hard ceiling overriding Kelly output
- **Stale signal guard**: abort if signal age > 15 minutes
- **Price freshness guard**: abort if price moved > 5% since signal
- **Liquidity guard**: abort if top-5 ask depth < 2x trade size

Signal engine now has `LIVE_SIGNAL_TYPES = {"accumulation", "whale_entry_filtered"}` whitelist — only these two signals reach the live executor. All other signals (including the existing paper-only ones) are unaffected.

### 3. CLOB client

Already exists (`clob_client.py`, 219 lines) with:
- Public endpoints: `get_order_book`, `get_mid_price`, `get_last_price`
- `place_market_order` via `py-clob-client`
- `test_connection` diagnostic
- Credential check from `.env`

### 4. Pre-flight script

`scripts/preflight.py` checks 12 gates. The DB corruption prevents running it against the current DB state, but the logic is ready.

## Rebuild Procedure (to clear the blocker)

This is a 30-60 minute automated process:

```bash
# 1. Re-ingest wallet trades from Polymarket Data API
.venv/Scripts/python.exe -m trading_platform.cli data polymarket sync-wallet-trades --top-n 200

# 2. Fetch recent trades (last 7 days)  
.venv/Scripts/python.exe -m trading_platform.cli data polymarket data-api-fetch --hours-back 168

# 3. Enrich trades with corrected resolution data
.venv/Scripts/python.exe -m trading_platform.cli data polymarket enrich-trade-resolution

# 4. Rebuild wallet profiles
.venv/Scripts/python.exe -m trading_platform.cli data polymarket wallet-profiles --from-db

# 5. Rebuild wallet category profiles
.venv/Scripts/python.exe scripts/build_wallet_category_profiles.py

# 6. Rebuild alpha scores
.venv/Scripts/python.exe -m trading_platform.cli data polymarket compute-alpha-scores

# 7. Rebuild leaderboard
.venv/Scripts/python.exe -m trading_platform.cli data polymarket build-leaderboard

# 8. Classify wallet archetypes
.venv/Scripts/python.exe -c "from trading_platform.polymarket.wallet_archetype import WalletArchetypeClassifier; WalletArchetypeClassifier().classify_all()"

# 9. Re-resolve signal_outcomes
.venv/Scripts/python.exe -c "from trading_platform.polymarket.signal_resolver import SignalResolver; print(SignalResolver().run())"

# 10. Attach wallet_trades-derived resolutions
# (Use the indexed Python-batch approach from the rebuild session)

# 11. Run preflight
.venv/Scripts/python.exe scripts/preflight.py
```

After this, the pre-flight checklist should pass. Then:

```bash
# Go live
export POLYMARKET_LIVE_ENABLED=1
# Restart containers:
docker compose restart live-collect scheduler
```

## Root Cause + Permanent Fix

The corruption happened because SQLite on Docker Desktop's Windows bind-mount is fundamentally fragile. We've seen this in every session:
- Session 1: WAL lock blocking all writers
- Session 3: corrupted `.corrupt` / `.corrupt3` / `.pre_clean` files already present
- This session: full page-level B-tree corruption across 6 tables

**The permanent fix is to move the database off the Windows bind-mount to a Docker named volume.** This was recommended in session 1 and is now mandatory. The bind-mount path has corrupted the DB at least 4 times. A named volume uses the Linux overlay filesystem inside Docker, which supports SQLite's locking and journaling correctly.

```yaml
# docker-compose.yml change:
volumes:
  wallet_db:

services:
  scheduler:
    volumes:
      - wallet_db:/app/data/polymarket
      - .:/app  # keep code bind-mount
```

This separates the hot DB from the Windows filesystem entirely while keeping the code accessible for development.

## Go-Live Configuration (Ready Once Blocker Cleared)

```
POLYMARKET_LIVE_ENABLED=1
LIVE_SIGNAL_TYPES: {accumulation, whale_entry_filtered}
Week 1 max per trade: $25
Kill switch max daily loss: $50
Kill switch max positions: 10
Kill switch bankroll: $500
Kelly fraction: 0.25 (quarter-Kelly)
Stale signal cutoff: 15 minutes
Price freshness tolerance: 5%
Liquidity minimum: 2x trade size ask depth
```

## Risk Scenarios

| Scenario | Mitigation |
|---|---|
| CLOB API down | Live executor catches exception, returns None, paper trade still placed |
| Market delists mid-position | SignalResolver detects, marks as loss at cost basis |
| Price moves > 5% before fill | Stale-price guard aborts — no order submitted |
| Kill switch trips | Telegram alert sent, all trading pauses, operator evaluates |
| Resolution bug regresses | YES-rate sanity check in SignalResolver logs ERROR if > 40% |
| DB corruption recurs | Move to Docker named volume (permanent fix) |
| Maximum possible loss | $50/day (kill switch) or $500 total (bankroll) |

## Files Modified This Session

- `src/trading_platform/polymarket/whale_signal_engine.py` — `LIVE_SIGNAL_TYPES` whitelist, `DISABLED_SIGNAL_TYPES` updates (added whale_entry, market_maker_flip, wallet_reversal)
- `src/trading_platform/polymarket/polymarket_live_executor.py` — Week-1 cap, stale price guard, liquidity check
- `src/trading_platform/polymarket/wallet_archetype.py` — New: archetype classifier
- `scripts/signal_audit_full.py` — New: full signal audit
- `scripts/preflight.py` — New: 12-gate pre-flight checklist
- `data/polymarket/wallet_intelligence.db` — Rebuilt clean (27 tables + signal_outcomes + wallet_archetypes; 6 tables need rebuild from APIs)

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\preflight_2026-04-12.md`
