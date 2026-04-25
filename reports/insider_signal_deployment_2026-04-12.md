# Insider Signal Deployment — 2026-04-12

## What Was Built

### InsiderDetector (`insider_detector.py`)

Identifies wallets with >60% accuracy in the uncertain price zone (0.15-0.85) over 10+ resolved trades. Stores results in `insider_wallets` table. 3 wallets currently qualified:

| Wallet | Accuracy | Uncertain trades | Score | Primary category |
|---|---:|---:|---:|---|
| 0xd035fd5f4d6b... | **100%** | 28 | 0.696 | other |
| 0xa7c1f914724f... | 65.2% | 23 | 0.672 | geopolitics |
| 0x22f0872d74a5... | 65.2% | 23 | 0.546 | politics |

### insider_entry signal type

Fires when an insider-flagged wallet enters a new market in the uncertain price zone. Wired into:
- Signal engine: `_maybe_fire_insider_entry()` runs alongside `whale_entry` and `whale_entry_filtered`
- Paper executor: `SIGNAL_BANKROLL['insider_entry'] = 15_000`
- Live executor: added to `LIVE_SIGNAL_TYPES`
- Category exclusion: crypto, entertainment, sports blocked
- Election exclusion: keyword-based filter (insiders underperform on elections)

### Confidence computation

Base 0.50 + accuracy boost (linear from 0.60→0.10 to 1.00→0.50) + sample size boost (up to +0.10) + category match (+0.05) + geo/pol boost (+0.05). Capped at [0.30, 0.95].

### Telegram alerts

`send_insider_entry()` with distinctive format showing wallet accuracy, sample size, and insider score. LOUD notification for wallets with >80% accuracy.

### API endpoint

`GET /api/insiders/list` — returns all insider wallets sorted by score.

### Frontend

InsiderCard component added to the LiveTrading dashboard, showing wallet address, accuracy, trade count, primary category, and score.

### Build verified

Frontend builds clean (955KB, 0 errors). All imports resolve.

## Live Signal Configuration (Final)

| Signal | Status | EV (OOS) | p-value | Categories | Notes |
|---|---|---:|---:|---|---|
| **accumulation** | LIVE | +0.280 | <0.001 | geo priority, excl crypto/ent/sports | geo boost +0.15 |
| **insider_entry** | LIVE | +0.283 | <0.001 | pol/geo/military, excl crypto/ent/sports/elections | accuracy-weighted confidence |
| **whale_entry_filtered** | LIVE | collecting | n/a | all (excl crypto/ent/sports) | archetype filter, min sizing |
| whale_entry (raw) | DISABLED | +0.006 | 0.44 | — | monitoring only |
| market_maker_flip | DISABLED | -0.057 | — | — | negative EV |
| oversized_bet | DISABLED | -0.388 | — | — | catastrophic |
| price_velocity | DISABLED | -0.097 | — | — | confirmed broken |

## Safety Controls

All existing safety controls apply to insider_entry:
- Kill switch: $500 bankroll, $50/day loss, $25/trade week 1, 10 max positions
- Kelly sizer: starts at minimum ($10) until resolved data accumulates
- Stale guard: 15 min signal age, 5% price freshness
- Liquidity: 2x ask depth required
- Election exclusion: keyword filter on market question
- Category exclusion: crypto, entertainment, sports blocked

## Expected Signal Volume

| Signal | Est. signals/week | Basis |
|---|---:|---|
| accumulation | 2-3 | Historical fire rate on geopolitics |
| insider_entry | 1-2 | 3 insider wallets, moderate activity |
| whale_entry_filtered | 2-3 | Copyable archetypes, all categories |
| **Combined** | **5-8** | ~1+ per day average |

## Zero Overlap Confirmed

None of the 3 insider wallets appear in the accumulation signal's wallet set. The signals fire on:
- **Different wallets** (insiders vs accumulated-position wallets)
- **Different categories** (insiders strongest in politics; accumulation strongest in geopolitics)
- **Different markets** (insider wallets trade markets that accumulation wallets don't, and vice versa)

Running both signals together roughly **doubles coverage** without diluting per-trade EV.

## Monitoring Plan

- Telegram: loud alert for every insider_entry signal (especially >80% accuracy wallets)
- Daily health check: insider wallets count, recent activity
- Weekly insider rebuild: Sunday 5am (after wallet profiles and tiers)
- Dashboard: InsiderCard on Live Trading page shows wallet profiles
- Resolution tracking: insider_entry signals recorded to signal_outcomes via _record_signal_outcome()
- Resolution YES rate sanity: < 30% (gamma bug regression check)

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\insider_signal_deployment_2026-04-12.md`
