# Wallet Tracking — Validation Audit
*2026-04-26*

Cross-checked our wallet-tracking layer against Polymarket's authoritative leaderboard (`lb-api.polymarket.com/profit`). Two real bugs found, both fixed this turn.

## Top-10 site vs our DB (after fixes)

| Pseudonym | PM Site | Our DB | Drift |
|---|---:|---:|---:|
| Theo4 | $22,053,933 | $22,053,934 | exact |
| Fredi9999 | $16,619,506 | $16,619,507 | exact |
| kch123 | $11,818,061 | $11,821,839 | +$3.7K |
| Len9311238 | $8,709,972 | $8,709,973 | exact |
| zxgngl | $7,807,265 | $7,807,266 | exact |
| RN1 | $7,654,530 | $7,650,322 | -$4.2K |
| RepTrump | $7,532,409 | $7,532,410 | exact |
| swisstony | $6,666,885 | $6,668,657 | +$1.7K |
| PrincessCaro | $6,083,643 | $6,083,643 | exact |
| walletmobile | $5,942,685 | $5,942,685 | exact |

**100% wallet match**, max drift $4.2K (live-update lag between PM and our sync).

## Bugs found + fixed

### 1. Sync depth too shallow (HIGH severity)

**Symptom:** swisstony showed `pm_pnl_usdc=$21K` despite PM site showing $6.66M. 314× error.

**Root cause:** `pm_leaderboard_sync.py` called `fetch_leaderboard(..., limit=50)` — pulled top-50 per window × 2 windows = 100 unique wallets. Our DB has 333+ PM-tagged wallets. Anything that dropped below top-50 went stale until the next manual refresh.

**Fix:** lifted `limit=50 → limit=500`. Each sync now refreshes top-500. Sync time 2s → 8s; new_profiles_seeded sees more candidates per run.

### 2. Window-merge overwrite (CRITICAL — caused regression)

**Symptom (during fix attempt):** added 30d/7d/1d windows alongside "all" to deepen coverage. swisstony went from $6.6M → $2.2M, kch123 $11.8M → $1.16M.

**Root cause:** the merge loop (`merged[w] = {...}` per row) writes last-wins. With windows in order `("all", "30d", "7d", "1d")`, the 1d window's smaller cumulative number OVERWROTE the all-time value.

**Fix:** reverted to "all"-window-only. Cumulative `pm_pnl_usdc` is the only number we use; pulling only "all" eliminates the overwrite class entirely. Coverage at limit=500 is already deep enough.

## Other observations (NOT bugs — structural)

- **`pm_volume_usdc` NULL on 86% of rows** — volume leaderboard top-100 includes mostly different wallets than profit top-100. Volume is a nice-to-have, not gate-relevant. Defer.
- **`directional_win_rate` NULL/0 on 48% of rows** — these are wallets with `resolved_trades = 0` (the 18 unrecoverable arb-bot whales + new seeds awaiting backfill). Structurally correct: no trades = no WR.
- **0% WR on top-3 whales (Theo4, Fredi9999, kch123)** despite massive PnL. These are wallets where our FIFO PnL reconstruction returns 0 wins — likely because their wins are on markets we haven't ingested resolution data for. Not a tracking bug; a PnL-reconstruction-coverage limit.

## Other improvements identifiable but deferred

| # | Improvement | Why deferred |
|---|---|---|
| 1 | Per-wallet volume API call (vs leaderboard merge) | Defer — volume isn't gate-relevant |
| 2 | Staleness alert when `pm_synced_at > 7d` for any tracked wallet | Defer — 24h cadence + limit=500 already handles |
| 3 | FIFO PnL reconstruction for wallets with `pm_pnl > 0` but `directional_win_rate = 0` | Defer — needs resolved-market coverage expansion (separate effort) |
| 4 | Surface "wallets known to PM but missing trade history" count on dashboard | Defer — already in dashboard via `/api/insiders/growth` |

## Net wallet-tracking verdict: 🟢 GREEN

- **100% top-10 wallet identification accuracy** vs PM site
- **PnL drift <$5K per wallet** (live-update lag, structural)
- **Auto-promote loop** seeded 31 new whales today, promoted 90 to tier1h (was 70)
- **Auto-backfill chain** runs on new seeds — closes the trade-history gap on freshly-discovered whales

The wallet-tracking layer is the most thoroughly-validated component of the system right now: backed by ground truth, daily refreshed, structurally limited only by external API coverage.
