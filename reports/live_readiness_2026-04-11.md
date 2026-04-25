# Live Readiness Report — 2026-04-11

Follow-up to `recovery_report_2026-04-11.md`. That session fixed the infrastructure; this session added the evaluation, sizing, and safety layer needed to flip to live. Two signal types now pass every live-readiness gate — `accumulation` and `market_maker_flip` — and `oversized_bet` is one category away.

## Infrastructure Status

| Item | Status | Notes |
|---|---|---|
| `signal_outcomes` table | **Built** | 22 columns, `UNIQUE(condition_id, signal_type, fired_at)`. Indexes on signal_type, condition_id, category, resolution_price, paper_trade_id. |
| Backfill from `market_signals` | **Done** | 9,714 rows backfilled. |
| Resolution attach (paper trades) | **Done** | 106 paper trades with `exit_reason='resolution'` mapped to their originating signals. 14 additional linked by `(condition_id, signal_type, ±300s)` proximity after the paper-trade-id link was missing. |
| Resolution attach (gamma CSV) | **Done** | `SignalResolver.run()` matched 1,239 more signals against `gamma_resolution.csv` (14,945 condition_ids available). Total resolved: **1,347 of 9,714** (13.9%). |
| `SignalResolver` module | **New** | `src/trading_platform/polymarket/signal_resolver.py`. Reads CSV, updates `signal_outcomes`, mirrors resolutions into any linked `polymarket_paper_trades` row that's still open. Idempotent. |
| `_record_signal_outcome` wired into `_fire_signal` | **Done** | `whale_signal_engine.py:713-717` calls `_record_signal_outcome()` after every `insert_signal()`. Non-fatal — wrapped in try/except so a recording failure never blocks signal firing. Paper-trade-id link propagated from the existing `market_signals.UPDATE...executed=1` block at line 757. |
| `SignalPerformanceCalculator` | **New** | `src/trading_platform/polymarket/signal_performance.py`. Computes EV, win rate, one-sample t-statistic, p-value, per-category breakdown, per-confidence-bucket breakdown, live-readiness gates. |
| `/api/signals/performance` | **Wired** | Now reads from `SignalPerformanceCalculator.compute_all()`. Falls back to legacy `reader.read_signals_performance()` if `signal_outcomes` is missing or empty. |
| `/api/signals/sizing` | **New** | Serves `KellySizer.get_sizing_report()` — per-signal Kelly fraction, recommended USD size at confidence 0.5/0.7/0.9, and `would_trade` boolean. |
| `KillSwitch` | **Wired** | Existing `kill_switch.py` was unused. Now checked inside `polymarket_paper_executor.execute_signal()` (line 356-371) — emergency stop file blocks every paper + live trade. New API endpoints: `GET /api/system/kill-switch` (status + config), `POST /api/system/kill-switch/trip` (activate with reason), `POST /api/system/kill-switch/reset` (clear). End-to-end test: tripped → executor returned None for a valid whale_entry → cleared. |
| `KellySizer` | **Refactored + wired** | Now reads from `signal_outcomes` instead of `polymarket_paper_trades`. Fallback to old path if `signal_outcomes` missing. Added `get_sizing_report()` method returning per-type recommendations. Negative edge → returns 0 USD (caller skips). Not wired into the paper executor because the executor's own profile-Kelly is fine for paper data collection; `KellySizer` is the canonical source for the *live* executor. |
| `/api/wallets/political-leaders` | **New** | Top tier S/A/B wallets in politics + geopolitics with 7-day activity count and open markets. |
| `/api/signals/political-performance` | **New** | `SignalPerformanceCalculator` output filtered to politics + geopolitics. |
| `/api/markets/political-activity` | **New** | Recent whale trades in political/geo markets, joined against `wallet_category_profiles` for tier + WR annotation. |
| Telegram political whale alert | **New** | `TelegramAlerter.send_political_whale()`. Loud for S/A tier, silent for B, skipped below. Wired into `_fire_signal` after the existing `send_whale_detection` / `send_signal` calls. Queries `wallet_category_profiles` to look up tier + WR + 30d PnL for the firing wallet. Non-blocking. |

## Signal Performance (from signal_outcomes)

| signal_type | fired | resolved | wins | WR | avg EV | p-value | **Live ready?** |
|---|---:|---:|---:|---:|---:|---:|---|
| price_velocity | 8,723 | 1,140 | 393 | 0.345 | −0.644 | 1.000 | ❌ disabled, no edge |
| whale_entry | 520 | 87 | 43 | 0.494 | −0.205 | 1.000 | ❌ no edge on current sample |
| oversized_bet | 95 | 47 | 40 | **0.851** | **+0.367** | **0.000** | ⚠️ only 1 category |
| **accumulation** | 133 | 34 | 28 | **0.824** | **+0.309** | **0.000** | ✅ **ALL GATES PASS** |
| **market_maker_flip** | 140 | 27 | 16 | **0.593** | **+0.138** | **0.097** | ✅ **ALL GATES PASS** |
| specialist_entry | 34 | 6 | 2 | 0.333 | −0.001 | 1.000 | ❌ n < 20 |
| wallet_reversal | 67 | 6 | 4 | 0.667 | −0.045 | 1.000 | ❌ n < 20 |
| no_position_entry | 2 | 0 | — | — | — | — | ❌ n=0 |

**Two signal types pass every gate** (sample ≥20, EV > 0, p < 0.15, WR > 0.52, positive EV in at least 2 categories). `oversized_bet` would also pass but all 47 of its resolved signals concentrate in a single category — fix by forcing category diversification in the alpha gate, or drop the multi-category gate for signals with n ≥ 40 and p < 0.001.

### Political & Geopolitical Breakdown

`/api/signals/political-performance` returns this filtered view:

| signal_type | category | n | wins | avg EV |
|---|---|---:|---:|---:|
| price_velocity | geopolitics | 259 | 204 | −0.150 |
| **accumulation** | **geopolitics** | **22** | **20 (91%)** | **+0.409** |
| **market_maker_flip** | **geopolitics** | **11** | **10 (91%)** | **+0.497** |
| whale_entry | geopolitics | 7 | 3 | −0.226 |
| price_velocity | politics | 6 | 2 | −0.495 |
| market_maker_flip | politics | 2 | 0 | −0.500 |
| oversized_bet | geopolitics | 1 | 0 | −0.200 |

**Geopolitics is where the alpha lives.** 22 of 34 resolved `accumulation` signals are geopolitics — 91% WR, +$0.41 avg EV per dollar of entry price. The two `market_maker_flip` winners in politics are too thin to trust.

## Political/Geopolitical Intelligence

### Top 15 active political & geopolitical wallets (tier S/A/B)

| # | wallet | tier | category | score | WR | resolved | 30d PnL | trades 7d |
|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | 0x59ee6c6a56… | S | politics | 85.8 | 0.903 | 2907 | 0 | 0 (dormant 2024 retiree) |
| 2 | 0xfedc381bf3… | A | politics | 79.2 | 0.611 | 537 | **+$15,245** | **7** |
| 3 | 0xf8ba34bf0e… | A | politics | 75.3 | 0.644 | 1871 | 0 | 0 (dormant) |
| 4 | 0x3cf3e8d542… | B | politics | 74.3 | 0.578 | 552 | 0 | 0 |
| 5 | 0xd42f6a1634… | A | politics | 71.9 | 0.626 | 439 | 0 | 0 |
| 6 | 0xa1d75a199e… | A | politics | 71.8 | 0.607 | 155 | 0 | 0 |
| 7 | 0x9c667a1d1c… | A | politics | 61.1 | 0.669 | 133 | +$1 | **3** |
| 8 | **0xbaa2bcb543…** | **A** | **geopolitics** | **57.4** | **0.713** | **87** | 0 | **224** |
| 9 | 0xe790b0e6bc… | B | politics | 51.5 | 0.632 | 38 | 0 | 0 |
| 10 | 0xfa21179f2c… | A | politics | 50.4 | 0.583 | 36 | −$944 | **43** |
| 11 | 0x5bffcf561b… | A | politics | 50.0 | 0.762 | 21 | −$429 | 3 |
| 12 | 0x2663daca3c… | B | politics | 49.0 | 0.941 | 17 | +$19 | 0 |
| 13 | 0x9d84ce0306… | A | geopolitics | 41.4 | 0.607 | 28 | 0 | 0 |
| 14 | 0x6d3c5bd139… | B | politics | 38.7 | 0.667 | 9 | **+$1,825** | **652** |
| 15 | 0xfa21179f2c… | B | geopolitics | 31.9 | 0.778 | 9 | 0 | 43 |

**Five wallets are live-active in politics/geo right now:**

- `0xbaa2bcb543…` — A-tier geopolitics, 224 trades in the last week. This is the single most active geopolitical whale in the DB.
- `0x6d3c5bd139…` — B-tier politics, 652 trades in 7d. High-frequency (likely arb-bot flavor), not a high-conviction signal source but worth tracking.
- `0xfedc381bf3…` — A-tier politics, 7 trades in 7d, **positive +$15k/30d** — the best "measured-alpha and currently active" political wallet.
- `0xfa21179f2c…` — A-tier politics, 43 trades/week, −$944/30d (hot hand recently gone cold).
- `0x9c667a1d1c…` — A-tier politics, 3 trades/week, neutral PnL.

The 2024-election retirees (`0x59ee6c6a56`, `0xf8ba34bf0e`, `0xa1d75a199e`) still dominate the all-time leaderboard but are not trading. The next tier-rebuild should apply a 90-day activity window to surface the current generation.

### Telegram political whale wiring

The `send_political_whale` path fires when a whale_entry (or any other signal) is created by a wallet that has an S / A / B tier row in `wallet_category_profiles` for `politics` or `geopolitics`. The tier letter is looked up inline from the same DB connection, so it adds zero latency per signal. S/A tier triggers a loud notification; B tier is silent but still sent. Tested the formatting path with a synthetic signal; message constructs cleanly under both enabled and disabled bot configurations.

## Live Trading Readiness Checklist

- [x] `signal_outcomes` built + backfilled (9,714 rows, 1,347 resolved)
- [x] `_fire_signal` records to `signal_outcomes` on every fire
- [x] `SignalResolver` auto-fills resolution data from gamma CSV
- [x] `SignalPerformanceCalculator` + `/api/signals/performance` live
- [x] `KillSwitch` wired into paper executor + API trip/reset/status
- [x] `KellySizer` reads `signal_outcomes` + `/api/signals/sizing` exposed
- [x] Political/geo endpoints live (`/api/wallets/political-leaders`, `/api/signals/political-performance`, `/api/markets/political-activity`)
- [x] Political whale Telegram path wired
- [x] **≥1 signal type with 20+ resolved + positive EV + all gates passing**: `accumulation` (n=34, WR 82%, EV +0.31, p < 0.001) and `market_maker_flip` (n=27, WR 59%, EV +0.14, p = 0.097)
- [ ] Manual "flip to live" step — `POLYMARKET_LIVE_ENABLED=1` in `.env` + initial bankroll decision
- [ ] Daily political-digest Telegram cron (Part 8 was alert-path only; the digest is noted as a scheduler task to add)
- [ ] Frontend Signal Lab page consuming `/api/signals/performance` new schema (noted for next session)

## Path to Live Trading

### Recommended first live config

- **Signal type**: `accumulation` only.
- **Categories**: `geopolitics` first, then `politics` + `geopolitics` once geopolitics accumulates 30+ resolved outcomes. The non-political accumulation sample is tiny — don't go live on that subset.
- **Starting bankroll**: **$500** per `KellySizer.BANKROLL`. This auto-caps trade size at $10 (2% of bankroll) until we grow into larger sizes.
- **Max trade size**: `KillSwitch.MAX_TRADE_USD = 100` is already the hard ceiling. With a $500 bankroll you'll never hit it.
- **Max open positions**: `KillSwitch.MAX_OPEN_POSITIONS = 10` — fine.
- **Daily loss limit**: `KillSwitch.MAX_DAILY_LOSS_PCT = 5%` of $100k BANKROLL = $5000. **Bug**: KillSwitch.BANKROLL is still hardcoded to 100,000 despite the starter bankroll being $500. Update `kill_switch.py:57` to 500 before flipping the master switch, or the daily loss gate is effectively disabled.
- **Min confidence**: 0.60 (accumulation signals have high natural confidence; don't waste fills on marginals).
- **Hold horizon**: untouched. `accumulation` average hold is short by construction (accumulation fires on fresh wallet activity, market resolves within days).

### Estimated days until `accumulation` clears preferred sample size (n ≥ 30)

- Currently at **n = 34 resolved** (`accumulation`). The preferred threshold in `KillSwitch.PREFERRED_MIN_RESOLVED = 30` is already cleared.
- `accumulation` is currently firing ~4–6 times/day based on the last 7 days of `market_signals`. Of those, historical resolution rate is ~25% (34 resolved / 133 fired). At 5 fires/day and 25% resolution, we pick up ~1.25 resolved/day.
- To hit `n = 50` (another ~16 resolved), we're looking at **~13 days**. `market_maker_flip` will clear n ≥ 30 at a similar pace.

### Flip procedure

1. **Update `kill_switch.py:57`**: `BANKROLL = 500` (not 100_000).
2. **Add a category filter to the live executor**: `if signal_type != "accumulation" or category != "geopolitics": skip`. Hard-gate for the first week.
3. **Set `POLYMARKET_LIVE_ENABLED=1`** in `.env`.
4. **Watch the first 3 fills through the kill-switch log**; validate `[KILL_SWITCH] ...` emits at every pre-trade check. Any failure returns `None` and paper-only continues.
5. **24h checkpoint**: pull `GET /api/signals/performance`, confirm `accumulation.live_ready.all_pass == true` is still true after the new live fills resolve. If any single fill loses badly, `SignalPerformanceCalculator` will re-compute on the next tick and can auto-revoke readiness.

## What Still Needs Building

1. **Daily political-digest Telegram cron** — Part 8 covered real-time alerts; the 8am digest is not yet implemented. One-line `TelegramAlerter.send_political_digest()` method + a scheduler task hitting `POST /api/alerts/send-political-digest`.
2. **Frontend `/signals` page** consuming the new `/api/signals/performance` schema. Right now it still reads the legacy `read_signals_performance()` shape. Need to add columns: resolved count, p-value, gate pass/fail, Kelly sizing.
3. **`KillSwitch.BANKROLL` hardcode fix** — currently 100,000; must drop to the real starter bankroll before flipping `POLYMARKET_LIVE_ENABLED=1`.
4. **`oversized_bet` multi-category gate workaround** — it passes every other gate and has the best raw EV (+0.37) but concentrates in one category. Either (a) loosen the gate to `OR (n ≥ 40 AND p < 0.001)`, or (b) wait for the signal to naturally diversify.
5. **Signal confidence scaling for Kelly** — currently `KellySizer.get_trade_size()` scales 0.5× → 1× over confidence, but the underlying edge computation doesn't weight wins/losses by confidence. Once sample sizes hit n ≥ 100 per type, switch to per-confidence-bucket Kelly (the `by_confidence` breakdown in `SignalPerformanceCalculator` already computes this — just needs to be read from the sizer).
6. **Automated tier rebuild on a 90-day window** — `build_wallet_category_profiles.py` is all-time, which surfaces 2024-election retirees as S-tier. Add a `--window-days` flag and wire it into the scheduler so the tier leaderboard stays current.
7. **Scheduler hook for `SignalResolver`** — the module exists; `run_daily_intelligence.py` doesn't call it yet. Add a Step 0 that invokes `SignalResolver().run()` so resolutions land before signals are re-evaluated each cycle.
8. **Correct `exit_reason` backfill** — the proximity-link step I used to attach paper trades to signals only found 14 matches because most paper trades from the pre-lock era predate the `paper_trade_id` column being wired. A more aggressive join on `(condition_id, signal_type, ±60s)` could recover more history.

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\live_readiness_2026-04-11.md`
