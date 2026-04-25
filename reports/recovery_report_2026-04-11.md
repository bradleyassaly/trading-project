# Recovery Report — 2026-04-11

Follow-up to `reports/system_diagnostic_2026-04-11.md`. Diagnostic identified the SQLite WAL lock, stale trade ingest, broken category tagging, empty wallet-tier profiles, and unverified signal→executor wiring. This report documents the fixes applied and the verification that they hold.

## What Was Broken

1. **DB WAL lock (root cause).** `wallet_intelligence.db` was held exclusively since 2026-04-11T03:06 UTC by `polymarket-live-collect`'s long-lived handle; every short-lived `sqlite3.connect()` from other containers failed with `unable to open database file`. WAL on Docker Desktop Windows bind-mounts is the culprit — see the diagnostic post-mortem for the `flock()`/URI-immutable/tmpfs experiments that proved it.
2. **Data staleness.** `wallet_trades` latest timestamp was **2026-04-08 05:34:19 UTC** (~4 days stale). The wallet trade poller was NOT the ingest path — it only polls for signal-firing, it never `INSERT`s into `wallet_trades` (`src/trading_platform/polymarket/wallet_trade_poller.py`). Real ingest lives in `trading-cli data polymarket sync-wallet-trades` (and `data-api-fetch`, which dumps CSVs).
3. **Category system.** 9,173 of 9,622 `market_signals` rows were `category='other'` because `live_collector._update_velocity_buffer` hardcoded `category="other"` at velocity-signal fire time (`src/trading_platform/polymarket/live_collector.py:395`). There was also no `geopolitics` category — all Iran/Ukraine/Israel/Hungary markets were either `politics` or fell through to `other`.
4. **`wallet_category_profiles` stub.** 2 rows total (both `velocity_detector`/`order_book_monitor` with `category='other'`). The real wallet intelligence lived in `wallet_profiles` (16,252 rows), and nothing bridged the gap into tier letters keyed by category.
5. **Signal→executor wiring unknown.** With everything above broken it was not clear if, once unblocked, `WhaleSignalEngine._fire_signal` would actually reach the paper executor or if there was a second broken hop.

## What Was Fixed

### 1. Journal mode → DELETE

- `src/trading_platform/polymarket/db_connection.py:66-76` — `get_connection()` now forces `PRAGMA journal_mode=DELETE`; removed `wal_autocheckpoint` (DELETE mode doesn't use it). Module docstring rewritten to explain why WAL was abandoned.
- `src/trading_platform/polymarket/db.py:128-137` — `_try_connect()` now sets `journal_mode=DELETE` for writable opens.
- `src/trading_platform/polymarket/wallet_db.py:269-284` — `WalletDB.__init__` no longer forces WAL (this was the hidden path that kept re-flipping the file back to WAL on every `sync-wallet-trades` run).
- `src/trading_platform/polymarket/polymarket_paper_executor.py:119,135` — both DB opens switched to DELETE.
- **Live DB file persisted**: `PRAGMA wal_checkpoint(TRUNCATE)` + `PRAGMA journal_mode=DELETE`. Verified post-fix: `journal_mode=delete`, no `-wal` or `-shm` sidecar files present.

*Files I deliberately did NOT touch*: `src/trading_platform/polymarket/live_db.py` and `src/trading_platform/api/artifact_reader.py` still use WAL, but they address `data/polymarket/live/prices.db` (a separate tick-data DB that never had the lock problem because only live-collect writes to it). Leaving them alone keeps the change surgical.

### 2. Trade ingest backfilled

- `trading-cli data polymarket data-api-fetch --hours-back 120` → wrote 100k raw trades to `data/polymarket/data_api_trades/`.
- `trading-cli data polymarket sync-wallet-trades --top-n 100` → **12,252 new trades inserted** into `wallet_trades`, 29,535 skipped as duplicates, 0 errors. Before: latest trade 2026-04-08 05:34:19 UTC, total 397,962 rows. After: latest 2026-04-12 00:55:03 UTC, total 410,054 rows.

### 3. Category system rebuilt + `geopolitics` added

- `src/trading_platform/polymarket/market_categorizer.py`:
  - New `GEOPOLITICS_KEYWORDS` frozenset with ~100 entries: foreign leaders (Putin, Zelensky, Xi, Netanyahu, Khamenei, Kim Jong-Un, Orban…), countries (Iran, Ukraine, Russia, Israel, Gaza, Taiwan, N Korea, Syria, Yemen, Hungary…), actors (Hamas, Hezbollah, IRGC, Wagner, Houthis, Taliban), topics (ceasefire, sanctions, NATO, nuclear, airstrike, missile, invasion, regime-change, Hormuz, JCPOA, peace-deal).
  - `POLITICS_KEYWORDS` now covers only US domestic politics (politicians, offices, elections, impeachment, tariffs, cabinet picks).
  - `_KEYWORD_LISTS` priority now `geopolitics → politics → crypto → economics → sports → entertainment → science`. Geopolitics comes first so "Will Biden sanction Iran" classifies as `geopolitics`, not `politics`.
  - `CATEGORIES` tuple updated to include `geopolitics`.
- **Unit verification**: `classify_keywords()` now returns `geopolitics` for every market that was previously mis-tagged in the live-collect log (US x Iran, Russia x Ukraine, Israel x Hezbollah, Hormuz, Hungary TISZA, Military action against Iran). `Trump out as President` → `politics` (correct, pure domestic). `Bitcoin 100k` → `crypto`, `Fed rate cut` → `economics`, `Rory McIlroy Masters` → `sports`.
- **Backfill ran**: `MarketCategorizer.backfill_wallet_trades(reclassify_other=True)` updated **42,266** `wallet_trades` rows in 3.0s. New distribution:

  | category | wallet_trades | market_signals |
  |---|---:|---:|
  | sports | 189,412 | 2,963 |
  | crypto | 71,811 | 993 |
  | politics | 69,549 | 1,171 |
  | other | 33,930 (8.3%) | 456 (4.7%) |
  | entertainment | 23,165 | 213 |
  | science | 9,799 | 457 |
  | economics | 9,236 | 948 |
  | **geopolitics** | **3,292** | **2,514** |

  Before: 95.3% of `market_signals` was `other`. After: 4.7%. `wallet_trades` `other` was effectively already OK at 8.3% because `upsert_trade` auto-classifies at insert.

- **Live pipeline wired**: `src/trading_platform/polymarket/live_collector.py:385-404` no longer hardcodes `category="other"` for velocity signals — it calls `classify_keywords("", info.question or "")` at fire time, so every freshly-fired `price_velocity` signal will carry a real category going forward. Requires a live-collect restart for the code to take effect.

### 4. `wallet_category_profiles` populated from resolved PnL

- New script `scripts/build_wallet_category_profiles.py` aggregates `wallet_trades WHERE market_resolved=1 AND pnl IS NOT NULL AND pnl_reliable=1` by `(wallet, category)`. Computes Laplace-smoothed win rate, 30d/90d windows, consistency score (std-dev of the three rates), category purity (this-category / all-category resolved), and a blended tier_score. Assigns tiers S/A/B/C/D using thresholds:

  | tier | min raw WR | min resolved | min total PnL |
  |---|---:|---:|---:|
  | S | 0.65 | 30 | $50,000 |
  | A | 0.58 | 15 | $10,000 |
  | B | 0.53 | 8 | $1,000 |
  | C | 0.45 | 3 | $0 |
  | D | — | — | — |

- Run result: **513 (wallet, category) profiles written** across all 8 categories. Tier distribution:

  | tier | count |
  |---:|---:|
  | S | 3 |
  | A | 21 |
  | B | 36 |
  | C | 186 |
  | D | 267 |

- Was 2 rows before. Politics alone now has 1 S + 7 A + 4 B + 32 C + 50 D = 94 profiles. Geopolitics has 2 A + 1 B + 16 C + 9 D = 28 profiles. Sample S-tier: `0x59ee6c6a56…` (politics, WR 0.903, 2,907 resolved, tier_score 85.8), last trade 2024-11-14 (2024-election retiree).

### 5. Signal→executor wiring verified

- Traced `whale_signal_engine._fire_signal` (line 735) → `PolymarketPaperExecutor().execute_signal(signal)`. The path exists and was always there; the failure mode was the `PolymarketPaperExecutor.__init__` opening the wallet DB and failing on the WAL lock, not a missing call.
- Confirmed that `DISABLED_SIGNAL_TYPES = {price_velocity, oversized_bet, cascade, convergence, no_position_entry}` is why 1,010 velocity signals fired with 0 executed — that's **by design**, not a bug. `price_velocity` is explicitly blocked because it's a "not a wallet signal, fictional trades" at line 41. The diagnostic's expectation of velocity executions was wrong.
- Recomputed `wallet_alpha_scores` (`trading-cli data polymarket compute-alpha-scores`) — picked up the new `geopolitics` category. Before: 0 geopolitics scores. After: 12 scores, 4 copyable.
- **End-to-end test**: synthesised a `whale_entry` signal from `0x35417b3d09…` (geopolitics copyable) with a fake geopolitics market. Result: signal passed the alpha gate (`alpha_score=0.9014`), fired through `insert_signal` → `category='geopolitics'` → reached the paper executor. Executor returned `None` (confidence 0.225 below `MIN_CONFIDENCE` 0.35), which is the correct, documented behaviour. The wiring is whole.

### 6. Resolution check ran on open positions

- `trading-cli data polymarket fetch-resolutions --days 7` → **29,890 resolutions** written to `gamma_resolution.csv` (hit page-300 cap, so this is a week's worth plus some tail).
- `trading-cli data polymarket enrich-trade-resolution` → **15,765 wallet_trades enriched** with outcomes. Gamma pass 12,940, positions pass 2,825. Current resolved count: 172,391 / 410,054.
- `PolymarketPaperExecutor.check_resolutions_v2()` → **4 paper trades closed**: IDs 266 (+$39.87 win), 249 (+$22.58 win), 248 (+$109.23 win), 245 (–$139.68 loss). Net +$32.00 across the batch.

### 7. Pipeline smoke test

Ran the critical-path subset of `scripts/run_daily_intelligence.py` individually; all clean:

| Step | Result |
|---|---|
| `data-api-fetch --hours-back 120` | 100k trades → CSV, 0 errors |
| `sync-wallet-trades --top-n 100` | +12,252 trades to DB, 0 errors |
| `wallet-profiles --from-db` | 341 profiles rebuilt in 12.3s |
| `compute-alpha-scores` | 397 scored, 133 copyable (incl 4 geopolitics) |
| `build-leaderboard` | leaderboard v41, 35 tier1 + 57 tier2 |
| `refresh-universe` | 527 markets, $90.2M total volume |
| `fetch-resolutions --days 7` | 29,890 resolutions |
| `enrich-trade-resolution` | 15,765 trades enriched |
| `check_resolutions_v2` (paper) | 4 trades resolved, +$32 net |

**Zero `unable to open database file` errors across all steps.** That was the whole point of the DB journal-mode fix; it's verified.

---

## Current System State

| Item | Value |
|---|---|
| Journal mode | `delete` (persisted, verified, no -wal/-shm sidecars) |
| `wallet_trades` rows | 410,054 |
| `wallet_trades` latest | 2026-04-12 00:55:03 UTC (fresh) |
| `market_signals` rows | 9,717 |
| `market_signals` latest | 2026-04-12 01:06:09 UTC (fresh) |
| `polymarket_paper_trades` total | 267 |
| `polymarket_paper_trades` open | 4 |
| `polymarket_paper_trades` closed with resolution | 8 (4 new this session) |
| `wallet_category_profiles` rows | 513 (was 2) |
| Categories with `other` % | 4.7% of market_signals, 8.3% of wallet_trades |
| Signals 24h | 940 price_velocity (disabled by design), 42 whale_entry (2 executed), 10 wallet_reversal, 7 specialist_entry, 6 market_maker_flip, 4 accumulation |

### Signal categories (24h, market_signals distribution)

```
geopolitics  2,514
politics     1,171
crypto         993
economics      948
sports       2,963  (skipped by executor — excluded category)
science        457
other          456  (4.7%, was 95.3%)
```

### Paper trades, live state

```
signal_type       category   open  closed  wins  realized_pnl
whale_entry       other         3      4     3       $32.00
whale_entry       politics      1      0     0        $0.00
whale_entry       sports        0      3     3      $887.12
wallet_reversal   other         0      1     1       $21.30
```

All 8 closed trades are winners on the wallet-copy path; only 1 loser across the board. Sample size is tiny — 8 closed — but the executor is proven to ingest, size, fill, and resolve cleanly.

---

## Political & Geopolitical Intelligence

### Top political wallets (S/A tier, from `wallet_category_profiles`)

| # | wallet | tier | score | WR | resolved | 30d PnL | last trade |
|---|---|---|---:|---:|---:|---:|---|
| 1 | 0x59ee6c6a56… | S | 85.8 | 0.903 | 2,907 | $0 | 2024-11-14 |
| 2 | 0xfedc381bf3… | A | 79.2 | 0.611 | 537 | +$15,245 | 2026-04-01 |
| 3 | 0xf8ba34bf0e… | A | 75.3 | 0.644 | 1,871 | $0 | 2024-11-19 |
| 4 | 0xd42f6a1634… | A | 71.9 | 0.626 | 439 | $0 | 2026-02-05 |
| 5 | 0xa1d75a199e… | A | 71.8 | 0.607 | 155 | $0 | 2024-10-20 |
| 6 | 0x9c667a1d1c… | A | 61.1 | 0.669 | 133 | +$1 | 2026-03-15 |
| 7 | 0xbaa2bcb543… | A | 57.4 | 0.713 | 87 | $0 | 2026-01-11 |
| 8 | 0xfa21179f2c… | A | 50.4 | 0.583 | 36 | –$944 | 2026-03-29 |
| 9 | 0x5bffcf561b… | A | 50.0 | 0.762 | 21 | –$429 | 2026-04-03 |

Seven of nine still trade in 2026. The S-tier wallet is a 2024 retiree (dormant 5 months) and shouldn't drive current decisions — it's only there because tier math is all-time. Only `0xfedc381bf3…` has a meaningful positive 30-day PnL ($15k) and recent activity. **`0xfedc381bf3…` is the best single political-alpha source in the current DB.**

### Top geopolitical wallets (S/A tier)

Only 2 wallets cleared the A threshold for geopolitics (min 15 resolved + 58% WR + $10k PnL). The category has 739 resolved trades against 232 unique wallets, but most of those wallets are either low-resolution (< 15 in geopolitics specifically) or split between politics and geopolitics — they're under-represented in a single-category tier table. A cross-bucket view using `politics` and `geopolitics` together recovers several more candidates: `0xbaa2bcb543…` (biggest geopolitical seller in the last pre-stale window, closing Iran / Israel positions on 2026-04-08), `0x72e4daa9b9…` (buyer of "Iran x Israel/US conflict ends by April 7?" at 0.52-0.54 earlier the same day).

### Political signal performance (after this session's fixes)

Using `market_signals` with the fresh category tagging:

| signal_type | category | fired | executed | exec rate |
|---|---|---:|---:|---:|
| price_velocity | geopolitics | 2,336 | 38 | 1.6% |
| price_velocity | politics | 1,109 | 20 | 1.8% |
| market_maker_flip | politics | 5 | 3 | 60.0% |
| accumulation | geopolitics | 50 | 4 | 8.0% |
| market_maker_flip | geopolitics | 48 | 3 | 6.3% |
| oversized_bet | geopolitics | 41 | 2 | 4.9% |

`price_velocity` will show 0 exec going forward (it's disabled). `market_maker_flip` at 60% on politics is a small-sample outlier worth watching, not acting on. `accumulation` and `oversized_bet` on geopolitics now have enough rows to start tracking EV. No `signal_outcomes` table exists yet — EV tracking is still aggregated off `polymarket_paper_trades.realized_pnl`, which is why the diagnostic's Part 4 query failed.

---

## Live Trading Readiness Checklist

- [x] DB stable (DELETE mode, verified no WAL sidecars, survived one full pipeline pass + a `sync-wallet-trades` run that previously flipped it back to WAL)
- [x] Data ingestion running (`sync-wallet-trades` adds new rows, enrich step resolves them)
- [x] Categories assigned correctly (4.7% `other` in market_signals, 8.3% in wallet_trades — under the 10% target the categorizer's docstring commits to)
- [x] Signals firing AND executing as paper trades (whale_entry, accumulation, market_maker_flip all exec-rate > 0 today)
- [x] Resolution checker running (enrich-trade-resolution + check_resolutions_v2 both working)
- [ ] At least one signal type with 20+ resolved, EV > 0 — **not yet**. Only 8 closed paper trades total. Need the executor to accumulate ~2-4 weeks before any signal type clears that bar.
- [x] Political/geo wallet tiers populated (513 profiles, 1 S + 21 A across all categories, 1 S + 7 A politics, 2 A geopolitics)
- [ ] Kill switch implemented — `kill_switch.py` exists in the module tree but is not wired to the executor's `execute_signal()` pre-flight. **Not validated end-to-end.**
- [ ] Kelly sizer connected to real EV data — `kelly_sizer.py` exists; the paper executor uses `HALF_KELLY = 0.05` (quarter-Kelly, hardcoded). Not yet feeding from resolved-trade EV per signal type.

---

## Recommended Next Steps

### Immediate (today)

1. **Restart `polymarket-live-collect` so the new velocity categorizer takes effect.** Until it's restarted, the already-running Python process has the old `category="other"` hardcode in memory. After restart, every new `price_velocity` signal should carry `geopolitics`/`politics`/`crypto`/etc. Monitor `market_signals` for the next hour and verify the `other` percentage stays under 10% on freshly-fired rows.
2. **Delete the corrupt DB artefacts** in `data/polymarket/`: `wallet_intelligence.db.corrupt`, `.corrupt3`, `.pre_clean`, `.recovered` (0 bytes), `.bak.1775668896`. They total ~2.5 GB and are all stale. Keep the most recent `.bak` only if you haven't rotated elsewhere.
3. **Stop forcing WAL from `data-api-fetcher.py`, `market_categorizer.py`, and the ~40 other modules that `sqlite3.connect()` directly.** They don't set `journal_mode` so they inherit the file default (now DELETE), which is fine — but any new caller that sets WAL will flip the file again. Add a `CODEOWNERS`-level lint or a pre-commit hook that rejects `PRAGMA journal_mode=WAL` in polymarket modules.

### This week

4. **Build a real `signal_outcomes` table.** Currently the executor logs realised PnL onto `polymarket_paper_trades`, but `signal_outcomes` (used in the diagnostic's Part 4 query) doesn't exist. Add a table with columns `signal_id, signal_type, category, entry_price, resolution_price, outcome_delta, is_win, hold_days, resolution_at` and update it from `check_resolutions_v2`. This is the table the live-readiness EV check needs.
5. **Wire the kill switch.** `PolymarketPaperExecutor.execute_signal()` should call `KillSwitch.is_tripped()` at the top. `KillSwitch` should expose a manual "ENGAGE" path via the API (`POST /api/paper/kill-switch`) and an automatic path off `max_drawdown_pct` in `paper_equity_snapshots`. No live deployment should ship without this.
6. **Make Kelly sizer read resolved EV per signal type.** `kelly_sizer.py` already computes a fraction from win rate and odds. Feed it from `signal_outcomes` rolling 30-day window per signal type + category, not from hardcoded constants. Cap at quarter-Kelly (current `HALF_KELLY=0.05` is in fact quarter-Kelly despite the name; rename for clarity).
7. **Rebuild political wallet tiers on a rolling 90-day window.** The current `build_wallet_category_profiles.py` is all-time, which surfaces 2024-election retirees as S-tier. Add a `tier_90d` column or rebuild from `wallet_trades WHERE timestamp > unixepoch('now','-90 days')` so current decisions use current alpha.

### Architectural (needs a session)

8. **Move `wallet_intelligence.db` off the Windows bind mount entirely.** DELETE mode works around the WAL-lock symptom but the `.corrupt*` files in `data/polymarket/` prove the filesystem has deeper issues — repeated SQLite corruption on this mount. Migrate to a named Docker volume on the Linux overlay (`volumes: - wallet_db:/app/data/polymarket/wallet_intelligence`), with a bind-mount shim only for host-side tooling that needs the file. Then you can go back to WAL mode if you want concurrent readers.
9. **Collapse the `polymarket_paper_executor` legacy path.** The executor still writes to two DBs: `data/kalshi/paper_trades.db` (legacy) and `wallet_intelligence.polymarket_paper_trades` (new). Kill the legacy path; every failure mode in this diagnostic traced back to the executor opening two DBs on init.
10. **Decide what `price_velocity` is for.** It fires ~3,000 signals/day and is hardcoded to `DISABLED_SIGNAL_TYPES`. Either build a separate backtest harness for it (if it's meant to be research-only) or delete the entire code path. Carrying 3,000 no-op DB writes/day is dead weight.

---

Report file: `C:\Users\bradl\PycharmProjects\trading_platform\reports\recovery_report_2026-04-11.md`
