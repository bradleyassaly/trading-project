# System Diagnostic — 2026-04-11

## Executive Summary

**Did trades execute today?** Almost none. 5 whale_entry paper trades landed (all before 02:00 UTC), then entry stopped. Compare to 2026-04-08 which had 200+ executions across 5 signal types.

**Root cause:** SQLite **WAL-mode file lock** on `data/polymarket/wallet_intelligence.db`. Starting 2026-04-11T03:06 UTC every *new* process that tries to open the DB fails with `unable to open database file`. The only Python process with a working handle is `polymarket-live-collect` (PID 1), which opened the DB at container start 29 hours ago and has kept it open ever since. Every short-lived Docker task — `paper_check_exits`, `paper_resolutions`, `paper_equity_snapshot`, `db_backup`, and, critically, the `polymarket_paper_executor.__init__()` call from inside the signal pipeline — fails at `sqlite3.connect()`. That is why `market_signals` keeps growing (live-collect writes it through its persistent handle) but the executor never runs: `get_wallet_derived_markets failed: unable to open database file` appears on every new signal and stake comes out `$0`.

**Compounding failure:** The category enrichment lookup, which also opens the DB fresh per call, is silently degraded. 9,173 of 9,622 `market_signals` rows are tagged `category='other'`, including markets titled "US x Iran", "Trump out as President", "Russia x Ukraine ceasefire", "TISZA – Respect and Freedom Party (Hungary)", etc. So even if execution recovered, the political-wallet alpha layer would not be consulted.

**Secondary failure:** `wallet_trades` is **stale since 2026-04-08 05:34:19 UTC**. The `wallet_trade_poller.log` reports "206 new trades, 0 signals fired" every run but nothing is actually landing in `wallet_trades`. Same root cause — the poller can write (ish) but downstream category/signal enrichment can't open the DB. Without fresh whale trades, the whale-follow signal family has nothing current to act on.

**Was it fixed?** No — diagnosed only. A safe fix requires taking down `live-collect` to release the lock, followed by either (a) a SQLite checkpoint + journal truncate, or (b) moving the hot DB off the Windows bind mount. Both options are non-trivial and need user approval. See §Recommended Next Actions.

**Is the pipeline running?** Partially. Signals fire (live-collect writes via its old handle). Nothing else works: paper executor, checkpointer, exit checker, backup, resolution fetcher, equity snapshotter — all returning `unable to open database file`.

---

## Pipeline Health

| Service | Container | Status | Last good run | Evidence |
|---|---|---|---|---|
| API | `polymarket-api` | Up 29h, healthy | now | responds to `curl /api/system/status`, reads from `/tmp/wallet_intelligence_ro.db` tmpfs snapshot |
| Signal engine (WebSocket → market_signals) | `polymarket-live-collect` | Up 29h, writes OK | now | market_signals MAX(fired_at)=2026-04-11 23:55:54 UTC |
| Signal engine → paper executor call | `polymarket-live-collect` | **Broken** | 2026-04-11 02:00:13 UTC | every signal logs `get_wallet_derived_markets failed: unable to open database file`, `stake=$0`, `executed=0` |
| Scheduler | `polymarket-scheduler` | Up 29h | first failure 2026-04-11T03:06 UTC | 30-min cadence is intact, every task returns `unable to open database file` |
| paper_check_exits | scheduler task | **Broken** | 2026-04-11T02:46 UTC (27 checked, 0 exited) | `{"checked":0,"exited":0,"error":"unable to open database file"}` every run since |
| paper_resolutions | scheduler task | **Broken** | 2026-04-11T02:46 UTC | same |
| paper_equity_snapshot | scheduler task | **Broken** | 2026-04-11T02:06 UTC (equity=10908.42) | same |
| db_backup | scheduler task | **Broken** | 2026-04-10T19:56 UTC | `{"error":"unable to open database file"}` |
| Wallet trade poller | scheduler task | Degraded | pre-Apr-8 | log says "206 new trades" but wallet_trades MAX(timestamp)=2026-04-08 05:34:19 UTC |
| Watchdog | `polymarket-watchdog` | Up 29h | — | unknown, did not alert on this condition |
| Frontend | `polymarket-frontend` | Up 29h | — | serves UI from tmpfs snapshot via API |

### Data freshness

| Table | Rows | Latest | Age |
|---|---|---|---|
| `wallet_trades` | 397,962 | 2026-04-08 05:34:19 UTC | **~4 days stale** |
| `market_signals` | 9,622 | 2026-04-11 23:55:54 UTC | current |
| `polymarket_paper_trades` | 267 | 2026-04-11 02:00:13 UTC | ~22h stale |
| `wallet_profiles` | 16,252 | last_synced_ts from Apr 8 window | ~4 days stale |

### Root cause detail (evidence)

1. `ls /proc/*/fd/` inside `polymarket-live-collect` shows PID 1 (`trading-cli`) holds ≥19 open FDs to `wallet_intelligence.db` AND an open FD to `wallet_intelligence.db-wal`.
2. From every other process (scheduler container, api container, a fresh Python inside live-collect), `sqlite3.connect(p)` → `OperationalError: unable to open database file`, even though `os.access(p, R_OK|W_OK)` returns True, `os.path.getsize(p)=638,148,608`, and a plain `touch` in that directory succeeds.
3. Read-only immutable URI works: `sqlite3.connect(f'file:{p}?mode=ro&immutable=1', uri=True)` returns the 9,622 market_signals count. That confirms the file itself is intact — the lock is the only thing blocking RW opens.
4. `fcntl.flock(fd, LOCK_EX | LOCK_NB)` on the file raises `BlockingIOError` — confirming a kernel-level exclusive lock is held by another process.
5. Copying the DB to `/tmp` (tmpfs, not the Windows bind mount) and opening it there works immediately and enables WAL mode without issue. This is a known pathology with SQLite WAL on Docker Desktop / WSL2 bind-mounted Windows volumes: SHM file semantics don't survive cross-process opens cleanly.
6. The `polymarket-api` container has already adopted a workaround — `/tmp/wallet_intelligence_ro.db` + `-wal`/`-shm` sidecars — visible in its `/proc/self/fd/` listing. The scheduler tasks have not.

Code path that fails in the executor: `src/trading_platform/polymarket/polymarket_paper_executor.py:124` → `_migrate()` at line 158 → `sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=60)` on `data/kalshi/paper_trades.db`. The kalshi DB itself is fine; the failing open is the *secondary* `_wallet_db_path` (`_WALLET_DB_PATH = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"`) that the executor opens for whale attribution and whose failure is swallowed into `stake=$0`.

---

## Signal Activity (7-day trend)

Signals fired ≠ signals traded. Firing is healthy; execution collapsed on Apr 9.

```
day         signal_type        fired  executed  avg_conf
2026-04-11  price_velocity      1010        0    0.821   ← firing, nothing executes
2026-04-11  whale_entry           94        5    0.355   ← only 5 trades today, all pre-02:00 UTC
2026-04-11  wallet_reversal       19        0    0.871
2026-04-11  market_maker_flip     17        0    0.680
2026-04-11  specialist_entry      13        0    0.274
2026-04-11  accumulation           6        0    0.880
2026-04-11  oversized_bet          2        0    0.000
2026-04-10  price_velocity      1141        0    0.822
2026-04-10  whale_entry          156       23    0.379
2026-04-10  wallet_reversal       34        1    0.802
2026-04-10  market_maker_flip     51        0    0.680
2026-04-10  accumulation           9        0    0.862
2026-04-09  price_velocity      2740        0    0.814
2026-04-09  whale_entry          271        3    0.242
2026-04-08  price_velocity      3738      157    0.813   ← last good day
2026-04-08  accumulation         118       11    0.814
2026-04-08  market_maker_flip     50       26    0.700
2026-04-08  oversized_bet         83        5    0.550
```

**Trend**: signal *generation* is stable (~1,200–3,700/day, fluctuates with market volatility). Execution collapsed cleanly on **2026-04-09** for `price_velocity`/`accumulation`/`oversized_bet`/`market_maker_flip`, survived partially for `whale_entry` (different code path) until **2026-04-11 02:00 UTC**.

**Dormant vs active:**
- Active (writes still reaching market_signals): all 7 signal types.
- Actually generating paper trades: only `whale_entry`, and only until 02:00 UTC today.
- Totally silent: `cascade`, `specialist_entry` (fires but 0 executions in 7d).

---

## Political & Geopolitical Intelligence

### Coverage

- `wallet_trades` categories: sports 185,872 · crypto 68,383 · **politics 68,002** · other 35,894 · entertainment 21,441 · science 9,356 · economics 9,014. No `geopolitics` category exists in the schema — Iran, Ukraine, Hungary, etc. all bucket into `politics` or get dropped into `other`.
- `market_signals` category tagging is broken: **9,173 of 9,622** rows are `other`, including clearly political markets. Only 55 signals ever tagged `politics`, 1 tagged `geopolitics`. This is the category-enrichment DB-open failing silently.
- `wallet_category_profiles` has **2 rows total** (both `category='other'`). The tier-profile table exists but was never populated — S/A-tier lookup by category is a dead query.
- The real wallet intelligence lives in `wallet_profiles` (16,252 wallets) with `politics_win_rate`, `ev_politics`, `wallet_type`, `volume_tier`, per-bucket PnL. That's the table the executor should be reading.

### Political wallet census (from `wallet_profiles`)

| metric | count |
|---|---|
| tracked with politics_win_rate | 177 |
| politics_win_rate ≥ 0.60 | 84 |
| net_pnl_usdc > $10k | 47 |
| active (trade within 7d of latest ingest) | 79 |
| active within 30d | 118 |

### Top 10 political wallets by all-time net PnL

| # | wallet | type | tier | pol_wr | resolved | net_pnl_usdc | last trade |
|---|---|---|---|---|---|---|---|
| 1 | 0x56687bf447… | directional | whale | 0.500 | 146 | $2,788,104 | 2024-11-13 |
| 2 | 0x033a07b3de… | directional | whale | 1.000 | 108 | $2,706,076 | 2024-11-05 |
| 3 | 0xa1d75a199e… | directional | whale | 0.474 | 252 | $1,601,234 | 2024-10-31 |
| 4 | 0x1f2dd6d473… | arb_bot | whale | 0.500 | 244 | $1,513,695 | 2024-10-24 |
| 5 | 0xd235973291… | directional | whale | 1.000 | 492 | $1,472,639 | 2024-11-16 |
| 6 | 0xe9ad918c76… | directional | whale | 0.000 | 30 | $1,471,975 | 2024-11-06 |
| 7 | 0xed2239a915… | directional | whale | 0.600 | 147 | $1,258,672 | 2024-10-17 |
| 8 | 0xd0c042c08f… | directional | whale | 0.500 | 45 | $1,118,809 | 2024-11-01 |
| 9 | 0x8857837608… | directional | whale | 1.000 | 57 | $1,064,500 | 2024-11-05 |
| 10 | 0x94a428cfa4… | directional | whale | 0.200 | 59 | $1,018,303 | 2026-03-08 |

**Insight:** 9 of the top 10 are dormant — they cashed out after the US 2024 election and stopped. The historical PnL leaderboard is a stale graveyard. Only #10 has traded in the last 60 days.

### Top 10 *actively profitable* political whales (pnl_30d > 0, last trade within latest-ingest window)

| # | wallet | type | pol_wr | res | pnl_30d | pnl_7d | net_pnl | last trade |
|---|---|---|---|---|---|---|---|---|
| 1 | 0x6d3c5bd139… | arb_bot | 1.000 | 1156 | +$415,044 | +$24,647 | +$415,044 | 2026-04-07 |
| 2 | 0x8c80d213c0… | directional | 1.000 | 32 | +$305,038 | 0 | +$307,187 | 2026-04-07 |
| 3 | 0xc6587b11a2… | directional | 0.440 | 649 | +$247,925 | +$175,930 | –$1,665,412 | 2026-04-08 |
| 4 | 0x848921be10… | directional | 0.857 | 79 | +$99,563 | 0 | +$146,064 | 2026-03-29 |
| 5 | 0xde7be6d489… | directional | 0.228 | 262 | +$56,963 | +$160,758 | –$463,411 | 2026-04-08 |
| 6 | 0xc8ab97a908… | arb_bot | 0.895 | 734 | +$52,510 | +$1,717 | –$4,817 | 2026-04-08 |
| 7 | 0x35417b3d09… | directional | 0.455 | 277 | +$49,611 | –$55,173 | +$49,611 | 2026-04-05 |
| 8 | 0xa3d48312c4… | directional | 0.500 | 149 | +$43,559 | 0 | +$45,697 | 2026-04-04 |
| 9 | 0xfedc381bf3… | directional | 0.746 | 814 | +$29,844 | +$6,484 | +$169,956 | 2026-04-06 |
| 10 | 0xa8b202e6e9… | directional | 0.500 | 40 | +$22,942 | –$4,500 | +$22,942 | 2026-04-06 |

**Aggregate read:** roughly 15-20 political whales are running positive 30d PnL. The top two (`0x6d3c5bd139`, `0x8c80d213c0`) are where current political alpha lives. But several high-volume whales (#3, #5) carry massive lifetime drawdowns — recent positive 30d looks like dead-cat rather than skill.

### Recent political activity (last trades in DB, before ingest went stale)

All within ~90 minutes of 2026-04-08 05:23 UTC. Dominant story was Iran / Israel conflict + nuclear-deal markets — `0xbaa2bcb543…` was the biggest seller, closing out Iran/Israel and US-Iran nuclear deal positions. `0x72e4daa9b9…` was a buyer of "Iran x Israel/US conflict ends by April 7?" at 0.52-0.54. Nothing from Trump-admin, Ukraine, or Hungary markets in the recent window despite signals firing on all three today.

### Political signal performance

Numbers are thin because of the category-tagging breakage, but what we have:

| signal_type | fired (politics-tagged) | executed | avg_conf |
|---|---|---|---|
| whale_entry | 51 | 1 | 0.392 |
| specialist_entry | 3 | 0 | 0.337 |
| market_maker_flip | 1 | 0 | 0.680 |
| **geopolitics** (separate tag, 1 row) | 1 | 1 | — |
| **wallet_derived** (separate tag, 2 rows) | 2 | 1 | — |

With most political markets mis-tagged as `other`, this table radically understates real political signal activity. The ~9,000 rows in `other` contain the bulk of actual political signals.

---

## Critical Issues

1. **[P0] SQLite WAL lock blocks all non-persistent DB opens.** File-level `flock()` shows the DB is exclusively held; every short-lived sqlite3.connect() in any container fails since 2026-04-11T03:06 UTC. This single fault propagates into at least 6 downstream tasks (paper entry, paper exit, resolution fetch, equity snapshot, backup, category enrichment). Evidence: `logs/scheduler/paper_check_exits.log`, `logs/scheduler/paper_resolutions.log`, `logs/scheduler/db_backup.log`, live-collect container stdout. **Fix status: diagnosed, not applied.**

2. **[P0] Category enrichment silently failing.** 95.3% of today's signals get `category='other'` even for obviously political markets. This cascades into: (a) no political-specific sizing, (b) no political-wallet-tier lookup, (c) the political-signal-performance tables are garbage. Root cause is the same DB-open failure in the enrichment path. **Fix: same as #1.**

3. **[P0] `wallet_trades` ingest halted on 2026-04-08 05:34:19 UTC.** No new whale trades landing for ~4 days despite poller claiming success. Without fresh trades, `whale_entry`/`accumulation`/`wallet_reversal`/`oversized_bet` all lose their primary alpha input. **Fix: same as #1 — once the DB lock is gone, re-run `scripts/run_daily_intelligence.py` to backfill and confirm poller writes land.**

4. **[P1] Watchdog did not alert.** `polymarket-watchdog` has been up 29h and did not page on ~22 hours of broken paper execution, broken backups, and broken equity snapshots. Whatever predicate it polls is not looking at these signals, or is looking at live-collect's market_signals write-rate (which is green). **Fix: add an alert predicate for "paper_trades MAX(entry_ts) older than 2h" and "scheduler task error rate > 20%".**

5. **[P1] `wallet_category_profiles` is empty stub (2 rows).** The tier-profile scoring system is unbuilt. Every reference to S/A-tier wallets by category (in docs, in SQL, in UI) returns nothing. Use `wallet_profiles.{politics_win_rate, ev_politics, wallet_type, volume_tier}` instead, or build the profile job. **Fix: decide — delete the stub and change code to use wallet_profiles, or build a nightly job that populates wallet_category_profiles from wallet_profiles + wallet_trades aggregates.**

6. **[P2] Historical political leaderboard is a graveyard.** 9 of top 10 all-time political whales stopped trading post 2024-11. Current political alpha is concentrated in ~15-20 wallets, many of whom carry large lifetime drawdowns. Whale-follow tier thresholds tuned on 2024 election data are almost certainly mis-calibrated for 2026 conditions. **Fix: rebuild tier thresholds on 2026-only data after #1 and #3 are resolved.**

7. **[P2] `polymarket_paper_executor` swallows DB open failures into `stake=$0` instead of raising.** The failure mode "every signal fires at $0 stake with `executed=0`" is invisible unless you grep live-collect stdout for `get_wallet_derived_markets failed`. **Fix: `polymarket_paper_executor.py` should raise on secondary DB open failure and let the watchdog pick up the exception count, not continue with a zero stake.**

---

## Recommended Next Actions

### Immediate (today, needs user approval for anything destructive)

1. **Stop the executor chain gracefully and release the lock.** Cleanest safe sequence, in order:
   - `docker compose stop scheduler` (idle tasks have been failing anyway, zero cost)
   - `docker compose stop live-collect` (this releases the exclusive lock; signals stop firing for the duration of the restart)
   - From host, with no container attached: run `.venv/Scripts/python.exe -c "import sqlite3; c=sqlite3.connect('data/polymarket/wallet_intelligence.db'); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE'); c.close()"` — forces WAL checkpoint and flips out of WAL mode temporarily, removes -wal/-shm sidecars.
   - Re-enable WAL: `.venv/Scripts/python.exe -c "import sqlite3; c=sqlite3.connect('data/polymarket/wallet_intelligence.db'); c.execute('PRAGMA journal_mode=WAL'); c.close()"`.
   - `docker compose start live-collect scheduler`. Tail `logs/scheduler/paper_check_exits.log` — should show `checked=N, error=None` within one run (30 min).

2. **Backfill missing whale trades.** Once the lock is released, run `python scripts/run_daily_intelligence.py 2>&1 | tee /tmp/pipeline_output.txt`. Expect ~4 days of `wallet_trades` to arrive. Confirm `MAX(timestamp)` advances to now.

3. **Add the category-tagging sanity check.** After #1 and #2, run `SELECT category, COUNT(*) FROM market_signals WHERE fired_at > unixepoch('now','-1 hour') GROUP BY category`. If more than 50% are still `other`, the enrichment fix didn't land and this is a separate code bug.

### This week

4. **Move the hot DB off the Windows bind mount.** The root cause is Docker Desktop's SQLite WAL behaviour on Windows bind mounts. Options:
   - (preferred) Mount a named Docker volume (`volumes: - wallet_db:/app/data/polymarket`) and let the DB live on the Linux overlay filesystem. This is the *actual* fix.
   - (fallback) Apply the `/tmp/wallet_intelligence_ro.db` snapshot-to-tmpfs pattern (which the API already uses) uniformly across the scheduler tasks and remove direct RW opens from short-lived tasks.
   - Either option also eliminates the `wallet_intelligence.db.corrupt`, `.corrupt3`, `.pre_clean`, `.recovered` artefacts that are clearly evidence of prior SQLite-on-bind-mount corruption.

5. **Fix `polymarket_paper_executor` to fail loud.** File: `src/trading_platform/polymarket/polymarket_paper_executor.py:124`. Current code silently enters with `stake=$0` when secondary wallet DB open fails. Change to raise; have the signal loop count the exceptions and push to watchdog.

6. **Rewire watchdog predicates.** Add two checks: (a) `polymarket_paper_trades.MAX(entry_ts) < now - 2h` → page, (b) >20% of scheduler tasks in a 1-hour window report non-empty `error` key in their JSON log line → page. Current watchdog watches live-collect signal write rate, which was green the whole time.

7. **Decide on `wallet_category_profiles`**. Either delete the table + references and switch to `wallet_profiles` everywhere, or build a populate job. Not both.

### Architectural (needs a planning session)

8. **Execute political alpha on 2026-only data.** Don't tier wallets on 2024-election-era performance. Compute politics tier scores on a rolling 90-day window: `pnl_30d`, `pnl_90d`, `sharpe_ratio`, `pnl_consistency` columns in `wallet_profiles` are the right inputs. Top-of-list currently is `0x6d3c5bd139` (arb_bot, pol_wr=1.0, +$415k/30d, 1,156 resolved) and `0x8c80d213c0` (directional, pol_wr=1.0, +$305k/30d).

9. **Build a real `geopolitics` category.** Iran / Ukraine / Middle East / sanctions markets are all collapsed into `politics` today. The signal performance, wallet expertise, and volatility profile are materially different from domestic politics. Worth its own category for sizing and tier thresholds.

10. **Reconcile dual executor paths.** `polymarket_paper_executor` currently maintains TWO DBs (`data/kalshi/paper_trades.db` legacy + `wallet_intelligence.polymarket_paper_trades` new). Comment at the top of the file describes it as a back-compat bridge. Kill the legacy path — it's the one that does the second DB open that fails.
