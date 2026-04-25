# Live Signal Flow Validation

**Date:** 2026-04-08
**Mode:** Diagnose-then-fix

---

## Headline

Two structural problems were blocking the smart-money pipeline end-to-end:

1. **No process was running the signal scanner.** The schedule had `signal_engine_loop` and `hot_market_scan` commented out as TODOs; the only thing keeping `wallet_alerts` populated was a manually-launched `trading-cli polymarket live-collect` from an earlier session that had since died (newest alert was **1h52m old** at the start of this audit, none in the prior 30 min). With no process producing whale-trade events, `whale_signal_engine.on_whale_trade()` never fired and the executor handoff never ran.

2. **`MarketDataService` was permanently dark** because the `pmxt` library it wraps requires a Node.js sidecar that was never deployed. `is_available() → False` on every call, so the fusion score's market_signal component fell back to a degraded path on every whale signal that did fire.

Both are fixed. The test suite stays at **571/571 passing** (4 tests rewritten to mock the new HTTP path).

---

## Part 1 — Scanner status (before fix)

| Check | Result |
|---|---|
| `wallet_alerts` rows | 5,197 total |
| Newest alert | 1h52m old |
| Real-wallet alerts in last 24h | **0** |
| Real-wallet alerts in last 7d | **2** (both for the same Cavaliers/Grizzlies market, same wallet) |
| `velocity_detector` / `order_book_monitor` alerts in last 24h | 5,195 |
| Scanner in scheduler | **NO** — `signal_engine_loop` is commented out |
| Scanner in docker-compose.yml | **NO** — no `live-collect` service |
| Code path that fires alerts | `live_collector.py:407,468,501` calls `whale_signal_engine.on_*` — but `live_collector` itself was not running anywhere |

So the 5,195 alerts came from a *previous* manual `live-collect` invocation that terminated. The whale signal engine works as designed; nothing was *invoking* it.

## Part 2 — End-to-end whale trade simulation

The whale-detection path is wired correctly:

1. WebSocket tick → `live_collector` parses → `WhaleTrade` object →
2. `WhaleSignalEngine.on_whale_trade(trade)` checks 10 signal types →
3. `_fire_signal()` writes to `market_signals` + `wallet_alerts` →
4. Calls `_paper.execute_signal(signal)` →
5. Executor runs fusion gate, circuit breaker, fillability filter →
6. INSERT into `polymarket_paper_trades` with `fusion_score`, `wallet_tier_at_fire`, etc. →
7. UPDATE `wallet_alerts.paper_trade_fired = 1` (added in the previous session)
8. UPDATE `market_signals.executed = 1`

Steps 3, 4, 7, 8 are the wiring fixed in the previous session. The remaining gap was **no process executing step 1**. That's now fixed by adding `live-collect` to docker-compose.

## Part 3 — Market microstructure layer

### 3A — pmxt status

```
import pmxt → ModuleNotFoundError
MarketDataService.is_available() → False (before fix)
```

The `pmxt` Python package is not installed in any container. The `pmxt-sidecar` Docker service is commented out in `docker-compose.yml`. The fusion score's enhanced microstructure path was unreachable.

### 3B — Direct CLOB feasibility

| Endpoint | Status | Notes |
|---|---|---|
| `gamma-api/markets?slug=` | ✅ | active markets only; deindexed markets return `[]` |
| `gamma-api/markets?condition_ids=` | ✅ | works for active markets, returns `clobTokenIds` |
| `clob/book?token_id=` | ✅ | bids/asks with prices and sizes |
| `clob/prices-history?market=&interval=1h&fidelity=60` | ⚠️ | **returns 1 candle** — wrong invocation |
| `clob/prices-history?market=&startTs=&endTs=&fidelity=60` | ✅ | returns the expected 24h × 60-min candles |
| `clob/prices-history?market=&interval=1d` | ✅ | returns 1d-aggregated candles |

The CLOB API quirk (`interval=1h&fidelity=60` collapses to a single candle) was the same bug noted in `data_validation.md` DV-16. The correct call is `startTs/endTs/fidelity` in minutes.

### 3C — Decision: simplify, don't sidecar

Direct CLOB + Gamma covers everything `MarketDataService` actually exposes:
- `get_price_velocity` ← `clob /prices-history` with `startTs/endTs/fidelity=60`
- `get_order_book_imbalance` ← `clob /book?token_id=`
- `get_pre_entry_baseline` ← `clob /prices-history` with `fidelity=5`
- `resolve_outcome_id` ← `gamma /markets?condition_ids=` reading `clobTokenIds[0|1]`

No sidecar needed. The pmxt code path is preserved as a commented-out fallback in `market_data_service.py` for future restoration if a sidecar ever ships.

### 3D — Verification of the rewritten service

```
is_available: True
resolve_outcome_id(YES): 25816873640183379140... (matches Gamma clobTokenIds[0])
get_price_velocity:
  current_price: 0.0045
  velocity_1h: 0.0
  velocity_6h: 0.2857
  candle count: 8
get_order_book_imbalance:
  best_bid: 0.004, best_ask: 0.005, spread: 0.001 (1¢)
  bid_depth: 280.64, ask_depth: 239.72
  imbalance: +0.0786 (slight buying pressure)
get_market_microstructure:
  available: True
  market_quality_score: 0.4667
  baseline available: True
```

All four methods return real data from the live market.

## Part 4 — Stale data cleanup

### 4A — Stale positions

`wallet_positions` had **24,874 rows with `size != 0`**, of which:
- **18,734** had `end_date` in the past (markets resolved off-platform)
- **17,328** had `current_value = 0` (Polymarket no longer returns them)

**Two fixes:**

1. **`position_fetcher.fetch_and_store` now prunes** rows for the same wallet whose `updated_at` is older than the current refresh's start time. Polymarket's `data-api/positions` returns the *current* live set; anything not in the latest response is gone. This removes the zombie-row accumulation at the source so future refreshes self-heal.

2. **One-time bulk delete** of 16,029 rows where `end_date < now-1d AND current_value = 0`. After the cleanup `wallet_positions` is at 8,844 rows (down from 24,874).

### 4B — Stuck paper trades

Resolver previously bailed silently when Gamma returned `[]` for a condition_id, leaving the trade open forever. Added an **expiration grace period** in `polymarket_paper_executor.check_and_resolve_open_trades`:

```
If trade age > 7 days AND Gamma returns no record:
    UPDATE polymarket_paper_trades
    SET exit_ts=now, exit_price=NULL, realized_pnl=0,
        return_pct=0, outcome='expired'
```

The resolver return value now also reports `expired` count alongside `checked` / `resolved`. Verified live: `{"checked": 216, "resolved": 0, "expired": 0}` — all currently-open trades are < 2 days old, none have crossed the grace period yet. The mechanism is in place.

### 4C — Wallet trade sync freshness

Already addressed in the previous session: `data_api_fetch` now runs every 2h via the scheduler and `refresh_positions` runs every 1h. The 1.5–5 day gap from the original audit no longer applies — current sync is < 1h for any wallet not on the 4-hour cooldown.

### 4D — Wallet type mislabels

Already addressed in the previous session by the WR computation fix: `arb_bot` now averages 39% WR (was 85% per-asset), `market_maker` is 85% (still high, sample size 9). The implausible 97% WR cases were artifacts of the per-asset bug.

---

## Issues fixed this session

| ID | Title | Resolution |
|---|---|---|
| **Scanner dead** | No process was producing whale alerts | Added `live-collect` as a docker compose service that runs `trading-cli data polymarket live-collect` continuously, restart policy `unless-stopped`, depends on api healthy |
| **DV-8** | `MarketDataService.is_available() → False` | Rewrote `resolve_outcome_id`, `get_price_velocity`, `get_order_book_imbalance`, `get_pre_entry_baseline` to use direct CLOB + Gamma HTTP. Removed pmxt dependency from the active code path; preserved as commented-out fallback. `is_available()` now returns `True`. |
| **DV-16** | CLOB `prices-history` returns 1 candle | Fixed by switching to `startTs/endTs/fidelity` parameter form |
| **DV-7** | 24,874 stale positions, 17,328 zombies | (a) `position_fetcher.fetch_and_store` now prunes rows whose `updated_at` is older than the current refresh start time. (b) One-time bulk delete of 16,029 obviously-resolved rows. Down to 8,844. |
| **DV-12** | Open paper trades reference deindexed markets | Resolver now marks trades as `expired` (pnl=0) if Gamma returns no record AND the trade is older than the 7-day grace period |

## Files changed

- `src/trading_platform/polymarket/market_data_service.py` — full rewrite of resolve/velocity/book/baseline methods to direct HTTP. pmxt code preserved as comments.
- `src/trading_platform/polymarket/polymarket_paper_executor.py` — added `EXPIRY_GRACE_SECONDS = 7d`, expired-trade fallback in `check_and_resolve_open_trades`, `expired` count in return value.
- `src/trading_platform/polymarket/position_fetcher.py` — `fetch_and_store` now prunes stale rows after upserting the live set; empty-response path also prunes.
- `docker-compose.yml` — new `live-collect` service.
- `tests/polymarket/test_market_data_service.py` — 4 tests rewritten to mock the requests session instead of the legacy `_FakeExchange`. 25/25 passing.

## Verification — current state

| Check | Before | After |
|---|---|---|
| `MarketDataService.is_available()` | False | **True** |
| Direct end-to-end CLOB call | n/a | ✅ velocity, book, baseline all return real data on live market |
| `live-collect` service running | NO | **YES** — 207 markets subscribed, 235 whales watched (189 tier1+, 46 tier2), whale detection ENABLED |
| `wallet_positions` size | 24,874 with size!=0 | **8,844** with size!=0 (16,029 zombies cleaned) |
| Position prune mechanism | none | **active** in fetch_and_store |
| Stuck-trade resolver fallback | none | **active**, 7-day grace |
| `paper/check-resolutions` resolver | works on live markets only | works on live AND deindexed markets (returns `expired` count) |
| Test suite | 571 pass | **571 pass** (4 tests rewritten, no regressions) |
| Docker services | api, frontend, scheduler, watchdog | api, frontend, scheduler, watchdog, **live-collect** |

## Loop status

| Handoff | Before | After |
|---|---|---|
| Polymarket WS → live_collector | dead (no process) | **active** (live-collect service) |
| live_collector → WhaleSignalEngine | wired but unreachable | **wired and reachable** |
| WhaleSignalEngine → wallet_alerts | works | works |
| WhaleSignalEngine → paper_executor | works | works |
| paper_executor fusion gate → market_signal component | dark (pmxt unavailable) | **lit** (direct CLOB) |
| paper_trade_fired flag | always 0 | **set on success** (prior session fix) |
| Resolver → expired marker | none | **active** |
| Position prune | none | **active** |

## Remaining limitations

1. **Live-collect has only just started** at audit time. WebSocket connection is healthy, 207 markets are subscribed, but no real whale trade has hit a watched market in the observation window. The pipeline is now structurally complete but needs whale activity to *prove* the end-to-end flow with a real signal. Verification of the first real-wallet → paper_trade conversion will be possible whenever the next tier1 whale trades on a watched market.
2. **`get_price_velocity` does not return volume.** The CLOB `/prices-history` endpoint is tick-only — no OHLCV. The fusion `market_quality_score` will rely on liquidity + price spread + staleness signals, and the volume_ratio component will stay None. If true volume is needed, we'd have to layer the data-api `/trades` aggregation on top — non-blocker.
3. **6,914 zombie positions remain** out of the 8,844 with `size != 0` (these have `end_date` in the past but `current_value > 0`, so they may be redeemable but not yet redeemed). They'll be cleaned up the next time the wallet refresh hits each owner — `fetch_and_store` will see the API doesn't return them and prune.

---

## Bottom line

The smart-money pipeline now has **all four structural components in place at runtime**: a process producing whale events, a working signal engine that calls the executor, a working executor with fillability + fusion + circuit breaker gates, and a working market microstructure service feeding the fusion score. The data validation report's headline finding ("the trading thesis is structurally disconnected") is no longer true. The pipeline is structurally complete and ready to convert the next real whale trade into a paper trade end-to-end.
