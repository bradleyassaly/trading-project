# Live-Execution Follow-up Audits — 2026-04-14

Three follow-up audits after the main live-executor audit. Each targeted a
specific failure mode flagged in the initial write-up.

## Audit #1 — asset_id / token_id Upstream Wiring

### 🔴 Critical Bug — ExecutionGates Silently Fail-Open

**Evidence:** `whale_signal_engine.py:_fire_signal` wrote
`token_id=trade.condition_id` into `market_signals.token_id`. Downstream
code (paper exec + live exec + ExecutionGates) read `signal["token_id"]`
assuming it was a real CLOB token ID and passed it to:

```python
requests.get(f"https://clob.polymarket.com/book?token_id={cid}", ...)
```

Which returned `404 "No orderbook exists for the requested token id"`.
Every gate was wrapped in a try/except that returned `True, "..._unknown_pass"`
on failure — **fail-open by design**. So:

- `check_depth` returned `(True, "depth_unknown_pass", {})`
- `check_spread_edge` returned `(True, "spread_unknown_pass", ...)`
- `check_price_staleness` returned `(True, "price_check_failed", ...)`

Every paper trade has been passing depth/spread/staleness gates regardless
of actual market conditions for the entire lifetime of the system.

### ✅ Fixed

1. **`markets_table` schema** — added `yes_token_id` / `no_token_id`
   columns. `_extract_row` parses `clobTokenIds` from Gamma and stores
   both IDs per condition_id.

2. **New `markets_table.get_token_ids(cid)`** — O(1) local lookup.

3. **`whale_signal_engine._fire_signal`** — now looks up both token IDs
   at signal-fire time, picks the side-correct one, and stashes all
   three in the signal dict (`token_id`, `yes_token_id`, `no_token_id`).

4. **`polymarket_paper_executor.execute_signal`** — `run_all_gates`
   receives the correct side's token ID.

5. **`polymarket_live_executor`** — reads `yes_token_id` / `no_token_id`
   from the signal dict first; Gamma fallback if markets table hasn't
   cached the cid yet.

### Verification

With a real token_id, `check_depth` returned `depth_ok` with
`available_depth=$210,252` for a $25 stake (previously returned
`depth_unknown_pass`). The gate is now actually working.

### Retroactive Impact

Existing 141 paper trades were entered with gates that silently passed.
Their PnL data is still reliable for signal-type EV ranking, but the
"passed ExecutionGates" decisions reflected no real information. Going
forward, gates will actually reject thin-book or wide-spread trades.

## Audit #2 — Cost Model Accuracy

### 🟡 Moderate Bug — Entry Costs Never Applied

**Evidence:** `cost_model.py` defines `entry_cost()` and `exit_cost()`.
`polymarket_paper_executor._close_position_early()` calls `exit_cost()`
on early exits (stop_loss, take_profit, time_decay). But:

- `entry_cost()` was never called from any code path.
- Resolution exits also skip cost model (correct — no spread/slippage
  at $0/$1 resolution).
- Paper trades' `raw_entry_price`, `spread_cost`, `slippage_cost`
  columns (which exist in the schema) were always `NULL`.

**Impact:**
- Paper PnL systematically overstates real PnL by entry cost.
- Per $25 trade at entry 0.34 with default spread: ~$0.96 (2%) inflation.
- Over 141 paper trades, roughly $100-200 of "phantom" paper PnL.
- Kill switch's EV gate (`avg_return_pct > 0`) uses this inflated data —
  a signal with real-EV of -1% looks like +1% on paper.

### ✅ Fixed

`execute_signal` now applies `CostModel.entry_cost(raw, side, stake)`
before insert. The resulting `effective_price` becomes `entry_price`;
`raw_entry_price`, `spread_cost`, `slippage_cost` are recorded for
audit. Resolution exits still get zero cost (correct).

### Verification

```
Entry at 0.34 $25 YES: effective=0.353 spread=0.01 slip=0.003
Entry at 0.34 $25 NO:  effective=0.327 spread=0.01 slip=0.003
Exit @ resolution:     effective=1.0  cost=0
```

### Known Gap — Not Fixed

**Cost model has never been calibrated from real fills.** `trade_fills`
has 0 rows. The defaults (`default_spread=0.02`, 0.3%-2.5% slippage
schedule) are conservative guesses. Calibration needs either:
- Replay actual CLOB order book snapshots at historical fire times
  (we don't store these; see Audit #1 for ExecutionGates fix which
  starts capturing real spread data going forward)
- A few real live fills to compare against raw signal prices

Default values may over- or under-penalize. 2% default spread is
reasonable for mid-liquidity markets but probably too high for the
top-volume markets we actually trade.

## Audit #3 — Signal Confidence Calibration

### 🔴 Critical Mis-calibration

**Query:** bucket resolved non-velocity signal outcomes by stored
confidence, compare to actual win rate.

| confidence bin | n | wins | actual WR | avg conf | calibration err |
|---|---|---|---|---|---|
| **[0.0, 0.3)** | **34** | **23** | **67.6%** | **22.1%** | **+45.6pp** |
| [0.9, 1.0) | 1 | 0 | 0.0% | 95.0% | -95pp (n=1) |

**All 34 resolved non-velocity outcomes with confidence < 0.3 actually
won 67.6%.** Confidence under-predicts WR by 45 percentage points.

The one sample in [0.9, 1.0) is too small to conclude.

### Signal-Type Breakdown

Only `whale_entry` has ≥20 resolved non-velocity outcomes:
- n=34 resolved, ALL with confidence <0.5
- Actual WR: 68%
- Expected WR per stored confidence: ~25%

### Root Cause

`whale_signal_engine._fire_whale_entry`:
```python
confidence = wr * min(conv / 10.0, 1.0)
tier_mult = 1.1 if tier1h else 1.0 if tier1 else 0.75
confidence = min(confidence * tier_mult, 0.95)
```

`conviction_score` values observed in our data: 1.0-4.7. The `/10.0`
divisor assumes a 0-10 scale but in practice conv_scores are 1-5, so
the multiplier is 0.1-0.5. Combined with wr≈0.6, that yields
`confidence ≈ 0.03-0.30` — exactly the low-confidence band that's
actually winning 68%.

### Downstream Impact

1. **Kelly sizer `get_trade_size`** scales 0.5x at low confidence to 1x
   at high confidence:
   ```python
   scale = 0.5 + 0.5 * min(confidence / 0.8, 1.0)
   ```
   At confidence=0.25, scale = 0.66. At actual-WR-equivalent 0.68, scale
   should be 0.925. **Kelly undersizes winning trades by ~30%.**

2. **Optimal-band confidence boost** (`polymarket_paper_executor`):
   ```python
   if OPTIMAL_BAND_LOW <= ep <= OPTIMAL_BAND_HIGH:
       confidence = min(0.95, confidence * OPTIMAL_BAND_BOOST)
   ```
   Multiplies by 1.15. Helps slightly but doesn't fix the root-scale bug.

3. **Telegram alerts show `conf=25%`** for trades that will actually win
   68% of the time. Operator reads the wrong quality signal.

### Fix Not Applied (Would Require Significant Design Work)

The correct fix is either:
- **Re-scale conviction_score** to 0-10 (current data is 1-5) — changes
  the `/10.0` divisor to `/5.0`, or normalizes conv scores at write time.
- **Replace formula with a calibrated lookup** — use historical
  resolved data (same signal_type + tier + price_band) to compute a
  data-driven probability estimate.

Both approaches need more data and validation. Flagged for follow-up
after more resolutions land.

### Immediate Workaround Option

A one-line fix: divide by `5.0` instead of `10.0` in
`_fire_whale_entry`. That rescales confidence ~2x, bringing the low
band from 0.25 → 0.50, which matches the actual 68% WR better (though
still not perfectly calibrated). This was NOT applied in this audit
pending operator review because it affects Kelly sizing for every
live trade.

## Summary

| Audit | Severity | Applied | Verified |
|---|---|---|---|
| #1 token_id wiring | 🔴 Critical | ✅ | ✅ depth=$210k on real token |
| #2 entry-cost CostModel | 🟡 Moderate | ✅ | ✅ price 0.34→0.353 effective |
| #3 confidence calibration | 🔴 Critical | ⚠️ documented only | N/A |

### Combined Impact For Live Trading

**Before these audits:**
- Every signal passed depth/spread/staleness gates silently
- Paper EV inflated by 2% on every trade (hiding unprofitable signals)
- Confidence under-predicted by ~3x, causing Kelly to undersize winners

**After audits #1, #2 fixed + #3 flagged:**
- Gates now actually check CLOB book state against real tokens
- Paper PnL reflects transaction costs (numbers will look worse but be
  closer to live reality)
- Confidence miscalibration documented — need to decide between the
  2-line rescale fix vs. full data-driven recalibration

### Next Recommended Actions

1. **Apply confidence rescale** (`/10.0` → `/5.0` in `_fire_whale_entry`)
   as a simple interim fix. Two-line change. Verify via replay that it
   doesn't flood paper exec with overconfident signals.
2. **Populate `markets` table** — scheduler task `markets_refresh` runs
   every 6h; backfill existing paper-trade cids in first run so gates
   work on historical positions too.
3. **First real-live-trade calibration** — when a live order fills,
   record the actual spread between quoted mid and fill price. Use 3-5
   real fills to replace the default cost model.
