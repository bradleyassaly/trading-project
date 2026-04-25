# Live Execution Path Audit — 2026-04-14

Scope: `PolymarketLiveExecutor`, `ClobClient`, `KellySizer`, `KillSwitch`,
`ExecutionGates`. Goal: verify the end-to-end path that turns a fired
signal into a real CLOB order before any real money moves.

## Summary

| Severity | Count | Status |
|---|---|---|
| 🔴 Critical (blocks correctness) | 2 | **fixed** |
| 🟡 Moderate (wrong-number risk) | 4 | flagged |
| 🟢 Cosmetic / hygiene | 3 | flagged |

The critical bugs would have caused **every SELL (NO-token) live trade to
go the wrong direction**. Caught pre-launch; fixed in this audit.

## 🔴 Critical — Fixed

### C1. Wrong-direction execution on SELL signals
**File:** `polymarket_live_executor.py:213` (before), `:234-252` (before)

**Bug:** The CLOB expects `side="BUY"` always; YES vs NO is chosen by
passing the appropriate `token_id`. The old code:
- `_resolve_token_id` always returned `tids[0]` (the YES token)
- `place_market_order` hardcoded `side="BUY"`

Result: a signal with `direction="SELL"` (intent: buy the NO token)
would execute as `BUY YES` — opposite side of the intended trade.

**Fix:**
- New `_resolve_token_ids(cid)` returns `[yes_tid, no_tid]`
- Executor now selects `tids[0]` for BUY signals, `tids[1]` for SELL
- Depth/staleness guards now run against the token we're actually buying

**Verified:** token resolver returns both IDs for real markets; routing
selects correct token based on `direction`.

### C2. Stale-price comparison used wrong frame for SELL
**File:** same

**Bug:** `entry_price` from the signal is recorded in the YES-token
reference frame. The old code compared `entry_price` directly to
`current_price` — which was fine for BUY, but on SELL (NO token) the
entry would be flipped (YES 0.34 = NO 0.66) and the 5% slippage check
was meaningless.

**Fix:** Entry price now translated to the token frame before comparing:
`entry_in_token_frame = entry_price if want_yes else (1.0 - entry_price)`.

## 🟡 Moderate Issues (flagged, not yet fixed)

### M1. No live-tradeable signal type today
`KillSwitch` requires **15+ resolved paper trades per signal_type** before
allowing a live order (`MIN_RESOLVED_HARD`). Current state:

| Signal type | Resolved | Status |
|---|---|---|
| price_velocity | 3,885 | excluded by category/type gate |
| whale_entry | 146 | EV=-0.086 → negative, Kelly 0 |
| accumulation | 59 | EV=+0.28 Kelly=0.58 ✅ only qualifier |
| oversized_bet | 32 | EV=-0.31 → negative |
| market_maker_flip | 29 | EV=-0.12 → negative |
| wallet_reversal | 10 | EV=-0.07, below threshold |
| specialist_entry | 6 | EV=+0.33 but n<10 → blocked |
| **tier_entry** | **0** | no paper resolutions yet |

Even with `POLYMARKET_LIVE_ENABLED=1`, only `accumulation` can
currently pass the kill switch — and only if the current category
(paper-exec gate: politics/geopolitics for live) also has enough
resolved accumulation trades. **Today we have 0 resolved accumulation
in politics/geopolitics.**

Implication: live trading literally cannot begin until 15+ resolved
paper trades accumulate in the specific signal_type × category combo.
At current firing rates (~228 live-candidates/30d from replay), this
takes ~2–6 weeks of market resolution after writer restart.

### M2. Kelly sizing is category-blind
**File:** `kelly_sizer.py:135` (`get_trade_size`)

`get_trade_size(signal_type, confidence)` doesn't accept a category.
Our data shows `whale_entry` is +$511K in geopolitics but -$45K in
politics (top wallets). Global signal-type Kelly averages these,
mis-sizing both directions.

**Fix (not applied):** plumb `category` through `execute()` → Kelly →
`compute_kelly(signal_type, category)` so sizing uses only same-cat
data. The method already accepts category; the live-exec caller just
doesn't pass it.

### M3. MIN_TRADE_USD override undoes Kelly's small sizing
**File:** `kelly_sizer.py:119`

```python
recommended = max(MIN_TRADE_USD, min(MAX_TRADE_USD, kelly_usd))
```
With `BANKROLL=350` and `MAX_PCT_OF_BANKROLL=0.02`, Kelly's cap is
$7 per trade. But `max(MIN_TRADE_USD=$10, ...)` bumps every trade to
$10 — that's **3% of bankroll**, over the intended 2% cap. Kelly's
caution is silently overridden.

**Fix (not applied):** either lower `MIN_TRADE_USD` to match
`MAX_PCT_OF_BANKROLL * BANKROLL`, or skip trades that Kelly sizes
below `MIN_TRADE_USD` rather than rounding up.

### M4. Depth check uses wrong token for SELL signals (partially fixed)
Both `PolymarketLiveExecutor` (fixed above) and `ExecutionGates` query
CLOB `/book` for depth. The paper-executor calling `ExecutionGates` passes
`token_id = signal.get("asset_id") or signal.get("token_id")`. If
either is the YES-token on a SELL signal, depth check is on the wrong
side. Need to audit how `asset_id`/`token_id` gets populated upstream.

## 🟢 Cosmetic / Hygiene

### H1. `kelly_sizer.py` + `kill_switch.py` used raw `sqlite3.connect` — **fixed**
Both now route through `db_connection.get_connection()`, matching the
project hard rule. Prevents the corruption pattern we hit earlier.

### H2. `WEEK1_MAX_TRADE=$25` hardcoded in executor, duplicates
`KillSwitch.MAX_TRADE_USD`. If an operator changes one they may forget
the other.

### H3. `live_trades` table has 5,986 rows from legacy test runs.
Noise in pipeline-status queries. Consider archiving pre-audit rows.

## Config Review

### Kill Switch Defaults (`kill_switch.py:49-55`)
```python
MAX_DAILY_LOSS_PCT    = 0.10  # 10% of bankroll per day
MAX_OPEN_POSITIONS    = 10
MAX_TRADE_USD         = 25    # phase 1
MIN_WIN_RATE          = 0.52
MIN_RESOLVED_HARD     = 15    # signal-type sample floor
PREFERRED_MIN_RESOLVED = 30
BANKROLL              = 350
```

### Execution Gates — Live Mode (`execution_gates.py:39-45`)
```python
max_price_move_pct    = 0.03  # abort if market moved 3% since whale
max_category_pct      = 0.25  # max 25% of bankroll in one category
max_positions         = 5     # live cap (paper has 10)
depth_multiple        = 3.0   # require 3x trade size in top 10 asks
spread_safety_factor  = 2.0   # EV must exceed 2x spread cost
```

### CLOB Client (`clob_client.py`)
- Base URL: `https://clob.polymarket.com` ✅
- Auth: API key + secret + passphrase + wallet private key
- Uses `py-clob-client` for order signing ✅
- Public read endpoints (book/midpoint) work without auth ✅
- Order type: **market order only** (`place_market_order`); no
  limit/post-only/IOC variants
- Slippage tolerance: default 2%
- Returns `OrderResult` with success, order_id, status, filled_price

**Finding:** only market orders are wired. For phase 1 with $25 trades
in thin books, a market order may fill well above mid. Consider adding
`place_limit_order` with IOC semantics for tighter slippage control.

## First Live Trade Checklist

Before flipping `DRY_RUN=False` + `POLYMARKET_LIVE_ENABLED=1`:

- [ ] **15+ resolved paper trades for the target signal type × category**
      (currently 0 for tier_entry/politics-geo; 59 for accumulation
      global but 0 in live-approved categories)
- [ ] **`accumulation` signal is the only live-eligible type today** —
      decide if that's the first-trade path
- [ ] Verify Polymarket API credentials in `.env`:
      `POLYMARKET_API_KEY`, `_SECRET`, `_PASSPHRASE`, `_PRIVATE_KEY`,
      `_WALLET_ADDRESS`
- [ ] Check wallet has USDC balance (phase 1 needs ~$50 to trade $25)
- [ ] Flip `DRY_RUN=False` in `polymarket_live_executor.py:47`
      **(in your local working copy only — don't commit)**
- [ ] Set `POLYMARKET_LIVE_ENABLED=1` in `.env`
- [ ] Manually invoke `le.test_dry_run()` and verify result
- [ ] First live trade: `$5–10`, politics or geopolitics, tier_entry
- [ ] Monitor `live_trades` table for the order_id and fill
- [ ] Monitor CLOB wallet on Polymarket UI for actual position

## What Got Done This Audit

- ✅ Read live executor + CLOB client + Kelly sizer + kill switch +
      execution gates end-to-end
- ✅ Ran dry-run simulation of both BUY and SELL signal paths
- ✅ Fixed critical wrong-direction bug (C1)
- ✅ Fixed entry-price frame bug for SELL guard (C2)
- ✅ Migrated `kelly_sizer.py` and `kill_switch.py` off raw
      `sqlite3.connect` to `db_connection.get_connection()`
- ✅ Verified token-id resolution returns both YES and NO on real markets

## Follow-ups

- M2 (Kelly category-blind) — add category plumbing in `execute()`
- M3 (MIN_TRADE_USD override) — fix sizing floor
- H2 (duplicate caps) — factor `MAX_TRADE_USD` out of both files
- Add limit-order variant to CLOB client for slippage control
- Audit `asset_id` vs `token_id` upstream population
