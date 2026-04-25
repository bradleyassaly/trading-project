# Data Validation Report

**Generated:** 2026-04-08
**Mode:** Diagnosis only — no fixes applied
**Targets:** 5 wallets cross-referenced against Polymarket Gamma / Data / CLOB / lb-api

---

## TL;DR

The smart-money pipeline that the entire trading thesis depends on is **structurally
disconnected from real wallet activity**. Of 235 paper trades placed and 5,197
"wallet alerts" logged, only **1 paper trade in the entire history was triggered by a
real Polymarket wallet**; the other 234 came from two technical scanners
(`velocity_detector`, `order_book_monitor`) that bypass the smart-money fusion gate
entirely. Concurrently, our leaderboard misses **6 of Polymarket's actual top 10
wallets by lifetime profit** even though we have profile rows and trade history for
all of them. PnL replication from `wallet_trades` is internally consistent (sum to
the cent), but the `directional_win_rate` advertised on each leaderboard row is
computed against a **filtered subset** that materially overstates true win rate by
20–46 points for the top conviction wallets. The reported $4.97M total realized PnL
on 15 resolved `price_velocity` paper trades is **mathematically real but not
economically realistic** — the strategy is a degenerate "buy NO at 0.001 on
unwinnable propositions" bet that wouldn't fill on a real book.

---

## Step 1 — Validation Targets

| Label | Address | Tier | Profile WR | Vol (USDC) | Net PnL | Type |
|---|---|---|---|---|---|---|
| **A** | `0xfedc381bf3fb…6398` | tier1 | 87.5% | $58.5M | $99,281 | directional |
| **B** | `0x849ccb590793…4009` | tier1 | 60.7% | $94K | $184 | directional |
| **C** | `0xed107a85a458…d2e5` | tier1 | 77.4% | $51.8M | $58,331 | arb_bot |
| **D** | `0x2d27e4d20f3b…76d8` | tier1 | 98.6% | $22.0M | $4,932 | market_maker |
| **E** | `0xa1d75a199ef0…e17e` | tier1h | 50.0% | $17.7M | $1,562,742 | directional |

**Note on `wallet_bucket`:** the schema has the column on both `leaderboard` and
`wallet_profiles` but **0 of 217 leaderboard rows and 0 of 16,259 wallet_profiles
rows have it populated**. The classification dimension this validation is supposed
to spot-check (`concentrated_whale`, `contrarian_whale`, `portfolio_diversifier`,
…) does not exist in the data. Substituted with `wallet_type`
(`directional|arb_bot|market_maker|unknown`) which has 250/16,259 ≈ 1.5% non-unknown.

---

## Step 2 — Wallet Data Accuracy vs Polymarket APIs

### 2A — Profile data

The `gamma-api/users/{wallet}` endpoint the original spec assumed **does not exist**
(returns HTTP 405). Polymarket's actual user APIs are:

- `data-api.polymarket.com/value?user={addr}` — current portfolio value (USD)
- `data-api.polymarket.com/positions?user={addr}` — open positions w/ `cashPnl`
- `data-api.polymarket.com/trades?user={addr}` — trade history (paginated)
- `lb-api.polymarket.com/profit?window=all` — global leaderboard by lifetime profit

| Wallet | Our Vol | (no comparable API field) | Our Net PnL | API cashPnl (open positions only) | API curr value | API position count |
|---|---|---|---|---|---|---|
| **A** | $58.5M | — | **$99,281** | **−$5,730** | $83,218 | 17 |
| **B** | $94K | — | **$184** | **−$64,102** | $2,495 | 500 |
| **C** | $51.8M | — | **$58,331** | **+$54,367** | $8.81M | 36 |
| **D** | $22.0M | — | **$4,932** | **+$131** | $3,486 | 29 |
| **E** | $17.7M | — | **$1,562,742** | **−$1,125** | **$0** | 12 |

**Important caveat:** Polymarket's public API does **not** expose lifetime realized
PnL — only open-position `cashPnl` (mark-to-market on currently held positions).
So a delta between our `net_pnl_usdc` and the API `cashPnl` is **not by itself a
bug**, because they measure different things. What it does mean is **we have no
external check on lifetime PnL**, only on the open-position slice.

Issues nonetheless flagged:

- 🔴 **Wallet B**: profile says lifetime PnL = **+$184**, but the wallet currently
  holds 500 open positions with initial cost basis $66.6K and current value $2.5K
  (open `cashPnl` = **−$64K**). It is implausible that a trader sitting on −$64K of
  unrealized losses has a +$184 lifetime realized PnL. Either historical realized
  trades wiped out the loss exactly, or our PnL formula is off by orders of magnitude.
- 🔴 **Wallet E**: profile says PnL = **+$1,562,742**, but the wallet currently has
  $0 portfolio value, 12 dead positions with cost basis $1,125, and **no trades
  since October 2024** (5+ months stale). The API has no way to confirm the $1.5M.
  Our number could be correct historical realized PnL, but **this wallet is dead**
  and should not be in tier1h on a live leaderboard.
- 🟡 **Wallet A**: leaderboard says volume $58.5M but our `wallet_trades` for that
  wallet sums to only **$406K** of `size*price` across 984 trades. The `total_volume_usdc`
  on the profile comes from a different aggregation (probably the leaderboard scrape's
  pseudonym field), not from the trade-level rows.

### 2B — Trade history sync freshness

| Wallet | API newest trade | Our newest trade | Sync gap | Our trade count | API sample (last 500) |
|---|---|---|---|---|---|
| A | `1775647557` | `1775517711` | **36 h** | 984 | 500 |
| B | `1775667229` | `1775442703` | **62 h** | 500 | 500 |
| C | `1775630909` | `1775491959` | **38 h** | 2,746 | 500 |
| D | `1775549867` | `1775115481` | **121 h** | 3,500 | 500 |
| E | `1730345180` | `1730345180` | dead wallet | 936 | 500 |

🔴 **Wallet sync runs ~1.5–5 days behind** for the watched wallets. The
`refresh_positions` task is scheduled hourly and completes in 6 s, but the actual
trade backfill is on a daily cadence (or longer for low-priority wallets), so the
signal engine is firing on a stale trade slice.

### 2C — Proxy vs EOA

`wallet_trades.proxy_wallet` is populated for every row (100%). The "wallet"
column in our DB is consistently the proxy wallet (the `proxyWallet` field
returned by data-api/trades). All 5 wallets resolved cleanly via
`/trades?user=`, `?maker=`, and `?taker=` (each returns the same data;
data-api uses the EOA→proxy mapping internally). **No proxy/EOA confusion in
the validation set.**

---

## Step 3 — PnL Replication

| Wallet | wallet_trades sum(pnl) | profile.net_pnl_usdc | Match? |
|---|---|---|---|
| A | $99,280.82 | $99,280.82 | ✅ exact |
| B | $183.72 | $183.72 | ✅ exact |
| C | $58,330.66 | $58,330.66 | ✅ exact |
| D | $4,932.47 | $4,932.47 | ✅ exact |
| E | $1,562,741.54 | $1,562,741.54 | ✅ exact |

**PnL replication: ✅ formula is internally consistent.** `profile.net_pnl_usdc` is
just `SUM(wallet_trades.pnl WHERE wallet=…)`, with no double-counting or buy/sell
sign error. The numbers agree to the cent.

### 🔴 Win-rate replication: NOT consistent

| Wallet | Computed wins / losses | Computed WR | Profile `directional_win_rate` | Profile `resolved_trades` | Computed resolved |
|---|---|---|---|---|---|
| A | 189 / 272 | **41.0%** | 87.5% | 32 | **461** |
| B | 110 / 107 | 50.7% | 60.7% | 28 | 217 |
| C | 98 / 17 | 85.2% | 77.4% | 31 | 115 |
| D | 179 / 5 | 97.3% | 98.6% | 70 | 184 |
| E | 108 / 73 | 59.7% | 50.0% | 20 | 181 |

🔴 **`directional_win_rate` is computed against a tiny filtered subset (typically
3–14× smaller than the actual resolved-trade pool)** and the resulting WR is
materially different from the true rate computed across the full set. The most
egregious case is **Wallet A** — our top-conviction-ranked wallet — where the
profile advertises **87.5% directional WR over 32 trades** but the actual resolved
trade set in `wallet_trades` is **461 trades with a 41.0% WR**. The signal engine
gates on the 87.5% number; reality is closer to a coin flip.

The 41% / 87.5% discrepancy is **the largest single data integrity issue in the
audit**: the entire signal engine is reading inflated WRs derived from a
non-representative subset.

---

## Step 4 — Wallet Classification

`wallet_bucket` is universally NULL. The `wallet_type` column has only 4 distinct
values (`unknown / directional / arb_bot / market_maker`) with **15,919 of 16,259
wallets (98%) classified as `unknown`**. Of the 5 validation targets:

| Wallet | Type | trades_per_market | avg_trade_usd | Unique markets | Reasonable? |
|---|---|---|---|---|---|
| A | directional | 9.3 | $413 | 106 | ⚠️ **Looks like a position_builder, not "directional"** |
| B | directional | 10.4 | $8 | 48 | ⚠️ Avg $8/trade is too small for "directional" |
| C | arb_bot | 2.5 | $18,281 | 1,101 | 🔴 **WR 85.2% is impossible for an arb bot** (real arb wins ~50%) |
| D | market_maker | 2.6 | $126 | 1,332 | 🔴 **WR 97.3% is impossible for a market maker** (real MM ~50%) |
| E | directional | 2.1 | $7,049 | 456 | ⚠️ Wallet is dead (no trades since Oct 2024) but still tier1h |

🔴 **The `wallet_type` classifier is wrong on at least 2 of 5 spot-checks.** A
wallet labelled `arb_bot` or `market_maker` should not have a 90%+ directional win
rate — that would be a free-money exploit. The labels are either applied to the
wrong wallets, or the underlying win-rate column is computed on the wrong slice
(see Step 3 finding above), or both.

🟡 **The `position_concentration` column is NULL on 4/5 targets** (only Wallet E
has 0.0). The "concentrated_whale vs portfolio_diversifier" classification the
spec asked us to validate cannot run because the underlying feature is not computed.

---

## Step 5 — Market Microstructure

### 5A — Top markets by wallet interest

The 10 highest-overlap markets in `wallet_positions`:

| Slug | Wallets w/ position |
|---|---|
| `nba-por-den-2026-04-06` | 21 |
| `btc-updown-5m-1775454900` | 20 |
| `mlb-sd-pit-2026-04-08` | 18 |
| `will-the-iranian-regime-fall-by-june-30` | 17 |
| `nhl-chi-sj-2026-04-06` | 17 |

🔴 **Two of the top three are stale**: the NBA game `por-den-2026-04-06` was played
two days ago and `btc-updown-5m-1775454900` is a 5-minute BTC up/down market that
expired well before the audit. Gamma returns empty for both slugs, yet our
`wallet_positions` table still shows 21 wallets holding open positions in
`nba-por-den-2026-04-06`, and several rows have non-zero `current_value`
(e.g. $277,806 marked as still open).

The `compute-open-positions` task does not clean up positions whose underlying
markets resolved off-platform.

### 5B — Live market data path

For the one validation market that is still active (`mlb-sd-pit-2026-04-08`):

| Probe | Result |
|---|---|
| Gamma `markets?slug=…` | ✅ vol $876,721, liquidity $44,929, active=true |
| `clob.polymarket.com/book` | ✅ 24 bids / 27 asks, best 0.52/0.53, **spread 1¢** |
| `clob.polymarket.com/prices-history` | ⚠️ **only 1 candle returned** at 60-min fidelity |

The order book is healthy, but the price-history endpoint returned a degenerate
1-candle response. Either we're requesting the wrong interval or the endpoint
needs different parameters than our wrapper passes.

### 5C — pmxt MarketDataService

```
MarketDataService.is_available() → False
```

🔴 **The pmxt-backed market data service is not initialized inside the api
container.** The fusion score's `market_signal` component depends on
`microstructure = mds.get_market_microstructure(...)`. When the service is
unavailable, that lookup silently returns `None` and the fusion score falls
back to a degraded path, which is part of why `fusion_score` is NULL on
every paper trade.

### 5D — Direct CLOB API

CLOB `book` endpoint works fine (verified above). This is a usable fallback for
market microstructure if pmxt is unavailable, but no code path currently consumes
it as a fallback.

---

## Step 6 — Signal Quality

### 6A — Resolved paper trades

| Signal type | Total | Resolved | Wins | Losses | Realized PnL |
|---|---|---|---|---|---|
| price_velocity | 192 | 15 | 11 | 4 | **+$4,974,419** |
| accumulation | 11 | 1 | 0 | 1 | −$1,800 |
| market_maker_flip | 27 | 0 | — | — | — |
| oversized_bet | 5 | 0 | — | — | — |
| **Total** | **235** | **16** | **11** | **5** | **+$4,972,619** |

The total reported realized PnL is **+$4.97M on $24K of stakes (207× return)** in 16
trades. The math is internally correct given the entry/exit prices, but the trades
themselves are **not economically realistic**:

| Trade | Side | Entry | Exit | Stake | PnL | Question |
|---|---|---|---|---|---|---|
| 106 | NO | 0.001 | 1.0 | $1,500 | **+$1,498,500** | "Will the next PM of Hungary be [obscure name]" |
| 96  | NO | 0.002 | 1.0 | $1,500 | +$748,500 | same |
| 26  | NO | 0.002 | 1.0 | $1,500 | +$748,500 | "Will Wolfgang Grozo win the 2026 Peruvian primary" |
| 75  | NO | 0.003 | 1.0 | $1,500 | +$498,500 | "Will the Fed decrease interest rates by [%]" |
| 65  | NO | 0.003 | 1.0 | $1,500 | +$498,500 | "Will Elon Musk post 160-179 tweets from [date]" |

🔴 **The strategy is buying NO tokens at 0.1–2.3¢ on long-tail propositions
("will random unknown person X win election Y") that the market correctly
prices at near-zero**, then claiming the resulting "win" when the market
resolves NO. The paper executor is computing payout as `stake / entry_price`,
which is mathematically valid but assumes infinite fillability at 0.1¢ — in
reality, no book has 1,500,000 NO shares available at 0.001 to fill a $1,500
order. **Win rate 11/16 = 68.8% is real on the simulated trades but the trades
themselves wouldn't fill on the real CLOB.**

### 🔴 Fusion scoring + tier metadata totally NULL

| Column | Non-null count |
|---|---|
| `fusion_score` | **0 / 235** |
| `wallet_tier_at_fire` | **0 / 235** |
| `wallet_bucket_at_fire` | **0 / 235** |
| `kelly_stake_pct` | **0 / 235** |

This is the same finding as the prior e2e audit's L3-D, and it has **not been
fixed by the Docker rebuild**. The reason is now visible:

```
234 of 235 paper_trades have wallet = "velocity_detector" (192) or "order_book_monitor" (42)
1   of 235 has   wallet = 0x7ea571c40408f340c1c8fc8eaacebab53c1bde7b (a real address)
```

🔴 **The fusion gate is conditional on the `signal` having wallet metadata
(`directional_win_rate`, `wallet_tier`, etc.). The technical scanners
(`velocity_detector`, `order_book_monitor`) emit signals with `wallet =
"velocity_detector"` — a string label, not an address — so the gate's wallet
lookups fail and the fusion components stay None.** The columns are then
written as NULL.

This means **the entire fusion-score / wallet-tier-multiplier / kelly-sizing
machinery is dark for 99.6% of paper trades** because those trades come from
technical scanners, not from the smart-money pipeline. The smart-money pipeline
itself is firing alerts (5,197 of them) but not generating paper trades:

### 6B — wallet_alerts → paper_trades wiring

| Field | Count |
|---|---|
| Total `wallet_alerts` rows | 5,197 |
| `signal_fired = 1` | 5,197 (100%) |
| `paper_trade_fired = 1` | **0 (0%)** |
| Distinct `wallet` values in alerts | 2 (`velocity_detector`, `order_book_monitor`) + 2 tier-1 flags |
| `wallet_alerts.tier = 1` | 2 rows |
| `wallet_alerts.tier = 2` | 5,195 rows |

🔴 **Zero wallet_alerts ever flipped `paper_trade_fired = 1`** — the bookkeeping
column that should link an alert to its resulting paper trade is unused. We
cannot trace any paper trade back to the wallet alert that triggered it because
(a) almost no paper trade was triggered by a wallet alert, and (b) the linking
column isn't being written even when one is.

### 6C — Conviction multiplier

```
paper trades with non-default kelly_stake_pct: 0
```

🔴 **`kelly_stake_pct` is NULL on every paper trade.** The conviction multiplier
the user asked us to validate isn't being computed (or isn't being persisted).

---

## Step 7 — Gap Analysis

### 7A — Wallet-data gaps

| Question | Answer |
|---|---|
| % of leaderboard wallets with complete trade history | 217/217 = 100% (every leaderboard wallet has at least 1 trade) |
| % with `directional_win_rate` populated | **73/217 = 33.6%** |
| % with `net_pnl_usdc` populated | 217/217 = 100% |
| % with `resolved_trades > 0` | 73/217 = 33.6% |
| `wallet_bucket` populated | **0/217 = 0%** |
| `wallet_profiles` total | 16,259 |
| Of which `directional_win_rate` populated | **268 / 16,259 = 1.65%** |
| Of which `net_pnl_usdc` populated | 342 / 16,259 = 2.10% |

🔴 **Two-thirds of the leaderboard rows (144 of 217) do not have a
`directional_win_rate`** — the very metric that drives conviction scoring and
tier assignment. Of all 16,259 wallet profiles in the database, only **1.65%**
have it populated.

#### Polymarket's actual top 10 vs ours

| Rank | PM Pseudonym | Address | Lifetime profit | In our leaderboard? | In wallet_profiles? | Trades in our DB |
|---|---|---|---|---|---|---|
| 1 | Theo4 | `0x56687bf447db…` | **$22.05M** | **❌ NO** | ✅ | 803 |
| 2 | Fredi9999 | `0x1f2dd6d473f3…` | $16.62M | **❌ NO** | ✅ | 1,250 |
| 3 | kch123 | `0x6a72f61820b2…` | $11.79M | ✅ | ✅ | 3,528 |
| 4 | Len9311238 | `0x78b9ac44a6d7…` | $8.71M | **❌ NO** | ✅ | 194 |
| 5 | zxgngl | `0xd235973291b2…` | $7.81M | **❌ NO** | ✅ | 500 |
| 6 | RepTrump | `0x863134d00841…` | $7.53M | ✅ | ✅ | 160 |
| 7 | RN1 | `0x2005d16a84ce…` | $7.13M | ✅ | ✅ | 4,413 |
| 8 | PrincessCaro | `0x8119010a6e58…` | $6.08M | **❌ NO** | ✅ | 376 |
| 9 | walletmobile | `0xe9ad918c7678…` | $5.94M | **❌ NO** | ✅ | 30 |
| 10 | swisstony | `0x204f72f35326…` | $5.75M | **❌ NO** | ✅ | 3,999 |

🔴 **6 of Polymarket's actual top 10 lifetime-profit wallets are missing from our
leaderboard**, even though we have `wallet_profiles` rows AND trade history for
every single one of them. The leaderboard build job is filtering them out (for an
unknown reason — possibly because their `directional_win_rate` is NULL after the
1.65%-population issue above).

Overlap with PM lb-api top-50 by profit: **18 / 50 = 36%**. Two-thirds of the
"actual" top wallets are not in our leaderboard.

### 7B — Market-data gaps

| Capability | Status |
|---|---|
| Gamma market metadata via slug | ✅ for active markets, ❌ for resolved/expired |
| Gamma market metadata via condition_id | ⚠️ returns `[]` for some active conditions in our DB |
| CLOB order book | ✅ working (tight spreads on liquid markets) |
| CLOB price-history | ⚠️ returned 1 candle on test market — wrapper params likely wrong |
| `wallet_positions` cleanup of resolved markets | 🔴 **NOT working** — 21 stale NBA positions still marked open |
| pmxt MarketDataService | 🔴 **`is_available() = False`** in api container |
| Fallback CLOB consumption when pmxt unavailable | ❌ no fallback wired |

### 7C — Resolution-data gaps

| Item | Count |
|---|---|
| Open paper trades | 219 |
| Oldest open paper trade | id=1, "Cavaliers vs. Grizzlies O/U 237.5", entry_ts ~3 days ago |
| Gamma can find oldest open trade by condition_id | ❌ **returns `[]`** — market doesn't exist in Gamma anymore |

🔴 The oldest open paper trade is in a market that no longer exists in Gamma. The
resolution checker can't resolve it because Gamma returns no record. There's no
fallback to direct chain query or to a different resolution oracle. **Some paper
trades will sit "open" forever** because their underlying market vanished from
the Gamma index.

---

## Issue Table

| ID | Severity | Layer | Title |
|---|---|---|---|
| **DV-1** | 🔴 | Wallet WR | `directional_win_rate` is computed on a 3–14× smaller filtered subset than the true resolved trade pool; Wallet A advertises 87.5% / actual is 41.0% on 461 resolved trades |
| **DV-2** | 🔴 | Signal flow | Of 235 paper trades, 234 come from technical scanners (`velocity_detector` / `order_book_monitor`), only 1 from a real wallet — the smart-money pipeline doesn't generate paper trades |
| **DV-3** | 🔴 | Signal metadata | `fusion_score`, `wallet_tier_at_fire`, `wallet_bucket_at_fire`, `kelly_stake_pct` NULL on 100% of paper trades because the technical scanners pass `wallet="velocity_detector"` (a label) to a code path that expects an address |
| **DV-4** | 🔴 | Leaderboard | 6 of Polymarket's top 10 wallets by lifetime profit (Theo4 $22M, Fredi9999 $16.6M, …) are missing from our leaderboard despite having profile rows + trade history |
| **DV-5** | 🔴 | Strategy realism | The 11/16 "winning" `price_velocity` resolved paper trades are buying NO tokens at 0.001–0.023 on degenerate long-tail markets; the +$4.97M PnL would not fill on the real CLOB |
| **DV-6** | 🔴 | wallet_alerts | `paper_trade_fired` flag is 0/5,197; we cannot trace any paper trade back to the wallet alert that should have triggered it |
| **DV-7** | 🔴 | Position cleanup | `wallet_positions` retains resolved/expired markets as open (21 wallets still hold the NBA `por-den` 4/06 game two days post-resolution) |
| **DV-8** | 🔴 | pmxt | `MarketDataService.is_available() = False` in the api container; the fusion score's market signal component is dark |
| **DV-9** | 🔴 | Wallet sync | Per-wallet trade backfill is 1.5–5 days behind data-api for active wallets |
| **DV-10** | 🔴 | Wallet classifier | `wallet_type` mislabels at least 2 of 5 spot-checks; arb_bot wallet has 85% WR, market_maker wallet has 97% WR (both physically implausible) |
| **DV-11** | 🔴 | Dead wallet in tier1h | Wallet E has zero trades since Oct 2024, $0 portfolio value, but is still in tier1h with stated +$1.56M PnL |
| **DV-12** | 🔴 | Resolution gap | Some paper trades reference condition_ids that no longer exist in Gamma; resolution checker has no fallback path |
| **DV-13** | 🔴 | Population | 144/217 leaderboard rows (66%) and 15,991/16,259 wallet_profiles (98.4%) have NULL `directional_win_rate` |
| **DV-14** | 🔴 | wallet_bucket | Column exists in leaderboard + wallet_profiles, populated on **0** rows total — the bucket-classification dimension does not exist in data |
| **DV-15** | 🟡 | Volume mismatch | Wallet A leaderboard volume $58.5M vs `SUM(wallet_trades.size*price)` = $406K — the two volume aggregations come from different sources |
| **DV-16** | 🟡 | CLOB price-history | Wrapper returns 1 candle for an active market — likely a parameter bug in the request |

---

## Data Quality Assessment

| Dataset | State |
|---|---|
| `wallet_trades` (raw fills) | ✅ **internally consistent**, PnL replication exact |
| `wallet_profiles.net_pnl_usdc` | ✅ matches `SUM(wallet_trades.pnl)` to the cent |
| `wallet_profiles.directional_win_rate` | 🔴 **wrong subset** — overstates by 20–46 points on top wallets |
| `wallet_profiles.wallet_type` | 🔴 wrong on 2/5 spot-checks; 98% NULL |
| `wallet_profiles.wallet_bucket` | 🔴 0/16,259 populated |
| `leaderboard` membership | 🔴 misses 6 of PM's top 10 by profit |
| `leaderboard.directional_win_rate` | 🔴 NULL on 144/217 (66%) |
| `wallet_positions` | 🔴 retains stale resolved positions as open |
| `polymarket_paper_trades` | 🔴 99.6% from technical scanners, 0% with fusion/tier/bucket/kelly metadata |
| `wallet_alerts` | 🔴 paper_trade_fired link is 0/5,197 |
| `signal_calibration` | 🟡 `price_velocity` LIVE with 15 sample, all 11 wins are degenerate cheap-NO bets |
| `circuit_breaker_state` | ✅ initialized correctly, just no trade events yet (tracks new resolutions only) |

---

## Recommendations (NOT applied — diagnosis only)

**Top priority — fix what makes signals trustworthy:**

1. 🔴 **Fix `directional_win_rate` to compute against the full resolved-trade pool**, not a filtered subset. The current value is the largest single source of misinformation in the system. After the fix, every tier and conviction score will need recomputation.
2. 🔴 **Wire the smart-money pipeline (wallet_alerts) into the paper executor.** Right now `velocity_detector` and `order_book_monitor` are 99.6% of paper trade flow, the wallet pipeline contributes 1 trade in history. Either set `paper_trade_fired = 1` when an alert converts, or accept that the wallet-alert path is dead and remove the dual book-keeping.
3. 🔴 **Fix the leaderboard build to include the 6 missing PM-top-10 wallets** (Theo4, Fredi9999, Len9311238, zxgngl, PrincessCaro, walletmobile, swisstony). Their `wallet_profiles` rows exist; whatever filter is excluding them is filtering on a NULL column.

**Medium priority — strategy realism:**

4. 🔴 **Reject `price_velocity` paper trades whose `entry_price` is below a fillability floor** (e.g. 0.05). Buying NO at 0.001 on "Will obscure unknown person win election X" is not a real trade — the +$4.97M PnL is fictional and is biasing every Bayesian win-rate / Kelly-fraction downstream.
5. 🔴 **Backfill `directional_win_rate` for the 144 leaderboard rows missing it** so the conviction scoring runs on a complete population.
6. 🔴 **Audit `wallet_type`** — `arb_bot` and `market_maker` labels with 85%+ WR are wrong by definition.

**Lower priority — plumbing & cleanup:**

7. 🟡 **Clean up resolved/expired markets from `wallet_positions`** (21 stale NBA positions still marked open).
8. 🟡 **Fix the resolution checker fallback** for paper trades whose condition_id has vanished from Gamma — fall back to chain query or write them off as `unresolvable`.
9. 🟡 **Initialize `MarketDataService` in the api container** (`is_available() = False` currently) so the fusion gate's market signal component lights up.
10. 🟡 **Tighten wallet sync cadence** — currently 1.5–5 days behind for active wallets; should be ≤1h for the leaderboard set.
11. 🟡 **Investigate why `wallet_bucket` is universally NULL** — the column exists in two tables, but no code path writes it. Either the bucket classifier was never wired, or it was removed.
12. 🟢 **Mark dead wallets** (no trades in N days, $0 portfolio value) and demote them out of tier1/tier1h. Wallet E is the worst offender.
13. 🟢 **Fix CLOB price-history wrapper** so it returns more than 1 candle.

---

## Bottom line

The fundamental claim of the platform — *"we identify smart-money wallets and trade
alongside them"* — is **not currently true**. The smart-money pipeline detects
events (`wallet_alerts` is filling at ~5 alerts/second) but doesn't connect them to
real trades; the trades that DO get placed come from generic technical scanners
that don't reference any wallet at all; the wallets we DO track have their win
rates computed on a subset that drastically overstates them; and the leaderboard
that should be naming our smartest tracked wallets is missing 6 of Polymarket's
actual top 10 by profit. The PnL math is internally consistent and the
infrastructure layer is healthy (post-prior-audit), but **on the level of "is the
data telling us the truth", the answer is no.**
