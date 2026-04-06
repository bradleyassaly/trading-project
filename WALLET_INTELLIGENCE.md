# Wallet Intelligence System

Detailed specification for the Polymarket wallet tracking and signal generation system.

## Overview

The wallet intelligence system tracks 16,259 Polymarket wallets, identifies top performers by directional win rate, monitors them in real time via WebSocket, and fires trading signals when watched wallets trade on monitored markets. It is the core of the Polymarket trading strategy.

## wallet_intelligence.db Schema

Central SQLite database at `data/polymarket/wallet_intelligence.db`.

### wallet_profiles

Per-wallet aggregated statistics. Rebuilt every 4 hours from wallet_trades.

| Column | Type | Description |
|--------|------|-------------|
| wallet | TEXT PK | Wallet address |
| directional_win_rate | REAL | Win rate on resolved directional positions (excl. sports, tweet_count) |
| crypto_win_rate | REAL | Win rate on crypto category only |
| politics_win_rate | REAL | Win rate on politics category only |
| resolved_trades | INTEGER | Number of resolved directional positions |
| total_volume_usdc | REAL | Total trade volume in USDC |
| wallet_type | TEXT | Classification: directional, market_maker, arb_bot, unknown |
| avg_position_size_usdc | REAL | Average absolute position size |
| equity_score | REAL | `directional_wr * log10(resolved + 1) * type_mult` |
| conviction_score | REAL | `equity_score * log10(avg_pos_size + 1)` |
| net_pnl_usdc | REAL | Net realized P&L |
| profit_factor | REAL | `total_won / total_lost` |
| volume_tier | TEXT | whale (>100K), active (>10K), casual (>1K), small |
| category_trades | TEXT | JSON dict of trade counts per category |
| last_trade_ts | INTEGER | Unix timestamp of most recent trade |
| first_seen_ts | INTEGER | Unix timestamp of earliest trade |

### wallet_trades

Individual trades. Accumulated from Data API fetch.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| wallet | TEXT | Wallet address |
| asset | TEXT | Token ID |
| side | TEXT | BUY or SELL |
| size | REAL | Trade size in USDC |
| price | REAL | Execution price |
| timestamp | INTEGER | Unix timestamp |
| category | TEXT | Market category |
| market_resolved | INTEGER | 1 if market has resolved |
| market_outcome | TEXT | YES/NO resolution |
| pnl | REAL | Realized P&L for this trade |
| transaction_hash | TEXT UNIQUE | Dedup key |

### market_signals

Signals fired by WhaleSignalEngine.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| token_id | TEXT | Condition ID or token |
| signal_type | TEXT | whale_entry or convergence |
| direction | TEXT | BUY or SELL |
| confidence | REAL | 0.0 - 1.0 |
| wallet | TEXT | Triggering wallet |
| price | REAL | Price at signal time |
| size | REAL | Trade size |
| category | TEXT | Market category |
| condition_id | TEXT | Polymarket condition ID |
| fired_at | INTEGER | Unix timestamp |
| computed_at | INTEGER | Legacy timestamp field |

### wallet_alerts

Whale activity detections.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| wallet | TEXT | Wallet address |
| token_id | TEXT | Condition ID |
| question | TEXT | Market question |
| category | TEXT | Market category |
| side | TEXT | BUY or SELL |
| size | REAL | Trade size |
| price | REAL | Price |
| tier | INTEGER | 1 or 2 |
| directional_win_rate | REAL | Wallet WR at time of alert |
| signal_fired | INTEGER | 1 if signal was generated |
| detected_at | INTEGER | Unix timestamp |

### category_performance

Per-category signal tracking.

| Column | Type | Description |
|--------|------|-------------|
| category | TEXT PK | Category name |
| signals_fired | INTEGER | Total signals fired |
| signals_resolved | INTEGER | Signals with known outcome |
| signals_won | INTEGER | Signals that were correct |
| win_rate | REAL | `signals_won / signals_resolved` |
| total_pnl | REAL | Net P&L from signals |
| last_updated | INTEGER | Unix timestamp |

## Daily Intelligence Pipeline

Runs every 4 hours. Each step must succeed before the next.

```
Step 1: data-api-fetch (5 min timeout)
  Input:  Polymarket Data API
  Output: New rows in wallet_trades
  Fail:   EXIT — do not rebuild on stale data

Step 2: wallet-profiles --from-db (10 min timeout)
  Input:  wallet_trades table
  Output: Updated wallet_profiles (directional_win_rate, conviction_score, etc.)
  Fail:   EXIT

Step 3: classify-wallet-buckets (5 min timeout)
  Input:  wallet_profiles
  Output: wallet_type updated per wallet
  Fail:   CONTINUE (non-fatal)

Step 4: refresh-universe (3 min timeout)
  Input:  Gamma API
  Output: data/polymarket/market_universe.json
  Fail:   CONTINUE (use cached)
```

## Wallet Quality Model

### Tier Assignment

| Tier | Min WR | Min Resolved | Min Volume |
|------|--------|-------------|------------|
| Tier 1 | 0.58 | 10 | $5,000 |
| Tier 2 | 0.53 | 5 | $1,000 |

Wallets not meeting tier2 criteria are not watched.

### Conviction Score

```
equity_score = directional_wr * log10(resolved_trades + 1) * type_multiplier
conviction_score = equity_score * log10(max(avg_position_size, 1) + 1)

type_multiplier:
  directional: 1.0
  all others:  0.3
```

### Directional Win Rate

Computed per wallet from resolved positions. A position is the aggregate of all trades on the same asset.

- **Win**: Net long and resolved YES, or net short and resolved NO
- **Excluded**: tweet_count and sports categories (noise)
- **Minimum**: At least 1 resolved position required (otherwise NULL)

## Category Classification

9 categories monitored. Assignment via keyword matching on market question (priority order — first match wins).

| Category | Sample Keywords |
|----------|----------------|
| politics | election, president, congress, trump, democrat, republican |
| economics | fed, cpi, gdp, jobs, inflation, fomc, unemployment |
| crypto | bitcoin, ethereum, btc, eth, solana, defi, token |
| finance | stock, sp500, nasdaq, dow, earnings, ipo |
| sports | nba, nfl, mlb, ufc, championship, playoff |
| tech | ai, openai, google, apple, microsoft, tesla |
| culture | oscar, grammy, award, movie, album, netflix |
| mentions | tweet, post, followers, views, trending |
| weather | hurricane, temperature, storm, climate, forecast |

Unmatched markets → "other" (still tracked).

## Signal Generation

### whale_entry

Fires for any tier1/tier2 wallet trade where `size >= $25`.

```
confidence = directional_win_rate * min(conviction_score / 10.0, 1.0)
if tier2: confidence *= 0.75
```

Skip if confidence < 0.01.

### convergence

After whale_entry, check market_signals for same condition_id + same direction within last 2 hours. If 2+ distinct wallets found:

```
boosted_confidence = min(base_confidence * 1.20, 1.0)
```

Both signals write to market_signals and wallet_alerts tables.

## Paper Trade Sizing

```
whale_entry tier1:  3% of bankroll max
whale_entry tier2:  1.5% of bankroll max
convergence:        5% of bankroll max

actual_stake = max_allocation * confidence
```

Skip if:
- `confidence < 0.40`
- Already have open position on same condition_id + same side
- `stake < $1`

## Live Execution Readiness

| Gate | Requirement | How Measured | Current |
|------|------------|-------------|---------|
| 1 | 50+ resolved paper trades > 55% WR | `paper_trades.db` resolved count + WR | 0 resolved |
| 2 | 2+ categories with independent edge | `category_performance` where `win_rate > 0.55 AND signals_resolved >= 20` | 0 qualifying |
| 3 | Max drawdown < 20% over 30 days | Rolling portfolio value from `portfolio` table | No data yet |
| 4 | Human approval | Manual flag | Pending |
| 5 | $500 capital, 1 category | Deployment config | Not deployed |

All gates must pass. No automation path to live — requires explicit human command.
