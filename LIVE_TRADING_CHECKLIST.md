# Live Trading Checklist

## Pre-Launch

- [ ] 70% hypothesis accuracy on 50+ trades
- [ ] WR calibration within +/- 15%
- [ ] Average detection lag < 5 min
- [ ] All execution gates tested on paper (spread, depth, staleness, exposure, drawdown)
- [ ] py-clob-client installed and configured
- [ ] CLOB API credentials derived (`client.create_or_derive_api_creds()`)
- [ ] Private key (PK) set in .env
- [ ] Starting capital deposited on Polymarket ($500)
- [ ] Circuit breaker thresholds confirmed (20% max drawdown, 5% daily)
- [ ] Kill switch tested (emergency stop + clear)
- [ ] Exit strategy documented (stop loss -30%, take profit +80%, time decay 14d)
- [ ] First 5 live trades in DRY_RUN mode (log only)

## Dry-Run Phase (BEFORE real money)

- [ ] Set `DRY_RUN=True`, `POLYMARKET_LIVE_ENABLED=1`
- [ ] Let run for 24h
- [ ] Verify dry-run trades appear in Telegram and `live_trades` table
- [ ] Verify order book is readable (`client.get_order_book(token_id)`)
- [ ] Verify limit prices are computed correctly vs actual book
- [ ] Check spread estimates vs actual book
- [ ] Review `dry_run_trades` table for signal quality
- [ ] Confirm no real orders were placed on Polymarket
- [ ] Check `stale_signals` table — how many did we miss due to gaps?

## Day 1 Live

- [ ] Set `DRY_RUN=False`
- [ ] `POLYMARKET_LIVE_ENABLED` already set from dry-run phase
- [ ] $500 capital, $50 max per trade, 5 max open positions
- [ ] Monitor first 3 trades manually in Telegram
- [ ] Check fill quality (slippage < 2%)
- [ ] Verify Telegram alerts arriving for: trade placed, trade resolved, whale exit
- [ ] Confirm execution gates logging in scheduler logs

## Week 1 Review

- [ ] Compare paper vs live fill prices (measure actual slippage)
- [ ] Check if execution gates are too tight (blocking good trades) or too loose
- [ ] Review any whale exit signals — did holding or exiting perform better?
- [ ] Verify circuit breaker hasn't triggered (if it did, investigate)
- [ ] Check daily digest for calibration accuracy
- [ ] Adjust thresholds if needed (spread, depth, staleness)

## Scaling Criteria

| Condition | Action |
|---|---|
| 10+ live trades, slippage < 2%, WR >= 60% | Increase to $1,000 capital |
| 25+ live trades, PF > 1.5, drawdown < 5% | Increase to $2,500 capital |
| 50+ live trades, Sharpe > 1.0 | Consider $5,000 capital |
| Circuit breaker triggered | STOP. Investigate. Do NOT auto-scale. |

## Gate Thresholds

| Gate | Paper | Live |
|---|---|---|
| Max price move since whale | 5% | 3% |
| Max category exposure | 30% | 25% |
| Max open positions | 10 | 5 |
| Depth multiple required | 2x | 3x |
| Spread safety factor | 1.5x | 2.0x |
