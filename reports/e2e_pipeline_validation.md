# End-to-End Pipeline Validation — Wallet Trade Poller

Generated: 2026-04-09

## Executive Summary

The whale detection → signal → paper trade → hypothesis pipeline is now
**fully operational**. Three critical bugs were found and fixed during
validation. The system went from 0 real wallet detections to 711 in a
single poll cycle.

---

## Poller Status

| Metric | Value |
|---|---|
| Running | Yes (manual; scheduler task registered for every 5 min) |
| Wallets tracked | 55 / 55 copyable |
| Trades found (last cycle) | 550 |
| Signals fired (last cycle) | 156 |

## Signal Detection

| Metric | Before | After |
|---|---|---|
| Total alerts | 8,177 | 8,886 |
| Synthetic (velocity/orderbook) | 8,175 | 8,175 |
| **Real wallet alerts** | **2** | **711** |

## Alpha Gate

| Metric | Value |
|---|---|
| Signals seen | 156 |
| Paper trades placed | 5 |
| Rejection rate | ~97% (correct — most signals lack category edge) |

## Hypotheses

| Metric | Value |
|---|---|
| Total | 5 |
| Resolved | 0 (pending) |
| Status | ACCUMULATING (need 50+ resolved for thesis verdict) |

---

## Bugs Found and Fixed

### 1. `conditionId` field mapping (CRITICAL — root cause of 0 detections)

Data API returns `conditionId` (camelCase) but code read `condition_id`.
Every trade silently dropped.

**Files**: `live_collector.py:608`, `wallet_trade_poller.py`

### 2. `ensure_schema` crash on recovered DB

`CREATE INDEX ON trade_hypotheses(resolved_at)` failed because the
column didn't exist yet on pre-migration tables. Hypothesis creation
silently failed for all paper trades.

**File**: `trade_hypotheses.py` — moved index after ALTER TABLE migration

### 3. Category name mismatch

Poller returned `science_tech`/`pop_culture` but DB uses
`science`/`entertainment`. ~20% of copyable combos affected.

**File**: `wallet_trade_poller.py:_infer_category()`

### 4. DB corruption recovery

`wallet_intelligence.db` had corrupted pages. Recovered 1.2M SQL
statements via `iterdump()`. All data preserved.

### 5. Data API 403 for urllib user-agent

Switched to `PolymarketDataApiFetcher` (uses requests.Session).

---

## Verdict: PIPELINE OPERATIONAL

The missing link — detecting which wallets trade — is now connected.
Next: accumulate 50+ resolved hypotheses to validate the thesis.
