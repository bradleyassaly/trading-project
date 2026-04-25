# Signal Backtest Candidates — 2026-04-12

All backtests on corrected resolution data (10.6% YES rate). Out-of-sample temporal split (first 50% for identification, second 50% for EV measurement). Uncertain zone only (0.15-0.85).

## Summary Table — All Significant Results (OOS, p < 0.15)

| Signal | Config | N (OOS) | WR | EV | p-value | Verdict |
|---|---|---:|---:|---:|---:|---|
| accumulation (geo) | geo_only | 23 | **100%** | **+0.500** | <0.001 | confirms geo focus |
| late_informed | late_75 + S/A tier | 16 | **94%** | **+0.287** | <0.001 | strong but thin |
| **specialist_entry** | all | **25** | **76%** | **+0.269** | **0.0002** | **IMPLEMENT** |
| specialist_entry | geopolitics | 24 | 75% | +0.255 | 0.0004 | geo dominates |
| **late_informed** | **late_90 + large** | **173** | **73%** | **+0.227** | **<0.001** | **IMPLEMENT (best n)** |
| accumulation | base | 30 | 73% | +0.200 | 0.010 | reference |
| **tier_divergence** | **S/A** | **20** | **80%** | **+0.195** | **0.009** | **IMPLEMENT** |
| tier_divergence | S/A/B | 112 | 61% | +0.112 | 0.013 | broader but weaker |
| late_informed | late_75 + large | 255 | 64% | +0.154 | <0.001 | robust |
| late_informed | late_90 | 586 | 61% | +0.111 | <0.001 | large n, thin EV |
| multi_factor | score >= 3 | 574 | 56% | +0.060 | 0.001 | volume play |
| contrarian | all | 693 | 46% | **−0.082** | 1.000 | **DEAD** |
| contrarian | S/A tier | 6 | 17% | **−0.321** | 0.943 | **catastrophic** |

## Overlap with accumulation: ZERO

| Wallet set | Overlap with accumulation wallets |
|---|---|
| Tier S/A wallets | 0/1 (0%) |
| Specialist wallets | 0/7 (0%) |

All three implementation candidates fire on different wallets and markets than accumulation. Running them together adds signal coverage without diluting per-trade EV.

## Signal-by-Signal Analysis

### 1. Specialist Entry — **IMPLEMENT** (already exists in signal engine)

- **Mechanism**: `specialist` archetype wallets entering markets in the uncertain zone
- **OOS**: N=25, WR=76%, EV=+0.269, p=0.0002
- **Category**: 24/25 are geopolitics. This is a geopolitics specialist signal.
- **Overlap**: 0% with accumulation
- **Status**: `specialist_entry` already fires in the signal engine but is NOT in DISABLED_SIGNAL_TYPES. It just needs more resolved data to pass live-readiness gates. Current resolved: N=6 (from signal_outcomes).
- **Action**: keep it active in paper mode, monitor as resolved count grows. At N=20 resolved, re-evaluate for live.

### 2. Late Informed Entry (late_90 + large size) — **IMPLEMENT** (new signal)

- **Mechanism**: wallets entering after 90% of a market's trading lifetime with top-25% position size
- **OOS**: N=173, WR=73%, EV=+0.227, p<0.001
- **Why it works**: late entry with high conviction indicates last-minute information advantage. The timing filter excludes penny collectors (who enter early) and the size filter excludes noise trades.
- **Category**: not category-specific (works across all categories)
- **Overlap**: minimal — fires on different trade timing than accumulation
- **Action**: create `late_conviction` signal type. Paper-trade for 2 weeks, then evaluate for live.

### 3. Tier Divergence (S/A only) — **IMPLEMENT** (new signal)

- **Mechanism**: S/A tier wallets (proven edge in their category) entering uncertain-zone markets
- **OOS**: N=20, WR=80%, EV=+0.195, p=0.009
- **Note**: all 20 OOS trades are in geopolitics (same as accumulation's dominant category). Overlap risk with accumulation is through MARKETS, not wallets.
- **Overlap**: 0% wallet overlap, but potential market overlap (both fire on geopolitics)
- **Action**: create `tier_entry` signal type. Paper-trade for 2 weeks.

### 4. Contrarian — **DEAD**

Contrarians lose money in every configuration tested. Smart contrarians (S/A tier) lose EVEN MORE (−0.321 EV). The market consensus is right more often than not. **Do not implement.**

### 5. Accumulation Variants

- **geo_only**: OOS N=23, WR=100%, EV=+0.500 — confirms the crypto/entertainment/sports exclusion was correct
- **no_bots**: same as base (archetype filter doesn't change accumulation because it's market-level)
- **SA_tier**: insufficient data for separate filtering

No accumulation variant beats the base + geo exclusion that's already deployed.

### 6. Multi-Factor Scoring

Works at the aggregate level (score >= 3: N=574, EV=+0.060) but per-trade EV is too thin for practical use at our bankroll. The individual factors that drive it (tier_SA, specialist, large size) are better implemented as separate signals with their own Kelly sizing.

## Implementation Priority

| Priority | Signal | EV | N (OOS) | Complexity | Timeline |
|---:|---|---:|---:|---|---|
| 1 | **specialist_entry** | +0.269 | 25 | Easy (exists) | Already active |
| 2 | **late_conviction** | +0.227 | 173 | Medium (new) | Build + 2 weeks paper |
| 3 | **tier_entry** | +0.195 | 20 | Medium (new) | Build + 2 weeks paper |
| — | accumulation | +0.280 | 59 | — | **LIVE** |

### Expected combined signal volume

| Signal | Est. signals/week |
|---|---:|
| accumulation | 2-3 |
| specialist_entry | 1-2 |
| late_conviction | 3-5 |
| tier_entry | 1-2 |
| **Total** | **7-12** (~1-2/day) |

## Time Stability Concern

All accumulation resolved data comes from April 8-10 (3 days). The new signals draw from the broader `wallet_trades` history which spans months. `late_90_large` at N=173 is the most time-distributed. However, the OOS split is temporal within each wallet's own history — NOT across calendar time. True calendar-distributed validation requires more production time.

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\signal_backtest_candidates_2026-04-12.md`
**Script:** `scripts/signal_backtests.py`
