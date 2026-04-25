# Morning Briefing — Thursday, April 09, 2026

## April 15 Countdown: 6 days until KXCPI/KXFED resolution

---

## Smart Money Alerts (Last 48h — Real Wallets)

**TIER 1 alerts:** 86 | **TIER 2 alerts:** 131 | **Total:** 217

### Top Markets by Alert Volume

| Market | Alerts | Direction | Tier |
|--------|--------|-----------|------|
| ETH above $2,100 (April) | 46 | 100% YES | Mixed |
| Michigan wins 2026 NCAA Tournament | 27 | 100% YES | Mixed |
| Connecticut wins 2026 NCAA Tournament | 25 | 100% YES | Mixed |
| BTC above $70,000 (April) | 15 | 93% YES | Mixed |
| Sarasota: Rybakov vs Damm | 19 | 89% YES | TIER 2 |

### Top Active Smart Money Wallets

| Wallet | Alerts | Domain |
|--------|--------|--------|
| 0xe9c6312464b5... | 60 | crypto (100% win rate, 40 early trades) |
| 0x507e52ef684c... | 52 | — |
| 0x68146921df11... | 23 | — |
| 0x204f72f35326... | 15 | — |
| 0x2005d16a84ce... | 15 | — |

**Note:** Last 24h only contained test wallet activity (0xaaa/0xbbb). Real smart money alerts last fired April 7 around 02:00–02:30 UTC.

---

## Resolved Markets

No newly resolved markets in the last 24–48 hours. Resolution checker last ran April 6; found 0 resolutions across 65 open trades.

**Cash balance:** $9.76 | **Open trades:** 65

---

## System Health

| Component | Status | Notes |
|-----------|--------|-------|
| Monitor collector | ⚠️ STALE | Last STATS line: April 7 02:27 UTC (~30h ago) |
| Daily refresh | ⚠️ MINIMAL | Last run April 6 — only test line logged, elapsed 0s |
| Resolution checker | ✅ OK | Ran April 6, checked 65 trades |
| Wallet profiles | ✅ OK | 16,043 wallets indexed, 206 smart money |

**Concern:** The monitor appears to have stopped producing STATS lines after April 7 02:27 UTC. The daily refresh on April 6 logged only a test step (elapsed=0s), suggesting the full refresh pipeline may not have completed. **Recommend restarting the monitor and verifying the daily refresh cron.**

---

## Smart Money Wallet Summary

- **Total wallets tracked:** 16,043
- **Smart money (is_early_informed):** 206
- **Top 5 by confidence score:**

| Wallet | Early Win Rate | Early Trades | Best Domain |
|--------|---------------|--------------|-------------|
| 0x9474...05091 | 74.2% | 62 | other |
| 0xa7c1...c0193 | 93.8% | 16 | geopolitics |
| 0x945a...a48c | 71.8% | 85 | other |
| 0x7ea5...de7b | 79.5% | 166 | other |
| 0xe9c6...b3dc9 | 100.0% | 40 | crypto |

---

## Action Items

1. **🔴 Restart monitor** — No STATS output in ~30 hours. Check if the collector process is alive and restart.
2. **🔴 Verify daily refresh** — Last run logged only a test line. Investigate whether the full pipeline (wallet scoring, market data) completed.
3. **🟡 Crypto signals** — Wallet 0xe9c6 (100% win rate, crypto domain) is heavily buying YES on ETH >$2,100 and BTC >$70,000 for April. Consider following if liquidity is sufficient.
4. **🟡 KXCPI/KXFED prep** — 6 days remain. Ensure positions are sized and any pre-resolution hedging is in place.
5. **🟢 NCAA Tournament** — Multiple smart wallets buying YES on Michigan and Connecticut. These are sports markets with fast resolution — lower priority but notable volume.
