# Edge survey — aggregate order flow (2026-07-31)

Does tick-rule order-flow imbalance predict the next move? `side` cannot be used (the firehose records both maker and taker perspectives, so buy volume == sell volume by construction), hence the tick rule.

- Tokens (>= 200 prints, 4d): 19
- Samples: 9,590 (60s buckets, 5 back, 15 forward)
- Dropped for crossing resolution: 351

## Forward return by flow-imbalance quintile

| quintile | imbalance | n | mean fwd (¢) | median (¢) | stdev (¢) |
|---|---|---|---|---|---|
| Q1 | -1.0..-0.617 | 1,918 | -0.0893 | -0.0000 | 4.775 |
| Q2 | -0.616..-0.124 | 1,918 | +0.4969 | +0.0000 | 9.045 |
| Q3 | -0.123..0.346 | 1,918 | +0.7520 | +0.0000 | 11.407 |
| Q4 | 0.346..0.784 | 1,918 | +0.9772 | +0.0017 | 7.732 |
| Q5 | 0.784..1.0 | 1,918 | +0.5103 | +0.0732 | 4.279 |

**Q5 − Q1 spread: +0.5996¢** vs a 2.5¢ round-trip toll → **below execution toll — not tradeable**
