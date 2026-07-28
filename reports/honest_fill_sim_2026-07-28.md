# Honest fill simulation — 2026-07-28

Cohort: closed YES-side paper trades since 2026-07-22T03:08:40+00:00 (6d tape-complete window), entry patience 300s.

Entry honest ⇔ the tape printed ≤ our claimed entry price at our size within the window. TP honest ⇔ the tape printed ≥ the claimed TP price before resolution; otherwise the trade rides to the resolution payout. Stops/expiries keep paper booking (honest EV is an UPPER bound for stop-heavy signals).

| signal | n | entry fill | TP honest | unresolved | paper P&L | honest P&L | paper EV/$ | honest EV/$ |
|---|---|---|---|---|---|---|---|---|
| tier_entry | 2 | 1/2 | 0/0 | 0 | $-0.05 | $-0.03 | -0.025 | -0.030 |
| synthetic | 1 | 0/1 | 0/0 | 0 | $-0.43 | $+0.00 | -0.860 | +0.000 |
| confluence_2plus | 3 | 1/3 | 0/0 | 0 | $-10.74 | $-2.01 | -0.716 | -0.402 |
| resolution_decay | 42 | 0/42 | 0/0 | 0 | $-81.23 | $+0.00 | -0.236 | +0.000 |
