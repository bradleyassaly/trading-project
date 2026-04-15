"""Strategic alpha report: which wallets are best at which strategies.

Reads wallet_strategy_profiles and ranks:
  1. Best wallet per strategy (highest PnL)
  2. Best strategy per wallet (alpha decomposition)
  3. Strategy-level rollup (is this strategy copyable system-wide?)
  4. "Pure" strategists (wallets concentrating in one strategy)

Output: stdout. Intended for ad-hoc analysis; the enhanced weekly digest
can pull highlights from the same table.
"""
from __future__ import annotations

import pandas as pd

from trading_platform.polymarket.db_connection import get_connection

c = get_connection()


def hdr(t):
    print("\n" + "=" * 72 + f"\n  {t}\n" + "=" * 72)


df = pd.read_sql(
    "SELECT w.wallet, w.strategy, w.n_trades, w.n_markets, w.avg_size_usd, "
    " w.wins, w.losses, w.net_pnl_usd, w.avg_pnl_per_trade, "
    " wp.net_pnl_usdc AS wallet_total_pnl, l.tier "
    "FROM wallet_strategy_profiles w "
    "LEFT JOIN wallet_profiles wp ON w.wallet = wp.wallet "
    "LEFT JOIN leaderboard l ON w.wallet = l.wallet",
    c,
)
df["wr"] = df["wins"] / (df["wins"] + df["losses"]).replace(0, 1)

hdr("1. STRATEGY-LEVEL ROLLUP (is each strategy copyable across wallets?)")
print(f"{'strategy':20s} {'n_wallets':>10s} {'total_trades':>13s} {'total_pnl':>12s} {'median_wallet_pnl':>17s} {'n_winners':>9s}")
for st in df["strategy"].unique():
    sub = df[df.strategy == st]
    total_trades = int(sub.n_trades.sum())
    total_pnl = sub.net_pnl_usd.sum()
    med = sub.net_pnl_usd.median()
    winners = (sub.net_pnl_usd > 0).sum()
    print(f"  {st:20s} {len(sub):>10d} {total_trades:>13,d} ${total_pnl:>11,.0f} ${med:>16,.0f} {winners:>9d}/{len(sub)}")


hdr("2. TOP 3 WALLETS PER STRATEGY (by net PnL)")
for st in sorted(df["strategy"].unique()):
    sub = df[df.strategy == st].nlargest(3, "net_pnl_usd")
    print(f"\n  [{st}]")
    for _, r in sub.iterrows():
        wr_s = f"{r.wr*100:.0f}%" if (r.wins + r.losses) > 0 else "n/a"
        tier = r.tier or "untracked"
        print(
            f"    {r.wallet[:14]} tier={tier:8s} n={r.n_trades:>4d} mkts={r.n_markets:>3d} "
            f"avg=${r.avg_size_usd or 0:>7,.0f} WR={wr_s:>4s} "
            f"pnl=${r.net_pnl_usd:>+11,.0f} / wallet_total=${r.wallet_total_pnl or 0:>+10,.0f}"
        )


hdr("3. PURE STRATEGISTS (wallets where one strategy drives ≥70% of their PnL)")
wallet_strategy_breakdown = df.pivot_table(
    index="wallet", columns="strategy", values="net_pnl_usd", aggfunc="sum", fill_value=0
)
for wallet in wallet_strategy_breakdown.index:
    row = wallet_strategy_breakdown.loc[wallet]
    positive = row[row > 0]
    if positive.empty:
        continue
    total_positive = positive.sum()
    top_strat = positive.idxmax()
    top_val = positive.max()
    if top_val / total_positive >= 0.70 and top_val > 1000:
        wallet_tot = df[df.wallet == wallet]["wallet_total_pnl"].iloc[0] or 0
        print(
            f"  {wallet[:14]} wallet_pnl=${wallet_tot:>+10,.0f}  "
            f"{top_strat:20s} ${top_val:>+10,.0f} "
            f"({top_val/total_positive*100:.0f}% of their strategy pnl)"
        )


hdr("4. ALPHA DECOMPOSITION — top 10 earning wallets, strategy breakdown")
top_earners = (
    df.groupby("wallet")["wallet_total_pnl"].first()
    .dropna().nlargest(10).index.tolist()
)
for wallet in top_earners:
    sub = df[df.wallet == wallet].sort_values("net_pnl_usd", ascending=False)
    wallet_pnl = sub["wallet_total_pnl"].iloc[0] or 0
    tier = sub["tier"].iloc[0] or "untracked"
    print(f"\n  {wallet[:16]} [{tier}] total=${wallet_pnl:,.0f}")
    for _, r in sub.iterrows():
        if r.net_pnl_usd == 0 and r.wins == 0 and r.losses == 0:
            continue
        wr_s = f"{r.wr*100:.0f}%" if (r.wins + r.losses) > 0 else "n/a"
        print(
            f"    {r.strategy:22s} n={r.n_trades:>4d} mkts={r.n_markets:>3d} "
            f"WR={wr_s:>4s} pnl=${r.net_pnl_usd:>+10,.0f}"
        )


hdr("5. STRATEGIES WORTH COPYING (positive rollup + N wallets validating)")
rollup = df.groupby("strategy").agg(
    n_wallets=("wallet", "nunique"),
    total_pnl=("net_pnl_usd", "sum"),
    winners=("net_pnl_usd", lambda s: (s > 0).sum()),
    median_pnl=("net_pnl_usd", "median"),
)
rollup["winner_pct"] = rollup["winners"] / rollup["n_wallets"]
rollup = rollup.sort_values("total_pnl", ascending=False)

print(f"{'strategy':20s} {'total_pnl':>12s} {'winner_pct':>11s} {'n_wallets':>9s} {'verdict':>30s}")
for st, row in rollup.iterrows():
    if row.total_pnl > 50_000 and row.winner_pct >= 0.5:
        verdict = "✓ COPYABLE — build signal"
    elif row.total_pnl > 0 and row.winner_pct >= 0.5:
        verdict = "◐ weak edge, monitor"
    elif row.total_pnl > 0:
        verdict = "◌ positive but uneven"
    else:
        verdict = "✗ not copyable"
    print(f"  {st:20s} ${row.total_pnl:>+11,.0f} {row.winner_pct*100:>10.0f}% {int(row.n_wallets):>9d}  {verdict}")

c.close()
