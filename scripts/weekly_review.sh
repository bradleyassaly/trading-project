#!/usr/bin/env bash
# Weekly review script — run when you're back after the unattended week.
#
# Prints one compact report covering: paper + live PnL, thesis scorecard,
# signal quality, category performance, incidents, and data freshness.
# Nothing here mutates state; it's read-only so you can run it as many
# times as you like while you're forming conclusions.
#
# Usage:  bash scripts/weekly_review.sh
#
# For individual queries, see the inline SQL — copy/paste into
# `docker compose exec -T postgres psql -U polymarket -d polymarket -c "..."`
set -euo pipefail

cd "$(dirname "$0")/.."

pg() { docker compose exec -T postgres psql -U polymarket -d polymarket -c "$1" 2>&1; }

echo "===================================================================="
echo " WEEKLY REVIEW — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "===================================================================="

echo
echo "-- Services ---------------------------------------------------------"
docker compose ps --format "table {{.Name}}\t{{.Status}}" | tail -n +2

echo
echo "-- Thesis scorecard --------------------------------------------------"
curl -s http://localhost:8001/api/thesis/scorecard 2>&1 \
  | sed -E 's/[,{]/\n  /g' | grep -E '"(total_hypotheses|correct|accuracy|recent_20|prior_20|trend|verdict)"' | head -10

echo
echo "-- Paper PnL (archived=0) -------------------------------------------"
pg "SELECT COUNT(*) total, COUNT(*) FILTER (WHERE exit_ts IS NOT NULL) closed,
          SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
          ROUND((SUM(CASE WHEN outcome='win' THEN 1.0 ELSE 0 END)/NULLIF(COUNT(*) FILTER (WHERE exit_ts IS NOT NULL),0))::numeric*100,1) wr_pct,
          ROUND(SUM(COALESCE(realized_pnl,0))::numeric,2) realized_pnl_usd
   FROM polymarket_paper_trades WHERE archived=0;"

echo
echo "-- Paper signal breakdown (closed trades only) -----------------------"
pg "SELECT signal_type, COUNT(*) closed, SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
          ROUND((SUM(CASE WHEN outcome='win' THEN 1.0 ELSE 0 END)/NULLIF(COUNT(*),0))::numeric*100,1) wr_pct,
          ROUND(SUM(COALESCE(realized_pnl,0))::numeric,2) pnl
   FROM polymarket_paper_trades
   WHERE archived=0 AND exit_ts IS NOT NULL
   GROUP BY signal_type
   HAVING COUNT(*) >= 3
   ORDER BY pnl DESC;"

echo
echo "-- Paper category breakdown -----------------------------------------"
pg "SELECT category, COUNT(*) closed, SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
          ROUND((SUM(CASE WHEN outcome='win' THEN 1.0 ELSE 0 END)/NULLIF(COUNT(*),0))::numeric*100,1) wr_pct,
          ROUND(SUM(COALESCE(realized_pnl,0))::numeric,2) pnl
   FROM polymarket_paper_trades
   WHERE archived=0 AND exit_ts IS NOT NULL
   GROUP BY category ORDER BY pnl DESC;"

echo
echo "-- Live trades this week (non-dry-run) -------------------------------"
pg "SELECT id, to_timestamp(submitted_at)::date AS date, signal_type, side, ROUND(entry_price::numeric,3) entry,
          ROUND(size_usd::numeric,2) size, status, outcome, ROUND(COALESCE(realized_pnl,0)::numeric,2) pnl, exit_reason
   FROM live_trades
   WHERE dry_run=0 AND submitted_at > extract(epoch FROM NOW()-interval '8 days')
   ORDER BY submitted_at DESC;"

echo
echo "-- Hypotheses resolved this week ------------------------------------"
pg "SELECT COUNT(*) resolved_7d,
          SUM(hypothesis_correct) correct,
          ROUND(AVG(hypothesis_correct::numeric)*100,1) accuracy_pct
   FROM trade_hypotheses
   WHERE resolved_at IS NOT NULL
     AND resolved_at > extract(epoch FROM NOW()-interval '7 days')
     AND actual_outcome != 'expired';"

echo
echo "-- Per-signal hypothesis accuracy (7d) ------------------------------"
pg "SELECT signal_type, COUNT(*) n, SUM(hypothesis_correct) correct,
          ROUND(AVG(hypothesis_correct::numeric)*100,1) acc_pct
   FROM trade_hypotheses
   WHERE resolved_at IS NOT NULL
     AND resolved_at > extract(epoch FROM NOW()-interval '7 days')
     AND actual_outcome != 'expired'
   GROUP BY signal_type ORDER BY n DESC;"

echo
echo "-- Scheduler failures (7d) ------------------------------------------"
docker compose logs --since 168h scheduler 2>&1 | grep -E '\[fail\]' | awk -F'"message": "' '{print $2}' | awk -F'"}' '{print $1}' | sort | uniq -c | sort -rn | head -10

echo
echo "-- Watchdog alerts (7d) ---------------------------------------------"
docker compose logs --since 168h watchdog 2>&1 | grep -E '\[alert\]|\[recover\]' | tail -15

echo
echo "-- Circuit breaker + kill switch status -----------------------------"
curl -s http://localhost:8001/api/circuit-breaker/status | sed -E 's/[,{]/\n  /g' | grep -E '"(is_halted|halted_reason|current_equity|current_drawdown|daily_pnl)"' | head -8
curl -s http://localhost:8001/api/system/kill-switch | head -c 200; echo

echo
echo "-- Data freshness ---------------------------------------------------"
pg "SELECT 'last_signal' AS m, to_timestamp(MAX(fired_at))::text AS ts, (NOW()-to_timestamp(MAX(fired_at)))::text AS age FROM market_signals
 UNION ALL SELECT 'last_paper_trade', to_timestamp(MAX(entry_ts))::text, (NOW()-to_timestamp(MAX(entry_ts)))::text FROM polymarket_paper_trades
 UNION ALL SELECT 'last_wallet_trade', to_timestamp(MAX(timestamp))::text, (NOW()-to_timestamp(MAX(timestamp)))::text FROM wallet_trades
 UNION ALL SELECT 'last_equity_snap', to_timestamp(MAX(ts))::text, (NOW()-to_timestamp(MAX(ts)))::text FROM paper_equity_snapshots
 UNION ALL SELECT 'last_hypothesis_resolved', to_timestamp(MAX(resolved_at))::text, (NOW()-to_timestamp(MAX(resolved_at)))::text FROM trade_hypotheses WHERE resolved_at IS NOT NULL
 ORDER BY 1;"

echo
echo "-- Backups (most recent) --------------------------------------------"
ls -lht backups/*.sql.gz 2>&1 | head -8

echo
echo "===================================================================="
echo " END REVIEW"
echo "===================================================================="
