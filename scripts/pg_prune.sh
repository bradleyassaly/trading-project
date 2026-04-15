#!/bin/sh
# Weekly Postgres pruning. Keeps the DB from growing without bound.
# Conservative defaults — tune via env if needed.
#
# market_ticks: keep 90 days (tick history rarely used beyond 30d but
#   useful for long-tail analysis; indexes are larger than the heap so
#   trimming helps).
# service_health: keep 7 days (heartbeat rows are diagnostic only).
# signal_outcomes: keep forever (needed for calibration).
# wallet_trades: keep forever (needed for alpha scoring).
set -e

MARKET_TICKS_DAYS="${MARKET_TICKS_RETENTION_DAYS:-90}"
SERVICE_HEALTH_DAYS="${SERVICE_HEALTH_RETENTION_DAYS:-7}"
CB_LOG_DAYS="${CB_LOG_RETENTION_DAYS:-90}"

run_sql() {
  PGPASSWORD="${POSTGRES_PASSWORD:-polymarket_dev}" psql \
    -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_PORT:-5432}" \
    -U "${POSTGRES_USER:-polymarket}" -d "${POSTGRES_DB:-polymarket}" \
    -At -c "$1"
}

deleted=$(run_sql "DELETE FROM market_ticks WHERE timestamp < EXTRACT(EPOCH FROM NOW() - INTERVAL '${MARKET_TICKS_DAYS} days')::bigint RETURNING 1" | wc -l)
echo "[prune] market_ticks: ${deleted} rows older than ${MARKET_TICKS_DAYS}d removed"

deleted=$(run_sql "DELETE FROM service_health WHERE checked_at < EXTRACT(EPOCH FROM NOW() - INTERVAL '${SERVICE_HEALTH_DAYS} days')::bigint RETURNING 1" | wc -l)
echo "[prune] service_health: ${deleted} rows older than ${SERVICE_HEALTH_DAYS}d removed"

deleted=$(run_sql "DELETE FROM circuit_breaker_log WHERE created_at < EXTRACT(EPOCH FROM NOW() - INTERVAL '${CB_LOG_DAYS} days')::bigint RETURNING 1" | wc -l)
echo "[prune] circuit_breaker_log: ${deleted} rows older than ${CB_LOG_DAYS}d removed"

# VACUUM to reclaim space (non-FULL, low-impact)
run_sql "VACUUM (ANALYZE) market_ticks, service_health, circuit_breaker_log" >/dev/null
echo "[prune] VACUUM ANALYZE complete"
