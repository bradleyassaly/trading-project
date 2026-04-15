#!/bin/sh
# Nightly Postgres backup with 7-day retention.
#
# Runs from the scheduler container. Requires postgresql-client in the
# image (installed in Dockerfile). Connects over the internal Docker
# network to the postgres service using env vars from docker-compose.
#
# The backup volume is bind-mounted at /backups via docker-compose.
set -e

BACKUP_DIR="${BACKUP_DIR:-/backups}"
mkdir -p "$BACKUP_DIR"
TS=$(date -u +%Y%m%d_%H%M%S)
OUTFILE="$BACKUP_DIR/polymarket_${TS}.sql.gz"

# pg_dump --format=plain + gzip keeps the backup small and grep-able.
# --no-owner/--no-acl so the dump restores cleanly into a fresh cluster.
PGPASSWORD="${POSTGRES_PASSWORD:-polymarket_dev}" pg_dump \
  -h "${POSTGRES_HOST:-postgres}" \
  -p "${POSTGRES_PORT:-5432}" \
  -U "${POSTGRES_USER:-polymarket}" \
  -d "${POSTGRES_DB:-polymarket}" \
  --no-owner --no-acl | gzip > "$OUTFILE"

SIZE=$(du -h "$OUTFILE" 2>/dev/null | cut -f1)
echo "[backup] wrote $OUTFILE ($SIZE)"

# Retention: keep the 7 most recent daily dumps; prune older ones.
ls -t "$BACKUP_DIR"/polymarket_*.sql.gz 2>/dev/null | tail -n +8 | xargs -r rm -f
KEPT=$(ls "$BACKUP_DIR"/polymarket_*.sql.gz 2>/dev/null | wc -l)
echo "[backup] retention pruned — $KEPT dumps kept"
