#!/usr/bin/env sh
set -eu

APP_DIR="${APP_DIR:-/opt/triotech}"
BACKUP_DIR="${BACKUP_DIR:-$APP_DIR/backups/postgres}"
CONTAINER="${POSTGRES_CONTAINER:-triotech-postgres}"
DB_NAME="${POSTGRES_DB:-triotech}"
DB_USER="${POSTGRES_USER:-triotech_admin}"
KEEP_DAYS="${KEEP_DAYS:-30}"

mkdir -p "$BACKUP_DIR"
chmod 700 "$APP_DIR/backups" "$BACKUP_DIR" 2>/dev/null || true

STAMP="$(date -u +%Y%m%d-%H%M%S)"
OUT="$BACKUP_DIR/${DB_NAME}-${STAMP}.dump"
LATEST="$BACKUP_DIR/${DB_NAME}-latest.dump"

docker exec "$CONTAINER" pg_dump \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  --format=custom \
  --no-owner \
  --no-privileges > "$OUT"

chmod 600 "$OUT"
ln -sfn "$(basename "$OUT")" "$LATEST"

find "$BACKUP_DIR" \
  -type f \
  -name "${DB_NAME}-*.dump" \
  -mtime "+$KEEP_DAYS" \
  -delete

SIZE="$(du -h "$OUT" | awk '{print $1}')"

echo "Backup created: $OUT ($SIZE)"
echo "Latest link: $LATEST"
