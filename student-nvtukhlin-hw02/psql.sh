#!/usr/bin/env bash
set -a
. "$(dirname "$0")/.env"
set +a
docker compose exec -T warehouse psql -U "$WAREHOUSE_USER" -d "$WAREHOUSE_DB" "$@"
