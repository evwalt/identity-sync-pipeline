#!/usr/bin/env bash
# Stop the local Kafka and Postgres started by dev_up.sh (data under .local/ is kept).
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f .local/kafka.pid ] && kill "$(cat .local/kafka.pid)" 2>/dev/null && rm -f .local/kafka.pid
[ -d .local/pgdata ] && pg_ctl -D .local/pgdata -m fast stop >/dev/null 2>&1
echo stopped
