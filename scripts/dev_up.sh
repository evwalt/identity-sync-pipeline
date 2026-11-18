#!/usr/bin/env bash
# Start a disposable local Postgres and a single-node Kafka (KRaft, no ZooKeeper) under .local/.
# Needs: Postgres binaries on PATH (initdb, pg_ctl) and a JDK 17+.
# KAFKA_HOME points at an extracted Apache Kafka 3.9 release.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${KAFKA_HOME:?set KAFKA_HOME to an extracted Apache Kafka 3.9 release}"
PGPORT="${PGPORT:-54329}"
KPORT="${KPORT:-19092}"
mkdir -p .local

if [ ! -d .local/pgdata ]; then
  initdb -D .local/pgdata -U postgres --auth=trust >/dev/null
fi
pg_ctl -D .local/pgdata -o "-p $PGPORT -k /tmp" -l .local/postgres.log status >/dev/null 2>&1 \
  || pg_ctl -D .local/pgdata -o "-p $PGPORT -k /tmp" -l .local/postgres.log -w start >/dev/null
psql -h 127.0.0.1 -p "$PGPORT" -U postgres -tc "SELECT 1 FROM pg_database WHERE datname='idsync'" | grep -q 1 \
  || psql -h 127.0.0.1 -p "$PGPORT" -U postgres -qc "CREATE DATABASE idsync"

CFG=.local/kraft.properties
if [ ! -f "$CFG" ]; then
  cat > "$CFG" <<PROPS
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:19093
listeners=PLAINTEXT://127.0.0.1:$KPORT,CONTROLLER://127.0.0.1:19093
advertised.listeners=PLAINTEXT://127.0.0.1:$KPORT
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=$PWD/.local/kafka-logs
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
PROPS
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" -c "$CFG" >/dev/null
fi
if ! nc -z 127.0.0.1 "$KPORT" 2>/dev/null; then
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$CFG" > .local/kafka.log 2>&1 &
  echo $! > .local/kafka.pid
  for _ in $(seq 60); do nc -z 127.0.0.1 "$KPORT" 2>/dev/null && break; sleep 1; done
fi
nc -z 127.0.0.1 "$KPORT" || { echo "kafka did not start; see .local/kafka.log"; exit 1; }
echo "postgres: postgresql://postgres@127.0.0.1:$PGPORT/idsync"
echo "kafka:    127.0.0.1:$KPORT"
