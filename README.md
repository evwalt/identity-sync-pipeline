# identity-sync-pipeline

Syncs non-human identity changes (service accounts, API keys, OAuth apps, AI agents) from third-party integrations through Kafka into Postgres, and answers "what can this identity access?" over gRPC.

The point of the project is correctness when the transport is at-least-once and out of order:

- **Duplicates are harmless.** Every event ID is recorded in `processed_events` in the same transaction that applies it, so a redelivered event is a no-op.
- **Late events can't roll access backwards.** Each event carries the identity's source version. An event older than the stored version is recorded as `stale` and not applied, so a delayed "grant admin" can't overwrite a newer "read only".
- **Per-identity ordering.** Events are keyed by identity ID, so one identity's events land on one partition, and the identity row is locked (`SELECT ... FOR UPDATE`) while an event applies.
- **Retryable vs. permanent failures.** Database errors that can clear on their own (lost connection, serialization failure, deadlock) retry with exponential backoff. Events that can never apply (bad JSON, unknown kind or op) go straight to a dead-letter topic with the reason, detail, and source offset in headers. Retries that run out also go to the dead-letter topic.
- **Offsets move only after a final outcome.** Auto-commit is off. The offset is committed after the event is applied, skipped, or durably written to the dead-letter topic, so a crash means redelivery, not loss.

## Layout

| Path | What |
|---|---|
| `src/idsync/events.py` | Event schema and validation (`InvalidEvent` = permanent failure) |
| `src/idsync/store.py` | Postgres apply logic: dedupe, version check, full entitlement replace |
| `src/idsync/consumer.py` | Kafka consumer loop, retries with backoff, dead-letter topic |
| `src/idsync/server.py` | gRPC `AccessService.GetAccess` |
| `proto/access.proto` | gRPC contract |
| `scripts/dev_up.sh` | Local Postgres + single-node Kafka (KRaft, no ZooKeeper, no Docker) |
| `scripts/bench.py` | Throughput and correctness run with duplicates and late events |

## Run it

Needs Python 3.11+, a JDK 17+, Postgres binaries on `PATH`, and an extracted [Apache Kafka 3.9](https://kafka.apache.org/downloads) release.

```sh
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
PATH=.venv/bin:$PATH scripts/gen_proto.sh
KAFKA_HOME=/path/to/kafka_2.13-3.9.1 scripts/dev_up.sh
.venv/bin/pytest -q
.venv/bin/python scripts/bench.py --events 20000 --identities 2000
scripts/dev_down.sh
```

Tests run against the real broker and database: unit tests for validation and the store, plus an end-to-end test that produces duplicates, a late event, and two poison messages, consumes them, checks the dead-letter topic, and queries the result over gRPC.

## Results

`scripts/bench.py` on a 2017 Intel i5 MacBook, single consumer, 6 partitions:

```
events: 20000 (redelivered 1949, late 1771)
outcomes: {'applied': 16441, 'stale': 1610, 'duplicate': 1949}
consumed 20000 in 53.6s = 373 events/s (single consumer)
identities at wrong version: 0 of 2000
```

Throughput is deliberately unoptimized: one transaction and one synchronous offset commit per event. Batching commits and running a consumer per partition are the obvious next steps.

## Not built

No metrics, tracing, or SLO alerting yet (failures are logged and dead-lettered). No schema registry; events are JSON. Single broker, no auth.
