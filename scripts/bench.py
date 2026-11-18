"""Throughput and correctness check against local Kafka + Postgres.

Produces N identity events across many identities, with ~10% redelivered duplicates and
~10% older versions arriving late, then consumes them all and checks that every identity
ends at its newest version. Prints events/second for the consume side.

  python scripts/bench.py --events 20000 --identities 2000
"""

import argparse
import json
import random
import time
import uuid

import psycopg
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from idsync.consumer import Pipeline, Settings
from idsync.store import migrate

ap = argparse.ArgumentParser()
ap.add_argument("--events", type=int, default=20000)
ap.add_argument("--identities", type=int, default=2000)
ap.add_argument("--kafka", default="127.0.0.1:19092")
ap.add_argument("--pg", default="postgresql://postgres@127.0.0.1:54329/idsync")
args = ap.parse_args()

rng = random.Random(7)
suffix = uuid.uuid4().hex[:8]
topic, dlq, schema = f"bench-{suffix}", f"bench-{suffix}.dlq", f"bench_{suffix}"

admin = AdminClient({"bootstrap.servers": args.kafka})
for f in admin.create_topics([NewTopic(topic, 6, 1), NewTopic(dlq, 1, 1)]).values():
    f.result(10)
with psycopg.connect(args.pg, autocommit=True) as c:
    c.execute(f"CREATE SCHEMA {schema}")
url = f"{args.pg}?options=-csearch_path%3D{schema}"
with psycopg.connect(url) as c:
    migrate(c)

versions = {f"id-{i}": 0 for i in range(args.identities)}
events, dupes, late = [], 0, 0
for n in range(args.events):
    ident = f"id-{rng.randrange(args.identities)}"
    roll = rng.random()
    if roll < 0.1 and events:
        events.append(rng.choice(events))  # redelivery of an earlier event
        dupes += 1
        continue
    versions[ident] += 1
    v = versions[ident]
    if roll < 0.2 and v > 1:
        v -= 1  # an older version arriving late
        late += 1
    body = {"event_id": f"{ident}-v{v}-{n}", "source": "bench", "identity_id": ident,
            "kind": "service_account", "version": v, "op": "upsert",
            "entitlements": [{"resource": f"res-{rng.randrange(50)}", "permission": "read"}]}
    events.append((ident.encode(), json.dumps(body).encode()))

p = Producer({"bootstrap.servers": args.kafka, "enable.idempotence": True, "linger.ms": 20})
for key, value in events:
    p.produce(topic, key=key, value=value)
p.flush(30)

pipe = Pipeline(Settings(bootstrap=args.kafka, topic=topic, dlq_topic=dlq, group_id=f"bench-{suffix}"),
                connect=lambda: psycopg.connect(url))
start = time.monotonic()
stats = pipe.run(stop_after_idle_s=3)
elapsed = time.monotonic() - start - 3  # minus the idle wait at the end

# Newest version produced for each identity (late events never exceed it).
newest = {}
for _, value in events:
    b = json.loads(value)
    newest[b["identity_id"]] = max(newest.get(b["identity_id"], 0), b["version"])
with psycopg.connect(url) as c:
    stored = dict(c.execute("SELECT id, version FROM identities").fetchall())
wrong = sum(1 for k, v in newest.items() if stored.get(k) != v)

total = sum(stats.outcomes.values())
print(f"events: {len(events)} (redelivered {dupes}, late {late})")
print(f"outcomes: {stats.outcomes}")
print(f"consumed {total} in {elapsed:.1f}s = {total / elapsed:,.0f} events/s (single consumer)")
print(f"identities at wrong version: {wrong} of {len(newest)}")

with psycopg.connect(args.pg, autocommit=True) as c:
    c.execute(f"DROP SCHEMA {schema} CASCADE")
admin.delete_topics([topic, dlq])
