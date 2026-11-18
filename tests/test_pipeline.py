"""End to end against real Kafka and Postgres: produce -> consume -> store -> gRPC."""

import json
import socket
import uuid

import grpc
import psycopg
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg_pool import ConnectionPool

from conftest import KAFKA
from idsync import access_pb2, access_pb2_grpc, consumer
from idsync.consumer import Pipeline, Settings
from idsync.server import serve


def kafka_up() -> bool:
    host, port = KAFKA.split(":")
    try:
        socket.create_connection((host, int(port)), timeout=1).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not kafka_up(), reason="Kafka not running (scripts/dev_up.sh)")


@pytest.fixture
def topics():
    suffix = uuid.uuid4().hex[:8]
    main, dlq = f"identity-events-{suffix}", f"identity-events-{suffix}.dlq"
    admin = AdminClient({"bootstrap.servers": KAFKA})
    for f in admin.create_topics([NewTopic(main, 3, 1), NewTopic(dlq, 1, 1)]).values():
        f.result(10)
    yield main, dlq
    admin.delete_topics([main, dlq])


def event(event_id, version, ents, identity="sa-deploy-bot", **extra):
    body = {
        "event_id": event_id, "source": "github", "identity_id": identity,
        "kind": "service_account", "version": version, "op": "upsert",
        "entitlements": [{"resource": r, "permission": p} for r, p in ents],
    }
    body.update(extra)
    return json.dumps(body).encode()


def produce(topic, items):
    p = Producer({"bootstrap.servers": KAFKA, "enable.idempotence": True})
    for key, value in items:
        p.produce(topic, key=key, value=value)
    assert p.flush(10) == 0


def read_dlq(topic, expected):
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"dlq-{uuid.uuid4().hex}",
                  "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    got = []
    for _ in range(40):
        msg = c.poll(0.5)
        if msg is not None and not msg.error():
            got.append({k: v.decode() for k, v in msg.headers()})
            if len(got) == expected:
                break
    c.close()
    return got


def test_pipeline_end_to_end(topics, pg_url):
    main, dlq = topics
    # Keyed by identity, so one identity's events stay ordered on one partition.
    produce(main, [
        (b"sa-deploy-bot", event("e1", 1, [("repo:payments", "read")])),
        (b"sa-deploy-bot", event("e1", 1, [("repo:payments", "read")])),          # redelivered
        (b"sa-deploy-bot", event("e3", 3, [("repo:payments", "read"), ("repo:infra", "write")])),
        (b"sa-deploy-bot", event("e2", 2, [("repo:payments", "admin")])),         # late, stale
        (b"key-ci-token", event("e4", 1, [("org", "admin")], identity="key-ci-token", kind="api_key")),
        (b"bad", b"{not json"),                                                     # poison
        (b"bad", event("e5", 1, [], kind="human")),                                 # invalid schema
    ])

    stats = Pipeline(
        Settings(bootstrap=KAFKA, topic=main, dlq_topic=dlq, group_id=f"g-{uuid.uuid4().hex}"),
        connect=lambda: psycopg.connect(pg_url),
    ).run(stop_after_idle_s=3)

    assert stats.outcomes == {"applied": 3, "duplicate": 1, "stale": 1, "dlq:invalid": 2}
    reasons = sorted(h["dlq.reason"] for h in read_dlq(dlq, 2))
    assert reasons == ["invalid", "invalid"]

    with ConnectionPool(pg_url, min_size=1, max_size=2, open=True) as pool:
        server, port = serve(pool, "127.0.0.1:0")
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as ch:
                stub = access_pb2_grpc.AccessServiceStub(ch)
                resp = stub.GetAccess(access_pb2.GetAccessRequest(identity_id="sa-deploy-bot"), timeout=5)
                assert resp.version == 3
                assert [(e.resource, e.permission) for e in resp.entitlements] == [
                    ("repo:infra", "write"), ("repo:payments", "read")]
                with pytest.raises(grpc.RpcError) as missing:
                    stub.GetAccess(access_pb2.GetAccessRequest(identity_id="nobody"), timeout=5)
                assert missing.value.code() == grpc.StatusCode.NOT_FOUND
        finally:
            server.stop(0)


def test_transient_db_errors_retry_then_succeed(monkeypatch, pg_url):
    calls = {"n": 0}
    real = consumer.apply_event

    def flaky(conn, ev):
        calls["n"] += 1
        if calls["n"] < 3:
            raise psycopg.OperationalError("connection reset")
        return real(conn, ev)

    monkeypatch.setattr(consumer, "apply_event", flaky)
    sleeps = []
    p = Pipeline(Settings(bootstrap=KAFKA), connect=lambda: psycopg.connect(pg_url), sleep=sleeps.append)
    assert p.handle(FakeMsg(event("e1", 1, [("a", "read")]))) == "applied"
    assert calls["n"] == 3
    assert sleeps == [0.2, 0.4]  # exponential backoff


def test_retries_exhausted_go_to_dlq(monkeypatch, topics, pg_url):
    _, dlq = topics

    def down(conn, ev):
        raise psycopg.OperationalError("database is down")

    monkeypatch.setattr(consumer, "apply_event", down)
    p = Pipeline(Settings(bootstrap=KAFKA, dlq_topic=dlq, max_attempts=3),
                 connect=lambda: psycopg.connect(pg_url), sleep=lambda s: None)
    assert p.handle(FakeMsg(event("e1", 1, []))) == "dlq:retries_exhausted"
    assert read_dlq(dlq, 1)[0]["dlq.reason"] == "retries_exhausted"


class FakeMsg:
    def __init__(self, value):
        self._value = value

    def value(self):
        return self._value

    def key(self):
        return b"k"

    def topic(self):
        return "identity-events"

    def partition(self):
        return 0

    def offset(self):
        return 0
