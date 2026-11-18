"""Kafka consumer: identity events -> Postgres, with retries and a dead-letter topic.

Delivery is at least once. The offset is committed only after an event is either
applied (or skipped as a duplicate/stale) or parked on the dead-letter topic, so a
crash mid-event means redelivery, which the store makes harmless.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import psycopg
from confluent_kafka import Consumer, KafkaError, Message, Producer

from .events import InvalidEvent, parse_event
from .store import apply_event

log = logging.getLogger("idsync.consumer")

# Errors worth retrying: the database is briefly unreachable or the transaction lost a race.
TRANSIENT = (psycopg.OperationalError, psycopg.errors.SerializationFailure, psycopg.errors.DeadlockDetected)


@dataclass
class Settings:
    bootstrap: str
    topic: str = "identity-events"
    dlq_topic: str = "identity-events.dlq"
    group_id: str = "idsync"
    max_attempts: int = 5
    base_backoff_s: float = 0.2


@dataclass
class Stats:
    outcomes: dict[str, int] = field(default_factory=dict)

    def bump(self, key: str) -> None:
        self.outcomes[key] = self.outcomes.get(key, 0) + 1


class Pipeline:
    def __init__(self, settings: Settings, connect, sleep=time.sleep):
        self.s = settings
        self.connect = connect  # () -> psycopg.Connection; called again after a lost connection
        self.sleep = sleep
        self.conn = connect()
        self.stats = Stats()
        self.dlq = Producer({"bootstrap.servers": settings.bootstrap, "enable.idempotence": True})

    def handle(self, msg: Message) -> str:
        """Process one message to a final outcome. Never raises for bad data."""
        try:
            ev = parse_event(msg.value())
        except InvalidEvent as e:
            return self._dead_letter(msg, "invalid", str(e))

        for attempt in range(1, self.s.max_attempts + 1):
            try:
                outcome = apply_event(self.conn, ev)
                self.stats.bump(outcome)
                log.info("event %s for %s: %s", ev.event_id, ev.identity_id, outcome)
                return outcome
            except TRANSIENT as e:
                log.warning("event %s attempt %d failed: %s", ev.event_id, attempt, e)
                if attempt == self.s.max_attempts:
                    return self._dead_letter(msg, "retries_exhausted", str(e))
                self.sleep(self.s.base_backoff_s * 2 ** (attempt - 1))
                if self.conn.closed or self.conn.broken:
                    self.conn = self.connect()
        raise AssertionError("unreachable")

    def _dead_letter(self, msg: Message, reason: str, detail: str) -> str:
        self.dlq.produce(
            self.s.dlq_topic,
            key=msg.key(),
            value=msg.value(),
            headers=[
                ("dlq.reason", reason.encode()),
                ("dlq.detail", detail[:500].encode()),
                ("dlq.source", f"{msg.topic()}/{msg.partition()}/{msg.offset()}".encode()),
            ],
        )
        # The DLQ write must be durable before the offset moves past this event.
        if self.dlq.flush(10) != 0:
            raise RuntimeError("dead-letter publish not confirmed; refusing to commit offset")
        self.stats.bump(f"dlq:{reason}")
        log.error("event at %s/%d/%d dead-lettered (%s): %s",
                  msg.topic(), msg.partition(), msg.offset(), reason, detail)
        return f"dlq:{reason}"

    def run(self, stop_after_idle_s: float | None = None) -> Stats:
        consumer = Consumer({
            "bootstrap.servers": self.s.bootstrap,
            "group.id": self.s.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })
        state = {"assigned": False, "idle_since": time.monotonic()}

        def on_assign(_consumer, _partitions):
            # Idle time only counts once partitions are assigned; a fresh group can take seconds.
            state["assigned"] = True
            state["idle_since"] = time.monotonic()

        consumer.subscribe([self.s.topic], on_assign=on_assign)
        try:
            while True:
                msg = consumer.poll(0.5)
                if msg is None:
                    idle = time.monotonic() - state["idle_since"]
                    if stop_after_idle_s is not None and state["assigned"] and idle > stop_after_idle_s:
                        return self.stats
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("kafka error: %s", msg.error())
                    continue
                self.handle(msg)
                consumer.commit(message=msg, asynchronous=False)
                state["idle_since"] = time.monotonic()
        finally:
            consumer.close()
