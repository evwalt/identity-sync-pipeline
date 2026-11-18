"""Postgres store: applies identity events idempotently and in version order."""

from __future__ import annotations

from importlib.resources import files

import psycopg

from .events import IdentityEvent

APPLIED, DUPLICATE, STALE = "applied", "duplicate", "stale"


def migrate(conn: psycopg.Connection) -> None:
    conn.execute(files("idsync").joinpath("schema.sql").read_text())
    conn.commit()


def apply_event(conn: psycopg.Connection, ev: IdentityEvent) -> str:
    """Apply one event in a single transaction.

    - Redelivery of the same event_id is a no-op (Kafka delivers at least once).
    - An event older than the stored version is recorded but not applied, so a
      late or replayed event can never roll an identity's access backwards.
    """
    with conn.transaction():
        claimed = conn.execute(
            "INSERT INTO processed_events (event_id, outcome) VALUES (%s, 'pending') "
            "ON CONFLICT (event_id) DO NOTHING",
            (ev.event_id,),
        ).rowcount
        if claimed == 0:
            return DUPLICATE

        # Lock the identity row so two consumers can't interleave updates to it.
        row = conn.execute(
            "SELECT version FROM identities WHERE id = %s FOR UPDATE", (ev.identity_id,)
        ).fetchone()
        if row is not None and row[0] >= ev.version:
            outcome = STALE
        else:
            conn.execute(
                "INSERT INTO identities (id, kind, source, version, deleted, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, now()) "
                "ON CONFLICT (id) DO UPDATE SET kind = EXCLUDED.kind, "
                "source = EXCLUDED.source, version = EXCLUDED.version, "
                "deleted = EXCLUDED.deleted, updated_at = now()",
                (ev.identity_id, ev.kind, ev.source, ev.version, ev.op == "delete"),
            )
            # Each event carries the identity's full entitlement set: replace, don't merge.
            conn.execute("DELETE FROM entitlements WHERE identity_id = %s", (ev.identity_id,))
            if ev.op == "upsert" and ev.entitlements:
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO entitlements (identity_id, resource, permission) "
                        "VALUES (%s, %s, %s)",
                        [(ev.identity_id, r, p) for r, p in ev.entitlements],
                    )
            outcome = APPLIED

        conn.execute(
            "UPDATE processed_events SET outcome = %s WHERE event_id = %s",
            (outcome, ev.event_id),
        )
        return outcome


def get_access(conn: psycopg.Connection, identity_id: str):
    """Returns (kind, version, [(resource, permission)]) or None if unknown or deleted."""
    row = conn.execute(
        "SELECT kind, version, deleted FROM identities WHERE id = %s", (identity_id,)
    ).fetchone()
    if row is None or row[2]:
        return None
    ents = conn.execute(
        "SELECT resource, permission FROM entitlements WHERE identity_id = %s "
        "ORDER BY resource, permission",
        (identity_id,),
    ).fetchall()
    return row[0], row[1], [(r, p) for r, p in ents]
