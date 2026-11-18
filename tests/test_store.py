from idsync.events import IdentityEvent
from idsync.store import APPLIED, DUPLICATE, STALE, apply_event, get_access


def ev(event_id, version, ents=(("repo:payments", "read"),), op="upsert", identity="sa-ci"):
    return IdentityEvent(event_id, "github", identity, "service_account", version, op, tuple(ents))


def test_applies_then_ignores_redelivery(conn):
    assert apply_event(conn, ev("e1", 1)) == APPLIED
    assert apply_event(conn, ev("e1", 1)) == DUPLICATE
    assert get_access(conn, "sa-ci") == ("service_account", 1, [("repo:payments", "read")])


def test_late_event_cannot_roll_access_back(conn):
    apply_event(conn, ev("e3", 3, [("repo:payments", "read")]))
    # v2 arrives after v3 and would have re-granted admin.
    assert apply_event(conn, ev("e2", 2, [("repo:payments", "admin")])) == STALE
    assert get_access(conn, "sa-ci")[2] == [("repo:payments", "read")]


def test_each_event_replaces_the_full_entitlement_set(conn):
    apply_event(conn, ev("e1", 1, [("a", "read"), ("b", "write")]))
    apply_event(conn, ev("e2", 2, [("a", "read")]))
    assert get_access(conn, "sa-ci")[2] == [("a", "read")]


def test_delete_removes_access(conn):
    apply_event(conn, ev("e1", 1))
    assert apply_event(conn, ev("e2", 2, (), op="delete")) == APPLIED
    assert get_access(conn, "sa-ci") is None
