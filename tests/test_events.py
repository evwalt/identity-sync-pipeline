import json

import pytest

from idsync.events import InvalidEvent, parse_event


def raw(**overrides):
    event = {
        "event_id": "e1",
        "source": "github",
        "identity_id": "sa-deploy-bot",
        "kind": "service_account",
        "version": 1,
        "op": "upsert",
        "entitlements": [{"resource": "repo:payments", "permission": "write"}],
    }
    event.update(overrides)
    return json.dumps(event).encode()


def test_parses_and_dedupes_entitlements():
    ev = parse_event(raw(entitlements=[
        {"resource": "b", "permission": "read"},
        {"resource": "a", "permission": "write"},
        {"resource": "b", "permission": "read"},
    ]))
    assert ev.entitlements == (("a", "write"), ("b", "read"))


@pytest.mark.parametrize("payload", [
    b"not json",
    b"[1, 2]",
    raw(kind="human"),
    raw(op="merge"),
    raw(version=0),
    raw(version="3"),
    raw(version=True),
    raw(identity_id=""),
    raw(entitlements=[{"resource": "x"}]),
])
def test_rejects_events_that_can_never_apply(payload):
    with pytest.raises(InvalidEvent):
        parse_event(payload)
