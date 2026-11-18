"""Identity change events as they arrive from third-party integrations."""

from __future__ import annotations

import json
from dataclasses import dataclass

KINDS = {"service_account", "api_key", "oauth_app", "ai_agent"}
OPS = {"upsert", "delete"}


class InvalidEvent(Exception):
    """The event can never be applied (bad JSON or schema). Retrying cannot help."""


@dataclass(frozen=True)
class IdentityEvent:
    event_id: str
    source: str
    identity_id: str
    kind: str
    version: int
    op: str
    entitlements: tuple[tuple[str, str], ...]

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "event_id": self.event_id,
                "source": self.source,
                "identity_id": self.identity_id,
                "kind": self.kind,
                "version": self.version,
                "op": self.op,
                "entitlements": [
                    {"resource": r, "permission": p} for r, p in self.entitlements
                ],
            }
        ).encode()


def parse_event(raw: bytes) -> IdentityEvent:
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as e:
        raise InvalidEvent(f"not JSON: {e}") from e
    if not isinstance(data, dict):
        raise InvalidEvent("event is not an object")

    def text(field: str) -> str:
        value = data.get(field)
        if not isinstance(value, str) or not value:
            raise InvalidEvent(f"missing or empty {field}")
        return value

    version = data.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise InvalidEvent("version must be a positive integer")
    kind, op = text("kind"), text("op")
    if kind not in KINDS:
        raise InvalidEvent(f"unknown kind {kind!r}")
    if op not in OPS:
        raise InvalidEvent(f"unknown op {op!r}")

    raw_ents = data.get("entitlements", [])
    if not isinstance(raw_ents, list):
        raise InvalidEvent("entitlements must be a list")
    ents = []
    for e in raw_ents:
        if (
            not isinstance(e, dict)
            or not isinstance(e.get("resource"), str)
            or not isinstance(e.get("permission"), str)
        ):
            raise InvalidEvent("entitlement needs resource and permission strings")
        ents.append((e["resource"], e["permission"]))

    return IdentityEvent(
        event_id=text("event_id"),
        source=text("source"),
        identity_id=text("identity_id"),
        kind=kind,
        version=version,
        op=op,
        entitlements=tuple(sorted(set(ents))),
    )
