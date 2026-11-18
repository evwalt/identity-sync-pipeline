import os
import uuid

import psycopg
import pytest

from idsync.store import migrate

PG_URL = os.environ.get("IDSYNC_PG", "postgresql://postgres@127.0.0.1:54329/idsync")
KAFKA = os.environ.get("IDSYNC_KAFKA", "127.0.0.1:19092")


def _pg_available() -> bool:
    try:
        psycopg.connect(PG_URL, connect_timeout=2).close()
        return True
    except psycopg.OperationalError:
        return False


@pytest.fixture
def pg_url():
    if not _pg_available():
        pytest.skip("Postgres not running (scripts/dev_up.sh)")
    # A fresh schema per test keeps tests independent without recreating the database.
    schema = f"t_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(PG_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    url = f"{PG_URL}?options=-csearch_path%3D{schema}"
    with psycopg.connect(url) as conn:
        migrate(conn)
    yield url
    with psycopg.connect(PG_URL, autocommit=True) as admin:
        admin.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.fixture
def conn(pg_url):
    with psycopg.connect(pg_url) as c:
        yield c
