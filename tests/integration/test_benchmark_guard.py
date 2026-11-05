"""`connected_peer` reports the socket libpq really opened, against a real PostgreSQL."""

import importlib.util
import os
from pathlib import Path

import pytest


pytestmark = pytest.mark.integration


def _bench():
    path = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-write-path.py"
    spec = importlib.util.spec_from_file_location("benchmark_write_path", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_peer_comes_from_the_connection_not_the_dsn_text(monkeypatch: pytest.MonkeyPatch) -> None:
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is required")
    bench = _bench()
    # The integration database is the disposable loopback container: the guard passes it.
    bench.require_local_database(dsn)
    # An empty host with PGHOST/PGPORT in the environment resolves to the same peer.
    from psycopg.conninfo import conninfo_to_dict

    parts = conninfo_to_dict(dsn)
    monkeypatch.setenv("PGHOST", str(parts.get("host", "127.0.0.1")))
    monkeypatch.setenv("PGPORT", str(parts.get("port", 5432)))
    monkeypatch.setenv("PGPASSWORD", str(parts.get("password", "")))
    family, peer = bench.connected_peer(f"user={parts.get('user', 'postgres')} dbname={parts.get('dbname', 'postgres')}")
    assert bench.peer_is_local(family, peer)
