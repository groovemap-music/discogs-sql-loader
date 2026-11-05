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
    # The session fixture has already applied the schema here, so the database guard (tested below on
    # a database of its own) would rightly refuse it; only the connection is under test.
    target = bench.inspect_target(dsn)
    assert bench.peer_is_local(target.family, target.peer)
    # An empty host with PGHOST/PGPORT in the environment resolves to the same peer.
    from psycopg.conninfo import conninfo_to_dict

    parts = conninfo_to_dict(dsn)
    monkeypatch.setenv("PGHOST", str(parts.get("host", "127.0.0.1")))
    monkeypatch.setenv("PGPORT", str(parts.get("port", 5432)))
    monkeypatch.setenv("PGPASSWORD", str(parts.get("password", "")))
    target = bench.inspect_target(f"user={parts.get('user', 'postgres')} dbname={parts.get('dbname', 'postgres')}")
    assert bench.peer_is_local(target.family, target.peer)


def test_a_database_with_tables_is_refused_until_it_carries_the_marker() -> None:
    import psycopg
    from psycopg import sql

    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is required")
    bench = _bench()
    name = f"guard_probe_{os.getpid()}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        probe = psycopg.conninfo.make_conninfo(dsn, dbname=name)
        assert "hostaddr=127.0.0.1" in bench.require_local_database(probe)  # empty: passes, and pins
        with psycopg.connect(probe, autocommit=True) as conn:
            conn.execute("CREATE TABLE somebody_elses (id int)")
        with pytest.raises(SystemExit, match="already holds"):
            bench.require_local_database(probe)
        bench.mark_benchmark_database(probe)
        pinned = bench.require_local_database(probe)  # marked: ours
        with psycopg.connect(pinned) as conn:  # the pinned DSN really connects
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
