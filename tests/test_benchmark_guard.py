"""The write-path benchmark refuses a database that is not loopback, whatever the DSN text says.

No PostgreSQL is needed: `connected_peer` (the one function that opens a connection) is
replaced with the peer a real libpq would have reported for each DSN shape.
"""

import importlib.util
import socket
from pathlib import Path
from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from types import ModuleType


def _load() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "benchmark-write-path.py"
    spec = importlib.util.spec_from_file_location("benchmark_write_path", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bench = _load()
REMOTE = (socket.AF_INET, ("10.1.2.3", 5432))


@pytest.mark.parametrize(
    ("family", "peer", "expected"),
    [
        (socket.AF_UNIX, "/var/run/postgresql/.s.PGSQL.5432", True),
        (socket.AF_INET, ("127.0.0.1", 5432), True),
        (socket.AF_INET, ("127.9.9.9", 5432), True),
        (socket.AF_INET6, ("::1", 5432, 0, 0), True),
        (socket.AF_INET6, ("::ffff:127.0.0.1", 5432, 0, 0), True),
        (socket.AF_INET, ("10.1.2.3", 5432), False),
        (socket.AF_INET6, ("2001:db8::1", 5432, 0, 0), False),
        (socket.AF_INET6, ("::ffff:10.1.2.3", 5432, 0, 0), False),
    ],
)
def test_peer_is_local(family: int, peer: object, expected: bool) -> None:
    assert bench.peer_is_local(family, peer) is expected


@pytest.mark.parametrize(
    "dsn",
    [
        # hostaddr wins over host: libpq connects to 10.1.2.3 although the text says localhost.
        "host=localhost hostaddr=10.1.2.3 dbname=x",
        # a service entry supplies the real host; the DSN text names none.
        "service=prod",
        # an empty host with PGHOST (or PGHOSTADDR) in the environment points anywhere.
        "dbname=x",
    ],
)
def test_a_dsn_that_connects_remotely_is_refused_whatever_it_says(dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bench, "connected_peer", lambda _dsn: REMOTE)
    with pytest.raises(SystemExit, match="not loopback"):
        bench.require_local_database(dsn)


def test_the_override_permits_remote_writes_but_is_off_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bench, "connected_peer", lambda _dsn: REMOTE)
    bench.require_local_database("dbname=x", allow_remote=True)
    with pytest.raises(SystemExit):
        bench.require_local_database("dbname=x")


def test_a_loopback_connection_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bench, "connected_peer", lambda _dsn: (socket.AF_INET, ("127.0.0.1", 5432)))
    bench.require_local_database("host=localhost")
