"""Time the loader's real batch write path on a disposable PostgreSQL.

Drives `PostgreSQLBatchWriter.process_batch` through a real `AsyncPostgreSQLPool` and
`WriteAdmission` with deterministic, Discogs-shaped synthetic releases, then prints one JSON
line: wall seconds, releases per second, and WAL bytes written. Absolute rates are not
comparable to a production host; compare variants (a change before and after, batch sizes,
worker counts) from the same machine and the same `--seed`.

Use only against a throwaway database: it writes rows and applies the pinned schema.
`scripts/benchmark-write-path.sh` starts and removes one. `TEST_DATABASE_URL` is required.
"""

import argparse
import asyncio
import ipaddress
import json
import os
import random
import socket
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import psycopg
from common import AsyncPostgreSQLPool, WriteAdmission
from groovemap_schema.postgres import create_postgres_schema
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from tableinator.batch_writer import PostgreSQLBatchWriter
from tableinator.media import media_for_release


GENRES = ["Rock", "Electronic", "Jazz", "Pop", "Hip Hop", "Folk, World, & Country", "Classical", "Funk / Soul"]
STYLES = [f"Style {index}" for index in range(60)]
ROLES = [
    "Producer",
    "Mastered By",
    "Engineer",
    "Mixed By",
    "Written-By",
    "Vocals",
    "Guitar",
    "Bass",
    "Drums",
    "Design",
    "Photography By",
    "Artwork By [Cover]",
    "Remix",
    "Recorded By",
    "Executive-Producer",
]
FORMATS = [
    {"name": "Vinyl", "qty": "1", "descriptions": {"description": ["LP", "Album"]}},
    {"name": "CD", "qty": "1", "descriptions": {"description": ["Album"]}},
    {"name": "Vinyl", "qty": "1", "descriptions": {"description": ['12"', "EP"]}},
    {"name": "Cassette", "qty": "1", "descriptions": {"description": ["Album"]}},
]


def peer_is_local(family: int, peer: Any) -> bool:
    """Return whether a connected socket's peer is a Unix socket or a loopback address."""
    if family == socket.AF_UNIX:
        return True
    if family not in (socket.AF_INET, socket.AF_INET6):
        return False
    try:
        address = ipaddress.ip_address(str(peer[0]).split("%", 1)[0])
    except ValueError, TypeError, IndexError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_loopback


MARKER = "discogs-sql-loader write-path benchmark"
"""The database comment the benchmark leaves after it applies its schema, so a rerun recognises its own."""


@dataclass(frozen=True)
class Target:
    """What one verification connection learned about where a DSN really points."""

    family: int
    peer: Any
    host: str
    port: str
    relations: int
    marker: str | None


def inspect_target(dsn: str) -> Target:
    """Connect with `dsn` and report the socket libpq really opened and what the database holds.

    The DSN text cannot say where libpq connects: `hostaddr`, a `service` entry, and `PGHOST`
    or `PGHOSTADDR` in the environment all override or replace its `host`. The open socket is
    the only answer. This issues two read-only queries and no DDL or write.
    """
    with psycopg.connect(dsn, connect_timeout=10, autocommit=True) as connection:
        duplicate = socket.socket(fileno=os.dup(connection.pgconn.socket))
        try:
            family, peer = duplicate.family, duplicate.getpeername()
        finally:
            duplicate.close()
        row = connection.execute(
            "SELECT (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            " WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')), "
            "shobj_description((SELECT oid FROM pg_database WHERE datname = current_database()), 'pg_database')"
        ).fetchone()
        assert row is not None
        return Target(family, peer, connection.info.host, str(connection.info.port), int(row[0]), row[1])


def _has_several_hosts(dsn: str) -> bool:
    """Return whether the DSN, or the environment it falls back on, names more than one host."""
    parts = conninfo_to_dict(dsn)
    values = [parts.get("host"), parts.get("hostaddr"), os.environ.get("PGHOST"), os.environ.get("PGHOSTADDR")]
    return any("," in str(value) for value in values if value)


def pinned_dsn(dsn: str, target: Target) -> str:
    """Return `dsn` with the verified address written into it, so later connections cannot go elsewhere.

    Explicit parameters beat `service` and the `PG*` environment, so a pool opened with the
    result reaches the host that was checked, not whatever the name resolves to next time.
    """
    if target.family == socket.AF_UNIX:
        return make_conninfo(dsn, host=target.host, port=target.port, hostaddr="")
    return make_conninfo(dsn, host=str(target.peer[0]), hostaddr=str(target.peer[0]), port=str(target.peer[1]))


def require_local_database(dsn: str, *, allow_remote: bool = False) -> str:
    """Exit unless `dsn` is safe to write to, before any DDL or write; return the DSN to use afterwards.

    Refused: a DSN naming more than one host; a connection that is not loopback or a Unix
    socket (unless `allow_remote`, which only the benchmark's own writes may use); and a
    database that already holds user relations without the marker this benchmark leaves,
    because a port-forward looks like loopback and the marker is what says it is ours.
    The returned DSN pins the verified address.
    """
    if _has_several_hosts(dsn):
        raise SystemExit("TEST_DATABASE_URL names more than one host; use exactly one so the checked address is the one used")
    try:
        target = inspect_target(dsn)
    except psycopg.Error as error:
        raise SystemExit(f"cannot connect to TEST_DATABASE_URL to check where it points: {type(error).__name__}") from error
    if not peer_is_local(target.family, target.peer) and not allow_remote:
        raise SystemExit(
            f"TEST_DATABASE_URL connects to {target.peer!r}, not loopback or a Unix socket; "
            "this script writes rows, so pass --allow-remote-host only for a disposable remote database"
        )
    if target.relations and target.marker != MARKER:
        raise SystemExit(
            f"TEST_DATABASE_URL points at a database that already holds {target.relations} relations and is not this "
            "benchmark's own; use an empty disposable database (a port-forward also looks like loopback)"
        )
    return pinned_dsn(dsn, target)


def mark_benchmark_database(dsn: str) -> None:
    """Leave the marker that lets a rerun recognise this database as the benchmark's own."""
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("COMMENT ON DATABASE {} IS {}").format(sql.Identifier(connection.info.dbname), sql.Literal(MARKER)))


@dataclass(frozen=True)
class Record:
    """One delivery as the writer sees it."""

    data_id: str
    data: dict[str, Any]
    sha256: str


def _zipf(rng: random.Random, population: int) -> int:
    """Pick an id with a heavy head, so popular entities collide across a batch as real ones do."""
    return min(population - 1, int(population ** rng.random()) - 1)


def release(rng: random.Random, index: int, entities: int) -> Record:
    """Build one release with artists, labels, tags, credits, companies, and tracks."""
    data_id = str(1_000_000 + index)
    track = [
        {
            "position": f"A{number}",
            "extraartists": {"artist": [{"id": str(_zipf(rng, entities)), "name": f"Person {_zipf(rng, entities)}", "role": rng.choice(ROLES)}]}
            if rng.random() < 0.3
            else None,
        }
        for number in range(1, rng.randint(4, 16))
    ]
    data = {
        "id": data_id,
        "title": f"Release {index}",
        "year": str(rng.choice([1969, 1984, 1999, 2005, 2016, 2021])),
        "artists": [{"id": str(_zipf(rng, entities))} for _ in range(rng.randint(1, 3))],
        "labels": [{"id": str(_zipf(rng, entities))} for _ in range(rng.randint(1, 2))],
        "genres": rng.sample(GENRES, rng.randint(1, 2)),
        "styles": rng.sample(STYLES, rng.randint(1, 4)),
        "extraartists": [
            {"id": str(_zipf(rng, entities)), "name": f"Person {_zipf(rng, entities)}", "role": rng.choice(ROLES)} for _ in range(rng.randint(0, 14))
        ],
        "companies": {
            "items": [
                {
                    "discogs_id": str(_zipf(rng, entities)),
                    "name": f"Company {_zipf(rng, entities)}",
                    "role": "Pressed By",
                    "role_category": "manufacture",
                }
                for _ in range(rng.randint(0, 4))
            ]
        },
        "formats": [rng.choice(FORMATS)],
        "tracklist": {"track": [{key: value for key, value in item.items() if value is not None} for item in track]},
    }
    return Record(data_id, data, f"hash-{data_id}")


def artist(rng: random.Random, index: int, entities: int) -> Record:
    """Build one artist: a name plus group memberships and aliases referencing other artists."""
    data_id = str(2_000_000 + index)
    data = {
        "id": data_id,
        "name": f"Artist {index}",
        "profile": "x" * rng.randint(0, 800),
        "members": [{"id": str(_zipf(rng, entities))} for _ in range(rng.choice([0, 0, 0, 2, 4, 8]))],
        "groups": [{"id": str(_zipf(rng, entities))} for _ in range(rng.choice([0, 0, 1, 2]))],
        "aliases": [{"id": str(_zipf(rng, entities))} for _ in range(rng.choice([0, 0, 1, 3]))],
    }
    return Record(data_id, data, f"hash-{data_id}")


def master(rng: random.Random, index: int, entities: int) -> Record:
    """Build one master release: artists, genres, and styles."""
    data_id = str(3_000_000 + index)
    data = {
        "id": data_id,
        "title": f"Master {index}",
        "year": str(rng.choice([1969, 1984, 1999, 2005, 2016, 2021])),
        "artists": [{"id": str(_zipf(rng, entities))} for _ in range(rng.randint(1, 3))],
        "genres": rng.sample(GENRES, rng.randint(1, 2)),
        "styles": rng.sample(STYLES, rng.randint(1, 4)),
    }
    return Record(data_id, data, f"hash-{data_id}")


BUILDERS = {"releases": release, "artists": artist, "masters": master}


async def statement_breakdown(dsn: str, limit: int) -> list[dict[str, Any]]:
    """Return the costliest statements since `pg_stat_statements_reset()`, by server time."""
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
        cursor = await connection.execute(
            "SELECT left(regexp_replace(query, '\\s+', ' ', 'g'), 110), calls, rows, round(total_exec_time::numeric, 1), wal_bytes "
            "FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) "
            "AND query NOT LIKE '%%pg_stat_statements%%' ORDER BY total_exec_time DESC LIMIT %s",
            (limit,),
        )
        return [
            {"statement": query, "calls": calls, "rows": rows, "server_ms": float(ms), "wal_kb": round(int(wal) / 1024)}
            for query, calls, rows, ms, wal in await cursor.fetchall()
        ]


async def wal_position(dsn: str) -> int:
    """Return the current WAL insert position in bytes."""
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
        cursor = await connection.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint")
        row = await cursor.fetchone()
        assert row is not None
        return int(row[0])


async def run(args: argparse.Namespace, dsn: str) -> dict[str, Any]:
    """Write `args.releases` releases in batches across `args.workers` concurrent writers."""
    pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": dsn},
        min_connections=1,
        max_connections=args.workers + 3,
        max_retries=1,
        health_check_interval=3600,
    )
    await pool.initialize()
    try:
        failures = await create_postgres_schema(pool)
        if failures:
            raise SystemExit(f"{failures} schema statements failed")
        mark_benchmark_database(dsn)
        rng = random.Random(args.seed)  # noqa: S311 -- deterministic load shape, not security
        entities = max(1000, args.releases // 2)
        records = [BUILDERS[args.data_type](rng, index, entities) for index in range(args.releases)]
        batches = [records[start : start + args.batch_size] for start in range(0, len(records), args.batch_size)]
        writer = PostgreSQLBatchWriter(pool, MagicMock(), media_for_release, WriteAdmission(args.workers))
        queue: asyncio.Queue[list[Record]] = asyncio.Queue()
        for batch in batches:
            queue.put_nowait(batch)

        async def worker() -> None:
            while True:
                try:
                    batch = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                await writer.process_batch(args.data_type, batch)

        if args.statements:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
                await connection.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
                await connection.execute("SELECT pg_stat_statements_reset()")
        wal_before = await wal_position(dsn)
        started = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(args.workers)))
        seconds = time.perf_counter() - started
        wal_bytes = await wal_position(dsn) - wal_before
        breakdown = await statement_breakdown(dsn, args.statements) if args.statements else None
    finally:
        await pool.close()
    result: dict[str, Any] = {
        "data_type": args.data_type,
        "releases": args.releases,
        "batch_size": args.batch_size,
        "workers": args.workers,
        "seed": args.seed,
        "seconds": round(seconds, 2),
        "releases_per_second": round(args.releases / seconds, 1),
        "wal_mb": round(wal_bytes / 1_048_576, 1),
    }
    if breakdown is not None:
        result["statements"] = breakdown
    return result


def main() -> None:
    """Parse arguments and print the result as one JSON line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--releases", type=int, default=5000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--data-type", choices=sorted(BUILDERS), default="releases", help="entity table to write; --releases counts records of it")
    parser.add_argument(
        "--statements",
        type=int,
        default=0,
        metavar="N",
        help="also print the N costliest statements from pg_stat_statements (loopback or Unix-socket databases only: it creates the extension and resets its counters)",
    )
    parser.add_argument(
        "--allow-remote-host",
        action="store_true",
        help="permit a TEST_DATABASE_URL that connects somewhere other than loopback or a Unix socket; the script writes rows and applies the schema",
    )
    args = parser.parse_args()
    if min(args.releases, args.batch_size, args.workers) < 1:
        parser.error("--releases, --batch-size and --workers must be positive")
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        parser.error("TEST_DATABASE_URL is required (a disposable database)")
    # --statements runs CREATE EXTENSION and pg_stat_statements_reset(), which change shared
    # server state, so it never honours --allow-remote-host.
    dsn = require_local_database(dsn, allow_remote=args.allow_remote_host and not args.statements)
    print(json.dumps(asyncio.run(run(args, dsn))))


if __name__ == "__main__":
    main()
