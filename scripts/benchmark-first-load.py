"""Measure a first-load-shaped write at scale, with and without deferred secondary indexes.

Loads `--releases` synthetic releases in `--chunk`-sized steps through the real batch writer
(the generator and the loopback guard come from `benchmark-write-path.py`) and prints one JSON
line per chunk (records per second so far in that chunk, database size, WAL) and a summary.
`--defer-indexes` drops every secondary index in `public` and `graph` first (primary keys and
unique indexes stay, because `ON CONFLICT` needs them), loads, then rebuilds them timed.

Disposable database only; run through `scripts/benchmark-first-load.sh`.
"""

import argparse
import asyncio
import importlib.util
import json
import os
import random
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import psycopg
from common import AsyncPostgreSQLPool, WriteAdmission
from groovemap_schema.postgres import create_postgres_schema

from tableinator.batch_writer import PostgreSQLBatchWriter
from tableinator.media import media_for_release


_SPEC = importlib.util.spec_from_file_location("benchmark_write_path", Path(__file__).with_name("benchmark-write-path.py"))
assert _SPEC is not None
assert _SPEC.loader is not None
_WRITE_PATH = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_WRITE_PATH)

SECONDARY_INDEXES = """
SELECT n.nspname, c.relname, pg_get_indexdef(i.indexrelid)
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname IN ('public', 'graph')
  AND NOT i.indisunique AND NOT i.indisprimary
  AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)
ORDER BY 1, 2
"""


def database_state(dsn: str) -> dict[str, Any]:
    """Return database size and WAL position in MB."""
    with psycopg.connect(dsn, autocommit=True) as connection:
        row = connection.execute(
            "SELECT pg_database_size(current_database()) / 1048576, pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') / 1048576"
        ).fetchone()
    assert row is not None
    return {"db_mb": int(row[0]), "wal_mb": int(row[1])}


async def run(args: argparse.Namespace, dsn: str) -> None:
    """Load in chunks and report the rate of each."""
    pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": dsn},
        min_connections=1,
        max_connections=args.workers + 3,
        max_retries=1,
        health_check_interval=3600,
    )
    await pool.initialize()
    try:
        if await create_postgres_schema(pool):
            raise SystemExit("schema statements failed")
        _WRITE_PATH.mark_benchmark_database(dsn)

        deferred: list[tuple[str, str, str]] = []
        dropped_in = 0.0
        if args.defer_indexes:
            with psycopg.connect(dsn, autocommit=True) as connection:
                deferred = list(connection.execute(SECONDARY_INDEXES).fetchall())
                started = time.perf_counter()
                for schema, name, _definition in deferred:
                    connection.execute(f'DROP INDEX "{schema}"."{name}"')
                dropped_in = time.perf_counter() - started
        before = database_state(dsn)
        writer = PostgreSQLBatchWriter(pool, MagicMock(), media_for_release, WriteAdmission(args.workers))
        rng = random.Random(args.seed)  # noqa: S311 -- deterministic load shape, not security
        entities = max(1000, args.releases // 2)
        loaded = 0
        load_started = time.perf_counter()
        while loaded < args.releases:
            count = min(args.chunk, args.releases - loaded)
            records = [_WRITE_PATH.release(rng, loaded + index, entities) for index in range(count)]
            batches = [records[start : start + args.batch_size] for start in range(0, count, args.batch_size)]
            queue: asyncio.Queue[list[Any]] = asyncio.Queue()
            for batch in batches:
                queue.put_nowait(batch)

            async def worker(queue: asyncio.Queue[list[Any]] = queue) -> None:
                while True:
                    try:
                        batch = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    await writer.process_batch("releases", batch)

            started = time.perf_counter()
            await asyncio.gather(*(worker() for _ in range(args.workers)))
            seconds = time.perf_counter() - started
            loaded += count
            print(json.dumps({"chunk_end": loaded, "records_per_second": round(count / seconds, 1), **database_state(dsn)}), flush=True)
        load_seconds = time.perf_counter() - load_started
        rebuild_seconds = 0.0
        if deferred:
            with psycopg.connect(dsn, autocommit=True) as connection:
                connection.execute(f"SET maintenance_work_mem = '{args.maintenance_work_mem}'")
                connection.execute("SET max_parallel_maintenance_workers = 2")
                started = time.perf_counter()
                for _schema, _name, definition in deferred:
                    connection.execute(definition)
                rebuild_seconds = time.perf_counter() - started
        after = database_state(dsn)
        print(
            json.dumps(
                {
                    "summary": True,
                    "releases": args.releases,
                    "defer_indexes": args.defer_indexes,
                    "indexes_deferred": len(deferred),
                    "load_seconds": round(load_seconds, 1),
                    "drop_seconds": round(dropped_in, 1),
                    "rebuild_seconds": round(rebuild_seconds, 1),
                    "total_seconds": round(load_seconds + dropped_in + rebuild_seconds, 1),
                    "load_wal_mb": after["wal_mb"] - before["wal_mb"],
                    "db_mb": after["db_mb"],
                }
            ),
            flush=True,
        )
    finally:
        await pool.close()


def main() -> None:
    """Parse arguments and run."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--releases", type=int, default=60000)
    parser.add_argument("--chunk", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--defer-indexes", action="store_true")
    parser.add_argument("--maintenance-work-mem", default="256MB")
    parser.add_argument("--allow-remote-host", action="store_true")
    args = parser.parse_args()
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        parser.error("TEST_DATABASE_URL is required (a disposable database)")
    dsn = _WRITE_PATH.require_local_database(dsn, allow_remote=args.allow_remote_host)
    asyncio.run(run(args, dsn))


if __name__ == "__main__":
    main()
