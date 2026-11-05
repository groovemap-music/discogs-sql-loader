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
import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import psycopg
from common import AsyncPostgreSQLPool, WriteAdmission
from groovemap_schema.postgres import create_postgres_schema

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
        rng = random.Random(args.seed)  # noqa: S311 -- deterministic load shape, not security
        entities = max(1000, args.releases // 2)
        records = [release(rng, index, entities) for index in range(args.releases)]
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
                await writer.process_batch("releases", batch)

        wal_before = await wal_position(dsn)
        started = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(args.workers)))
        seconds = time.perf_counter() - started
        wal_bytes = await wal_position(dsn) - wal_before
    finally:
        await pool.close()
    return {
        "releases": args.releases,
        "batch_size": args.batch_size,
        "workers": args.workers,
        "seed": args.seed,
        "seconds": round(seconds, 2),
        "releases_per_second": round(args.releases / seconds, 1),
        "wal_mb": round(wal_bytes / 1_048_576, 1),
    }


def main() -> None:
    """Parse arguments and print the result as one JSON line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--releases", type=int, default=5000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    if min(args.releases, args.batch_size, args.workers) < 1:
        parser.error("--releases, --batch-size and --workers must be positive")
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        parser.error("TEST_DATABASE_URL is required (a disposable database)")
    print(json.dumps(asyncio.run(run(args, dsn))))


if __name__ == "__main__":
    main()
