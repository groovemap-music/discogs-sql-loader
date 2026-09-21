"""Measure the production derived-relation pass on an already populated disposable DB.

The connection string is read from TEST_DATABASE_URL, never echoed or written. One run
executes the actual loader function, including its transaction and latch stamp. A second
run models retry/idempotence on the same unchanged graph; compare row counts separately.
"""

import argparse
import asyncio
import json
import os
import platform
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg

from tableinator.graph_counters import refresh_derived_relations


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


RELATIONS = (
    "artists",
    "labels",
    "masters",
    "releases",
    "graph.by_artist",
    "graph.on_label",
    "graph.in_genre",
    "graph.in_style",
    "graph.credited_on",
    "graph.issued_on",
    "graph.member_of",
    "graph.same_as",
    "graph.artist_member_of",
    "graph.vertex_degree",
)


class Pool:
    """One short-lived connection with the autocommit contract of the runtime pool."""

    def __init__(self, dsn: str, timeout_seconds: int) -> None:
        self.dsn = dsn
        self.timeout_seconds = timeout_seconds

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        async with await psycopg.AsyncConnection.connect(self.dsn, autocommit=True) as connection:
            await connection.execute("SELECT set_config('statement_timeout', %s, false)", (f"{self.timeout_seconds}s",))
            yield connection


class StageLogger:
    """Capture existing structured timing fields, excluding unneeded environment data."""

    def __init__(self) -> None:
        self.stages: dict[str, float] = {}

    def info(self, event: str, **fields: Any) -> None:
        duration = fields.get("duration_seconds")
        if duration is not None:
            relation = fields.get("relation") or fields.get("function")
            if relation is not None:
                self.stages[str(relation)] = float(duration)
            elif "Refreshed the derived graph relations" in event:
                self.stages["total_logged"] = float(duration)

    def warning(self, _event: str, **_fields: Any) -> None:
        """Warnings are not part of the timing record."""


async def metadata(dsn: str) -> dict[str, Any]:
    """Capture the exact database version, tuning, catalog scale, and relation sizes."""
    async with await psycopg.AsyncConnection.connect(dsn) as connection, connection.cursor() as cursor:
        await cursor.execute(
            "SELECT version(), current_setting('shared_buffers'), current_setting('work_mem'), current_setting('max_parallel_workers_per_gather')"
        )
        version, buffers, work_mem, workers = await cursor.fetchone() or (None, None, None, None)
        sizes: dict[str, dict[str, int]] = {}
        for relation in RELATIONS:
            await cursor.execute(f"SELECT count(*), pg_total_relation_size('{relation}') FROM {relation}")  # noqa: S608 -- fixed, internal relation names
            rows, bytes_ = await cursor.fetchone() or (0, 0)
            sizes[relation] = {"rows": rows, "bytes": bytes_}
    return {
        "database": version,
        "shared_buffers": buffers,
        "work_mem": work_mem,
        "max_parallel_workers_per_gather": workers,
        "host": platform.platform(),
        "host_cpu_count": os.cpu_count(),
        "relations": sizes,
    }


async def main() -> None:
    """Print newline-delimited JSON after each committed pass, including the retry."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=2, help="one first pass plus one unchanged-data retry by default")
    parser.add_argument("--timeout-seconds", type=int, default=1800, help="per-statement safety cap; a timeout is reported as a censored stage")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    if args.timeout_seconds < 1:
        parser.error("--timeout-seconds must be positive")
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        parser.error("TEST_DATABASE_URL is required (a disposable populated database)")

    before = await metadata(dsn)
    print(json.dumps({"kind": "environment", "at": datetime.now(UTC).isoformat(), **before}), flush=True)
    pool = Pool(dsn, args.timeout_seconds)
    for run in range(1, args.runs + 1):
        logger = StageLogger()
        started = time.perf_counter()
        try:
            counts = await refresh_derived_relations(pool, logger, f"benchmark-{run}")
        except psycopg.errors.QueryCanceled:
            print(
                json.dumps(
                    {
                        "kind": "censored_pass",
                        "run": run,
                        "elapsed_lower_bound_seconds": round(time.perf_counter() - started, 3),
                        "statement_timeout_seconds": args.timeout_seconds,
                        "completed_stages_seconds": logger.stages,
                        "transaction": "rolled back; relation rows and latch unchanged",
                    }
                ),
                flush=True,
            )
            break
        elapsed = time.perf_counter() - started
        after = await metadata(dsn)
        print(
            json.dumps(
                {
                    "kind": "pass",
                    "run": run,
                    "case": "initial" if run == 1 else "unchanged-data retry",
                    "total_seconds": round(elapsed, 3),
                    "stages_seconds": logger.stages,
                    "counter_rows": counts,
                    "relation_rows": {name: entry["rows"] for name, entry in after["relations"].items()},
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
