"""Profile each exact production refresh stage independently on a disposable graph.

This is diagnostic, not a replacement for the all-or-nothing production transaction.
Each stage commits separately to isolate its duration and bound an expensive counter;
`measure-derived-refresh.py` measures the real whole-pass transaction.
"""

import argparse
import asyncio
import json
import os
import time
from typing import Any

import psycopg

from tableinator.graph_counters import GRAPH_SCHEMA, PATH_REFRESH_FUNCTIONS, REFRESH_ORDER, _refill, _truncate, reconcile_additive_edges


class SilentLogger:
    """The profiler emits only its machine-readable result, not application logs."""

    def info(self, _event: str, **_fields: Any) -> None:
        """Suppress the source function's informational line."""

    def warning(self, _event: str, **_fields: Any) -> None:
        """Suppress the source function's warning line."""


async def main() -> None:
    """Run each stage with an independent bound and print JSON per stage."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()
    if args.timeout_seconds < 1:
        parser.error("--timeout-seconds must be positive")
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        parser.error("TEST_DATABASE_URL is required (a disposable populated database)")

    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
        await connection.execute("SELECT set_config('statement_timeout', %s, false)", (f"{args.timeout_seconds}s",))
        for stage in ("reconcile_additive_edges", *REFRESH_ORDER, *PATH_REFRESH_FUNCTIONS):
            started = time.perf_counter()
            try:
                async with connection.transaction(), connection.cursor() as cursor:
                    if stage == "reconcile_additive_edges":
                        rows = await reconcile_additive_edges(cursor, SilentLogger())
                    elif stage in REFRESH_ORDER:
                        await cursor.execute(_truncate(stage))
                        await cursor.execute(_refill(stage))
                        rows = cursor.rowcount
                    else:
                        await cursor.execute(f"SELECT * FROM {GRAPH_SCHEMA}.{stage}()")  # noqa: S608 -- fixed internal function names
                        rows = await cursor.fetchall()
                outcome = "committed"
            except psycopg.errors.QueryCanceled:
                outcome = "censored, transaction rolled back"
                rows = None
            print(
                json.dumps(
                    {
                        "stage": stage,
                        "seconds": round(time.perf_counter() - started, 3),
                        "outcome": outcome,
                        "rows": rows,
                        "statement_timeout_seconds": args.timeout_seconds,
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    asyncio.run(main())
