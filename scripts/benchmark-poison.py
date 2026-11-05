"""Measure what one poison record costs at each batch size, against a real PostgreSQL.

For each `--batch-size`, submits that many releases to the real `PostgreSQLBatchProcessor`
(real pool, real writer) with one record the database rejects (a NUL in a JSON string,
which `jsonb` cannot store), then drains the queue. Prints one JSON line per size: how many
deliveries were acked, how many were dead-lettered (rejected), how many writer calls it
took, and the wall time. Rejected minus one is the healthy records dead-lettered with the
poison one.

Same safety rules as `benchmark-write-path.py`: disposable database only, loopback unless
`--allow-remote-host`. Use `scripts/benchmark-write-path.sh`'s container recipe, or
`just benchmark-poison`.
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

from common import AsyncPostgreSQLPool, WriteAdmission
from groovemap_schema.postgres import create_postgres_schema

from tableinator.batch_processor import BatchConfig, PostgreSQLBatchProcessor


_SPEC = importlib.util.spec_from_file_location("benchmark_write_path", Path(__file__).with_name("benchmark-write-path.py"))
assert _SPEC is not None
assert _SPEC.loader is not None
_WRITE_PATH = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_WRITE_PATH)


async def measure(pool: Any, batch_size: int, position: float, seed: int, max_poison_retries: int) -> dict[str, Any]:
    """Drain one batch containing a single poison record and report the settlements."""
    rng = random.Random(seed)  # noqa: S311 -- deterministic load shape, not security
    acked: list[str] = []
    rejected: list[str] = []
    config = BatchConfig(batch_size=batch_size, max_pending=batch_size * 2, max_poison_retries=max_poison_retries, backoff_initial=0.001)
    processor = PostgreSQLBatchProcessor(pool, config, WriteAdmission(1))
    calls = 0
    original = processor._process_batch

    async def counted(data_type: str, messages: list[Any]) -> Any:
        nonlocal calls
        calls += 1
        return await original(data_type, messages)

    processor._process_batch = counted  # type: ignore[method-assign]
    poison_index = int(batch_size * position)
    started = time.perf_counter()
    for index in range(batch_size):
        record = _WRITE_PATH.release(rng, index + batch_size * 1000 + seed * 10_000_000, 1000)
        data = {**record.data, "sha256": record.sha256}
        if index == poison_index:
            data["title"] = "poison\u0000record"
        data_id = record.data_id

        async def ack(item: str = data_id) -> None:
            acked.append(item)

        async def nack(item: str = data_id) -> None:
            rejected.append(item)

        await processor.add_message("releases", data, ack, nack)
    while not await processor.flush_queue("releases"):
        pass
    seconds = time.perf_counter() - started
    return {
        "batch_size": batch_size,
        "poison_position": position,
        "max_poison_retries": max_poison_retries,
        "acked": len(acked),
        "rejected": len(rejected),
        "healthy_rejected": max(0, len(rejected) - 1),
        "writer_calls": calls,
        "seconds": round(seconds, 2),
    }


async def run(args: argparse.Namespace, dsn: str) -> None:
    """Apply the schema once, then measure each batch size on a fresh pool."""
    pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": dsn},
        min_connections=1,
        max_connections=4,
        max_retries=1,
        health_check_interval=3600,
    )
    await pool.initialize()
    try:
        failures = await create_postgres_schema(pool)
        if failures:
            raise SystemExit(f"{failures} schema statements failed")
        _WRITE_PATH.mark_benchmark_database(dsn)
        for batch_size in args.batch_size:
            print(json.dumps(await measure(pool, batch_size, args.position, args.seed, args.max_poison_retries)), flush=True)
    finally:
        await pool.close()


def main() -> None:
    """Parse arguments and measure each size."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, nargs="+", default=[100, 500, 1000, 2000, 4000])
    parser.add_argument("--position", type=float, default=0.5, help="where in the batch the poison record sits, 0 to 1")
    parser.add_argument("--max-poison-retries", type=int, default=BatchConfig.max_poison_retries)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--allow-remote-host", action="store_true")
    args = parser.parse_args()
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        parser.error("TEST_DATABASE_URL is required (a disposable database)")
    dsn = _WRITE_PATH.require_local_database(dsn, allow_remote=args.allow_remote_host)
    asyncio.run(run(args, dsn))


if __name__ == "__main__":
    main()
