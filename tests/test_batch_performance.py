"""Performance regression tests for the PostgreSQL batch boundary."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from tableinator.batch_processor import BatchConfig, PostgreSQLBatchProcessor


def _pool() -> MagicMock:
    cursor = AsyncMock()
    cursor.fetchall.return_value = []
    cursor_context = MagicMock()
    cursor_context.__aenter__ = AsyncMock(return_value=cursor)
    cursor_context.__aexit__ = AsyncMock(return_value=None)
    transaction = AsyncMock()
    connection = AsyncMock()
    connection.set_autocommit = AsyncMock()
    connection.transaction = MagicMock(return_value=transaction)
    connection.cursor = MagicMock(return_value=cursor_context)
    connection_context = MagicMock()
    connection_context.__aenter__ = AsyncMock(return_value=connection)
    connection_context.__aexit__ = AsyncMock(return_value=None)
    pool = MagicMock()
    pool.connection.return_value = connection_context
    return pool


async def _add(processor: PostgreSQLBatchProcessor, data_type: str, count: int) -> None:
    for index in range(count):
        await processor.add_message(
            data_type,
            {"id": f"{data_type}-{index}", "name": f"Record {index}", "sha256": f"hash-{index}"},
            AsyncMock(),
            AsyncMock(),
        )


@pytest.mark.asyncio
async def test_batch_size_500_processes_1000_records_with_two_writes() -> None:
    processor = PostgreSQLBatchProcessor(_pool(), BatchConfig(batch_size=500, flush_interval=2, max_pending=5000))
    started = time.perf_counter()
    await _add(processor, "artists", 1000)
    await processor.flush_all()
    assert time.perf_counter() - started < 1.5
    assert processor.processed_counts["artists"] == 1000
    assert processor.batch_counts["artists"] == 2


@pytest.mark.asyncio
async def test_four_entity_streams_use_the_pool_and_preserve_counts() -> None:
    pool = _pool()
    processor = PostgreSQLBatchProcessor(pool, BatchConfig(batch_size=500, flush_interval=2, max_pending=5000))
    await asyncio.gather(*(_add(processor, data_type, 500) for data_type in ("artists", "labels", "masters", "releases")))
    await processor.flush_all()
    assert pool.connection.call_count >= 4
    assert sum(processor.processed_counts.values()) == 2000


@pytest.mark.asyncio
async def test_mocked_throughput_remains_above_500_records_per_second() -> None:
    processor = PostgreSQLBatchProcessor(_pool(), BatchConfig(batch_size=500, flush_interval=2, max_pending=5000))
    started = time.perf_counter()
    await _add(processor, "artists", 5000)
    await processor.flush_all()
    throughput = 5000 / (time.perf_counter() - started)
    assert throughput >= 500
