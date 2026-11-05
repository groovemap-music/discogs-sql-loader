"""The purge veto on real RabbitMQ and real PostgreSQL, through the real handler.

A record in the DLQ never refreshed its row, so a purge would delete a current record. This
proves the veto survives what process memory cannot: a restart between the dead-letter and the
signal, and a dead-letter the broker made itself, which this process never hears about.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import aio_pika
import psycopg
import pytest
import pytest_asyncio
import structlog
from common import AsyncPostgreSQLPool, WriteAdmission
from orjson import dumps

import tableinator.tableinator as service
from tableinator.batch_processor import BatchConfig, PostgreSQLBatchProcessor
from tableinator.extraction_latch import probe_latch_relation


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.timeout(60)]

# Preserved before the default suite fixture stubs them: these are what is under test.
_DEAD_LETTERS_WAITING = service.dead_letters_waiting
_PURGE_WAS_VETOED = service.purge_was_vetoed
_RECORD_PURGE_VETO = service.record_purge_veto

DATA_TYPE = "artists"
FRESH_IDS = ("2", "3", "4", "5")
STALE_ID = "1"
STALE_AT = "2026-01-15T00:00:00Z"


@dataclass
class Stack:
    url: str
    dsn: str
    pool: AsyncPostgreSQLPool
    connection: aio_pika.abc.AbstractRobustConnection
    channel: aio_pika.abc.AbstractChannel
    queue: aio_pika.abc.AbstractQueue
    dlq: aio_pika.abc.AbstractQueue
    consuming: list[str]
    settled: asyncio.Queue[str]

    async def publish(self, body: bytes) -> None:
        await self.channel.default_exchange.publish(
            aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=self.queue.name
        )

    async def consume(self) -> None:
        handler = service.make_data_handler(DATA_TYPE)

        async def handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            await handler(message)
            if b"extraction_complete" in message.body:
                self.settled.put_nowait("marker")

        self.consuming.append(await self.queue.consume(handle))

    async def stop_consuming(self) -> None:
        await self.queue.cancel(self.consuming.pop())

    async def load_rows(self) -> None:
        """Write one row to go stale and four current ones, through the real batch path."""
        for record_id in (STALE_ID, *FRESH_IDS):
            await self.publish(dumps({"id": record_id, "name": f"Artist {record_id}", "sha256": record_id * 64}))
        assert service.batch_processor is not None
        async with asyncio.timeout(20):
            while await self.rows() != 1 + len(FRESH_IDS):
                await service.batch_processor.flush_queue(DATA_TYPE)
                await asyncio.sleep(0.05)
        async with await psycopg.AsyncConnection.connect(self.dsn, autocommit=True) as connection:
            await connection.execute("UPDATE artists SET updated_at = %s WHERE data_id = %s", (STALE_AT, STALE_ID))

    async def rows(self) -> int:
        async with await psycopg.AsyncConnection.connect(self.dsn) as connection:
            cursor = await connection.execute("SELECT count(*) FROM artists")
            row = await cursor.fetchone()
        assert row is not None
        return int(row[0])

    async def vetoed(self, version: str) -> list[str]:
        async with await psycopg.AsyncConnection.connect(self.dsn) as connection:
            cursor = await connection.execute(
                "SELECT purge_vetoed FROM loader_extraction_latch WHERE loader = 'discogs' AND version = %s", (version,)
            )
            row = await cursor.fetchone()
        return list(row[0]) if row else []

    async def signal(self, version: str) -> None:
        await self.publish(
            dumps(
                {
                    "type": "extraction_complete",
                    "version": version,
                    "started_at": "2026-02-02T00:00:00Z",
                    "record_counts": {DATA_TYPE: 5},
                    "total_processed": 5,
                }
            )
        )
        await asyncio.wait_for(self.settled.get(), 20)

    async def dead_lettered(self, count: int) -> None:
        async with asyncio.timeout(20):
            while await _DEAD_LETTERS_WAITING(DATA_TYPE) != count:
                await asyncio.sleep(0.05)


def fresh_processor(stack_pool: AsyncPostgreSQLPool, admission: WriteAdmission) -> PostgreSQLBatchProcessor:
    """What a restart leaves: a processor that has never heard of any dead letter."""
    return PostgreSQLBatchProcessor(stack_pool, BatchConfig(batch_size=10, flush_interval=0.05), admission)


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[Stack]:
    url = os.environ.get("TEST_AMQP_URL")
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not url or not dsn:
        pytest.skip("scripts/test-consumer-recovery.sh must set TEST_AMQP_URL and TEST_DATABASE_URL")

    pool = AsyncPostgreSQLPool(connection_params={"conninfo": dsn}, min_connections=1, max_connections=4, max_retries=1, health_check_interval=3600)
    await pool.initialize()
    admission = WriteAdmission(2)
    latch = await probe_latch_relation(pool, structlog.get_logger(__name__))
    assert latch is not None

    connection = await aio_pika.connect_robust(url)
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=20, global_=False)
    dlx_name = service.catalog_dead_letter_exchange_name(service.AMQP_CONSUMER_NAME, DATA_TYPE)
    exchange = await channel.declare_exchange(dlx_name, service.AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)
    dlq = await channel.declare_queue(
        name=service.catalog_dead_letter_queue_name(service.AMQP_CONSUMER_NAME, DATA_TYPE), durable=True, arguments={"x-queue-type": "classic"}
    )
    await dlq.bind(exchange)
    queue = await channel.declare_queue(
        name=service.catalog_queue_name(service.AMQP_CONSUMER_NAME, DATA_TYPE),
        durable=True,
        arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": dlx_name, "x-delivery-limit": 20},
    )
    await queue.purge()
    await dlq.purge()
    built = Stack(url, dsn, pool, connection, channel, queue, dlq, [], asyncio.Queue())

    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as admin:
        await admin.execute("DELETE FROM loader_extraction_latch WHERE loader = 'discogs'")
        await admin.execute("TRUNCATE artists CASCADE")

    with (
        patch.object(service, "active_connection", connection),
        patch.object(service, "dead_letters_waiting", _DEAD_LETTERS_WAITING),
        patch.object(service, "purge_was_vetoed", _PURGE_WAS_VETOED),
        patch.object(service, "record_purge_veto", _RECORD_PURGE_VETO),
        patch.object(service, "connection_pool", pool),
        patch.object(service, "write_admission", admission),
        patch.object(service, "extraction_latch", latch),
        patch.object(service, "durable_refresh_active", False),
        patch.object(service, "batch_processor", fresh_processor(pool, admission)),
        patch.object(service, "BATCH_MODE", True),
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "CONSUMER_CANCEL_DELAY", 0),
        patch.object(service, "refresh_derived_relations", _no_refresh),
    ):
        await built.consume()
        try:
            yield built
        finally:
            await queue.delete(if_unused=False, if_empty=False)
            await dlq.delete(if_unused=False, if_empty=False)
            await connection.close()
            await pool.close()


async def _no_refresh(*_args: Any, **_kwargs: Any) -> None:
    return None


async def poison(stack: Stack) -> None:
    """A record PostgreSQL itself refuses: jsonb cannot hold a NUL, so it is dead-lettered."""
    await stack.publish(dumps({"id": "99", "name": "bad\x00name", "sha256": "9" * 64}))
    assert service.batch_processor is not None
    await service.batch_processor.flush_queue(DATA_TYPE)
    await stack.dead_lettered(1)


async def test_a_clean_type_purges_and_records_no_veto(stack: Stack) -> None:
    await stack.load_rows()
    await stack.signal("20260201")
    assert await stack.rows() == len(FRESH_IDS)
    assert await stack.vetoed("20260201") == []


async def test_a_restart_between_the_dead_letter_and_the_signal_still_vetoes(stack: Stack) -> None:
    await stack.load_rows()
    await poison(stack)
    assert service.batch_processor is not None
    assert service.batch_processor.had_dlq_nacks(DATA_TYPE)

    with patch.object(service, "batch_processor", fresh_processor(stack.pool, service.write_admission)):
        assert not service.batch_processor.had_dlq_nacks(DATA_TYPE)
        await stack.signal("20260201")

    assert await stack.rows() == 1 + len(FRESH_IDS)
    assert await stack.vetoed("20260201") == [DATA_TYPE]


async def test_a_broker_delivery_limit_dead_letter_vetoes(stack: Stack) -> None:
    """Twenty-one closed channels: the broker dead-letters and this process never knows."""
    await stack.load_rows()
    await stack.stop_consuming()
    await stack.publish(dumps({"id": "77", "name": "never settled", "sha256": "7" * 64}))
    for _ in range(21):
        connection = await aio_pika.connect(stack.url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(stack.queue.name, passive=True)
        arrived = asyncio.Event()

        async def hold(_message: aio_pika.abc.AbstractIncomingMessage, arrived: asyncio.Event = arrived) -> None:
            arrived.set()

        await queue.consume(hold)
        await asyncio.wait_for(arrived.wait(), 5)
        await connection.close()
    await stack.dead_lettered(1)
    message = await stack.dlq.get(fail=True, timeout=5)
    assert message is not None
    assert (message.headers or {})["x-first-death-reason"] == "delivery_limit"
    await message.nack(requeue=True)
    await stack.dead_lettered(1)
    await stack.consume()
    assert service.batch_processor is not None
    assert not service.batch_processor.had_dlq_nacks(DATA_TYPE)

    await stack.signal("20260201")

    assert await stack.rows() == 1 + len(FRESH_IDS)
    assert await stack.vetoed("20260201") == [DATA_TYPE]


async def test_clearing_the_dlq_lets_the_next_extraction_purge(stack: Stack) -> None:
    """The operator path: the dead letter is handled, and the next dump purges."""
    await stack.load_rows()
    await poison(stack)
    await stack.signal("20260201")
    assert await stack.rows() == 1 + len(FRESH_IDS)

    await stack.dlq.purge()
    await stack.signal("20260301")

    assert await stack.rows() == len(FRESH_IDS)
    assert await stack.vetoed("20260301") == []
