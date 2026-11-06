"""The loader's hold bounds through the real handler, on real RabbitMQ and real PostgreSQL.

`extraction_complete` runs the stale-row purge, so a marker that lands while its records are on the
broker purges rows those records were about to refresh. And RabbitMQ's clock on a delivery starts
when it is sent, so every wait in this process has to end before it runs out.

PostgreSQL is reached through a TCP proxy this test can cut, and a hung store is `docker pause`. This
runs in `scripts/test-consumer-recovery.sh`, whose broker times deliveries out after 2 s, so each
queue sets its own `x-consumer-timeout`. The connection is deliberately not robust: a robust one
consumes again after the broker cancels it, and the consumer count would read 1 either way.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import aio_pika
import aiormq
import psycopg
import pytest
import pytest_asyncio
import structlog
from common import AsyncPostgreSQLPool, WriteAdmission
from common import batch as engine
from orjson import dumps
from psycopg.conninfo import conninfo_to_dict, make_conninfo

import tableinator.tableinator as service
from tableinator.batch_processor import BatchConfig, PostgreSQLBatchProcessor
from tableinator.extraction_latch import probe_latch_relation


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

# Preserved before the default suite fixture stubs them: these are what is under test.
_DEAD_LETTERS_WAITING = service.dead_letters_waiting
_PURGE_WAS_VETOED = service.purge_was_vetoed
_RECORD_PURGE_VETO = service.record_purge_veto
_CONSECUTIVE_PURGE_VETOES = service.consecutive_purge_vetoes
_PURGE_STALE_ROWS = service.purge_stale_rows

DATA_TYPE = "artists"
HOLD_S = 2.0
KEPT = 20
TAIL = ("901", "902", "903")
STALE_AT = "2026-01-15T00:00:00Z"
STARTED_AT = "2026-02-02T00:00:00Z"
PREFETCH = 40
# Lux's shape, scaled: four windows unacked against one of capacity, and a hold 12 times the write
# bound. With a write bound near the hold, the waves behind capacity fit inside the stated bound and
# the capacity test passes without the fix.
CAPACITY = 3
PAST_CAPACITY = CAPACITY * 4
CAPACITY_HOLD_S = 2.0
CAPACITY_WRITE_S = 0.2
CAPACITY_GRACE_S = 0.05
CAPACITY_FLUSH_S = 0.5
CAPACITY_BOUND_S = CAPACITY_HOLD_S + 2 * (CAPACITY_WRITE_S + CAPACITY_GRACE_S) + CAPACITY_FLUSH_S


class TcpProxy:
    """A TCP proxy to PostgreSQL that can drop every connection and refuse new ones."""

    def __init__(self, host: str, port: int) -> None:
        self.host, self.port = host, port
        self.listen_port = 0
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def open(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", self.listen_port)
        self.listen_port = self._server.sockets[0].getsockname()[1]

    async def cut(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
        for writer in list(self._writers):
            writer.transport.abort()
        self._writers.clear()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_reader, upstream_writer = await asyncio.open_connection(self.host, self.port)
        self._writers.update((writer, upstream_writer))
        await asyncio.gather(self._pipe(reader, upstream_writer), self._pipe(upstream_reader, writer))

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(OSError, RuntimeError):
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        writer.close()
        self._writers.discard(writer)


@dataclass
class Stack:
    proxy: TcpProxy
    dsn: str
    channel: aio_pika.abc.AbstractChannel
    queue: aio_pika.abc.AbstractQueue
    broker: aiormq.abc.AbstractChannel
    processor: PostgreSQLBatchProcessor
    purges: list[int]

    async def publish(self, body: dict[str, Any]) -> None:
        await self.channel.default_exchange.publish(
            aio_pika.Message(dumps(body), delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=self.queue.name
        )

    async def broker_view(self) -> tuple[int, int]:
        """(ready messages, consumers), from a connection this test's consumer does not share."""
        declared = await self.broker.queue_declare(self.queue.name, passive=True)
        return int(declared.message_count), int(declared.consumer_count)

    async def scalar(self, query: Any, *params: Any) -> int:
        async with await psycopg.AsyncConnection.connect(self.dsn) as connection:
            row = await (await connection.execute(query, params or None)).fetchone()
        assert row is not None
        return int(row[0])

    async def stale_tail(self) -> int:
        return await self.scalar("SELECT count(*) FROM artists WHERE data_id = ANY(%s) AND updated_at < %s", list(TAIL), STARTED_AT)

    async def load_rows(self) -> None:
        """Write the kept rows and the tail, and leave the tail stale, as a prior extraction would."""
        for index in range(1, KEPT + 1):
            await self.publish({"id": str(index), "name": f"Artist {index}", "sha256": f"{index:064d}"})
        for value in TAIL:
            await self.publish({"id": value, "name": f"Artist {value}", "sha256": value * 21 + "0"})
        async with asyncio.timeout(20):
            while await self.scalar("SELECT count(*) FROM artists") != KEPT + len(TAIL):
                await self.processor.flush_queue(DATA_TYPE)
                await asyncio.sleep(0.05)
        async with await psycopg.AsyncConnection.connect(self.dsn, autocommit=True) as connection:
            await connection.execute("UPDATE artists SET updated_at = %s WHERE data_id = ANY(%s)", (STALE_AT, list(TAIL)))

    async def publish_tail_refresh(self) -> None:
        for value in TAIL:
            await self.publish({"id": value, "name": f"Renamed {value}", "sha256": value * 21 + "1"})

    async def tail_held(self) -> None:
        async with asyncio.timeout(15):
            while self.processor.get_stats()["pending"][DATA_TYPE] != len(TAIL):
                await asyncio.sleep(0.02)


@pytest.fixture
def config() -> BatchConfig:
    # flush_interval is long so the periodic flush cannot write the tail first: the drain answering
    # the marker has to be the one that returns it, which is the case under test.
    return BatchConfig(batch_size=50, flush_interval=600.0, max_hold_s=HOLD_S)


@pytest.fixture
def consumer_timeout_s() -> float:
    return 120.0


@pytest_asyncio.fixture
async def stack(config: BatchConfig, consumer_timeout_s: float) -> AsyncIterator[Stack]:
    url = os.environ.get("TEST_AMQP_URL")
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not url or not dsn:
        pytest.skip("scripts/test-consumer-recovery.sh must set TEST_AMQP_URL and TEST_DATABASE_URL")

    target = conninfo_to_dict(dsn)
    proxy = TcpProxy(str(target["host"]), int(str(target["port"])))
    await proxy.open()
    pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": make_conninfo(dsn, host="127.0.0.1", port=proxy.listen_port)},
        min_connections=1,
        max_connections=4,
        max_retries=1,
        health_check_interval=3600,
    )
    await pool.initialize()
    admission = WriteAdmission(2)
    latch = await probe_latch_relation(pool, structlog.get_logger(__name__))
    assert latch is not None

    connection = await aio_pika.connect(url)
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=PREFETCH, global_=False)
    # The service's own connection, for the DLQ read the purge veto makes. Only the consumer is not robust.
    active = await aio_pika.connect_robust(url)
    observer = await aiormq.connect(url)
    broker = await observer.channel()
    dlx_name = service.catalog_dead_letter_exchange_name(service.AMQP_CONSUMER_NAME, DATA_TYPE)
    exchange = await channel.declare_exchange(dlx_name, service.AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)
    dlq = await channel.declare_queue(
        name=service.catalog_dead_letter_queue_name(service.AMQP_CONSUMER_NAME, DATA_TYPE), durable=True, arguments={"x-queue-type": "classic"}
    )
    await dlq.bind(exchange)
    queue = await channel.declare_queue(
        name=service.catalog_queue_name(service.AMQP_CONSUMER_NAME, DATA_TYPE),
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": dlx_name,
            "x-delivery-limit": 20,
            "x-consumer-timeout": int(consumer_timeout_s * 1000),
        },
    )
    await queue.purge()
    await dlq.purge()
    processor = PostgreSQLBatchProcessor(pool, config, admission)
    built = Stack(proxy, dsn, channel, queue, broker, processor, [])

    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as admin:
        await admin.execute("DELETE FROM loader_extraction_latch WHERE loader = 'discogs'")
        await admin.execute("TRUNCATE artists CASCADE")

    async def purge(data_type: str, started_at: str, record_count: int | None = None) -> None:
        built.purges.append(await built.stale_tail())
        await _PURGE_STALE_ROWS(data_type, started_at, record_count)

    with (
        patch.object(service, "active_connection", active),
        patch.object(service, "dead_letters_waiting", _DEAD_LETTERS_WAITING),
        patch.object(service, "purge_was_vetoed", _PURGE_WAS_VETOED),
        patch.object(service, "record_purge_veto", _RECORD_PURGE_VETO),
        patch.object(service, "consecutive_purge_vetoes", _CONSECUTIVE_PURGE_VETOES),
        patch.object(service, "purge_stale_rows", purge),
        patch.object(service, "connection_pool", pool),
        patch.object(service, "write_admission", admission),
        patch.object(service, "extraction_latch", latch),
        patch.object(service, "durable_refresh_active", False),
        patch.object(service, "batch_processor", processor),
        patch.object(service, "BATCH_MODE", True),
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "CONSUMER_CANCEL_DELAY", 0),
        patch.object(service, "refresh_derived_relations", _no_refresh),
    ):
        tag = await queue.consume(service.make_data_handler(DATA_TYPE))
        periodic = asyncio.create_task(processor.periodic_flush())
        try:
            yield built
        finally:
            processor.shutdown()
            await asyncio.wait_for(asyncio.gather(periodic, return_exceptions=True), 15)
            with contextlib.suppress(Exception):
                await queue.cancel(tag)
            if proxy._server is None:
                await proxy.open()
            await queue.delete(if_unused=False, if_empty=False)
            await dlq.delete(if_unused=False, if_empty=False)
            await connection.close()
            await active.close()
            await observer.close()
            await asyncio.wait_for(pool.close(), 30)
            await proxy.cut()


async def _no_refresh(*_args: Any, **_kwargs: Any) -> None:
    return None


def docker(action: str) -> None:
    subprocess.run(["docker", action, os.environ["POSTGRES_CONTAINER"]], check=True, timeout=30)  # noqa: S603, S607


needs_container = pytest.mark.skipif(not os.environ.get("POSTGRES_CONTAINER"), reason="POSTGRES_CONTAINER not set")


@pytest.mark.timeout(90)
async def test_extraction_complete_cannot_purge_rows_whose_records_are_on_the_broker(stack: Stack) -> None:
    await stack.load_rows()
    await stack.publish_tail_refresh()
    await stack.tail_held()
    await stack.proxy.cut()
    assert await stack.processor._engine.flush(DATA_TYPE) is False
    await asyncio.sleep(HOLD_S + 0.2)
    await stack.proxy.open()

    await stack.publish(
        {
            "type": "extraction_complete",
            "version": "20261105",
            "started_at": STARTED_AT,
            "record_counts": {DATA_TYPE: KEPT + len(TAIL)},
            "total_processed": KEPT + len(TAIL),
        }
    )
    async with asyncio.timeout(60):
        while not (stack.purges and (await stack.broker_view())[0] == 0):
            await asyncio.sleep(0.05)
    await asyncio.sleep(0.5)

    rows = await stack.scalar("SELECT count(*) FROM artists")
    print(f"\nMARKER: stale tail rows when the purge ran: {stack.purges[-1]} of {len(TAIL)}, rows after: {rows} of {KEPT + len(TAIL)}")
    assert stack.purges[-1] == 0
    assert rows == KEPT + len(TAIL)


@needs_container
@pytest.mark.timeout(420)
@pytest.mark.parametrize("consumer_timeout_s", [300.0])
@pytest.mark.parametrize("config", [BatchConfig(batch_size=50, flush_interval=600.0, max_hold_s=1.0)])
async def test_a_hung_store_returns_its_deliveries_at_the_production_write_bound(stack: Stack) -> None:
    await stack.load_rows()
    await stack.publish_tail_refresh()
    await stack.tail_held()

    docker("pause")
    started = time.monotonic()
    try:
        flush = asyncio.create_task(stack.processor._engine.flush(DATA_TYPE))
        done, _ = await asyncio.wait({flush}, timeout=engine.DEFAULT_MAX_WRITE_S + engine.WRITE_CANCEL_GRACE_S + 5.0)
        elapsed = time.monotonic() - started
        assert done, f"the drain was still inside the write after {elapsed:.1f} s"
        await asyncio.sleep(1.1)
        await stack.processor._engine.flush(DATA_TYPE)
        await asyncio.sleep(0.5)
        state = stack.processor.get_stats()
        print(
            f"\nHANG: drain returned after {elapsed:.1f} s of docker pause, consumers {(await stack.broker_view())[1]}, pending {state['pending'][DATA_TYPE]}"
        )
    finally:
        docker("unpause")
    assert (await stack.broker_view())[1] == 1


@needs_container
@pytest.mark.timeout(120)
@pytest.mark.parametrize("consumer_timeout_s", [CAPACITY_BOUND_S + 2.0])
@pytest.mark.parametrize(
    "config",
    [
        BatchConfig(
            batch_size=50,
            min_batch_size=1,
            flush_interval=CAPACITY_FLUSH_S,
            backoff_initial=0.1,
            backoff_max=0.2,
            max_pending=CAPACITY,
            max_hold_s=CAPACITY_HOLD_S,
            max_write_s=CAPACITY_WRITE_S,
        )
    ],
)
async def test_past_capacity_nothing_reaches_the_consumer_timeout(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    """Frozen first, or the store writes these before the pause. The broker is the witness."""
    monkeypatch.setattr(engine, "WRITE_CANCEL_GRACE_S", CAPACITY_GRACE_S)
    frozen_s = 15.0

    docker("pause")
    fewest_consumers = 1
    try:
        for index in range(PAST_CAPACITY):
            await stack.publish({"id": str(index + 1), "name": f"Artist {index}", "sha256": f"{index:064d}"})
        deadline = time.monotonic() + frozen_s
        while time.monotonic() < deadline:
            fewest_consumers = min(fewest_consumers, (await stack.broker_view())[1])
            await asyncio.sleep(0.25)
    finally:
        docker("unpause")

    print(
        f"\nCAPACITY: {PAST_CAPACITY} delivered against {CAPACITY} slots, {frozen_s:g} s frozen, consumer timeout {CAPACITY_BOUND_S + 2.0:.2f} s, fewest consumers {fewest_consumers}"
    )
    assert fewest_consumers == 1, "the broker timed a delivery out and cancelled the consumer"

    async with asyncio.timeout(45):
        while await stack.scalar("SELECT count(*) FROM artists") != PAST_CAPACITY:
            await asyncio.sleep(0.1)
    # A delivery settled inside submit must leave nothing accepted, or health stalls until a restart.
    async with asyncio.timeout(15):
        while any(stack.processor._observer.outstanding_accepted().values()):
            await asyncio.sleep(0.1)
    assert stack.processor.stalled(0.001) == {}
