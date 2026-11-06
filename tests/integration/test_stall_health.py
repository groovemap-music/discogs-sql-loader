"""Health, observer accounting and channel isolation, proved on a real RabbitMQ and PostgreSQL.

The stall is real: another session holds an ACCESS EXCLUSIVE lock on the table, so the loader's own
writer blocks in PostgreSQL and never returns, which is how submit blocked forever with health
green. Nothing but the stall bound is scaled down (`STALL_S`), so the suite runs in seconds.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import aio_pika
import aiormq
import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool, WriteAdmission
from orjson import dumps
from psycopg import OperationalError

import tableinator.tableinator as service
from tableinator.batch_processor import BatchConfig, PostgreSQLBatchProcessor


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    import psycopg

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.timeout(120)]

# The lane's broker times a delivery out after 2 s (consumer_timeout in test-consumer-recovery.sh), so
# every scenario here, stalled or busy, has to settle its deliveries inside that.
STALL_S = 0.8
# The health endpoint is evaluated on request, so the bound is the stall plus one probe interval.
PROBE_S = 0.25
DATA_TYPE = "artists"


async def until(predicate: Callable[[], Awaitable[bool] | bool], timeout: float, what: str) -> float:
    """Wait for ``predicate``; return how long it took. A timeout is a failure."""
    started = time.monotonic()
    while True:
        result = predicate()
        if (await result) if asyncio.iscoroutine(result) else result:
            return time.monotonic() - started
        assert time.monotonic() - started < timeout, f"timed out after {timeout:g}s waiting for {what}"
        await asyncio.sleep(0.05)


class Loader:
    """A real batch processor on a real pool, fed by real broker deliveries through the real handler."""

    def __init__(self, processor: PostgreSQLBatchProcessor, publisher: aio_pika.abc.AbstractChannel, queue_name: str) -> None:
        self.processor = processor
        self.publisher = publisher
        self.queue_name = queue_name

    async def publish(self, count: int, *, start: int = 1) -> None:
        for number in range(start, start + count):
            body = dumps({"id": str(number), "name": f"Artist {number}", "sha256": f"hash-{number}"})
            await self.publisher.default_exchange.publish(
                aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=self.queue_name
            )

    def health(self) -> dict[str, Any]:
        # The consumer is live, so the older "consumers died" check is not what reports here.
        with (
            patch.object(service, "connection_pool", object()),
            patch.object(service, "consumer_tags", {DATA_TYPE: "live"}),
            patch.object(service, "BATCH_STALL_S", STALL_S),
        ):
            return service.get_health_data()


async def _database_rows(connection: psycopg.AsyncConnection[Any]) -> int:
    row = await (await connection.execute("SELECT count(*) FROM artists")).fetchone()
    assert row is not None
    return int(row[0])


@pytest_asyncio.fixture
async def loader(postgres_connection: psycopg.AsyncConnection[Any]) -> AsyncIterator[Loader]:
    amqp_url = os.environ.get("TEST_AMQP_URL")
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not amqp_url or not database_url:
        pytest.skip("scripts/test-consumer-recovery.sh must set TEST_AMQP_URL and TEST_DATABASE_URL")

    search_path = (await (await postgres_connection.execute("SHOW search_path")).fetchone() or ("",))[0]
    pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": database_url, "options": f"-c search_path={search_path}"},
        min_connections=1,
        max_connections=2,
        max_retries=1,
        health_check_interval=3600,
    )
    await pool.initialize()
    config = BatchConfig(batch_size=4, min_batch_size=1, flush_interval=0.05, max_pending=12, backoff_initial=0.05, backoff_max=0.1, max_hold_s=0.5)
    processor = PostgreSQLBatchProcessor(pool, config, WriteAdmission(1))

    connection = await aio_pika.connect(amqp_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=8, global_=False)
    queue_name = f"gm-yl0k-{uuid.uuid4().hex[:8]}"
    queue = await channel.declare_queue(queue_name, durable=True, arguments={"x-queue-type": "quorum"})
    handler = service.make_data_handler(DATA_TYPE)

    with (
        patch.object(service, "batch_processor", processor),
        patch.object(service, "BATCH_MODE", True),
        patch.object(service, "shutdown_requested", False),
    ):
        periodic = asyncio.create_task(processor.periodic_flush())
        await queue.consume(handler)
        try:
            yield Loader(processor, channel, queue_name)
        finally:
            processor.shutdown()
            await asyncio.wait_for(asyncio.gather(periodic, return_exceptions=True), 10)
            await queue.delete(if_unused=False, if_empty=False)
            await connection.close()
            await pool.close()


async def test_a_batch_that_stops_settling_turns_health_unhealthy_within_the_bound(
    loader: Loader, postgres_connection: psycopg.AsyncConnection[Any]
) -> None:
    assert loader.health()["status"] == "healthy"

    await postgres_connection.set_autocommit(False)
    try:
        await postgres_connection.execute("LOCK TABLE artists IN ACCESS EXCLUSIVE MODE")
        blocked_at = time.monotonic()
        await loader.publish(4)

        async def unhealthy() -> bool:
            return loader.health()["status"] == "unhealthy"

        await asyncio.sleep(STALL_S / 2)
        assert loader.health()["status"] == "healthy", "a stall younger than the bound must not trip health"
        await until(unhealthy, STALL_S + 5, "health to turn unhealthy while the write is blocked")
        detected_after = time.monotonic() - blocked_at
        health = loader.health()
        print(f"\nSTALL: blocked write reported unhealthy after {detected_after:.2f}s (bound {STALL_S:g}s)")
        assert STALL_S <= detected_after <= STALL_S + PROBE_S + 1.0
        assert DATA_TYPE in health["stalled_batches"]
        assert health["stalled_batches"][DATA_TYPE] >= STALL_S

    finally:
        await postgres_connection.rollback()
        await postgres_connection.set_autocommit(True)
    await until(lambda: loader.health()["status"] == "healthy", 30, "health to recover once the write completes")
    await until(lambda: loader.processor.get_stats()["pending"][DATA_TYPE] == 0, 30, "the queue to drain")
    assert await _database_rows(postgres_connection) == 4
    assert loader.processor._observer.outstanding_accepted()[DATA_TYPE] == 0


async def test_a_long_busy_flush_that_keeps_settling_does_not_trip_health(loader: Loader, postgres_connection: psycopg.AsyncConnection[Any]) -> None:
    """The oldest delivery waits well past the bound behind slow flushes, yet every flush settles."""
    real_write = loader.processor._process_batch

    async def slow_write(data_type: str, messages: list[Any]) -> Any:
        await asyncio.sleep(0.5)
        return await real_write(data_type, messages)

    total = 12
    with patch.object(loader.processor, "_process_batch", slow_write):
        started = time.monotonic()
        await loader.publish(total)
        oldest_seen = 0.0
        statuses: set[str] = set()
        while loader.processor.get_stats()["processed"][DATA_TYPE] < total:
            assert time.monotonic() - started < 100, "the load did not finish"
            oldest_seen = max(
                oldest_seen,
                loader.processor._snapshot()["oldest_pending_age_s"][DATA_TYPE],
                loader.processor._observer.oldest_unsettled_age_s(DATA_TYPE),
            )
            statuses.add(loader.health()["status"])
            await asyncio.sleep(0.1)

    print(f"\nBUSY: {total} records, oldest unsettled delivery reached {oldest_seen:.1f}s (bound {STALL_S:g}s), statuses seen {sorted(statuses)}")
    assert oldest_seen > STALL_S, "the load was not heavy enough to prove age alone is the wrong signal"
    assert statuses == {"healthy"}
    assert await _database_rows(postgres_connection) == total


async def test_requeued_and_lost_deliveries_leave_nothing_in_the_observer(loader: Loader) -> None:
    """Held, requeued, dead-lettered and lost deliveries each release their accepted_at entry."""
    observer = loader.processor._observer
    real_write = loader.processor._process_batch
    outage = True

    async def write(data_type: str, messages: list[Any]) -> Any:
        if outage:
            raise OperationalError("server closed the connection unexpectedly")
        return await real_write(data_type, messages)

    with patch.object(loader.processor, "_process_batch", write):
        await loader.publish(4)
        # Held past max_hold_s, the deliveries go back to the broker and come back: each pass is a
        # REQUEUE settlement that must release the entry its acceptance created.
        await until(lambda: observer.requeued_counts[DATA_TYPE] >= 4, 30, "held deliveries to be requeued to the broker")
        outage = False
        await until(lambda: observer.processed_counts[DATA_TYPE] == 4, 30, "the backlog to be written")
        await until(lambda: loader.processor.get_stats()["pending"][DATA_TYPE] == 0, 30, "the queue to drain")

    print(f"\nREQUEUE: requeued {observer.requeued_counts[DATA_TYPE]} times, outstanding accepted_at after drain {observer.outstanding_accepted()}")
    assert observer.outstanding_accepted() == dict.fromkeys(service.DATA_TYPES, 0)
    assert loader.processor.had_dlq_nacks(DATA_TYPE) is False


async def test_closing_one_types_channel_leaves_the_others_consuming() -> None:
    url = os.environ.get("TEST_AMQP_URL")
    if not url:
        pytest.skip("scripts/test-consumer-recovery.sh must set TEST_AMQP_URL")

    observer = await aiormq.connect(url)
    broker = await observer.channel()
    publishing = await aio_pika.connect(url)
    publisher = await publishing.channel()
    prefix = f"gm-yl0k-{uuid.uuid4().hex[:6]}"
    handled: list[str] = []
    names = {data_type: f"{prefix}-{data_type}" for data_type in service.DATA_TYPES}
    manager_connection = await aio_pika.connect_robust(url)

    async def consumers(data_type: str) -> int:
        return int((await broker.queue_declare(names[data_type], passive=True)).consumer_count)

    async def on_data_message(message: Any, data_type: str) -> None:
        handled.append(data_type)
        await message.ack()

    try:
        for name in names.values():
            await publisher.declare_queue(
                name,
                durable=True,
                arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": f"{name}.dlx", "x-delivery-limit": 20},
            )
        with (
            patch.object(service, "active_connection", manager_connection),
            patch.object(service, "on_data_message", on_data_message),
            patch.object(service, "catalog_queue_name", lambda _consumer, data_type: names[data_type]),
            patch.object(service, "catalog_dead_letter_exchange_name", lambda _consumer, data_type: f"{names[data_type]}.dlx"),
            patch.object(service, "consumer_tags", {}),
            patch.object(service, "delivery_channels", {}),
            patch.object(service, "queues", {}),
        ):
            for data_type in service.DATA_TYPES:
                service.consumer_tags[data_type] = await service._start_consumer(data_type)
            channels = {data_type: service.delivery_channels[data_type] for data_type in service.DATA_TYPES}
            assert len({id(channel) for channel in channels.values()}) == len(service.DATA_TYPES)
            before = {data_type: await consumers(data_type) for data_type in service.DATA_TYPES}

            await channels["labels"].close()
            after = {data_type: await consumers(data_type) for data_type in service.DATA_TYPES}
            print(f"\nCHANNELS: consumers per queue before {before}, after closing the labels channel {after}")
            assert before == dict.fromkeys(service.DATA_TYPES, 1)
            assert after == {"artists": 1, "labels": 0, "masters": 1, "releases": 1}

            for data_type in ("artists", "masters", "releases"):
                await publisher.default_exchange.publish(aio_pika.Message(b'{"id": "1"}'), routing_key=names[data_type])
            await until(lambda: sorted(handled) == ["artists", "masters", "releases"], 10, "the surviving queues to consume")

            tags = dict(service.consumer_tags)
            assert await service._recover_data_type("labels")
            assert await consumers("labels") == 1
            assert {data_type: tag for data_type, tag in service.consumer_tags.items() if data_type != "labels"} == {
                data_type: tag for data_type, tag in tags.items() if data_type != "labels"
            }
            assert service.delivery_channels["labels"] is not channels["labels"]
            for data_type in ("artists", "masters", "releases"):
                assert service.delivery_channels[data_type] is channels[data_type]
            await service._close_delivery_channels()
    finally:
        for name in names.values():
            with_queue = await broker.queue_delete(name)
            _ = with_queue
        await manager_connection.close()
        await publishing.close()
        await observer.close()
