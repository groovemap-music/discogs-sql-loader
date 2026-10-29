"""Owned RabbitMQ/PostgreSQL proof for shared consumer admission and recovery."""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika
import psycopg
import pytest
from common import AsyncPostgreSQLPool, WriteAdmission
from orjson import dumps
from psycopg import sql

import tableinator.durable_refresh as durable
import tableinator.tableinator as service


pytestmark = pytest.mark.integration


async def _count_rows(dsn: str, schema: str) -> int:
    async with await psycopg.AsyncConnection.connect(dsn) as connection:
        cursor = await connection.execute(sql.SQL("SELECT count(*) FROM {}.writes").format(sql.Identifier(schema)))
        row = await cursor.fetchone()
        assert row is not None
        return int(row[0])


async def _declare_loader_queues(channel: Any) -> dict[str, Any]:
    queues = {}
    for entity in service.DATA_TYPES:
        exchange_name = service.catalog_exchange_name(entity)
        queue_name = service.catalog_queue_name(service.AMQP_CONSUMER_NAME, entity)
        dlx_name = service.catalog_dead_letter_exchange_name(service.AMQP_CONSUMER_NAME, entity)
        dlq_name = service.catalog_dead_letter_queue_name(service.AMQP_CONSUMER_NAME, entity)
        exchange = await channel.declare_exchange(exchange_name, service.AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)
        dlx = await channel.declare_exchange(dlx_name, service.AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)
        dlq = await channel.declare_queue(name=dlq_name, durable=True, auto_delete=False, arguments={"x-queue-type": "classic"})
        await dlq.bind(dlx)
        queue = await channel.declare_queue(
            name=queue_name,
            durable=True,
            auto_delete=False,
            arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": dlx_name, "x-delivery-limit": 20},
        )
        await queue.bind(exchange)
        queues[entity] = queue
    return queues


@pytest.mark.asyncio
async def test_real_consumers_bound_writes_and_recover_cancelled_delivery() -> None:
    """The loader handlers stay bounded and `_recover_consumers` settles redelivery."""
    dsn = os.environ.get("TEST_DATABASE_URL")
    amqp_url = os.environ.get("TEST_AMQP_URL")
    if not dsn or not amqp_url:
        pytest.skip("owned integration script must set TEST_DATABASE_URL and TEST_AMQP_URL")

    suffix = uuid.uuid4().hex
    schema = f"discogs_admission_{suffix}"
    admin = await psycopg.AsyncConnection.connect(dsn)
    await admin.set_autocommit(True)
    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    await admin.execute(sql.SQL("CREATE TABLE {}.writes (id text PRIMARY KEY, entity text NOT NULL)").format(sql.Identifier(schema)))
    await admin.close()

    raw_pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": dsn},
        min_connections=1,
        max_connections=2,
        max_retries=1,
        health_check_interval=3600,
    )
    await raw_pool.initialize()

    class TrackedPool:
        def __init__(self) -> None:
            self.active = 0
            self.peak = 0

        @asynccontextmanager
        async def connection(self) -> Any:
            self.active += 1
            self.peak = max(self.peak, self.active)
            try:
                async with raw_pool.connection() as connection:
                    yield connection
            finally:
                self.active -= 1

    pool = TrackedPool()
    admission = WriteAdmission(2)
    connection = await aio_pika.connect_robust(amqp_url)
    recovery_connection = None
    channels: list[Any] = []
    active_writers = 0
    peak_writers = 0
    recovery_id = ""
    recovery_started = asyncio.Event()
    block_recovery = True
    first_delivery_tasks: list[asyncio.Task[Any]] = []

    class TestPersistence:
        def __init__(self, connection_pool: Any, *_args: Any) -> None:
            self.connection_pool = connection_pool

        async def persist_record(self, data_type: str, data_id: str, _data: dict[str, Any]) -> str:
            nonlocal active_writers, peak_writers
            active_writers += 1
            peak_writers = max(peak_writers, active_writers)
            try:
                if data_id == recovery_id and block_recovery:
                    recovery_started.set()
                    await asyncio.Future()
                await asyncio.sleep(0.02)
                async with self.connection_pool.connection() as db:
                    await db.execute(
                        sql.SQL("INSERT INTO {}.writes (id, entity) VALUES (%s, %s)").format(sql.Identifier(schema)),
                        (data_id, data_type),
                    )
                return "processed"
            finally:
                active_writers -= 1

    queues: dict[str, Any] = {}
    try:
        with (
            patch.object(service, "BATCH_MODE", False),
            patch.object(service, "connection_pool", pool),
            patch.object(service, "write_admission", admission),
            patch.object(service, "PostgreSQLRecordPersistence", TestPersistence),
            patch.object(service, "message_counts", dict.fromkeys(service.DATA_TYPES, 0)),
            patch.object(service, "last_message_time", dict.fromkeys(service.DATA_TYPES, 0.0)),
            patch.object(service, "shutdown_requested", False),
            patch.object(service, "durable_refresh_active", False),
            patch.object(service, "consumer_tags", {}),
            patch.object(service, "queues", {}),
        ):
            channel = await connection.channel()
            channels.append(channel)
            await channel.set_qos(prefetch_count=2, global_=False)
            queues = await _declare_loader_queues(channel)
            for entity, queue in queues.items():
                service.queues[entity] = queue
                service.consumer_tags[entity] = await queue.consume(service.make_data_handler(entity))

            for index in range(4):
                for entity in service.DATA_TYPES:
                    data_id = str(uuid.uuid4())
                    body = dumps({"id": data_id, "name": "admission control", "sha256": f"{suffix}-{index}-{entity}"})
                    await channel.default_exchange.publish(aio_pika.Message(body=body), routing_key=queues[entity].name)

            deadline = time.monotonic() + 30
            while sum(service.message_counts.values()) < 16 and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            assert sum(service.message_counts.values()) == 16
            assert pool.peak <= 2
            assert peak_writers == 2
            assert pool.active == 0

            # Interrupt a real delivery while its database permit is held. Closing the
            # tagged transport makes RabbitMQ return it unacked to the durable queue.
            artist_queue = queues["artists"]
            await artist_queue.cancel(service.consumer_tags["artists"])
            recovery_id = str(uuid.uuid4())

            async def cancel_first(message: Any) -> None:
                task = asyncio.current_task()
                assert task is not None
                first_delivery_tasks.append(task)
                await service.on_data_message(message, "artists")

            first_consumer_tag = await artist_queue.consume(cancel_first)
            recovery_payload = dumps({"id": recovery_id, "name": "recovery", "sha256": f"{suffix}-recovery"})
            await channel.default_exchange.publish(aio_pika.Message(body=recovery_payload), routing_key=artist_queue.name)
            await asyncio.wait_for(recovery_started.wait(), timeout=5)
            assert first_delivery_tasks
            first_delivery_tasks[0].cancel()
            await artist_queue.cancel(first_consumer_tag)
            await channel.close()
            with pytest.raises(asyncio.CancelledError):
                await first_delivery_tasks[0]
            assert active_writers == 0
            assert pool.active == 0
            assert await _count_rows(dsn, schema) == 16

            block_recovery = False
            service.active_connection = connection
            service.active_channel = channel
            recovery_connection = await aio_pika.connect_robust(amqp_url)
            with patch.object(service, "rabbitmq_manager", SimpleNamespace(connect=AsyncMock(return_value=recovery_connection))):
                await service._recover_consumers()
            channels.append(service.active_channel)

            # Recovery subscribed every entity queue with per-consumer QoS and settles
            # the broker-returned artist delivery on the fresh transport.
            assert set(service.consumer_tags) == set(service.DATA_TYPES)
            assert service.active_channel is not None and not service.active_channel.is_closed
            deadline = time.monotonic() + 30
            while sum(service.message_counts.values()) < 17 and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            assert sum(service.message_counts.values()) == 17
            assert await _count_rows(dsn, schema) == 17
            assert pool.peak <= 2
            assert peak_writers <= 2
    finally:
        for channel in channels:
            if channel is not None and not channel.is_closed:
                await channel.close()
        await connection.close()
        if recovery_connection is not None and not recovery_connection.is_closed:
            await recovery_connection.close()
        await raw_pool.close()
        cleanup = await psycopg.AsyncConnection.connect(dsn)
        await cleanup.set_autocommit(True)
        await cleanup.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        await cleanup.close()


@pytest.mark.asyncio
async def test_durable_refresh_reservations_progress_with_writer_budget_saturated(monkeypatch: pytest.MonkeyPatch) -> None:
    """The refresh, lease heartbeat, and health query get the three reserved pool slots."""
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("owned integration script must set TEST_DATABASE_URL")

    loader = f"discogs-admission-{uuid.uuid4().hex}"
    monkeypatch.setattr(durable, "LOADER_DISCRIMINATOR", loader)
    monkeypatch.setattr(durable, "_HEARTBEAT_SECONDS", 0.005)
    raw_pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": dsn},
        min_connections=1,
        max_connections=4,
        max_retries=1,
        health_check_interval=3600,
    )
    await raw_pool.initialize()

    refresh_entered = asyncio.Event()
    release_refresh = asyncio.Event()
    heartbeat_connection_acquired = asyncio.Event()
    release_heartbeat_connection = asyncio.Event()
    health_connection_acquired = asyncio.Event()
    release_health_connection = asyncio.Event()
    writer_connection_acquired = asyncio.Event()
    release_writer = asyncio.Event()
    blocked_writer_attempted = asyncio.Event()
    blocked_writer_acquired = asyncio.Event()

    class ReservedConnectionPool:
        def __init__(self) -> None:
            self.active = 0
            self.peak = 0

        @asynccontextmanager
        async def connection(self) -> Any:
            async with raw_pool.connection() as connection:
                self.active += 1
                self.peak = max(self.peak, self.active)
                task = asyncio.current_task()
                task_name = task.get_name() if task is not None else ""
                try:
                    if task_name == "saturated-writer":
                        writer_connection_acquired.set()
                        await release_writer.wait()
                    elif task_name == "durable-lease-heartbeat":
                        heartbeat_connection_acquired.set()
                        await release_heartbeat_connection.wait()
                    elif task_name == "durable-health-sampler":
                        health_connection_acquired.set()
                        await release_health_connection.wait()
                    yield connection
                finally:
                    self.active -= 1

    pool = ReservedConnectionPool()
    admission = WriteAdmission(service.writer_admission_capacity(4, durable_refresh=True))
    logger = MagicMock()
    worker: asyncio.Task[None] | None = None
    heartbeat: asyncio.Task[None] | None = None
    health: asyncio.Task[dict[str, Any]] | None = None
    writer: asyncio.Task[None] | None = None
    blocked_writer: asyncio.Task[None] | None = None
    initial_lease_expiry = None
    try:
        for entity in service.DATA_TYPES:
            await durable.record_durable_signal(pool, "20991230", entity, service.DATA_TYPES)
        job = await durable.claim_due_job(pool, "admission-reservation-test", logger)
        assert job is not None

        async with raw_pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT lease_expires_at FROM public.loader_derived_refresh_job WHERE loader = %s AND version = %s",
                (loader, job.version),
            )
            row = await cursor.fetchone()
            assert row is not None
            initial_lease_expiry = row[0]

        original_before_refresh = durable._before_refresh

        async def hold_refresh_transaction(cursor: Any, claimed_job: durable.ClaimedJob) -> None:
            await original_before_refresh(cursor, claimed_job)
            refresh_entered.set()
            await release_refresh.wait()

        monkeypatch.setattr(durable, "_before_refresh", hold_refresh_transaction)

        async def saturate_writer() -> None:
            async with admission.slot(), pool.connection() as connection:
                await connection.execute("SELECT 1")

        writer = asyncio.create_task(saturate_writer(), name="saturated-writer")
        await asyncio.wait_for(writer_connection_acquired.wait(), timeout=5)

        async def refresh_transaction() -> None:
            await durable.refresh_derived_relations(
                pool,
                logger,
                job.version,
                before_refresh=lambda cursor: durable._before_refresh(cursor, job),
                before_commit=lambda cursor: durable._before_commit(cursor, job),
            )

        worker = asyncio.create_task(refresh_transaction(), name="durable-refresh-transaction")
        await asyncio.wait_for(refresh_entered.wait(), timeout=5)
        heartbeat = asyncio.create_task(durable._heartbeat(pool, job, asyncio.Event()), name="durable-lease-heartbeat")
        health = asyncio.create_task(durable.read_refresh_health(pool), name="durable-health-sampler")
        await asyncio.wait_for(asyncio.gather(heartbeat_connection_acquired.wait(), health_connection_acquired.wait()), timeout=5)

        async def compete_for_writer_slot() -> None:
            blocked_writer_attempted.set()
            async with admission.slot():
                blocked_writer_acquired.set()
                async with pool.connection():
                    pass

        blocked_writer = asyncio.create_task(compete_for_writer_slot(), name="waiting-writer")
        await asyncio.wait_for(blocked_writer_attempted.wait(), timeout=2)
        await asyncio.sleep(0)
        assert not blocked_writer_acquired.is_set()
        assert pool.peak == 4

        release_heartbeat_connection.set()
        release_health_connection.set()
        health_snapshot = await asyncio.wait_for(health, timeout=5)
        assert health_snapshot["phase"] == "leased"
        await asyncio.sleep(0.02)
        async with raw_pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT lease_expires_at FROM public.loader_derived_refresh_job WHERE loader = %s AND version = %s",
                (loader, job.version),
            )
            row = await cursor.fetchone()
            assert row is not None and initial_lease_expiry is not None
            assert row[0] > initial_lease_expiry

        release_refresh.set()
        await asyncio.wait_for(worker, timeout=10)
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
        release_writer.set()
        await asyncio.wait_for(writer, timeout=5)
        await asyncio.wait_for(blocked_writer, timeout=5)
        assert blocked_writer_acquired.is_set()
    finally:
        release_refresh.set()
        release_heartbeat_connection.set()
        release_health_connection.set()
        release_writer.set()
        for task in (worker, heartbeat, health, writer, blocked_writer):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (worker, heartbeat, health, writer, blocked_writer) if task is not None), return_exceptions=True)
        async with raw_pool.connection() as connection:
            await connection.set_autocommit(False)
            async with connection.transaction(), connection.cursor() as cursor:
                await cursor.execute("DELETE FROM public.loader_derived_refresh_job WHERE loader = %s", (loader,))
                await cursor.execute("DELETE FROM public.loader_extraction_latch WHERE loader = %s", (loader,))
                await cursor.execute("DELETE FROM public.loader_derived_refresh_cursor WHERE loader = %s", (loader,))
        await raw_pool.close()
