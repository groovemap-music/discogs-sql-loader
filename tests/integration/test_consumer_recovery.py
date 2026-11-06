"""Consumer loss and recovery, proved on a real RabbitMQ with the loader's quorum queues.

The unit suite replaces the broker, which is how a check that never reached it passed
review: `RobustChannel.declare_queue(passive=True)` answers from a client-side cache, so
`consumer_count` read 0 on every healthy loader. These tests run the real checker, recovery,
handlers and aio-pika stack, and read the broker's view from a separate connection. Each
lost-consumer case is caused by the broker itself, with `consumer_timeout` set to 2 s by
`scripts/test-consumer-recovery.sh`.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import aio_pika
import aiormq
import pytest
import pytest_asyncio
from aio_pika.robust_connection import RobustConnection
from common import AsyncResilientRabbitMQ
from orjson import dumps, loads

import tableinator.tableinator as service


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable


pytestmark = pytest.mark.integration

CHECK_INTERVAL = 1
SEPARATE_CHECK_CYCLES = 5
# RabbitMQ 4.4 enforces consumer_timeout within seconds; 4.1 took about a minute when run here.
BROKER_ACK_DEADLINE = 90
HOLD_TYPE = "labels"


@dataclass
class Loader:
    """The loader as `main()` leaves it, plus an independent view of the broker."""

    url: str
    broker: aiormq.abc.AbstractChannel
    publisher: aio_pika.abc.AbstractChannel
    recoveries: list[float] = field(default_factory=list)
    held: list[Any] = field(default_factory=list)
    hold: bool = False
    handled: list[tuple[str, str | None]] = field(default_factory=list)

    async def consumers(self, data_type: str) -> int:
        result = await self.broker.queue_declare(service.catalog_queue_name(service.AMQP_CONSUMER_NAME, data_type), passive=True)
        return int(result.consumer_count)

    async def ready(self, data_type: str) -> int:
        result = await self.broker.queue_declare(service.catalog_queue_name(service.AMQP_CONSUMER_NAME, data_type), passive=True)
        return int(result.message_count)

    async def publish(self, data_type: str, **body: Any) -> None:
        exchange = await self.publisher.get_exchange(service.catalog_exchange_name(data_type))
        await exchange.publish(aio_pika.Message(dumps(body), delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key="")

    def was_handled(self, data_type: str, message_type: str) -> bool:
        return (data_type, message_type) in self.handled


async def _until(predicate: Callable[[], Awaitable[bool]], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not await predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.1)


async def _declare_queues(channel: Any) -> dict[str, Any]:
    """Declare the topology with the arguments `main()` uses."""
    queues = {}
    for entity in service.DATA_TYPES:
        dlx_name = service.catalog_dead_letter_exchange_name(service.AMQP_CONSUMER_NAME, entity)
        exchange = await channel.declare_exchange(service.catalog_exchange_name(entity), service.AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)
        dlx = await channel.declare_exchange(dlx_name, service.AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)
        dlq = await channel.declare_queue(
            name=service.catalog_dead_letter_queue_name(service.AMQP_CONSUMER_NAME, entity),
            durable=True,
            auto_delete=False,
            arguments={"x-queue-type": "classic"},
        )
        await dlq.bind(dlx)
        queue = await channel.declare_queue(
            name=service.catalog_queue_name(service.AMQP_CONSUMER_NAME, entity),
            durable=True,
            auto_delete=False,
            arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": dlx_name, "x-delivery-limit": 20},
        )
        await queue.bind(exchange)
        queues[entity] = queue
    return queues


@pytest_asyncio.fixture
async def loader() -> AsyncIterator[Loader]:
    url = os.environ.get("TEST_AMQP_URL")
    if not url:
        pytest.skip("scripts/test-consumer-recovery.sh must set TEST_AMQP_URL")

    observer = await aiormq.connect(url)
    broker = await observer.channel()
    publishing = await aio_pika.connect(url)
    publisher = await publishing.channel()
    state = Loader(url=url, broker=broker, publisher=publisher)

    real_on_data_message = service.on_data_message
    real_recover = service._recover_consumers

    async def on_data_message(message: Any, data_type: str) -> None:
        message_type = loads(message.body).get("type")
        if message_type == "hold":
            if state.hold:
                state.held.append(message)
                return
            await message.ack()
            state.handled.append((data_type, message_type))
            return
        await real_on_data_message(message, data_type)
        state.handled.append((data_type, message_type))

    async def recover() -> None:
        state.recoveries.append(time.monotonic())
        await real_recover()

    real_recover_type = service._recover_data_type

    async def recover_type(data_type: str) -> bool:
        state.recoveries.append(time.monotonic())
        return await real_recover_type(data_type)

    service.rabbitmq_manager = AsyncResilientRabbitMQ(connection_url=url, max_retries=3, heartbeat=600, connection_attempts=3, retry_delay=1.0)
    service.active_connection = await service.rabbitmq_manager.connect()
    service.active_channel = await service.active_connection.channel()
    await service.active_channel.set_qos(prefetch_count=service.channel_prefetch(), global_=False)
    service.queues = await _declare_queues(service.active_channel)

    with (
        patch.object(service, "on_data_message", on_data_message),
        patch.object(service, "_recover_consumers", recover),
        patch.object(service, "_recover_data_type", recover_type),
        patch.object(service, "STUCK_CHECK_INTERVAL", CHECK_INTERVAL),
        patch.object(service, "STARTUP_IDLE_TIMEOUT", 1),
    ):
        for data_type in service.DATA_TYPES:
            service.consumer_tags[data_type] = await service._start_consumer(data_type)
        checker = asyncio.create_task(service.periodic_queue_checker())
        try:
            yield state
        finally:
            service.shutdown_requested = True
            checker.cancel()
            await asyncio.gather(checker, return_exceptions=True)
            if service.active_connection is not None:
                await asyncio.wait_for(service.active_connection.close(), timeout=10)
            for entity in service.DATA_TYPES:
                await broker.queue_delete(service.catalog_queue_name(service.AMQP_CONSUMER_NAME, entity))
            await publishing.close()
            await observer.close()


async def _all_attached(loader: Loader) -> bool:
    return all([await loader.consumers(data_type) == 1 for data_type in service.DATA_TYPES])


@pytest.mark.asyncio
async def test_idle_loader_keeps_consumers_through_checker_cycles_and_consumes_later_messages(loader: Loader) -> None:
    """The homelab smoke: an idle loader must still be attached when the extractor publishes."""
    with patch.object(service, "last_message_time", dict.fromkeys(service.DATA_TYPES, 0.0)):
        reporter = asyncio.create_task(service.progress_reporter())
        try:
            await asyncio.sleep(11 * CHECK_INTERVAL)
            assert service.idle_mode, "the loader must have entered the idle path"
        finally:
            reporter.cancel()
            await asyncio.gather(reporter, return_exceptions=True)

    assert loader.recoveries == []
    assert await _all_attached(loader)

    for data_type in service.DATA_TYPES:
        await loader.publish(data_type, type="extraction_complete", version="20261101", started_at="2026-11-01T00:00:00Z")

    async def signalled() -> bool:
        return all(loader.was_handled(data_type, "extraction_complete") for data_type in service.DATA_TYPES)

    await _until(signalled, 5, "extraction_complete on every queue")
    assert loader.recoveries == []


@pytest.mark.asyncio
async def test_recovery_on_empty_queues_keeps_incomplete_types_attached(loader: Loader) -> None:
    """The second defect: finding no messages must not leave the loader disconnected."""
    await service._recover_consumers()

    await _until(lambda: _all_attached(loader), 5, "consumers on every queue after recovery")
    assert service.active_connection is not None
    await loader.publish(HOLD_TYPE, type="hold")

    async def consumed() -> bool:
        return loader.was_handled(HOLD_TYPE, "hold")

    await _until(consumed, 5, "a message published after recovery")


@pytest.mark.asyncio
async def test_failed_recovery_is_retried_on_a_loader_that_processed_nothing(loader: Loader) -> None:
    """A dropped loader that never saw a message used to wait for the hourly check."""
    await service._close_recovery_broker()
    assert service.consumer_tags == {}
    assert sum(service.message_counts.values()) == 0

    await _until(lambda: _all_attached(loader), 10, "the checker to re-attach the loader")


@pytest.mark.asyncio
async def test_all_files_complete_and_empty_queues_may_disconnect(loader: Loader) -> None:
    service.completed_files.update(service.DATA_TYPES)
    await service._recover_consumers()

    assert service.active_connection is None
    assert service.consumer_tags == {}
    assert not any([await loader.consumers(data_type) for data_type in service.DATA_TYPES])


@pytest.mark.timeout(2 * BROKER_ACK_DEADLINE)
@pytest.mark.asyncio
async def test_real_ack_timeout_resumes_consumption(loader: Loader) -> None:
    """The lux incident: the broker times out a held delivery and consumption must resume."""
    loader.hold = True
    await loader.publish(HOLD_TYPE, type="hold")

    async def timed_out_and_redelivered() -> bool:
        return any(message.redelivered for message in loader.held)

    await _until(timed_out_and_redelivered, BROKER_ACK_DEADLINE, "the broker to time out and redeliver the held delivery")
    loader.hold = False
    await loader.held[-1].ack()

    await _until(lambda: _all_attached(loader), BROKER_ACK_DEADLINE, "consumers after the ack timeout")
    await loader.publish(HOLD_TYPE, type="file_complete", total_processed=0)

    async def later_message_consumed() -> bool:
        return loader.was_handled(HOLD_TYPE, "file_complete")

    await _until(later_message_consumed, BROKER_ACK_DEADLINE, "a message published after the ack timeout")
    assert loader.recoveries == []


@pytest.mark.timeout(2 * BROKER_ACK_DEADLINE)
@pytest.mark.asyncio
async def test_real_broker_cancel_is_detected_when_the_client_does_not_restore_it(loader: Loader) -> None:
    """The backstop: a basic.cancel the client never heals is recovered by the checker."""
    version = loader.broker.connection.server_properties["version"]
    if tuple(int(part) for part in str(version).split(".")[:2]) < (4, 3):
        pytest.skip("this broker does not send basic.cancel on an ack timeout")
    channel = await service.delivery_channels[HOLD_TYPE].get_underlay_channel()
    tag = service.consumer_tags[HOLD_TYPE]
    loader.hold = True
    with patch.object(RobustConnection, "_request_recovery", lambda _self: None):
        await loader.publish(HOLD_TYPE, type="hold")

        async def cancelled() -> bool:
            return tag not in channel.consumers

        await _until(cancelled, BROKER_ACK_DEADLINE, "the broker to cancel the consumer")
        loader.hold = False
        await _until(lambda: _recovered(loader), 2 * CHECK_INTERVAL + 10, "the checker to recover the lost consumer")

    await _until(lambda: _all_attached(loader), 10, "consumers after recovery")
    await loader.publish(HOLD_TYPE, type="hold")

    async def consumed() -> bool:
        return loader.was_handled(HOLD_TYPE, "hold")

    await _until(consumed, 5, "a message published after recovery")


async def _recovered(loader: Loader) -> bool:
    return len(loader.recoveries) > 0


@pytest.mark.asyncio
async def test_real_channel_close_resumes_consumption_without_a_false_recovery(loader: Loader) -> None:
    """A broker-closed channel reopens with its tags; closing the connection would requeue work."""
    channel = await service.delivery_channels[HOLD_TYPE].get_underlay_channel()
    with pytest.raises(aiormq.exceptions.ChannelPreconditionFailed):
        await channel.queue_declare(service.catalog_queue_name(service.AMQP_CONSUMER_NAME, HOLD_TYPE), durable=False)

    await _until(lambda: _all_attached(loader), 10, "aio-pika to reopen the channel")
    await asyncio.sleep(SEPARATE_CHECK_CYCLES * CHECK_INTERVAL)

    assert loader.recoveries == []
    await loader.publish(HOLD_TYPE, type="hold")

    async def consumed() -> bool:
        return loader.was_handled(HOLD_TYPE, "hold")

    await _until(consumed, 5, "a message published after the channel closed")


@pytest.mark.asyncio
async def test_channel_the_client_will_not_reopen_is_recovered(loader: Loader) -> None:
    await service.delivery_channels[HOLD_TYPE].close()

    await _until(lambda: _recovered(loader), 5, "the checker to notice the closed channel")
    await _until(lambda: _all_attached(loader), 10, "consumers after recovery")


@pytest.mark.asyncio
async def test_consumer_cancelled_on_purpose_is_not_treated_as_lost(loader: Loader) -> None:
    other_types = [data_type for data_type in service.DATA_TYPES if data_type != HOLD_TYPE]
    with patch.object(service, "CONSUMER_CANCEL_DELAY", 1):
        await loader.publish(HOLD_TYPE, type="file_complete", total_processed=0)

        async def cancelled() -> bool:
            return await loader.consumers(HOLD_TYPE) == 0 and HOLD_TYPE not in service.consumer_tags

        await _until(cancelled, 10, "the completed type's consumer to be cancelled")

    await asyncio.sleep(SEPARATE_CHECK_CYCLES * CHECK_INTERVAL)

    assert loader.recoveries == []
    assert await loader.consumers(HOLD_TYPE) == 0
    assert all([await loader.consumers(data_type) == 1 for data_type in other_types])
