"""The batch requeue budget through the real handler on a real RabbitMQ.

RabbitMQ does not bound a nacked message, so the loader counts the broker's ``x-acquired-count``.
This proves the whole path a service has to get right: the handler passes the headers, the adapter
exposes them, and the engine's shipped budget dead-letters at the stated count and no sooner. The
write is the only thing replaced, because the hold is what is under test, not PostgreSQL.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import aio_pika
import pytest
import pytest_asyncio
from common import WriteAdmission
from orjson import dumps
from psycopg import OperationalError

import tableinator.tableinator as service
from tableinator.batch_processor import DEFAULT_MAX_REQUEUES, BatchConfig, BatchWriteResult, PostgreSQLBatchProcessor


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.timeout(60)]

DATA_TYPE = "artists"


@dataclass
class Stack:
    processor: PostgreSQLBatchProcessor
    channel: aio_pika.abc.AbstractChannel
    queue: aio_pika.abc.AbstractQueue
    dlq: aio_pika.abc.AbstractQueue
    fail_writes: int = 1_000_000
    deliveries: list[dict[str, Any]] = field(default_factory=list)

    async def publish(self) -> None:
        body = dumps({"id": "1", "name": "Healthy Artist", "sha256": "a" * 64})
        await self.channel.default_exchange.publish(
            aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=self.queue.name
        )

    async def dead_letter_reason(self) -> str | None:
        deadline = asyncio.get_running_loop().time() + 2.0
        while (message := await self.dlq.get(fail=False, timeout=5)) is None:
            if asyncio.get_running_loop().time() >= deadline:
                return None
            await asyncio.sleep(0.05)
        await message.ack()
        reason = (message.headers or {}).get("x-first-death-reason")
        return reason if isinstance(reason, str) else None


async def until(predicate: Any, timeout: float) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.02)


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[Stack]:
    url = os.environ.get("TEST_AMQP_URL")
    if not url:
        pytest.skip("scripts/test-consumer-recovery.sh must set TEST_AMQP_URL")

    admission = WriteAdmission(2)
    config = BatchConfig(batch_size=1, min_batch_size=1, flush_interval=0.02, max_hold_s=0.1, backoff_initial=0.01, backoff_max=0.02)
    assert config.max_requeues == DEFAULT_MAX_REQUEUES
    processor = PostgreSQLBatchProcessor(MagicMock(), config, admission)

    connection = await aio_pika.connect(url)
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=4, global_=False)
    name = f"gm-5kd2-{uuid.uuid4().hex[:8]}"
    exchange = await channel.declare_exchange(f"{name}.dlx", aio_pika.ExchangeType.FANOUT, durable=True)
    dlq = await channel.declare_queue(f"{name}.dlq", durable=True, arguments={"x-queue-type": "classic"})
    await dlq.bind(exchange)
    queue = await channel.declare_queue(
        name, durable=True, arguments={"x-queue-type": "quorum", "x-delivery-limit": 20, "x-dead-letter-exchange": f"{name}.dlx"}
    )
    built = Stack(processor, channel, queue, dlq)

    async def write(_data_type: str, _messages: list[Any]) -> BatchWriteResult:
        if len(built.deliveries) <= built.fail_writes:
            raise OperationalError("server closed the connection unexpectedly")
        return BatchWriteResult(set(), set(), set())

    handler = service.make_data_handler(DATA_TYPE)

    async def handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        built.deliveries.append(dict(message.headers or {}))
        await handler(message)

    with (
        patch.object(service, "batch_processor", processor),
        patch.object(service, "write_admission", admission),
        patch.object(service, "BATCH_MODE", True),
        patch.object(service, "shutdown_requested", False),
        patch.object(processor, "_process_batch", write),
    ):
        periodic = asyncio.create_task(processor._engine.run_periodic())
        await queue.consume(handle)
        try:
            yield built
        finally:
            processor._engine.shutdown()
            await asyncio.wait_for(asyncio.gather(periodic, return_exceptions=True), 5)
            await queue.delete(if_unused=False, if_empty=False)
            await dlq.delete(if_unused=False, if_empty=False)
            await exchange.delete()
            await connection.close()


async def test_a_record_that_keeps_failing_dead_letters_at_the_shipped_budget(stack: Stack) -> None:
    await stack.publish()
    await until(lambda: stack.processor.had_dlq_nacks(DATA_TYPE) and len(stack.deliveries) > DEFAULT_MAX_REQUEUES, 30)

    assert len(stack.deliveries) == DEFAULT_MAX_REQUEUES + 1
    assert stack.deliveries[-1]["x-acquired-count"] == DEFAULT_MAX_REQUEUES
    assert "x-delivery-count" not in stack.deliveries[-1]
    assert await stack.dead_letter_reason() == "rejected"


async def test_a_healthy_record_survives_holds_up_to_the_budget(stack: Stack) -> None:
    """The outage ends on the last delivery the budget allows, and the record is written."""
    stack.fail_writes = DEFAULT_MAX_REQUEUES
    await stack.publish()
    await until(lambda: len(stack.deliveries) > DEFAULT_MAX_REQUEUES and stack.processor._engine.snapshot()["pending"][DATA_TYPE] == 0, 30)

    assert len(stack.deliveries) == DEFAULT_MAX_REQUEUES + 1
    assert await stack.dead_letter_reason() is None
    assert not stack.processor.had_dlq_nacks(DATA_TYPE)
    ready = await stack.channel.declare_queue(stack.queue.name, passive=True)
    assert ready.declaration_result.message_count == 0
