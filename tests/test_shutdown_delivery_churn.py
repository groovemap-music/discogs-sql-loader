"""Regression coverage for shutdown delivery churn (historical issue discogsography-lnn4)."""

from unittest.mock import AsyncMock, patch

import pytest

import tableinator.tableinator as service


async def _dispatch(message: AsyncMock) -> None:
    await service.on_data_message(message, "artists")


@pytest.mark.asyncio
async def test_shutdown_guard_leaves_repeated_deliveries_unsettled() -> None:
    messages = []
    with patch.object(service, "shutdown_requested", True), patch.object(service, "logger"):
        for _ in range(25):
            message = AsyncMock(body=b'{"id":"1"}', routing_key="artists")
            await _dispatch(message)
            messages.append(message)
    assert not any(message.nack.called or message.ack.called for message in messages)


@pytest.mark.asyncio
async def test_shutdown_cancels_every_consumer_before_connection_close() -> None:
    queue = AsyncMock()
    tags = {"artists": "tag-artists", "labels": "tag-labels"}
    with (
        patch.object(service, "consumer_tags", dict(tags)),
        patch.object(service, "queues", dict.fromkeys(tags, queue)),
        patch.object(service, "logger"),
    ):
        await service.cancel_all_consumers()
        assert {call.args[0] for call in queue.cancel.call_args_list} == set(tags.values())
        assert all(call.kwargs["nowait"] is True for call in queue.cancel.call_args_list)
        assert service.consumer_tags == {}


@pytest.mark.asyncio
async def test_consumer_cancellation_failure_does_not_block_teardown() -> None:
    queue = AsyncMock()
    queue.cancel.side_effect = RuntimeError("channel already closed")
    with (
        patch.object(service, "consumer_tags", {"artists": "tag-artists"}),
        patch.object(service, "queues", {"artists": queue}),
        patch.object(service, "logger"),
    ):
        await service.cancel_all_consumers()


@pytest.mark.asyncio
async def test_missing_queue_handle_is_tolerated() -> None:
    with (
        patch.object(service, "consumer_tags", {"artists": "tag-artists"}),
        patch.object(service, "queues", {}),
        patch.object(service, "logger"),
    ):
        await service.cancel_all_consumers()
        assert service.consumer_tags == {}
