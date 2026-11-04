"""Regression coverage for confirmed cancellation and successive extraction runs."""

import asyncio
import os
import signal
import subprocess
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import tableinator.tableinator as service


class SerialCancelChannel:
    """Model one channel's RPC reply slot, released only by a cancel-ok round trip."""

    def __init__(self) -> None:
        self.reply_slot = asyncio.Lock()
        self.cancelled: list[str] = []

    async def cancel(self, tag: str, *, nowait: bool, timeout: float) -> None:
        await asyncio.wait_for(self.reply_slot.acquire(), timeout=timeout)
        if not nowait:
            await asyncio.sleep(0)
            self.cancelled.append(tag)
            self.reply_slot.release()


@pytest.mark.asyncio
async def test_several_consecutive_cancels_share_one_channel() -> None:
    queue = SerialCancelChannel()
    tags = {name: f"tag-{name}" for name in service.DATA_TYPES}
    service.consumer_tags.update(tags)
    service.queues.update(dict.fromkeys(tags, queue))
    await asyncio.wait_for(service.cancel_all_consumers(), timeout=0.5)
    assert queue.cancelled == list(tags.values())
    assert service.consumer_tags == {}
    assert not service.consumer_cancellation_failed


@pytest.mark.asyncio
async def test_tag_is_retained_until_cancel_ok() -> None:
    confirm = asyncio.Event()
    started = asyncio.Event()

    async def cancel(*_args: object, **_kwargs: object) -> None:
        started.set()
        await confirm.wait()

    queue = MagicMock(cancel=AsyncMock(side_effect=cancel))
    service.consumer_tags["artists"] = "original"
    with patch.object(service, "CONSUMER_CANCEL_DELAY", 0):
        await service.schedule_consumer_cancellation("artists", queue)
        task = service.consumer_cancel_tasks["artists"]
        await started.wait()
        assert service.consumer_tags == {"artists": "original"}
        confirm.set()
        await task
    assert service.consumer_tags == {}


@pytest.mark.asyncio
async def test_completion_timeout_keeps_tag_and_triggers_confirmed_recovery() -> None:
    async def hanging_cancel(*_args: object, **_kwargs: object) -> None:
        await asyncio.Event().wait()

    queue = MagicMock(cancel=AsyncMock(side_effect=hanging_cancel))
    service.consumer_tags["artists"] = "original"
    service.active_connection = AsyncMock()
    service.active_channel = AsyncMock()
    with (
        patch.object(service, "CONSUMER_CANCEL_DELAY", 0),
        patch.object(service, "CONSUMER_CANCEL_TIMEOUT", 0.01),
        patch.object(service, "logger") as log,
    ):
        await service.schedule_consumer_cancellation("artists", queue)
        await asyncio.wait_for(service.consumer_cancel_tasks["artists"], timeout=0.2)
        assert service.consumer_tags == {"artists": "original"}
        assert service.consumer_cancellation_failed
        assert log.error.call_args.kwargs["error_type"] == "TimeoutError"

        async def recovered() -> None:
            assert service.consumer_tags == {}
            service.shutdown_requested = True

        with patch.object(service, "STUCK_CHECK_INTERVAL", 0), patch.object(service, "_recover_consumers", side_effect=recovered) as recover:
            await asyncio.wait_for(service.periodic_queue_checker(), timeout=0.2)
        recover.assert_awaited_once()
    assert not service.consumer_cancellation_failed


@pytest.mark.asyncio
async def test_new_extraction_record_resets_completion_and_old_timer() -> None:
    service.completed_files.update(service.DATA_TYPES)
    service.consumer_tags["artists"] = "original"
    queue = AsyncMock()
    batch = AsyncMock()
    batch.add_message.return_value = True
    message = AsyncMock(body=b'{"id":"7","name":"Next extraction artist"}')
    with (
        patch.object(service, "CONSUMER_CANCEL_DELAY", 0.05),
        patch.object(service, "BATCH_MODE", True),
        patch.object(service, "batch_processor", batch),
    ):
        await service.schedule_consumer_cancellation("artists", queue)
        old_timer = service.consumer_cancel_tasks["artists"]
        await service.on_data_message(message, "artists")
        await asyncio.gather(old_timer, return_exceptions=True)
    queue.cancel.assert_not_awaited()
    batch.add_message.assert_awaited_once()
    assert "artists" not in service.completed_files
    assert "artists" not in service.consumer_cancel_tasks
    assert service.message_counts["artists"] == 1
    service.consumer_tags.clear()  # Simulate connection loss on the next run.
    assert await service.check_consumers_unexpectedly_dead()


@pytest.mark.asyncio
async def test_new_record_interrupting_cancel_requires_broker_recovery() -> None:
    started = asyncio.Event()

    async def cancel(*_args: object, **_kwargs: object) -> None:
        started.set()
        await asyncio.Event().wait()

    service.consumer_tags["artists"] = "original"
    service.completed_files.add("artists")
    queue = MagicMock(cancel=AsyncMock(side_effect=cancel))
    batch = AsyncMock()
    message = AsyncMock(body=b'{"id":"8","name":"Next extraction artist"}')
    with (
        patch.object(service, "CONSUMER_CANCEL_DELAY", 0),
        patch.object(service, "BATCH_MODE", True),
        patch.object(service, "batch_processor", batch),
    ):
        await service.schedule_consumer_cancellation("artists", queue)
        timer = service.consumer_cancel_tasks["artists"]
        await started.wait()
        await service.on_data_message(message, "artists")
        await asyncio.gather(timer, return_exceptions=True)
    assert service.consumer_cancellation_failed
    assert service.consumer_tags == {"artists": "original"}
    assert "artists" not in service.completed_files


@pytest.mark.asyncio
async def test_replaced_timer_cannot_remove_the_next_timer() -> None:
    queue = AsyncMock()
    service.consumer_tags["artists"] = "original"
    with patch.object(service, "CONSUMER_CANCEL_DELAY", 0.02):
        await service.schedule_consumer_cancellation("artists", queue)
        old_timer = service.consumer_cancel_tasks["artists"]
        await asyncio.sleep(0)
        await service.schedule_consumer_cancellation("artists", queue)
        new_timer = service.consumer_cancel_tasks["artists"]
        await asyncio.gather(old_timer, return_exceptions=True)
        assert service.consumer_cancel_tasks["artists"] is new_timer
        await new_timer
    assert "artists" not in service.consumer_cancel_tasks
    queue.cancel.assert_awaited_once()


def test_sigterm_exits_cancel_teardown_without_sigkill() -> None:
    """Deliver a real SIGTERM while shutdown encounters an unresponsive broker RPC."""
    program = """
import asyncio, signal
from unittest.mock import AsyncMock
import tableinator.tableinator as service
async def hangs(*args, **kwargs):
    await asyncio.Event().wait()
async def main():
    service.CONSUMER_CANCEL_TIMEOUT = 0.02
    service.consumer_tags = {'artists': 'tag'}
    service.queues = {'artists': AsyncMock(cancel=AsyncMock(side_effect=hangs))}
    service.active_connection = AsyncMock()
    signal.signal(signal.SIGTERM, service.signal_handler)
    print('READY', flush=True)
    while not service.shutdown_requested:
        await asyncio.sleep(0.01)
    await service.cancel_all_consumers()
    if service.consumer_cancellation_failed:
        assert await service._reset_durable_broker()
    await service.close_rabbitmq_connection()
asyncio.run(main())
"""
    with subprocess.Popen(  # noqa: S603 -- fixed local regression program and current test interpreter
        [sys.executable, "-c", program], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    ) as process:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "READY"
        os.kill(process.pid, signal.SIGTERM)
        _stdout, stderr = process.communicate(timeout=3)
    assert process.returncode == 0, stderr


async def _hanging_close() -> None:
    await asyncio.Event().wait()


@pytest.mark.asyncio
@pytest.mark.parametrize("hang", [False, True])
async def test_recovery_retains_old_transport_until_confirmed_close(hang: bool) -> None:
    """An old close failure cannot authorize replacement consumers or healthy state."""
    failure = _hanging_close if hang else RuntimeError("close failed")
    connection = AsyncMock(close=AsyncMock(side_effect=failure))
    channel = AsyncMock(close=AsyncMock(side_effect=failure))
    queue = AsyncMock()
    service.active_connection, service.active_channel = connection, channel
    service.consumer_tags["artists"] = "old-tag"
    service.queues["artists"] = queue
    service.connection_pool = MagicMock()
    manager = AsyncMock()
    with (
        patch.object(service, "CONSUMER_CANCEL_TIMEOUT", 0.01),
        patch.object(service, "rabbitmq_manager", manager),
        patch.object(service.telemetry, "record_consumer_stopped") as stopped,
    ):
        await asyncio.wait_for(service._recover_consumers(), timeout=0.2)
        manager.connect.assert_not_awaited()
        assert service.active_connection is connection
        assert service.active_channel is channel
        assert service.queues == {"artists": queue}
        assert service.consumer_tags == {"artists": "old-tag"}
        assert service.consumer_cancellation_failed
        assert service.get_health_data()["status"] == "unhealthy"
        stopped.assert_not_called()

        connection.close.side_effect = None
        channel.close.side_effect = None
        manager.connect.side_effect = RuntimeError("offline")
        await service._recover_consumers()
        manager.connect.assert_awaited_once()
        assert service.active_connection is None
        assert service.active_channel is None
        assert service.queues == {}
        assert service.consumer_tags == {}
        assert not service.consumer_cancellation_failed
        stopped.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("hang", [False, True])
async def test_partial_registration_retains_failed_cleanup_and_retries(hang: bool) -> None:
    """A broker may still run the first consumer when registering the second fails."""
    failure = _hanging_close if hang else RuntimeError("close failed")
    registered_queue = AsyncMock(consume=AsyncMock(return_value="new-artists"))

    def declare_queue(**kwargs: object) -> MagicMock:
        if kwargs.get("passive"):
            queue = MagicMock()
            queue.declaration_result.message_count = 1
            return queue
        name = str(kwargs.get("name"))
        if name.endswith("-artists"):
            return registered_queue
        if name.endswith("-labels"):
            return AsyncMock(consume=AsyncMock(side_effect=RuntimeError("registration failed")))
        return AsyncMock()

    channel = AsyncMock(declare_queue=AsyncMock(side_effect=declare_queue), close=AsyncMock(side_effect=failure))
    connection = AsyncMock(channel=AsyncMock(return_value=channel), close=AsyncMock(side_effect=failure))
    manager = AsyncMock(connect=AsyncMock(return_value=connection))
    service.connection_pool = MagicMock()
    with (
        patch.object(service, "CONSUMER_CANCEL_TIMEOUT", 0.01),
        patch.object(service, "rabbitmq_manager", manager),
        patch.object(service.telemetry, "record_consumer_stopped") as stopped,
    ):
        await asyncio.wait_for(service._recover_consumers(), timeout=0.2)
        assert service.active_connection is connection
        assert service.active_channel is channel
        assert service.queues["artists"] is registered_queue
        assert service.consumer_tags == {"artists": "new-artists"}
        assert service.consumer_cancellation_failed
        assert service.get_health_data()["status"] == "unhealthy"
        stopped.assert_not_called()
        # The checker must keep retrying even though a tag and transport remain.
        with patch.object(service, "STUCK_CHECK_INTERVAL", 0):
            checker = asyncio.create_task(service.periodic_queue_checker())
            await asyncio.sleep(0.03)
            service.shutdown_requested = True
            await asyncio.wait_for(checker, timeout=0.2)
        assert service.consumer_tags == {"artists": "new-artists"}
        manager.connect.assert_awaited_once()

        # Once cleanup is confirmed, remove the old metric/tag once and permit reconnect.
        connection.close.side_effect = None
        channel.close.side_effect = None
        manager.connect.side_effect = RuntimeError("offline")
        await service._recover_consumers()
        assert manager.connect.await_count == 2
        assert service.consumer_tags == {}
        assert service.queues == {}
        assert service.active_connection is None
        assert not service.consumer_cancellation_failed
        stopped.assert_called_once()


@pytest.mark.asyncio
async def test_failed_channel_open_keeps_temporary_connection_for_cleanup_retry() -> None:
    """A new connection is owned before opening its channel can fail."""
    connection = AsyncMock(channel=AsyncMock(side_effect=RuntimeError("channel unavailable")), close=AsyncMock(side_effect=_hanging_close))
    with (
        patch.object(service, "CONSUMER_CANCEL_TIMEOUT", 0.01),
        patch.object(service, "rabbitmq_manager", AsyncMock(connect=AsyncMock(return_value=connection))),
    ):
        await asyncio.wait_for(service._recover_consumers(), timeout=0.2)
    assert service.active_connection is connection
    assert service.active_channel is None
    assert service.consumer_cancellation_failed


@pytest.mark.asyncio
async def test_broker_cancelled_consumers_are_detected_and_recovered() -> None:
    """A closed channel (ack timeout) leaves stale tags; the checker must still recover."""
    service.consumer_tags["labels"] = "stale-tag"
    service.queues["labels"] = MagicMock()
    service.active_channel = MagicMock(is_closed=True)
    service.shutdown_requested = False
    assert await service.consumers_lost_at_broker()

    async def recovered() -> None:
        service.shutdown_requested = True

    with patch.object(service, "STUCK_CHECK_INTERVAL", 0), patch.object(service, "_recover_consumers", side_effect=recovered) as recover:
        await asyncio.wait_for(service.periodic_queue_checker(), timeout=0.5)
    recover.assert_awaited_once()


def _delivery_channel(*tags: str) -> MagicMock:
    """A robust channel whose underlay currently holds these consumer tags."""
    underlay = MagicMock(consumers=dict.fromkeys(tags))
    return MagicMock(is_closed=False, get_underlay_channel=AsyncMock(return_value=underlay))


@pytest.mark.asyncio
async def test_live_consumers_are_not_lost_and_the_stale_declare_cache_is_never_read() -> None:
    service.consumer_tags["labels"] = "tag"
    service.active_channel = _delivery_channel("tag")
    assert not await service.consumers_lost_at_broker()
    assert not await service.consumers_lost_at_broker()
    service.active_channel.declare_queue.assert_not_called()


@pytest.mark.asyncio
async def test_a_tag_that_aio_pika_restores_between_checks_is_not_lost() -> None:
    service.consumer_tags["labels"] = "tag"
    service.active_channel = _delivery_channel()
    assert not await service.consumers_lost_at_broker()
    service.active_channel = _delivery_channel("tag")
    assert not await service.consumers_lost_at_broker()
    service.active_channel = _delivery_channel()
    assert not await service.consumers_lost_at_broker()


@pytest.mark.asyncio
async def test_a_tag_missing_on_two_consecutive_checks_is_lost() -> None:
    service.consumer_tags["labels"] = "tag"
    service.active_channel = _delivery_channel()
    assert not await service.consumers_lost_at_broker()
    assert await service.consumers_lost_at_broker()


@pytest.mark.asyncio
async def test_an_unavailable_delivery_channel_counts_as_missing() -> None:
    service.consumer_tags["labels"] = "tag"
    service.active_channel = MagicMock(is_closed=False, get_underlay_channel=AsyncMock(side_effect=RuntimeError("reconnecting")))
    assert not await service.consumers_lost_at_broker()
    assert await service.consumers_lost_at_broker()


@pytest.mark.asyncio
async def test_a_completed_type_is_not_watched() -> None:
    service.consumer_tags["labels"] = "tag"
    service.completed_files.add("labels")
    service.active_channel = _delivery_channel()
    assert not await service.consumers_lost_at_broker()
    assert not await service.consumers_lost_at_broker()


@pytest.mark.asyncio
async def test_no_check_runs_during_shutdown_and_suspicion_is_dropped() -> None:
    service.consumer_tags["labels"] = "tag"
    service.active_channel = _delivery_channel()
    assert not await service.consumers_lost_at_broker()
    service.shutdown_requested = True
    assert not await service.consumers_lost_at_broker()
    assert service.suspected_lost_tags == set()
    assert not await service.check_consumers_unexpectedly_dead()


def _recovery_broker(message_count: int) -> tuple[AsyncMock, AsyncMock]:
    """A recovery transport whose passive declares report `message_count`, and its channel."""
    declared = MagicMock(declaration_result=MagicMock(message_count=message_count))
    queue = AsyncMock(declaration_result=declared.declaration_result)
    queue.consume = AsyncMock(return_value="new-tag")
    channel = AsyncMock(declare_queue=AsyncMock(return_value=queue))
    return AsyncMock(channel=AsyncMock(return_value=channel)), channel


@pytest.mark.asyncio
async def test_recovery_keeps_consumers_when_queues_are_empty_and_files_are_incomplete() -> None:
    connection, channel = _recovery_broker(message_count=0)
    with patch.object(service, "rabbitmq_manager", AsyncMock(connect=AsyncMock(return_value=connection))):
        await service._recover_consumers()
    assert set(service.consumer_tags) == set(service.DATA_TYPES)
    assert service.active_connection is connection
    assert service.active_channel is channel
    connection.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_disconnects_only_when_every_file_completed_and_queues_are_empty() -> None:
    connection, _channel = _recovery_broker(message_count=0)
    service.completed_files.update(service.DATA_TYPES)
    with patch.object(service, "rabbitmq_manager", AsyncMock(connect=AsyncMock(return_value=connection))):
        await service._recover_consumers()
    assert service.consumer_tags == {}
    assert service.active_connection is None
    connection.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_recovery_message_counts_bypass_the_robust_declare_cache() -> None:
    connection, channel = _recovery_broker(message_count=0)
    service.completed_files.update(service.DATA_TYPES)
    with patch.object(service, "rabbitmq_manager", AsyncMock(connect=AsyncMock(return_value=connection))):
        await service._recover_consumers()
    passive = [call for call in channel.declare_queue.await_args_list if call.kwargs.get("passive")]
    assert len(passive) == len(service.DATA_TYPES)
    assert all(call.kwargs["robust"] is False for call in passive)
