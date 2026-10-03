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
