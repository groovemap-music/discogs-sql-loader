"""Offline safety checks for the durable acknowledgement boundary."""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika.abc import AbstractIncomingMessage

import tableinator.tableinator as service
from tableinator.durable_refresh import DurableSignal, ordered_version, probe_durable_schema
from tableinator.tableinator import on_data_message


def test_only_source_ordered_dump_versions_advance_generation() -> None:
    assert ordered_version("20260920") == "20260920"
    for value in (None, "", "unknown", "20260230", "2026-09-20", "2026092", "202609200"):
        with pytest.raises(ValueError):
            ordered_version(value)


@pytest.mark.asyncio
async def test_missing_declared_job_relation_fails_the_startup_probe(mock_postgres_connection: AsyncMock, mock_async_pool: Any) -> None:
    cursor = mock_postgres_connection.cursor.return_value
    cursor.fetchall.return_value = []
    logger = MagicMock()
    assert not await probe_durable_schema(mock_async_pool(mock_postgres_connection), logger)
    logger.error.assert_called_once()


def _terminal(version: str | None = "20260920") -> AsyncMock:
    message = AsyncMock(spec=AbstractIncomingMessage)
    message.body = json.dumps({"type": "extraction_complete", "version": version, "started_at": "2026-09-20T00:00:00Z"}).encode()
    return message


@pytest.mark.asyncio
async def test_ack_follows_committed_signal_and_never_inline_refreshes() -> None:
    message = _terminal()
    recorded = AsyncMock(return_value=DurableSignal("20260920", 1, frozenset({"artists"}), False, False, False))
    inline = AsyncMock()
    with (
        patch("tableinator.tableinator.durable_refresh_active", True),
        patch("tableinator.tableinator.durable_refresh_ready", True),
        patch("tableinator.tableinator.connection_pool", MagicMock()),
        patch("tableinator.tableinator.purge_stale_rows", new=AsyncMock()),
        patch("tableinator.tableinator.record_durable_signal", new=recorded),
        patch("tableinator.tableinator.refresh_derived_relations", new=inline),
    ):
        await on_data_message(message, "artists")
    recorded.assert_awaited_once()
    message.ack.assert_awaited_once()
    inline.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ready,raises", [(False, False), (True, True)])
async def test_missing_schema_or_commit_failure_never_acks(ready: bool, raises: bool) -> None:
    message = _terminal()
    recorded = AsyncMock(side_effect=RuntimeError("commit failed")) if raises else AsyncMock()
    with (
        patch("tableinator.tableinator.durable_refresh_active", True),
        patch("tableinator.tableinator.durable_refresh_ready", ready),
        patch("tableinator.tableinator.connection_pool", MagicMock()),
        patch("tableinator.tableinator.purge_stale_rows", new=AsyncMock()),
        patch("tableinator.tableinator.record_durable_signal", new=recorded),
    ):
        await on_data_message(message, "artists")
    message.ack.assert_not_awaited()
    message.nack.assert_awaited_once_with(requeue=True)
    if ready:
        recorded.assert_awaited_once()
    else:
        recorded.assert_not_awaited()


@pytest.mark.asyncio
async def test_versionless_terminal_is_dead_lettered_before_purge_or_schedule() -> None:
    message = _terminal(None)
    purge = AsyncMock()
    recorded = AsyncMock()
    with (
        patch("tableinator.tableinator.durable_refresh_active", True),
        patch("tableinator.tableinator.durable_refresh_ready", True),
        patch("tableinator.tableinator.connection_pool", MagicMock()),
        patch("tableinator.tableinator.purge_stale_rows", new=purge),
        patch("tableinator.tableinator.record_durable_signal", new=recorded),
    ):
        await on_data_message(message, "artists")
    message.ack.assert_not_awaited()
    message.nack.assert_awaited_once_with(requeue=False)
    purge.assert_not_awaited()
    recorded.assert_not_awaited()


@pytest.mark.asyncio
async def test_repeated_commit_failure_pauses_broker_before_nack_then_recovers() -> None:
    """Many DB probes consume no additional x-delivery-count; one recovery commits."""
    message = _terminal()
    queue = MagicMock()
    queue.cancel = AsyncMock()
    queue.consume = AsyncMock(return_value="resumed-tag")
    service.consumer_tags = {"artists": "original-tag"}
    service.queues = {"artists": queue}
    service.active_connection = MagicMock()
    service.active_channel = MagicMock()
    service.durable_refresh_worker_task = MagicMock()  # Already running.
    committed = DurableSignal("20260920", 1, frozenset({"artists"}), False, False, False)
    recorded = AsyncMock(side_effect=[RuntimeError("DB down"), RuntimeError("still down"), RuntimeError("still down"), committed, committed])

    async def nack_after_cancel(*, requeue: bool) -> None:
        assert requeue
        queue.cancel.assert_awaited_once_with("original-tag", nowait=False)

    message.nack.side_effect = nack_after_cancel
    with (
        patch.object(service, "durable_refresh_active", True),
        patch.object(service, "durable_refresh_ready", True),
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "purge_stale_rows", new=AsyncMock()),
        patch.object(service, "record_durable_signal", new=recorded),
        patch.object(service, "probe_durable_schema", new=AsyncMock(return_value=True)),
        patch.object(service, "reconcile_legacy_latches", new=AsyncMock()),
        patch.object(service, "read_refresh_health", new=AsyncMock(return_value={"status": "enabled"})),
    ):
        await on_data_message(message, "artists")
        message.ack.assert_not_awaited()
        message.nack.assert_awaited_once_with(requeue=True)
        assert service.durable_refresh_paused
        assert service.consumer_tags == {}
        assert not await service.attempt_durable_recovery()
        assert not await service.attempt_durable_recovery()
        message.nack.assert_awaited_once()  # Failed DB probes did not reconsume.
        queue.consume.assert_not_awaited()
        assert await service.attempt_durable_recovery()
        queue.consume.assert_awaited_once()
        assert service.consumer_tags == {"artists": "resumed-tag"}
        assert not service.durable_refresh_paused
        replay = _terminal()
        await on_data_message(replay, "artists")
        replay.ack.assert_awaited_once()
        replay.nack.assert_not_awaited()
    assert recorded.await_count == 5  # Failed delivery, two probes, recovery, broker replay.


@pytest.mark.asyncio
async def test_missing_schema_keeps_startup_consumers_closed_until_probe_recovers() -> None:
    queue = MagicMock()
    queue.consume = AsyncMock(return_value="restored-tag")
    service.queues = {"artists": queue}
    service.active_connection = MagicMock()
    service.active_channel = MagicMock()
    service.durable_refresh_worker_task = MagicMock()
    service.durable_refresh_paused = True
    service.durable_refresh_resume_types = {"artists"}
    probe = AsyncMock(side_effect=[False, True])
    with (
        patch.object(service, "durable_refresh_active", True),
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "probe_durable_schema", new=probe),
        patch.object(service, "reconcile_legacy_latches", new=AsyncMock()),
        patch.object(service, "read_refresh_health", new=AsyncMock(return_value={"status": "enabled"})),
    ):
        assert not await service.attempt_durable_recovery()
        queue.consume.assert_not_awaited()
        assert not service.durable_refresh_ready
        assert await service.attempt_durable_recovery()
    queue.consume.assert_awaited_once()
    assert service.consumer_tags == {"artists": "restored-tag"}


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_queue", [False, True])
async def test_uncertain_cancellation_forces_broker_reset_before_database_recovery(missing_queue: bool) -> None:
    message = _terminal()
    queue = MagicMock()
    queue.cancel = AsyncMock(side_effect=RuntimeError("broker unavailable"))
    service.consumer_tags = {"artists": "original-tag"}
    service.queues = {} if missing_queue else {"artists": queue}
    connection = MagicMock()
    connection.close = AsyncMock(side_effect=[RuntimeError("connection close failed"), None])
    channel = MagicMock()
    channel.close = AsyncMock(side_effect=RuntimeError("channel close failed"))
    service.active_connection = connection
    service.active_channel = channel
    service.durable_refresh_worker_task = MagicMock()
    committed = DurableSignal("20260920", 1, frozenset({"artists"}), False, False, False)
    recorded = AsyncMock(side_effect=[RuntimeError("DB down"), committed, committed, committed])
    reconnect_attempts = 0

    async def reconnect() -> None:
        nonlocal reconnect_attempts
        reconnect_attempts += 1
        if reconnect_attempts == 1:
            return  # A stale tag must not make this a successful recovery.
        service.active_connection = MagicMock()
        service.active_channel = MagicMock()
        service.consumer_tags["artists"] = "resumed-tag"

    with (
        patch.object(service, "durable_refresh_active", True),
        patch.object(service, "durable_refresh_ready", True),
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "purge_stale_rows", new=AsyncMock()),
        patch.object(service, "record_durable_signal", new=recorded),
        patch.object(service, "probe_durable_schema", new=AsyncMock(return_value=True)),
        patch.object(service, "reconcile_legacy_latches", new=AsyncMock()),
        patch.object(service, "read_refresh_health", new=AsyncMock(return_value={"status": "enabled"})),
        patch.object(service, "_recover_consumers", new=AsyncMock(side_effect=reconnect)) as recover,
    ):
        await on_data_message(message, "artists")
        assert service.durable_refresh_broker_reset_pending
        assert service.consumer_tags == {"artists": "original-tag"}
        assert service.durable_refresh_recovery_signals == {("artists", "20260920")}
        assert not await service.attempt_durable_recovery()
        assert service.durable_refresh_paused
        assert service.durable_refresh_recovery_signals == {("artists", "20260920")}
        assert await service.attempt_durable_recovery()
        assert recover.await_count == 2
        assert service.consumer_tags == {"artists": "resumed-tag"}
        assert not service.durable_refresh_recovery_signals
        replay = _terminal()
        await on_data_message(replay, "artists")
        replay.ack.assert_awaited_once()
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()
    assert connection.close.await_count == 2
    assert channel.close.await_count == 1
    assert queue.cancel.await_count == (0 if missing_queue else 1)
    assert not service.durable_refresh_paused


@pytest.mark.asyncio
async def test_partial_resubscription_cancel_failure_requires_broker_reset() -> None:
    artists = MagicMock()
    artists.consume = AsyncMock(return_value="artist-first-tag")
    artists.cancel = AsyncMock(side_effect=RuntimeError("cancel outcome unknown"))
    labels = MagicMock()
    labels.consume = AsyncMock(side_effect=RuntimeError("subscribe failed"))
    service.queues = {"artists": artists, "labels": labels}
    connection = MagicMock()
    connection.close = AsyncMock()
    service.active_connection = connection
    service.active_channel = MagicMock()
    service.durable_refresh_paused = True
    service.durable_refresh_resume_types = {"artists", "labels"}
    service.durable_refresh_recovery_signals = {("artists", "20260920")}
    service.durable_refresh_worker_task = MagicMock()
    committed = DurableSignal("20260920", 1, frozenset({"artists"}), False, False, False)
    recorded = AsyncMock(return_value=committed)

    async def reconnect() -> None:
        service.active_connection = MagicMock()
        service.active_channel = MagicMock()
        service.consumer_tags.update({"artists": "artist-new-tag", "labels": "label-new-tag"})

    with (
        patch.object(service, "durable_refresh_active", True),
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "probe_durable_schema", new=AsyncMock(return_value=True)),
        patch.object(service, "reconcile_legacy_latches", new=AsyncMock()),
        patch.object(service, "read_refresh_health", new=AsyncMock(return_value={"status": "enabled"})),
        patch.object(service, "record_durable_signal", new=recorded),
        patch.object(service, "purge_stale_rows", new=AsyncMock()),
        patch.object(service, "_recover_consumers", new=AsyncMock(side_effect=reconnect)) as recover,
    ):
        assert not await service.attempt_durable_recovery()
        artists.cancel.assert_awaited_once_with("artist-first-tag", nowait=False)
        connection.close.assert_awaited_once()
        assert service.consumer_tags == {}
        assert service.durable_refresh_paused
        assert service.durable_refresh_recovery_signals == {("artists", "20260920")}
        assert not service.durable_refresh_broker_reset_pending
        assert await service.attempt_durable_recovery()
        recover.assert_awaited_once()
        assert service.consumer_tags == {"artists": "artist-new-tag", "labels": "label-new-tag"}
        replay = _terminal()
        await on_data_message(replay, "artists")
        replay.ack.assert_awaited_once()
    assert recorded.await_count == 3  # First probe, recovery probe, broker replay.
