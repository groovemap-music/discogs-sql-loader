"""Offline safety checks for the durable acknowledgement boundary."""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika.abc import AbstractIncomingMessage

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
