"""Regression tests for terminal file-completion deliveries."""

from unittest.mock import AsyncMock, patch

import pytest

import tableinator.tableinator as service


@pytest.mark.asyncio
async def test_file_complete_flushes_marks_and_acknowledges() -> None:
    message = AsyncMock(body=b'{"type":"file_complete","total_processed":54321}', routing_key="labels")
    processor = AsyncMock()
    processor.flush_queue.return_value = True
    completed: set[str] = set()
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "batch_processor", processor),
        patch.object(service, "completed_files", completed),
        patch.object(service, "CONSUMER_CANCEL_DELAY", 0),
    ):
        await service.on_data_message(message, "labels")
    processor.flush_queue.assert_awaited_once_with("labels")
    assert completed == {"labels"}
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_drain_requeues_marker_without_marking_complete() -> None:
    message = AsyncMock(body=b'{"type":"file_complete"}', routing_key="artists")
    processor = AsyncMock()
    processor.flush_queue.return_value = False
    completed: set[str] = set()
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "batch_processor", processor),
        patch.object(service, "completed_files", completed),
    ):
        await service.on_data_message(message, "artists")
    assert completed == set()
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_normal_record_continues_to_batch_processor() -> None:
    message = AsyncMock(body=b'{"id":"456","name":"Label","sha256":"def456"}', routing_key="labels")
    processor = AsyncMock()
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "BATCH_MODE", True),
        patch.object(service, "batch_processor", processor),
        patch.object(service, "message_counts", {"labels": 0}),
        patch.object(service, "last_message_time", {"labels": 0.0}),
    ):
        await service.on_data_message(message, "labels")
    processor.add_message.assert_awaited_once()
