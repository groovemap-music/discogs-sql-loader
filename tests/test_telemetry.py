"""Tests for the OpenTelemetry domain instruments in tableinator.telemetry.

Uses an in-memory OpenTelemetry MeterProvider (the `metrics_collector` fixture) so
assertions run against what was actually recorded rather than against mocked calls,
matching groovemap-runtime's own `collector`-based test pattern
(tests/test_runtime_metrics.py at the pinned revision).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika.abc import AbstractIncomingMessage

from tableinator import telemetry
from tableinator.batch_processor import BatchConfig, PendingMessage, PostgreSQLBatchProcessor
from tableinator.tableinator import on_data_message


if TYPE_CHECKING:
    from tests.conftest import MetricsCollector


class TestDomainInstrumentShapes:
    """Each domain instrument's name, unit, and attribute keys match the GrooveMap
    OpenTelemetry metrics conventions (design ADR-0006)."""

    def test_pipeline_messages_is_a_counter_with_the_conventional_attributes(self, metrics_collector: MetricsCollector) -> None:
        telemetry.record_message("artists", "processed", 0.01)

        assert telemetry.PIPELINE_MESSAGES in metrics_collector.metrics()
        assert metrics_collector.attributes(telemetry.PIPELINE_MESSAGES) == [{"source": "discogs", "entity": "artists", "outcome": "processed"}]

    def test_pipeline_message_duration_is_a_seconds_histogram(self, metrics_collector: MetricsCollector) -> None:
        telemetry.record_message("labels", "failed", 0.25)

        metric = metrics_collector.metrics()[telemetry.PIPELINE_MESSAGE_DURATION]
        assert metric.unit == "s"
        assert metrics_collector.attributes(telemetry.PIPELINE_MESSAGE_DURATION) == [{"source": "discogs", "entity": "labels"}]

    def test_batch_size_is_an_items_histogram(self, metrics_collector: MetricsCollector) -> None:
        telemetry.record_batch_size("masters", 42)

        metric = metrics_collector.metrics()[telemetry.PIPELINE_BATCH_SIZE]
        assert metric.unit == "{items}"
        assert metrics_collector.attributes(telemetry.PIPELINE_BATCH_SIZE) == [{"store": "postgresql", "entity": "masters"}]
        assert metrics_collector.points(telemetry.PIPELINE_BATCH_SIZE)[0].sum == 42

    def test_batch_flush_duration_carries_outcome(self, metrics_collector: MetricsCollector) -> None:
        telemetry.record_batch_flush("releases", 0.5, "success")

        metric = metrics_collector.metrics()[telemetry.PIPELINE_BATCH_FLUSH_DURATION]
        assert metric.unit == "s"
        assert metrics_collector.attributes(telemetry.PIPELINE_BATCH_FLUSH_DURATION) == [
            {"store": "postgresql", "entity": "releases", "outcome": "success"}
        ]

    def test_consumers_active_is_an_up_down_counter(self, metrics_collector: MetricsCollector) -> None:
        telemetry.record_consumer_started()
        telemetry.record_consumer_started()
        telemetry.record_consumer_stopped()

        points = metrics_collector.points(telemetry.PIPELINE_CONSUMERS_ACTIVE)
        assert len(points) == 1
        assert points[0].value == 1
        assert dict(points[0].attributes) == {"source": "discogs"}

    def test_consumed_message_matches_the_shared_wrapper_shape(self, metrics_collector: MetricsCollector) -> None:
        """Same metric names and attribute keys as common.runtime_metrics.record_consumed_message."""
        telemetry.record_consumed_message("groovemap-discogs-tableinator-artists", 0.1)

        assert metrics_collector.attributes(telemetry.MESSAGING_CONSUMED_MESSAGES) == [
            {
                "messaging.system": "rabbitmq",
                "messaging.destination.name": "groovemap-discogs-tableinator-artists",
                "messaging.operation.name": "process",
            }
        ]
        duration_metric = metrics_collector.metrics()[telemetry.MESSAGING_OPERATION_DURATION]
        assert duration_metric.unit == "s"

    def test_consumed_message_records_error_type_on_failure(self, metrics_collector: MetricsCollector) -> None:
        telemetry.record_consumed_message("groovemap-discogs-tableinator-artists", 0.1, "missing_id")

        attrs = metrics_collector.attributes(telemetry.MESSAGING_CONSUMED_MESSAGES)[0]
        assert attrs["error.type"] == "missing_id"

    def test_recording_never_raises_without_a_provider(self) -> None:
        """No metrics_collector fixture here: the no-op path (no setup_telemetry call
        in this process) must swallow everything silently."""
        telemetry.record_message("artists", "processed", 0.01)
        telemetry.record_batch_size("artists", 1)
        telemetry.record_batch_flush("artists", 0.01, "success")
        telemetry.record_consumer_started()
        telemetry.record_consumer_stopped()
        telemetry.record_consumed_message("artists", 0.01)


class TestOnDataMessageTelemetry:
    """The per-message handler (non-batch mode, the default in this test suite via the
    autouse disable_batch_mode fixture) records the domain instruments on the main path."""

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_successful_write_records_processed(
        self,
        sample_artist_data: dict[str, Any],
        mock_postgres_connection: MagicMock,
        mock_async_pool: Any,
        metrics_collector: MetricsCollector,
    ) -> None:
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = json.dumps(sample_artist_data).encode()

        mock_cursor = AsyncMock()
        mock_cursor_cm = AsyncMock()
        mock_cursor_cm.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_postgres_connection.cursor = MagicMock(return_value=mock_cursor_cm)

        pool = mock_async_pool(mock_postgres_connection)

        with patch("tableinator.tableinator.connection_pool", pool):
            await on_data_message(mock_message, "artists")

        assert metrics_collector.attributes(telemetry.PIPELINE_MESSAGES) == [{"source": "discogs", "entity": "artists", "outcome": "processed"}]
        consumed_attrs = metrics_collector.attributes(telemetry.MESSAGING_CONSUMED_MESSAGES)
        assert consumed_attrs == [
            {
                "messaging.system": "rabbitmq",
                "messaging.destination.name": "artists",
                "messaging.operation.name": "process",
            }
        ]

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_missing_id_records_failed_with_error_type(self, metrics_collector: MetricsCollector) -> None:
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = json.dumps({"name": "no id here"}).encode()

        await on_data_message(mock_message, "artists")

        assert metrics_collector.attributes(telemetry.PIPELINE_MESSAGES) == [{"source": "discogs", "entity": "artists", "outcome": "failed"}]
        consumed_attrs = metrics_collector.attributes(telemetry.MESSAGING_CONSUMED_MESSAGES)[0]
        assert consumed_attrs["error.type"] == "missing_id"

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", True)
    async def test_shutdown_leaves_the_delivery_unrecorded(self, metrics_collector: MetricsCollector) -> None:
        """Not a terminal disposition: the message stays unsettled, so it must not be
        counted as processed or failed."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)

        await on_data_message(mock_message, "artists")

        assert metrics_collector.metrics().get(telemetry.PIPELINE_MESSAGES) is None


class TestBatchProcessorTelemetry:
    """The batch processor records the domain instruments at each terminal flush."""

    @staticmethod
    def _connection_pool(fetchall_result: list[tuple[Any, ...]]) -> MagicMock:
        mock_connection = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.fetchall = AsyncMock(return_value=fetchall_result)

        mock_cursor_cm = AsyncMock()
        mock_cursor_cm.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_connection.cursor = MagicMock(return_value=mock_cursor_cm)

        mock_tx_cm = AsyncMock()
        mock_tx_cm.__aenter__ = AsyncMock(return_value=None)
        mock_tx_cm.__aexit__ = AsyncMock(return_value=None)
        mock_connection.transaction = MagicMock(return_value=mock_tx_cm)

        mock_connection_cm = AsyncMock()
        mock_connection_cm.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_cm.__aexit__ = AsyncMock(return_value=None)

        mock_pool = MagicMock()
        mock_pool.connection = MagicMock(return_value=mock_connection_cm)
        return mock_pool

    @pytest.mark.asyncio
    async def test_successful_flush_records_processed_skipped_and_batch_metrics(self, metrics_collector: MetricsCollector) -> None:
        # data_id "1" already has the same hash on record (unchanged -> skipped);
        # "2" has a different hash on record (changed -> processed).
        pool = self._connection_pool([("1", "abc"), ("2", "old-hash")])
        processor = PostgreSQLBatchProcessor(pool, BatchConfig(batch_size=10))

        processor.queues["artists"].append(PendingMessage("artists", "1", {"id": "1"}, "abc", AsyncMock(), AsyncMock()))
        processor.queues["artists"].append(PendingMessage("artists", "2", {"id": "2"}, "new-hash", AsyncMock(), AsyncMock()))

        with patch("tableinator.batch_processor.logger"):
            await processor._flush_queue("artists")

        message_attrs = metrics_collector.attributes(telemetry.PIPELINE_MESSAGES)
        outcomes = {attrs["outcome"] for attrs in message_attrs}
        assert outcomes == {"processed", "skipped"}
        assert all(attrs["source"] == "discogs" and attrs["entity"] == "artists" for attrs in message_attrs)

        assert metrics_collector.attributes(telemetry.PIPELINE_BATCH_SIZE) == [{"store": "postgresql", "entity": "artists"}]
        assert metrics_collector.points(telemetry.PIPELINE_BATCH_SIZE)[0].sum == 2

        assert metrics_collector.attributes(telemetry.PIPELINE_BATCH_FLUSH_DURATION) == [
            {"store": "postgresql", "entity": "artists", "outcome": "success"}
        ]

    @pytest.mark.asyncio
    async def test_media_backfill_is_recorded_apart_from_a_plain_skip(self, metrics_collector: MetricsCollector) -> None:
        """A hash-unchanged `releases` row whose NULL `media` this flush filled is
        recorded as `media_backfilled`, not folded into `skipped` (ADR 0007). Both rows
        match on hash; only "1" still has a NULL media column."""
        pool = self._connection_pool([("1", "abc", True), ("2", "def", False)])
        processor = PostgreSQLBatchProcessor(pool, BatchConfig(batch_size=10))

        release = {"id": "1", "formats": [{"name": "Vinyl", "qty": "1"}]}
        processor.queues["releases"].append(PendingMessage("releases", "1", release, "abc", AsyncMock(), AsyncMock()))
        processor.queues["releases"].append(PendingMessage("releases", "2", dict(release, id="2"), "def", AsyncMock(), AsyncMock()))

        with patch("tableinator.batch_processor.logger"):
            await processor._flush_queue("releases")

        outcomes = {attrs["outcome"] for attrs in metrics_collector.attributes(telemetry.PIPELINE_MESSAGES)}
        assert outcomes == {"media_backfilled", "skipped"}

    @pytest.mark.asyncio
    async def test_poison_batch_records_failed_and_flush_failure(self, metrics_collector: MetricsCollector) -> None:
        config = BatchConfig(batch_size=5, max_poison_retries=1, backoff_initial=0.0, min_batch_size=1)
        processor = PostgreSQLBatchProcessor(MagicMock(), config=config)
        processor._process_batch = AsyncMock(side_effect=ValueError("invalid jsonb"))  # type: ignore[method-assign]

        processor.queues["artists"].append(PendingMessage("artists", "1", {"id": "1"}, "abc", AsyncMock(), AsyncMock()))

        with patch("tableinator.batch_processor.logger"):
            await processor._flush_queue("artists")

        assert metrics_collector.attributes(telemetry.PIPELINE_MESSAGES) == [{"source": "discogs", "entity": "artists", "outcome": "failed"}]
        consumed_attrs = metrics_collector.attributes(telemetry.MESSAGING_CONSUMED_MESSAGES)[0]
        assert consumed_attrs["error.type"] == "ValueError"

        assert metrics_collector.attributes(telemetry.PIPELINE_BATCH_FLUSH_DURATION) == [
            {"store": "postgresql", "entity": "artists", "outcome": "failed"}
        ]

    @pytest.mark.asyncio
    async def test_transient_failure_records_nothing_terminal(self, metrics_collector: MetricsCollector) -> None:
        """A transient PostgreSQL error re-enqueues the batch for an in-process retry;
        it is not a terminal disposition, so no message/flush outcome is recorded yet
        (only the batch-size gauge, which fires per attempt)."""
        from common.db_resilience import DatabaseUnavailableError

        config = BatchConfig(batch_size=5, backoff_initial=0.0)
        processor = PostgreSQLBatchProcessor(MagicMock(), config=config)
        processor._process_batch = AsyncMock(side_effect=DatabaseUnavailableError("db down"))  # type: ignore[method-assign]

        processor.queues["artists"].append(PendingMessage("artists", "1", {"id": "1"}, "abc", AsyncMock(), AsyncMock()))

        with patch("tableinator.batch_processor.logger"):
            await processor._flush_queue("artists")

        assert metrics_collector.metrics().get(telemetry.PIPELINE_MESSAGES) is None
        assert metrics_collector.metrics().get(telemetry.PIPELINE_BATCH_FLUSH_DURATION) is None
        assert metrics_collector.points(telemetry.PIPELINE_BATCH_SIZE)[0].sum == 1

    @pytest.mark.asyncio
    async def test_immediate_reject_records_failed_without_reaching_the_queue(self, metrics_collector: MetricsCollector) -> None:
        """A message rejected inside add_message() (missing 'id') never reaches
        _flush_queue_locked, so add_message itself must record the terminal outcome."""
        processor = PostgreSQLBatchProcessor(MagicMock(), BatchConfig(batch_size=10))

        accepted = await processor.add_message(
            data_type="artists",
            data={"name": "no id"},
            ack_callback=AsyncMock(),
            nack_callback=AsyncMock(),
        )

        assert accepted is False
        assert metrics_collector.attributes(telemetry.PIPELINE_MESSAGES) == [{"source": "discogs", "entity": "artists", "outcome": "failed"}]
        consumed_attrs = metrics_collector.attributes(telemetry.MESSAGING_CONSUMED_MESSAGES)[0]
        assert consumed_attrs["error.type"] == "missing_id"
