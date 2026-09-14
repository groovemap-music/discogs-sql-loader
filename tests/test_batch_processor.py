"""Owner-boundary tests for the shared Discogs SQL batch engine."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common.batch import AsyncBatchEngine
from common.db_resilience import DatabaseUnavailableError
from common.delivery import FailureKind
from psycopg.errors import DataError, InterfaceError, OperationalError

from tableinator.batch_processor import (
    DATA_TYPES,
    BatchConfig,
    BatchWriteResult,
    PendingMessage,
    PostgreSQLBatchProcessor,
    PostgreSQLBatchSink,
    PostgreSQLFailureClassifier,
)


def result(
    unchanged: set[str] | None = None,
    media: set[str] | None = None,
    identity: set[str] | None = None,
) -> BatchWriteResult:
    return BatchWriteResult(unchanged or set(), media or set(), identity or set())


def processor_config(**overrides: Any) -> BatchConfig:
    values: dict[str, Any] = {
        "batch_size": 10,
        "flush_interval": 0.01,
        "max_pending": 20,
        "max_concurrent_flushes": 2,
        "min_batch_size": 1,
        "backoff_initial": 0.001,
        "backoff_max": 0.002,
        "backoff_multiplier": 2.0,
        "max_flush_retries": 2,
        "max_poison_retries": 2,
    }
    values.update(overrides)
    return BatchConfig(**values)


def message(data_id: str, *, data_type: str = "artists") -> PendingMessage:
    return PendingMessage(
        data_type,
        data_id,
        {"id": data_id, "sha256": f"hash-{data_id}"},
        f"hash-{data_id}",
        AsyncMock(),
        AsyncMock(),
    )


class TestPolicyAdapters:
    def test_runtime_is_pinned_to_the_reviewed_revision(self) -> None:
        pyproject = Path("pyproject.toml").read_text()
        lock = Path("uv.lock").read_text()
        revision = "24704f5fd48d3ef4fff29398585e9924e225b0c5"
        assert revision in pyproject
        assert f"#{revision}" in lock

    def test_config_maps_every_lifecycle_setting_to_the_runtime_policy(self) -> None:
        config = processor_config()
        policy = config.runtime_policy()
        assert policy.batch_size == config.batch_size
        assert policy.flush_interval_s == config.flush_interval
        assert policy.max_pending == config.max_pending
        assert policy.max_concurrent_flushes == config.max_concurrent_flushes
        assert policy.max_drain_retries == config.max_flush_retries
        assert policy.max_poison_retries == config.max_poison_retries

    def test_owner_uses_shared_engine_instead_of_a_local_queue_lifecycle(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config())
        assert isinstance(processor._engine, AsyncBatchEngine)
        assert not hasattr(processor, "queues")
        assert not hasattr(processor, "_flush_semaphore")
        assert not hasattr(processor, "_flush_locks")

    @pytest.mark.parametrize("error", [InterfaceError("lost"), OperationalError("down"), DatabaseUnavailableError("open")])
    def test_postgres_connectivity_failures_are_transient(self, error: BaseException) -> None:
        assert PostgreSQLFailureClassifier()(error) is FailureKind.TRANSIENT

    @pytest.mark.parametrize("error", [DataError("bad cast"), ValueError("bad payload"), RuntimeError("missing identity")])
    def test_deterministic_data_failures_are_not_retried_as_outages(self, error: BaseException) -> None:
        assert PostgreSQLFailureClassifier()(error) is FailureKind.DETERMINISTIC

    @pytest.mark.asyncio
    async def test_sink_preserves_outcome_sets_and_precedence(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config())
        batch_result = result({"unchanged", "media", "identity"}, {"media"}, {"identity"})
        processor._process_batch = AsyncMock(return_value=batch_result)  # type: ignore[method-assign]
        sink = PostgreSQLBatchSink(processor, processor._observer)
        processor._observer._states["artists"] = MagicMock()
        outcomes = await sink.write(
            "artists",
            [message("changed"), message("unchanged"), message("media"), message("identity")],
        )
        assert [item.outcome for item in outcomes] == ["processed", "skipped", "media_backfilled", "skipped"]


class TestSubmissionAndSettlement:
    @pytest.mark.asyncio
    async def test_valid_message_is_normalized_and_acked_after_the_write(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config(batch_size=1))
        processor._process_batch = AsyncMock(return_value=result())  # type: ignore[method-assign]
        ack = AsyncMock()
        nack = AsyncMock()

        assert await processor.add_message("artists", {"id": "1", "sha256": "h"}, ack, nack)

        processor._process_batch.assert_awaited_once()  # type: ignore[attr-defined]
        ack.assert_awaited_once()
        nack.assert_not_awaited()
        assert processor.get_stats()["pending"]["artists"] == 0

    @pytest.mark.asyncio
    async def test_missing_id_is_rejected_and_vetoes_purge(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config())
        nack = AsyncMock()
        assert not await processor.add_message("artists", {"name": "missing"}, AsyncMock(), nack)
        nack.assert_awaited_once()
        assert processor.had_dlq_nacks("artists")
        processor.reset_dlq_nacks("artists")
        assert not processor.had_dlq_nacks("artists")

    @pytest.mark.asyncio
    async def test_normalization_failure_is_rejected_and_vetoes_purge(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config())
        nack = AsyncMock()
        with patch("tableinator.batch_processor.normalize_record", side_effect=ValueError("bad")):
            assert not await processor.add_message("releases", {"id": "1"}, AsyncMock(), nack)
        nack.assert_awaited_once()
        assert processor.had_dlq_nacks("releases")

    @pytest.mark.asyncio
    async def test_unknown_entity_is_rejected_without_entering_shared_engine(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config())
        nack = AsyncMock()
        assert not await processor.add_message("unknown", {"id": "1"}, AsyncMock(), nack)
        nack.assert_awaited_once()
        assert sum(processor.get_stats()["pending"].values()) == 0

    @pytest.mark.asyncio
    async def test_all_outcomes_settle_exactly_once_and_update_owner_stats(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config(batch_size=4))
        processor._process_batch = AsyncMock(  # type: ignore[method-assign]
            return_value=result({"same", "media", "identity"}, {"media"}, {"identity"})
        )
        acks = [AsyncMock() for _ in range(4)]
        nacks = [AsyncMock() for _ in range(4)]
        with patch("tableinator.batch_processor.telemetry.record_message") as record_message:
            for data_id, ack, nack in zip(("changed", "same", "media", "identity"), acks, nacks, strict=True):
                await processor.add_message("artists", {"id": data_id}, ack, nack)

        assert all(ack.await_count == 1 for ack in acks)
        assert all(nack.await_count == 0 for nack in nacks)
        assert [call.args[1] for call in record_message.call_args_list] == [
            "processed",
            "skipped",
            "media_backfilled",
            "skipped",
        ]
        stats = processor.get_stats()
        assert stats["processed"]["artists"] == 4
        assert stats["batches"]["artists"] == 1
        assert stats["media_backfilled"]["artists"] == 1
        assert stats["identity_backfilled"]["artists"] == 1


class TestSharedEngineSafety:
    @pytest.mark.asyncio
    async def test_transient_retry_never_charges_poison_or_nacks(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config(batch_size=1))
        processor._process_batch = AsyncMock(  # type: ignore[method-assign]
            side_effect=[OperationalError("down"), result()]
        )
        ack = AsyncMock()
        nack = AsyncMock()

        assert await processor.add_message("artists", {"id": "1"}, ack, nack)
        first = processor.get_stats()
        assert first["pending"]["artists"] == 1
        assert first["transient_failures"]["artists"] == 1
        assert first["consecutive_failures"]["artists"] == 0
        assert await processor.flush_queue("artists")
        ack.assert_awaited_once()
        nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bounded_drain_keeps_transient_messages_pending(self) -> None:
        processor = PostgreSQLBatchProcessor(
            MagicMock(),
            processor_config(batch_size=2, max_flush_retries=1),
        )
        processor._process_batch = AsyncMock(side_effect=DatabaseUnavailableError("down"))  # type: ignore[method-assign]
        ack = AsyncMock()
        nack = AsyncMock()
        await processor.add_message("artists", {"id": "1"}, ack, nack)

        assert not await processor.flush_queue("artists")
        assert processor.get_stats()["pending"]["artists"] == 1
        ack.assert_not_awaited()
        nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_poison_is_bounded_rejected_once_and_vetoes_purge(self) -> None:
        processor = PostgreSQLBatchProcessor(
            MagicMock(),
            processor_config(batch_size=1, max_poison_retries=2),
        )
        processor._process_batch = AsyncMock(side_effect=ValueError("invalid jsonb"))  # type: ignore[method-assign]
        ack = AsyncMock()
        nack = AsyncMock()

        await processor.add_message("artists", {"id": "poison"}, ack, nack)

        assert processor._process_batch.await_count == 2  # type: ignore[attr-defined]
        ack.assert_not_awaited()
        nack.assert_awaited_once()
        assert processor.had_dlq_nacks("artists")
        assert processor.get_stats()["pending"]["artists"] == 0

    @pytest.mark.asyncio
    async def test_cancellation_restores_the_unsettled_batch_in_order(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config(batch_size=2))
        entered = asyncio.Event()
        release = asyncio.Event()

        async def blocked(_data_type: str, _messages: list[PendingMessage]) -> BatchWriteResult:
            entered.set()
            await release.wait()
            return result()

        processor._process_batch = blocked  # type: ignore[method-assign]
        first_ack = AsyncMock()
        second_ack = AsyncMock()
        await processor.add_message("artists", {"id": "1"}, first_ack, AsyncMock())
        submitting = asyncio.create_task(processor.add_message("artists", {"id": "2"}, second_ack, AsyncMock()))
        await entered.wait()
        submitting.cancel()
        with suppress(asyncio.CancelledError):
            await submitting
        assert processor.get_stats()["pending"]["artists"] == 2
        assert first_ack.await_count == second_ack.await_count == 0

        processor._process_batch = AsyncMock(return_value=result())  # type: ignore[method-assign]
        assert await processor.flush_queue("artists")
        first_ack.assert_awaited_once()
        second_ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_same_entity_flushes_are_serialized(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config(batch_size=1))
        active = 0
        maximum = 0

        async def write(_data_type: str, _messages: list[PendingMessage]) -> BatchWriteResult:
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.005)
            active -= 1
            return result()

        processor._process_batch = write  # type: ignore[method-assign]
        await asyncio.gather(
            processor.add_message("artists", {"id": "1"}, AsyncMock(), AsyncMock()),
            processor.add_message("artists", {"id": "2"}, AsyncMock(), AsyncMock()),
        )
        assert maximum == 1

    @pytest.mark.asyncio
    async def test_different_entities_respect_global_flush_bound(self) -> None:
        processor = PostgreSQLBatchProcessor(
            MagicMock(),
            processor_config(batch_size=1, max_concurrent_flushes=2),
        )
        active = 0
        maximum = 0

        async def write(_data_type: str, _messages: list[PendingMessage]) -> BatchWriteResult:
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.005)
            active -= 1
            return result()

        processor._process_batch = write  # type: ignore[method-assign]
        await asyncio.gather(*(processor.add_message(entity, {"id": entity}, AsyncMock(), AsyncMock()) for entity in DATA_TYPES))
        assert maximum == 2

    @pytest.mark.asyncio
    async def test_capacity_blocks_and_recovers_after_settlement(self) -> None:
        processor = PostgreSQLBatchProcessor(
            MagicMock(),
            processor_config(batch_size=2, max_pending=2),
        )
        entered = asyncio.Event()
        release = asyncio.Event()

        async def write(_data_type: str, _messages: list[PendingMessage]) -> BatchWriteResult:
            entered.set()
            await release.wait()
            return result()

        processor._process_batch = write  # type: ignore[method-assign]
        await processor.add_message("artists", {"id": "1"}, AsyncMock(), AsyncMock())
        second = asyncio.create_task(processor.add_message("artists", {"id": "2"}, AsyncMock(), AsyncMock()))
        await entered.wait()
        third = asyncio.create_task(processor.add_message("labels", {"id": "3"}, AsyncMock(), AsyncMock()))
        await asyncio.sleep(0)
        assert not third.done()
        release.set()
        await second
        await third
        assert processor.get_stats()["pending"]["labels"] == 1

    @pytest.mark.asyncio
    async def test_observer_failure_cannot_change_successful_settlement(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config(batch_size=1))
        processor._process_batch = AsyncMock(return_value=result())  # type: ignore[method-assign]
        ack = AsyncMock()
        with patch("tableinator.batch_processor.telemetry.record_batch_size", side_effect=RuntimeError("otel down")):
            await processor.add_message("artists", {"id": "1"}, ack, AsyncMock())
        ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_shutdown_stops_periodic_work_but_allows_final_drain(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock(), processor_config(batch_size=2))
        processor._process_batch = AsyncMock(return_value=result())  # type: ignore[method-assign]
        ack = AsyncMock()
        await processor.add_message("artists", {"id": "1"}, ack, AsyncMock())
        processor.shutdown()
        assert await processor.flush_all()
        ack.assert_awaited_once()
        assert processor.get_stats()["shutdown"] is True
