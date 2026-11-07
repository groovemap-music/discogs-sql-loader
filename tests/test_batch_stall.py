"""Stall accounting: what counts as progress, how a held record's wait is kept, and the settings."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common.delivery import DeliveryResult, Settlement

import tableinator.tableinator as service
from tableinator import telemetry
from tableinator.batch_processor import BatchConfig, PostgreSQLBatchObserver, PostgreSQLBatchProcessor, validate_stall_s
from tableinator.batch_writer import BatchWriteResult


def settle(observer: PostgreSQLBatchObserver, settlement: Settlement, outcome: str = "processed") -> None:
    observer.accepted("artists", time.perf_counter())
    observer.settled(entity="artists", result=DeliveryResult(settlement, outcome), duration_s=0.0, span=None)


class TestWhatCountsAsProgress:
    def test_only_ack_and_reject_advance_last_settled_at(self) -> None:
        # The observer stamps time.monotonic(), whose origin is the boot time on Linux: a fixed number such as
        # 100.0 is in the future on a freshly booted CI runner. The baseline is taken from the same clock instead.
        observer = PostgreSQLBatchObserver()
        baseline = time.monotonic() - 1000.0
        observer.last_settled_at["artists"] = baseline

        settle(observer, Settlement.REQUEUE, "held_timeout")
        assert observer.last_settled_at["artists"] == baseline
        settle(observer, Settlement.REQUEUE, "delivery_lost")
        assert observer.last_settled_at["artists"] == baseline

        settle(observer, Settlement.ACK)
        assert observer.last_settled_at["artists"] > baseline
        observer.last_settled_at["artists"] = baseline
        settle(observer, Settlement.REJECT, "poison")
        assert observer.last_settled_at["artists"] > baseline

    def test_a_requeue_keeps_the_wait_and_only_ack_or_reject_ends_it(self) -> None:
        observer = PostgreSQLBatchObserver()
        observer.accepted("artists", time.perf_counter() - 50.0)
        observer.settled(entity="artists", result=DeliveryResult(Settlement.REQUEUE, "held_timeout"), duration_s=0.0, span=None)
        assert observer.held_age_s("artists") == pytest.approx(50.0, abs=1.0), "the wait began when the record was first accepted"

        # It comes back from the broker and is accepted again: a new accepted_at, the same wait.
        observer.accepted("artists", time.perf_counter())
        observer.settled(entity="artists", result=DeliveryResult(Settlement.REQUEUE, "held_timeout"), duration_s=0.0, span=None)
        assert observer.held_age_s("artists") == pytest.approx(50.0, abs=1.0)
        assert observer.outstanding_accepted()["artists"] == 0

        settle(observer, Settlement.ACK)
        assert observer.held_age_s("artists") == 0.0

    def test_a_reject_also_ends_the_wait(self) -> None:
        observer = PostgreSQLBatchObserver()
        settle(observer, Settlement.REQUEUE, "held_timeout")
        assert observer.held_since["artists"] is not None
        settle(observer, Settlement.REJECT, "redelivery_exhausted")
        assert observer.held_since["artists"] is None

    def test_requeues_are_counted_by_reason(self) -> None:
        observer = PostgreSQLBatchObserver()
        with patch.object(telemetry, "record_batch_requeued") as counter:
            settle(observer, Settlement.REQUEUE, "held_timeout")
            settle(observer, Settlement.ACK)
        counter.assert_called_once_with("artists", "held_timeout")
        assert observer.requeued_counts["artists"] == 1


class TestLostDeliveriesDoNotStartAWait:
    def test_a_delivery_lost_report_never_sets_held_since(self) -> None:
        observer = PostgreSQLBatchObserver()
        settle(observer, Settlement.REQUEUE, "delivery_lost")
        assert observer.held_since["artists"] is None
        assert observer.requeued_counts["artists"] == 1

    @pytest.mark.asyncio
    async def test_the_real_engine_reports_a_lost_delivery_after_the_acks_and_health_stays_clear(self) -> None:
        """A batch whose ack fails on a dead channel: the engine acks the rest, then reports the loss."""
        processor = PostgreSQLBatchProcessor(MagicMock(), BatchConfig(batch_size=3, min_batch_size=1, flush_interval=0.01))
        processor._process_batch = AsyncMock(return_value=BatchWriteResult(set(), set(), set()))  # type: ignore[method-assign]
        good, dead = AsyncMock(), AsyncMock(side_effect=ConnectionResetError("channel closed"))
        for record_id, ack in (("1", good), ("2", dead), ("3", good)):
            await processor.add_message("artists", {"id": record_id, "name": record_id, "sha256": record_id}, ack, AsyncMock())
        assert await processor.flush_queue("artists")

        observer = processor._observer
        assert good.await_count == 2
        assert observer.processed_counts["artists"] == 2
        assert observer.requeued_counts["artists"] == 1, "the lost delivery was reported as a requeue"
        assert observer.held_since["artists"] is None, "...and that report did not start a wait the broker's redelivery will never end"
        assert observer.outstanding_accepted()["artists"] == 0

        observer.last_settled_at["artists"] -= 10_000.0
        assert processor.stalled(300.0) == {}


class TestStalledIncludesTheHeldWait:
    def test_a_requeued_record_still_counts_after_it_returns_to_the_broker(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock())
        observer = processor._observer
        long_ago = time.monotonic() - 1000.0
        observer.last_settled_at["artists"] = long_ago
        observer.held_since["artists"] = long_ago
        # Nothing queued and nothing accepted: the record is back at the broker.
        assert processor._snapshot()["oldest_pending_age_s"]["artists"] == 0.0

        stalled = processor.stalled(300.0)

        assert set(stalled) == {"artists"}
        assert stalled["artists"] >= 1000.0

    def test_gauges_publish_stalled_state_and_age(self) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock())
        long_ago = time.monotonic() - 500.0
        processor._observer.last_settled_at["labels"] = long_ago
        processor._observer.held_since["labels"] = long_ago
        with patch.object(telemetry, "record_batch_stall") as gauge:
            assert set(processor.report_stall_gauges(300.0)) == {"labels"}
        by_entity = {call.args[0]: call.args[1:] for call in gauge.call_args_list}
        assert by_entity["labels"][0] is True
        assert by_entity["labels"][1] >= 500.0
        assert by_entity["artists"] == (False, 0.0)


class TestStallSetting:
    @pytest.mark.parametrize("value", [0.0, -1.0, float("nan")])
    def test_a_stall_bound_must_be_positive(self, value: float) -> None:
        with pytest.raises(ValueError, match="positive"):
            validate_stall_s(value, 1500.0)

    @pytest.mark.parametrize("value", [1500.0, 2000.0])
    def test_a_stall_bound_must_be_below_the_hold(self, value: float) -> None:
        with pytest.raises(ValueError, match="below"):
            validate_stall_s(value, 1500.0)

    def test_a_valid_bound_passes_and_a_disabled_hold_only_needs_it_positive(self) -> None:
        assert validate_stall_s(300.0, 1500.0) == 300.0
        assert validate_stall_s(5000.0, None) == 5000.0


@pytest.mark.asyncio
async def test_a_durable_pause_stops_a_per_type_recovery_before_it_closes_anything() -> None:
    channel = MagicMock(close=AsyncMock(), is_closed=False)
    service.delivery_channels = {"labels": channel}
    service.consumer_tags = {"labels": "tag"}
    with (
        patch.object(service, "durable_refresh_active", True),
        patch.object(service, "durable_refresh_paused", True),
        patch.object(service.telemetry, "record_consumer_stopped") as stopped,
    ):
        assert not await service._recover_data_type("labels")

    channel.close.assert_not_awaited()
    stopped.assert_not_called()
    assert service.delivery_channels == {"labels": channel}
    assert service.consumer_tags == {"labels": "tag"}
