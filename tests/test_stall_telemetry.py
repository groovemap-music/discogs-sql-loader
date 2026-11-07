"""Stall and requeue gauges, read back from the real OpenTelemetry instruments.

tests/test_batch_stall.py pins what counts as a stall by patching the recorder. These
tests stop at the recorder: they drive the real batch processor and read what the
in-memory MeterProvider actually holds, so a renamed instrument or a dropped
attribute fails here, not on the dashboard.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from common.delivery import DeliveryResult, Settlement

from tableinator import telemetry
from tableinator.batch_processor import PostgreSQLBatchObserver, PostgreSQLBatchProcessor


if TYPE_CHECKING:
    from tests.conftest import MetricsCollector


STALL_S = 300.0


def _gauge_value(collector: MetricsCollector, name: str, entity: str) -> float:
    matches = [point.value for point in collector.points(name) if dict(point.attributes).get("entity") == entity]
    assert len(matches) == 1, f"expected one {name} point for {entity}, got {len(matches)}"
    return matches[0]


def _settle(observer: PostgreSQLBatchObserver, entity: str, settlement: Settlement, outcome: str) -> None:
    observer.accepted(entity, time.perf_counter())
    observer.settled(entity=entity, result=DeliveryResult(settlement, outcome), duration_s=0.0, span=None)


class TestStalledBatchReachesTelemetry:
    def test_a_stalled_batch_raises_the_stalled_gauge_with_its_age(self, metrics_collector: MetricsCollector) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock())
        observer = processor._observer
        long_ago = time.monotonic() - 1000.0
        observer.last_settled_at["artists"] = long_ago
        observer.held_since["artists"] = long_ago

        stalled = processor.report_stall_gauges(STALL_S)

        assert set(stalled) == {"artists"}
        assert _gauge_value(metrics_collector, telemetry.PIPELINE_BATCH_STALLED, "artists") == 1
        assert _gauge_value(metrics_collector, telemetry.PIPELINE_BATCH_OLDEST_UNSETTLED_AGE, "artists") >= 1000.0
        assert dict(metrics_collector.points(telemetry.PIPELINE_BATCH_STALLED)[0].attributes)["loader"] == "discogs"

    def test_a_requeue_is_counted_on_the_real_counter_by_reason(self, metrics_collector: MetricsCollector) -> None:
        observer = PostgreSQLBatchObserver()

        _settle(observer, "artists", Settlement.REQUEUE, "held_timeout")
        _settle(observer, "artists", Settlement.REQUEUE, "held_timeout")
        _settle(observer, "artists", Settlement.REQUEUE, "delivery_lost")

        by_reason = {
            attrs["reason"]: point.value
            for point, attrs in zip(
                metrics_collector.points(telemetry.PIPELINE_BATCH_REQUEUED),
                metrics_collector.attributes(telemetry.PIPELINE_BATCH_REQUEUED),
                strict=True,
            )
        }
        assert by_reason == {"held_timeout": 2, "delivery_lost": 1}


class TestNormalLongFlushStaysSilent:
    def test_a_deep_queue_that_keeps_settling_does_not_raise_the_gauge(self, metrics_collector: MetricsCollector) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock())
        observer = processor._observer
        # The oldest record has waited well past the bound, but flushes are still
        # acking: the last settlement is recent.
        observer.accepted("artists", time.perf_counter() - 900.0)
        observer.last_settled_at["artists"] = time.monotonic()

        stalled = processor.report_stall_gauges(STALL_S)

        assert "artists" not in stalled
        assert _gauge_value(metrics_collector, telemetry.PIPELINE_BATCH_STALLED, "artists") == 0
        # The age is still visible, so a long but progressing flush can be read on a dashboard.
        assert _gauge_value(metrics_collector, telemetry.PIPELINE_BATCH_OLDEST_UNSETTLED_AGE, "artists") >= 900.0

    def test_an_idle_entity_reports_zero_not_absent(self, metrics_collector: MetricsCollector) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock())

        processor.report_stall_gauges(STALL_S)

        assert _gauge_value(metrics_collector, telemetry.PIPELINE_BATCH_STALLED, "labels") == 0


class TestRecoveryClearsTheGauge:
    def test_an_ack_after_a_stall_drops_the_gauge_back_to_zero(self, metrics_collector: MetricsCollector) -> None:
        processor = PostgreSQLBatchProcessor(MagicMock())
        observer = processor._observer
        long_ago = time.monotonic() - 1000.0
        observer.last_settled_at["artists"] = long_ago
        observer.held_since["artists"] = long_ago
        processor.report_stall_gauges(STALL_S)
        assert _gauge_value(metrics_collector, telemetry.PIPELINE_BATCH_STALLED, "artists") == 1

        _settle(observer, "artists", Settlement.ACK, "processed")
        processor.report_stall_gauges(STALL_S)

        assert _gauge_value(metrics_collector, telemetry.PIPELINE_BATCH_STALLED, "artists") == 0
