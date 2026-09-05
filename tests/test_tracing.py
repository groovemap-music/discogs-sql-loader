"""Behavioral tests for tableinator's wave-2 spans.

Two spans belong to this service. The CONSUMER `process {entity}` span joins the trace the
extractor started when it published the record, because this service consumes via
`queue.consume(handler)` and so never reaches `common.process_message_with_retry`, which
would otherwise open that span for free. The INTERNAL `flush postgresql {entity}` span
covers one batch write and links back to the deliveries it carries.

Assertions run against an in-memory `TracerProvider` (the `span_collector` fixture), the
tracing counterpart of the `metrics_collector` fixture the metric suites use.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from common import telemetry as common_telemetry
from opentelemetry.sdk.metrics.export import MetricExporter, MetricExportResult
from opentelemetry.trace import NoOpTracerProvider, SpanKind, StatusCode

from tableinator import telemetry
from tableinator.batch_processor import BatchConfig, PostgreSQLBatchProcessor
from tableinator.tableinator import main, make_data_handler


if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.conftest import MetricsCollector, SpanCollector


# A well-formed W3C parent with the sampled flag set — the wire format a broker delivers,
# built by hand rather than by a live span so the expected ids are literal.
PARENT_TRACE_ID = 0x4BF92F3577B34DA6A3CE929D0E0E4736
PARENT_SPAN_ID = 0x00F067AA0BA902B7
SAMPLED_PARENT = {"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}

ENDPOINT = "http://otel-collector:4318"
METRIC_EXPORTER_IMPORT_PATH = "opentelemetry.exporter.otlp.proto.http.metric_exporter"
SPAN_EXPORTER_IMPORT_PATH = "opentelemetry.exporter.otlp.proto.http.trace_exporter"


class FakeQueue:
    """The one aio-pika surface this service consumes through.

    `queue.consume(handler)` is the whole reason tableinator never sees
    `process_message_with_retry`, so a delivery in these tests goes through the same
    registration the service performs rather than calling the handler directly.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.handler: Any = None

    async def consume(self, handler: Any) -> str:
        self.handler = handler
        return f"consumer-tag-{self.name}"

    async def deliver(self, body: bytes, headers: dict[str, Any] | None = None) -> AsyncMock:
        """Push one message to the registered handler and return the message double."""
        message = AsyncMock()
        message.body = body
        message.headers = headers
        await self.handler(message)
        return message


def artist_message(data_id: str = "123456") -> bytes:
    return json.dumps({"id": data_id, "name": "Test Artist", "sha256": f"hash-{data_id}"}).encode()


def batch_pool(fetchall_result: list[tuple[Any, ...]]) -> MagicMock:
    """An async PostgreSQL pool double shaped like the one the batch processor drives."""
    connection = AsyncMock()
    cursor = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=fetchall_result)

    cursor_cm = AsyncMock()
    cursor_cm.__aenter__ = AsyncMock(return_value=cursor)
    cursor_cm.__aexit__ = AsyncMock(return_value=None)
    connection.cursor = MagicMock(return_value=cursor_cm)

    transaction_cm = AsyncMock()
    transaction_cm.__aenter__ = AsyncMock(return_value=None)
    transaction_cm.__aexit__ = AsyncMock(return_value=None)
    connection.transaction = MagicMock(return_value=transaction_cm)

    connection_cm = AsyncMock()
    connection_cm.__aenter__ = AsyncMock(return_value=connection)
    connection_cm.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    pool.connection = MagicMock(return_value=connection_cm)
    return pool


class TestConsumerSpan:
    """Every delivery is processed inside `process {entity}`, joined to its traceparent."""

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_the_span_joins_the_trace_carried_in_the_message_headers(
        self,
        span_collector: SpanCollector,
        mock_postgres_connection: MagicMock,
        mock_async_pool: Any,
    ) -> None:
        queue = FakeQueue("groovemap-discogs-tableinator-artists")
        await queue.consume(make_data_handler("artists"))

        cursor_cm = AsyncMock()
        cursor_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_postgres_connection.cursor = MagicMock(return_value=cursor_cm)

        with patch("tableinator.tableinator.connection_pool", mock_async_pool(mock_postgres_connection)):
            await queue.deliver(artist_message(), headers=SAMPLED_PARENT)

        span = span_collector.one("process artists")
        assert span.kind is SpanKind.CONSUMER
        assert span.context.trace_id == PARENT_TRACE_ID
        assert span.parent is not None
        assert span.parent.span_id == PARENT_SPAN_ID
        assert dict(span.attributes) == {
            "messaging.system": "rabbitmq",
            "messaging.destination.name": "artists",
            "messaging.operation.name": "process",
        }

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_a_delivery_without_headers_starts_a_new_trace(
        self,
        span_collector: SpanCollector,
        mock_postgres_connection: MagicMock,
        mock_async_pool: Any,
    ) -> None:
        queue = FakeQueue("groovemap-discogs-tableinator-labels")
        await queue.consume(make_data_handler("labels"))

        cursor_cm = AsyncMock()
        cursor_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_postgres_connection.cursor = MagicMock(return_value=cursor_cm)

        with patch("tableinator.tableinator.connection_pool", mock_async_pool(mock_postgres_connection)):
            await queue.deliver(artist_message(), headers=None)

        span = span_collector.one("process labels")
        assert span.parent is None
        assert span.context.trace_id != PARENT_TRACE_ID

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_a_malformed_traceparent_starts_a_new_trace_instead_of_failing_the_message(
        self,
        span_collector: SpanCollector,
        mock_postgres_connection: MagicMock,
        mock_async_pool: Any,
    ) -> None:
        queue = FakeQueue("groovemap-discogs-tableinator-masters")
        await queue.consume(make_data_handler("masters"))

        cursor_cm = AsyncMock()
        cursor_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_postgres_connection.cursor = MagicMock(return_value=cursor_cm)

        with patch("tableinator.tableinator.connection_pool", mock_async_pool(mock_postgres_connection)):
            message = await queue.deliver(artist_message(), headers={"traceparent": "not-a-traceparent"})

        span = span_collector.one("process masters")
        assert span.parent is None
        message.ack.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_bytes_headers_from_the_broker_round_trip(
        self,
        span_collector: SpanCollector,
        mock_postgres_connection: MagicMock,
        mock_async_pool: Any,
    ) -> None:
        """An AMQP client may hand back bytes; the trace must survive it."""
        queue = FakeQueue("groovemap-discogs-tableinator-artists")
        await queue.consume(make_data_handler("artists"))

        cursor_cm = AsyncMock()
        cursor_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_postgres_connection.cursor = MagicMock(return_value=cursor_cm)

        headers = {"traceparent": SAMPLED_PARENT["traceparent"].encode()}
        with patch("tableinator.tableinator.connection_pool", mock_async_pool(mock_postgres_connection)):
            await queue.deliver(artist_message(), headers=headers)

        assert span_collector.one("process artists").context.trace_id == PARENT_TRACE_ID

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_a_failed_delivery_records_error_type_without_a_payload(self, span_collector: SpanCollector) -> None:
        queue = FakeQueue("groovemap-discogs-tableinator-artists")
        await queue.consume(make_data_handler("artists"))

        await queue.deliver(b"{not json", headers=SAMPLED_PARENT)

        span = span_collector.one("process artists")
        # The handler catches the parse failure and nacks, so the span itself stays OK:
        # what must not appear is a recorded exception carrying the payload.
        assert span.events == ()

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_the_database_span_would_nest_under_the_consumer_span(self, span_collector: SpanCollector) -> None:
        """The pool's CLIENT span is opened from inside the handler, so it is a child.

        The pool is a double here, so the wrapper's own span cannot fire; opening one at
        exactly the point the wrapper would proves the context is current for it.
        """
        queue = FakeQueue("groovemap-discogs-tableinator-artists")
        await queue.consume(make_data_handler("artists"))

        class SpanningConnection:
            """Opens the CLIENT span where AsyncPostgreSQLPool's wrapper opens its own."""

            def __init__(self) -> None:
                self._span_cm: Any = None

            async def __aenter__(self) -> Any:
                tracer = common_telemetry.tracer_provider().get_tracer("test")
                self._span_cm = tracer.start_as_current_span("execute postgresql", kind=SpanKind.CLIENT)
                self._span_cm.__enter__()
                connection_double = MagicMock()
                cursor_cm = AsyncMock()
                cursor_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
                cursor_cm.__aexit__ = AsyncMock(return_value=None)
                connection_double.cursor = MagicMock(return_value=cursor_cm)
                return connection_double

            async def __aexit__(self, *_args: Any) -> None:
                self._span_cm.__exit__(None, None, None)

        pool = MagicMock()
        pool.connection = MagicMock(side_effect=SpanningConnection)

        with patch("tableinator.tableinator.connection_pool", pool):
            await queue.deliver(artist_message(), headers=SAMPLED_PARENT)

        consumer_span = span_collector.one("process artists")
        db_span = span_collector.one("execute postgresql")
        assert db_span.parent is not None
        assert db_span.parent.span_id == consumer_span.context.span_id
        assert db_span.context.trace_id == PARENT_TRACE_ID


class TestFlushSpan:
    """Every batch write runs inside `flush postgresql {entity}`."""

    @pytest.mark.asyncio
    async def test_a_successful_flush_links_its_deliveries_and_records_the_outcome(self, span_collector: SpanCollector) -> None:
        processor = PostgreSQLBatchProcessor(batch_pool([]), BatchConfig(batch_size=10))

        contexts = []
        for data_id in ("1", "2"):
            with telemetry.consume_span("artists", SAMPLED_PARENT) as span:
                contexts.append(telemetry.span_context_of(span))
                await processor.add_message(
                    data_type="artists",
                    data={"id": data_id, "sha256": f"hash-{data_id}"},
                    ack_callback=AsyncMock(),
                    nack_callback=AsyncMock(),
                    span_context=telemetry.span_context_of(span),
                )

        with patch("tableinator.batch_processor.logger"):
            await processor._flush_queue("artists")

        span = span_collector.one("flush postgresql artists")
        assert span.kind is SpanKind.INTERNAL
        assert dict(span.attributes) == {
            "db.system.name": "postgresql",
            "groovemap.entity": "artists",
            "outcome": "success",
        }
        assert [link.context.span_id for link in span.links] == [context.span_id for context in contexts]
        # Linked, never parented: each delivery's span closed when the handler queued it.
        assert span.parent is None

    @pytest.mark.asyncio
    async def test_the_links_are_capped_at_sixty_four(self, span_collector: SpanCollector) -> None:
        """A batch of ten thousand rows must not carry ten thousand links to the collector."""
        from common.tracing import MAX_FLUSH_LINKS

        processor = PostgreSQLBatchProcessor(batch_pool([]), BatchConfig(batch_size=100))

        for index in range(100):
            with telemetry.consume_span("releases", SAMPLED_PARENT) as span:
                await processor.add_message(
                    data_type="releases",
                    data={"id": str(index), "sha256": f"hash-{index}"},
                    ack_callback=AsyncMock(),
                    nack_callback=AsyncMock(),
                    span_context=telemetry.span_context_of(span),
                )

        with patch("tableinator.batch_processor.logger"):
            await processor._flush_queue("releases")

        assert MAX_FLUSH_LINKS == 64
        assert len(span_collector.one("flush postgresql releases").links) == MAX_FLUSH_LINKS

    @pytest.mark.asyncio
    async def test_the_database_span_nests_inside_the_flush_span(self, span_collector: SpanCollector) -> None:
        """_process_batch's pool calls run inside the flush span, so their spans are children."""
        processor = PostgreSQLBatchProcessor(MagicMock(), BatchConfig(batch_size=10))

        async def process_batch(*_args: Any) -> None:
            tracer = common_telemetry.tracer_provider().get_tracer("test")
            with tracer.start_as_current_span("execute postgresql", kind=SpanKind.CLIENT):
                pass

        processor._process_batch = process_batch  # type: ignore[method-assign]
        await processor.add_message(
            data_type="artists",
            data={"id": "1", "sha256": "hash-1"},
            ack_callback=AsyncMock(),
            nack_callback=AsyncMock(),
        )

        with patch("tableinator.batch_processor.logger"):
            await processor._flush_queue("artists")

        flush = span_collector.one("flush postgresql artists")
        db_span = span_collector.one("execute postgresql")
        assert db_span.parent is not None
        assert db_span.parent.span_id == flush.context.span_id

    @pytest.mark.asyncio
    async def test_a_poison_batch_fails_the_span_with_error_type_only(self, span_collector: SpanCollector) -> None:
        config = BatchConfig(batch_size=5, max_poison_retries=1, backoff_initial=0.0, min_batch_size=1)
        processor = PostgreSQLBatchProcessor(MagicMock(), config=config)
        processor._process_batch = AsyncMock(side_effect=ValueError("invalid jsonb"))  # type: ignore[method-assign]

        await processor.add_message(
            data_type="artists",
            data={"id": "1", "sha256": "hash-1"},
            ack_callback=AsyncMock(),
            nack_callback=AsyncMock(),
        )

        with patch("tableinator.batch_processor.logger"):
            await processor._flush_queue("artists")

        span = span_collector.one("flush postgresql artists")
        assert span.attributes["outcome"] == "failed"
        assert span.attributes["error.type"] == "ValueError"
        assert span.status.status_code is StatusCode.ERROR
        assert span.status.description is None
        assert span.events == ()

    @pytest.mark.asyncio
    async def test_a_transient_failure_fails_the_span_but_records_no_outcome(
        self,
        span_collector: SpanCollector,
        metrics_collector: MetricsCollector,
    ) -> None:
        """The batch goes back on the deque, so the flush concluded in neither outcome —
        and the span's `outcome` attribute shares its closed set with the flush-duration
        metric, which is not recorded here either."""
        from common.db_resilience import DatabaseUnavailableError

        processor = PostgreSQLBatchProcessor(MagicMock(), BatchConfig(batch_size=5, backoff_initial=0.0))
        processor._process_batch = AsyncMock(side_effect=DatabaseUnavailableError("db down"))  # type: ignore[method-assign]

        await processor.add_message(
            data_type="artists",
            data={"id": "1", "sha256": "hash-1"},
            ack_callback=AsyncMock(),
            nack_callback=AsyncMock(),
        )

        with patch("tableinator.batch_processor.logger"):
            await processor._flush_queue("artists")

        span = span_collector.one("flush postgresql artists")
        assert "outcome" not in span.attributes
        assert span.attributes["error.type"] == "DatabaseUnavailableError"
        assert span.status.status_code is StatusCode.ERROR
        assert metrics_collector.metrics().get(telemetry.PIPELINE_BATCH_FLUSH_DURATION) is None


class TestConsumerAndFlushSpansTogether:
    """The whole delivery-to-write path, driven through the fake channel."""

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_a_batched_delivery_is_linked_from_the_flush_that_writes_it(self, span_collector: SpanCollector) -> None:
        processor = PostgreSQLBatchProcessor(batch_pool([]), BatchConfig(batch_size=1))
        queue = FakeQueue("groovemap-discogs-tableinator-artists")
        await queue.consume(make_data_handler("artists"))

        with (
            patch("tableinator.tableinator.BATCH_MODE", True),
            patch("tableinator.tableinator.batch_processor", processor),
            patch("tableinator.batch_processor.logger"),
        ):
            await queue.deliver(artist_message(), headers=SAMPLED_PARENT)

        consumer_span = span_collector.one("process artists")
        flush = span_collector.one("flush postgresql artists")
        assert [link.context.span_id for link in flush.links] == [consumer_span.context.span_id]
        assert consumer_span.context.trace_id == PARENT_TRACE_ID


class DiscardingMetricExporter(MetricExporter):
    """Stands in for the OTLP metric exporter so the bootstrap tests never open a socket."""

    def __init__(self, **_kwargs: Any) -> None:
        super().__init__(preferred_temporality={}, preferred_aggregation={})

    def export(self, metrics_data: Any, timeout_millis: float = 10_000, **_kwargs: Any) -> MetricExportResult:  # noqa: ARG002
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 10_000) -> bool:  # noqa: ARG002
        return True

    def shutdown(self, timeout_millis: float = 30_000, **_kwargs: Any) -> None:
        """Discard the shutdown."""


@pytest.fixture
def isolated_bootstrap(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Give a test pristine provider handles and an exporter that never opens a socket."""
    monkeypatch.setattr(f"{METRIC_EXPORTER_IMPORT_PATH}.OTLPMetricExporter", DiscardingMetricExporter)
    for name in ("_provider", "_sdk_provider", "_tracer_provider", "_sdk_tracer_provider"):
        monkeypatch.setattr(common_telemetry, name, None)
    telemetry.reset_instruments()
    yield
    for name in ("_provider", "_sdk_provider", "_tracer_provider", "_sdk_tracer_provider"):
        monkeypatch.setattr(common_telemetry, name, None)
    telemetry.reset_instruments()


class TestTelemetryBootstrap:
    """The env-var-only contract: either signal can be off without touching the other."""

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.shutdown_requested", False)
    async def test_no_endpoint_creates_no_spans_and_changes_no_behavior(
        self,
        isolated_bootstrap: None,  # noqa: ARG002
        mock_postgres_connection: MagicMock,
        mock_async_pool: Any,
    ) -> None:
        """The default deployment shape: OTEL_EXPORTER_OTLP_ENDPOINT unset."""
        from common import setup_telemetry, shutdown_telemetry

        setup_telemetry("tableinator")
        assert isinstance(common_telemetry.tracer_provider(), NoOpTracerProvider)

        queue = FakeQueue("groovemap-discogs-tableinator-artists")
        await queue.consume(make_data_handler("artists"))

        cursor_cm = AsyncMock()
        cursor_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_postgres_connection.cursor = MagicMock(return_value=cursor_cm)

        with patch("tableinator.tableinator.connection_pool", mock_async_pool(mock_postgres_connection)):
            message = await queue.deliver(artist_message(), headers=SAMPLED_PARENT)

        message.ack.assert_awaited_once()
        with telemetry.consume_span("artists", SAMPLED_PARENT) as span:
            assert span is not None
            assert not span.is_recording()
            assert telemetry.span_context_of(span) is None
        shutdown_telemetry()

    def test_traces_exporter_none_leaves_metrics_exporting(
        self,
        isolated_bootstrap: None,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from common import setup_telemetry, shutdown_telemetry
        from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider

        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", ENDPOINT)
        monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")

        provider = setup_telemetry("tableinator")

        assert isinstance(provider, SdkMeterProvider)
        assert isinstance(common_telemetry.tracer_provider(), NoOpTracerProvider)

        telemetry.record_batch_flush("artists", 0.1, "success")
        with telemetry.batch_flush_span("artists") as span:
            telemetry.set_flush_outcome(span, "success")
            assert span is None or not span.is_recording()
        shutdown_telemetry()


class TestEventLoopMonitor:
    """The one runtime signal no library supplies has to start from the running loop."""

    @pytest.mark.asyncio
    @patch("tableinator.tableinator.setup_logging")
    @patch("tableinator.tableinator.HealthServer")
    async def test_main_starts_the_monitor_from_the_running_loop_after_setup(
        self,
        _mock_health_server: Mock,
        _mock_setup_logging: Mock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[tuple[str, asyncio.AbstractEventLoop]] = []

        def record(name: str) -> Any:
            def recorder(*_args: Any, **_kwargs: Any) -> None:
                calls.append((name, asyncio.get_running_loop()))

            return recorder

        monkeypatch.setattr("tableinator.tableinator.setup_telemetry", record("setup_telemetry"))
        monkeypatch.setattr("tableinator.tableinator.start_event_loop_monitor", record("start_event_loop_monitor"))
        # Fail configuration so main() returns immediately after the telemetry bootstrap.
        monkeypatch.setattr("tableinator.tableinator.TableinatorConfig.from_env", Mock(side_effect=ValueError("no config")))

        await main()

        assert [name for name, _ in calls] == ["setup_telemetry", "start_event_loop_monitor"]
        assert calls[1][1] is asyncio.get_running_loop()


class TestSpanHelperContract:
    """The helpers never raise into application code and tolerate a None span."""

    def test_an_exception_escaping_the_consumer_span_records_error_type_only(self, span_collector: SpanCollector) -> None:
        with pytest.raises(RuntimeError), telemetry.consume_span("artists", SAMPLED_PARENT):
            raise RuntimeError("handler blew up")

        span = span_collector.one("process artists")
        assert span.attributes["error.type"] == "RuntimeError"
        assert span.status.status_code is StatusCode.ERROR
        assert span.status.description is None
        assert span.events == ()

    def test_every_helper_tolerates_a_none_span(self) -> None:
        """Tracing off yields None from the library's flush_span, so nothing may assume a span."""
        assert telemetry.span_context_of(None) is None
        telemetry.mark_span_error(None, ValueError("boom"))
        telemetry.set_flush_outcome(None, "success")
