"""Pytest configuration for tableinator tests."""

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import telemetry as common_telemetry
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from tableinator import telemetry as tableinator_telemetry


if TYPE_CHECKING:
    from collections.abc import Iterator

    from opentelemetry.sdk.metrics.export import Metric


# Every standard OpenTelemetry variable that changes what the SDK records or exports.
# The telemetry suites assert on what an in-memory provider recorded, so the ambient
# environment (a developer's shell, or CI's own OTEL_SDK_DISABLED=true for its own
# instrumentation) must never leak in and turn every instrument into a no-op or point
# it at a real collector.
OTEL_ENVIRONMENT = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_METRICS_EXEMPLAR_FILTER",
    "OTEL_METRICS_EXPORTER",
    "OTEL_METRIC_EXPORT_INTERVAL",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SDK_DISABLED",
    "OTEL_SERVICE_NAME",
)


@pytest.fixture(autouse=True)
def isolated_otel_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Run every test against a known-empty OpenTelemetry configuration."""
    for name in OTEL_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    yield


@pytest.fixture(autouse=True)
def service_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide deterministic dummy configuration for isolated service tests."""
    values = {
        "POSTGRES_DATABASE": "test",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PASSWORD": "test-password",
        "POSTGRES_USERNAME": "test-user",
        "RABBITMQ_HOST": "localhost",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USERNAME": "guest",
        "STARTUP_DELAY": "0",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


@pytest.fixture
def mock_postgres_connection() -> MagicMock:
    """Return a PostgreSQL connection boundary suitable for unit tests."""
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchone.return_value = None
    return connection


@pytest.fixture
def sample_artist_data() -> dict[str, Any]:
    """Return representative normalized Discogs artist data."""
    return {
        "id": "123456",
        "name": "Test Artist",
        "sha256": "abc123def456",
        "members": [{"id": "234567", "name": "Member 1"}, {"id": "345678", "name": "Member 2"}],
        "aliases": [{"id": "456789", "name": "Alias 1"}],
    }


@pytest.fixture(autouse=True)
def reset_service_state() -> Iterator[None]:
    """Prevent mutable tableinator state from leaking between tests."""
    import tableinator.tableinator as service

    def reset() -> None:
        service.shutdown_requested = False
        service.config = None
        service.connection_pool = None
        service.rabbitmq_manager = None
        service.active_connection = None
        service.active_channel = None
        service.connection_check_task = None
        service.batch_processor = None
        service.current_task = None
        service.current_progress = 0.0
        service.connection_params = {}
        service.message_counts = {"artists": 0, "labels": 0, "masters": 0, "releases": 0}
        service.last_message_time = {"artists": 0.0, "labels": 0.0, "masters": 0.0, "releases": 0.0}
        service.consumer_tags = {}
        service.consumer_cancel_tasks = {}
        service.completed_files = set()
        service.queues = {}
        service.idle_mode = False

    reset()
    yield
    reset()


@pytest.fixture(autouse=True)
def fast_outage_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Preserve outage accounting without adding wall-clock delay."""
    from common.outage_backoff import OutageBackoff

    async def wait(backoff: OutageBackoff) -> float:
        return backoff.next_delay()

    monkeypatch.setattr(OutageBackoff, "wait", wait)


@pytest.fixture(autouse=True)
def disable_batch_mode() -> Iterator[None]:
    """Disable batch mode for all tableinator tests.

    The tests mock the old per-message processing flow, so we need to
    disable batch mode to use that code path.
    """
    with patch("tableinator.tableinator.BATCH_MODE", False), patch("tableinator.tableinator.batch_processor", None):
        yield


@pytest.fixture
def mock_async_pool():
    """Mock AsyncPostgreSQLPool with async context manager support.

    Returns a function that creates a mock pool with a given connection mock.
    This allows tests to configure the connection's behavior before creating the pool.

    Usage:
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.fetchone.return_value = None

        pool = mock_async_pool(mock_conn)
        with patch("tableinator.tableinator.connection_pool", pool):
            # test code
    """

    def create_pool(mock_connection: Any = None) -> MagicMock:
        """Create a mock pool that returns the given connection."""
        if mock_connection is None:
            mock_connection = MagicMock()

        mock_pool = MagicMock()

        # Create async context manager for connection
        mock_connection_cm = AsyncMock()
        mock_connection_cm.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_cm.__aexit__ = AsyncMock(return_value=None)

        # For async with connection_pool.connection() pattern:
        # connection() should return the context manager directly (not a coroutine)
        mock_pool.connection = MagicMock(return_value=mock_connection_cm)
        mock_pool.close = AsyncMock()

        return mock_pool

    return create_pool


class MetricsCollector:
    """An in-memory MeterProvider plus helpers for reading what was recorded."""

    def __init__(self) -> None:
        self.reader = InMemoryMetricReader()
        self.provider = SdkMeterProvider(metric_readers=[self.reader])

    def metrics(self) -> dict[str, Metric]:
        """Collect once and return every recorded metric by name."""
        data = self.reader.get_metrics_data()
        if data is None:
            return {}
        return {
            metric.name: metric
            for resource_metrics in data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
        }

    def points(self, name: str) -> list[Any]:
        """Return the data points recorded for one metric name."""
        metric = self.metrics().get(name)
        return [] if metric is None else list(metric.data.data_points)

    def attributes(self, name: str) -> list[dict[str, Any]]:
        """Return the attribute dicts recorded for one metric name, in recording order."""
        return [dict(point.attributes) for point in self.points(name)]


@pytest.fixture
def metrics_collector(monkeypatch: pytest.MonkeyPatch) -> Iterator[MetricsCollector]:
    """Install an in-memory provider and make tableinator's instruments build against it.

    Mirrors groovemap-runtime's own `collector` fixture (tests/test_runtime_metrics.py)
    so tableinator's domain instruments and the shared wrappers can be asserted on with
    the same pattern.
    """
    active = MetricsCollector()
    monkeypatch.setattr(common_telemetry, "_provider", active.provider)
    monkeypatch.setattr(common_telemetry, "_generation", common_telemetry.provider_generation() + 1)
    tableinator_telemetry.reset_instruments()
    assert common_telemetry._active_provider() is active.provider
    yield active
    monkeypatch.setattr(common_telemetry, "_provider", None)
    tableinator_telemetry.reset_instruments()
