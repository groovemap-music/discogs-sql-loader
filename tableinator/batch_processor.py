"""Discogs SQL policy adapters for the shared asynchronous batch runtime.

The shared runtime owns queue capacity, per-entity serialization, cross-entity
concurrency, adaptive sizing, retry timing, periodic flushing, cancellation
restoration, and delivery settlement. This module deliberately keeps the SQL
writer, failure taxonomy, telemetry, and stale-row purge veto in the owner hive.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog
from common import normalize_record
from common.batch import AsyncBatchEngine, BatchItemResult, BatchPolicy
from common.db_resilience import DatabaseUnavailableError
from common.delivery import DeliveryResult, FailureKind, Settlement
from psycopg.errors import DataError, IntegrityError, InterfaceError, OperationalError

from tableinator import telemetry
from tableinator.batch_writer import BatchWriteResult, PostgreSQLBatchWriter
from tableinator.media import media_for_release


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator, Sequence


logger = structlog.get_logger(__name__)
DATA_TYPES = ("artists", "labels", "masters", "releases")


@dataclass
class BatchConfig:
    """Owner-facing configuration translated to :class:`BatchPolicy`."""

    batch_size: int = 100
    flush_interval: float = 5.0
    max_pending: int = 1000
    max_concurrent_flushes: int = 2
    min_batch_size: int = 10
    backoff_initial: float = 1.0
    backoff_max: float = 30.0
    backoff_multiplier: float = 2.0
    max_flush_retries: int = 5
    max_poison_retries: int = 5

    def runtime_policy(self) -> BatchPolicy:
        """Return the transport-neutral policy consumed by the shared engine."""
        return BatchPolicy(
            batch_size=self.batch_size,
            flush_interval_s=self.flush_interval,
            max_pending=self.max_pending,
            max_concurrent_flushes=self.max_concurrent_flushes,
            min_batch_size=min(self.min_batch_size, self.batch_size),
            backoff_initial_s=self.backoff_initial,
            backoff_max_s=self.backoff_max,
            backoff_multiplier=self.backoff_multiplier,
            max_drain_retries=self.max_flush_retries,
            max_poison_retries=self.max_poison_retries,
        )


@dataclass
class PendingMessage:
    """Normalized PostgreSQL payload submitted to the shared batch engine."""

    data_type: str
    data_id: str
    data: dict[str, Any]
    sha256: str
    ack_callback: Callable[[], Awaitable[None] | None]
    nack_callback: Callable[[], Awaitable[None] | None]
    received_at: float = field(default_factory=time.time)
    telemetry_started_at: float = field(default_factory=time.perf_counter)
    span_context: Any = None


@dataclass
class _CallbackDelivery:
    ack_callback: Callable[[], Awaitable[None] | None]
    nack_callback: Callable[[], Awaitable[None] | None]

    async def ack(self) -> None:
        result = self.ack_callback()
        if inspect.isawaitable(result):
            await result

    async def nack(self, *, requeue: bool) -> None:
        if requeue:
            raise ValueError("batch deliveries are retried in-process, not broker-requeued")
        result = self.nack_callback()
        if inspect.isawaitable(result):
            await result


class PostgreSQLFailureClassifier:
    """Map owner-specific PostgreSQL failures onto the shared retry taxonomy."""

    _TRANSIENT = (InterfaceError, OperationalError, DatabaseUnavailableError)
    _DETERMINISTIC = (DataError, IntegrityError, ValueError, RuntimeError)

    def __call__(self, error: BaseException) -> FailureKind:
        if isinstance(error, self._TRANSIENT):
            return FailureKind.TRANSIENT
        if isinstance(error, self._DETERMINISTIC):
            return FailureKind.DETERMINISTIC
        # Preserve the historical safety posture for unknown infrastructure
        # exceptions: retry rather than silently dead-lettering a valid record.
        return FailureKind.TRANSIENT


@dataclass
class _FlushState:
    started_at: float
    size: int
    settled: int = 0
    rejected: int = 0
    media_backfilled: int = 0
    identity_backfilled: int = 0
    error: BaseException | None = None


class PostgreSQLBatchObserver:
    """Preserve Discogs SQL metrics and traces around shared-engine events."""

    def __init__(self) -> None:
        self._accepted_at = {data_type: deque[float]() for data_type in DATA_TYPES}
        self._states: dict[str, _FlushState] = {}
        self._had_dlq_nacks = dict.fromkeys(DATA_TYPES, False)
        self.processed_counts = dict.fromkeys(DATA_TYPES, 0)
        self.batch_counts = dict.fromkeys(DATA_TYPES, 0)
        self.media_backfilled_counts = dict.fromkeys(DATA_TYPES, 0)
        self.identity_backfilled_counts = dict.fromkeys(DATA_TYPES, 0)

    def accepted(self, data_type: str, started_at: float) -> None:
        self._accepted_at[data_type].append(started_at)

    def write_result(self, data_type: str, result: BatchWriteResult) -> None:
        state = self._states[data_type]
        state.media_backfilled = len(result.media_backfilled_ids)
        state.identity_backfilled = len(result.identity_backfilled_ids)

    def write_failed(self, data_type: str, error: BaseException) -> None:
        self._states[data_type].error = error

    @contextmanager
    def consume(self, destination: str, headers: object | None) -> Iterator[Any]:
        with telemetry.consume_span(destination, headers if isinstance(headers, dict) else None) as span:
            yield span

    @contextmanager
    def flush(self, key: str, size: int, links: Sequence[object]) -> Iterator[Any]:
        state = _FlushState(time.perf_counter(), size)
        self._states[key] = state
        telemetry.record_batch_size(key, size)
        try:
            with telemetry.batch_flush_span(key, links) as span:
                yield span
                if state.settled == state.size:
                    outcome = "failed" if state.rejected else "success"
                    telemetry.record_batch_flush(key, time.perf_counter() - state.started_at, outcome)
                    telemetry.set_flush_outcome(span, outcome)
        finally:
            self._states.pop(key, None)

    def retry(self, *, key: str, kind: FailureKind, attempt: int, delay_s: float, span: Any) -> None:
        state = self._states[key]
        if state.error is not None:
            telemetry.mark_span_error(span, state.error)
        if kind is FailureKind.TRANSIENT:
            logger.warning(
                "⏳ PostgreSQL batch deferred for retry",
                data_type=key,
                attempt=attempt,
                backoff_seconds=round(delay_s, 3),
            )
        else:
            logger.warning(
                "🧪 Deterministic batch failure retained for bounded poison isolation",
                data_type=key,
                attempt=attempt,
            )

    def settled(self, *, entity: str, result: DeliveryResult, duration_s: float, span: Any) -> None:
        _ = duration_s, span
        state = self._states[entity]
        state.settled += 1
        started_at = self._accepted_at[entity].popleft()
        terminal_duration = time.perf_counter() - started_at
        if result.settlement is Settlement.ACK:
            outcome = result.outcome
            self.processed_counts[entity] += 1
            if outcome == "media_backfilled":
                self.media_backfilled_counts[entity] += 1
        else:
            outcome = "failed"
            state.rejected += 1
            self._had_dlq_nacks[entity] = True
            if state.error is not None:
                telemetry.mark_span_error(span, state.error)

        error_type = type(state.error).__name__ if state.error is not None and outcome == "failed" else None
        telemetry.record_message(entity, outcome, terminal_duration)
        telemetry.record_consumed_message(entity, terminal_duration, error_type)

        if state.settled == state.size:
            if state.rejected == 0:
                self.batch_counts[entity] += 1
                self.identity_backfilled_counts[entity] += state.identity_backfilled
                logger.info(
                    "✅ Batch processed",
                    data_type=entity,
                    batch_size=state.size,
                    duration_ms=round((time.perf_counter() - state.started_at) * 1000),
                    total_processed=self.processed_counts[entity],
                    media_backfilled=state.media_backfilled,
                    identity_backfilled=state.identity_backfilled,
                )
            else:
                logger.error(
                    "❌ Poison batch nacked to the DLQ",
                    data_type=entity,
                    batch_size=state.size,
                )

    def had_dlq_nacks(self, data_type: str) -> bool:
        return self._had_dlq_nacks.get(data_type, False)

    def reset_dlq_nacks(self, data_type: str) -> None:
        self._had_dlq_nacks[data_type] = False


class PostgreSQLBatchSink:
    """Adapt the repository-owned PostgreSQL writer to the runtime BatchSink port."""

    def __init__(self, processor: PostgreSQLBatchProcessor, observer: PostgreSQLBatchObserver) -> None:
        self._processor = processor
        self._observer = observer

    async def write(self, key: str, payloads: Sequence[PendingMessage]) -> Sequence[BatchItemResult]:
        try:
            result = await self._processor._process_batch(key, list(payloads))
        except BaseException as error:
            self._observer.write_failed(key, error)
            raise
        self._observer.write_result(key, result)
        return [BatchItemResult(Settlement.ACK, self._outcome(message, result)) for message in payloads]

    @staticmethod
    def _outcome(message: PendingMessage, result: BatchWriteResult) -> str:
        # Preserve the established telemetry precedence. Native-identity backfills
        # remain an independently reported result set, but because they are a subset
        # of hash-unchanged rows their delivery outcome is still ``skipped``.
        if message.data_id in result.media_backfilled_ids:
            return "media_backfilled"
        if message.data_id in result.unchanged_ids:
            return "skipped"
        return "processed"


class PostgreSQLBatchProcessor:
    """Owner adapter around :class:`common.batch.AsyncBatchEngine`."""

    def __init__(self, connection_pool: Any, config: BatchConfig | None = None) -> None:
        self.connection_pool = connection_pool
        self.config = config or BatchConfig()
        env_batch_size = os.environ.get("POSTGRES_BATCH_SIZE")
        if env_batch_size:
            try:
                self.config.batch_size = int(env_batch_size)
            except ValueError:
                logger.warning(
                    "⚠️ Invalid POSTGRES_BATCH_SIZE, using default",
                    value=env_batch_size,
                    default=self.config.batch_size,
                )
        self._observer = PostgreSQLBatchObserver()
        self._sink = PostgreSQLBatchSink(self, self._observer)
        self._engine = AsyncBatchEngine(
            DATA_TYPES,
            policy=self.config.runtime_policy(),
            sink=self._sink,
            classifier=PostgreSQLFailureClassifier(),
            observer=self._observer,
        )

    @property
    def processed_counts(self) -> dict[str, int]:
        return self._observer.processed_counts

    @property
    def batch_counts(self) -> dict[str, int]:
        return self._observer.batch_counts

    @property
    def media_backfilled_counts(self) -> dict[str, int]:
        return self._observer.media_backfilled_counts

    async def add_message(
        self,
        data_type: str,
        data: dict[str, Any],
        ack_callback: Callable[[], Awaitable[None] | None],
        nack_callback: Callable[[], Awaitable[None] | None],
        span_context: Any = None,
    ) -> bool:
        """Normalize and submit one delivery; invalid input is rejected locally."""
        started = time.perf_counter()

        async def reject(error_type: str) -> bool:
            self._observer._had_dlq_nacks[data_type] = True
            await _CallbackDelivery(ack_callback, nack_callback).nack(requeue=False)
            duration = time.perf_counter() - started
            telemetry.record_message(data_type, "failed", duration)
            telemetry.record_consumed_message(data_type, duration, error_type)
            return False

        if data_type not in DATA_TYPES:
            logger.error("❌ Unknown data type", data_type=data_type)
            result = nack_callback()
            if inspect.isawaitable(result):
                await result
            duration = time.perf_counter() - started
            telemetry.record_message(data_type, "failed", duration)
            telemetry.record_consumed_message(data_type, duration, "unknown_data_type")
            return False
        data_id = data.get("id")
        if not data_id:
            logger.error("❌ Message missing 'id' field", data_type=data_type)
            return await reject("missing_id")
        try:
            normalized_data = normalize_record(data_type, data)
        except Exception as error:
            logger.error("❌ Failed to normalize data", data_type=data_type, error=str(error))
            return await reject(telemetry.error_type_of(error))

        pending = PendingMessage(
            data_type=data_type,
            data_id=data_id,
            data=normalized_data,
            sha256=data.get("sha256", ""),
            ack_callback=ack_callback,
            nack_callback=nack_callback,
            telemetry_started_at=started,
            span_context=span_context,
        )
        await self._engine.submit(
            data_type,
            pending,
            _CallbackDelivery(ack_callback, nack_callback),
            span_context=span_context,
        )
        self._observer.accepted(data_type, started)
        snapshot = self._snapshot()
        threshold = min(snapshot["batch_sizes"][data_type], self.config.max_pending)
        if snapshot["pending"][data_type] >= threshold:
            await self._engine.flush(data_type)
        return True

    async def _process_batch(self, data_type: str, messages: list[PendingMessage]) -> BatchWriteResult:
        writer = PostgreSQLBatchWriter(self.connection_pool, logger, media_for_release)
        return await writer.process_batch(data_type, messages)

    async def flush_queue(self, data_type: str) -> bool:
        """Boundedly drain one entity without dead-lettering transient failures."""
        for attempt in range(1, self.config.max_flush_retries + 1):
            if await self._engine.flush(data_type):
                return True
            if self._snapshot()["pending"][data_type] == 0:
                return True
            delay = min(
                self.config.backoff_max,
                self.config.backoff_initial * self.config.backoff_multiplier ** (attempt - 1),
            )
            await asyncio.sleep(delay)
        logger.error(
            "❌ Flush retry limit reached — messages kept for retry",
            data_type=data_type,
            remaining=self._snapshot()["pending"][data_type],
            max_retries=self.config.max_flush_retries,
        )
        return False

    async def flush_all(self) -> bool:
        results = await asyncio.gather(*(self.flush_queue(data_type) for data_type in DATA_TYPES))
        return all(results)

    async def periodic_flush(self) -> None:
        await self._engine.run_periodic()

    def shutdown(self) -> None:
        self._engine.shutdown()

    def _snapshot(self) -> dict[str, Any]:
        snapshot = self._engine.snapshot()
        if not isinstance(snapshot, dict):  # pragma: no cover - runtime contract guard
            raise TypeError("shared batch snapshot must be a mapping")
        return snapshot

    def get_stats(self) -> dict[str, Any]:
        snapshot = self._snapshot()
        return {
            "processed": self.processed_counts.copy(),
            "batches": self.batch_counts.copy(),
            "media_backfilled": self.media_backfilled_counts.copy(),
            "identity_backfilled": self._observer.identity_backfilled_counts.copy(),
            "pending": snapshot["pending"],
            "effective_batch_size": snapshot["batch_sizes"],
            "configured_batch_size": self.config.batch_size,
            "consecutive_failures": snapshot["poison_attempts"],
            "transient_failures": snapshot["transient_attempts"],
            "shutdown": snapshot["shutdown"],
        }

    def had_dlq_nacks(self, data_type: str) -> bool:
        return self._observer.had_dlq_nacks(data_type)

    def reset_dlq_nacks(self, data_type: str) -> None:
        self._observer.reset_dlq_nacks(data_type)
