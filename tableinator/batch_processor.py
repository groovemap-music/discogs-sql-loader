"""Discogs SQL policy adapters for the shared asynchronous batch runtime.

The shared runtime owns queue capacity, per-entity serialization, cross-entity
concurrency, adaptive sizing, retry timing, periodic flushing, cancellation
restoration, and delivery settlement. This module deliberately keeps the SQL
writer, failure taxonomy, telemetry, and stale-row purge veto in the owner hive.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog
from common import WriteAdmission, normalize_record
from common.batch import DEFAULT_MAX_WRITE_S, WRITE_CANCEL_GRACE_S, AsyncBatchEngine, BatchItemResult, BatchPolicy
from common.delivery import DeliveryResult, FailureKind, Settlement
from common.postgres_resilient import PostgreSQLFailureClassifier

from tableinator import telemetry
from tableinator.batch_writer import BatchWriteResult, PostgreSQLBatchWriter
from tableinator.media import media_for_release


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence


logger = structlog.get_logger(__name__)
DATA_TYPES = ("artists", "labels", "masters", "releases")
# RabbitMQ consumer_timeout on lux (30 minutes). A held delivery must be returned before it.
BROKER_CONSUMER_TIMEOUT_S = 1800.0
DEFAULT_MAX_HOLD_S = 1500.0
# Requeues a held delivery may use before it is dead-lettered. RabbitMQ 4.3.5 and 4.4.0 do not
# spend x-delivery-limit on nack(requeue=True), so the engine counts x-acquired-count instead.
# 61 holds of 1500 s is about 25.4 hours of continuous failure. See docs/shared-delivery-runtime.md.
DEFAULT_MAX_REQUEUES = 60
# A type is stalled when its oldest queued delivery is older than this AND nothing of that type has
# settled for as long. Either alone is normal: a deep queue behind long flushes has old heads while
# still settling, and a quiet queue has no old head. Health reports unhealthy within this bound of
# the last settlement (or of the stuck delivery's arrival, if that came later).
DEFAULT_STALL_S = 300.0


def validate_stall_s(stall_s: float, max_hold_s: float | None) -> float:
    """Return ``stall_s`` if it is positive and below ``max_hold_s``; raise ``ValueError`` otherwise.

    A bound at or above the hold would let a delivery be returned to the broker before health could
    ever report it, and a bound of zero or less would report every busy moment as a stall.
    """
    if not stall_s > 0:
        raise ValueError(f"POSTGRES_BATCH_STALL_S={stall_s:g} must be positive")
    if max_hold_s is not None and stall_s >= max_hold_s:
        raise ValueError(f"POSTGRES_BATCH_STALL_S={stall_s:g} must be below POSTGRES_BATCH_MAX_HOLD_S={max_hold_s:g}")
    return stall_s


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
    # Longest a delivery may be held in transient backoff before it is returned to RabbitMQ
    # with requeue=True. None disables the bound. See __post_init__ for the budget it must meet.
    max_hold_s: float | None = DEFAULT_MAX_HOLD_S
    # Times a held delivery may go back to RabbitMQ before it is dead-lettered. None keeps
    # requeueing without a bound.
    max_requeues: int | None = DEFAULT_MAX_REQUEUES
    # Longest one write may run before it counts as a transient failure. Keep it above the longest
    # legitimate write. None leaves a frozen PostgreSQL unbounded, so a hold bound rejects it.
    max_write_s: float | None = DEFAULT_MAX_WRITE_S

    def __post_init__(self) -> None:
        if self.max_hold_s is None:
            return
        if self.max_write_s is None:
            raise ValueError("max_write_s=None leaves a hung write unbounded, so max_hold_s cannot hold")
        # A delivery can start a write just inside the hold, having waited behind another type's
        # write for a flush slot: two writes, each with its cancel grace. Then the flush tick that
        # returns it, and the longest backoff sleep. All of it must stay below the broker's timeout.
        write_budget_s = 2 * (self.max_write_s + WRITE_CANCEL_GRACE_S)
        worst_case_s = self.max_hold_s + write_budget_s + self.flush_interval + self.backoff_max
        if worst_case_s >= BROKER_CONSUMER_TIMEOUT_S:
            raise ValueError(
                f"max_hold_s={self.max_hold_s:g} plus {write_budget_s:g} s of writes at max_write_s={self.max_write_s:g}, "
                f"flush_interval={self.flush_interval:g} and backoff_max={self.backoff_max:g} can reach {worst_case_s:g} s, "
                f"which is not below the {BROKER_CONSUMER_TIMEOUT_S:g} s consumer_timeout"
            )

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
            max_hold_s=self.max_hold_s,
            max_requeues=self.max_requeues,
            max_write_s=self.max_write_s,
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
    # Only set for deliveries the shared engine may return to the broker (max_hold_s). Without
    # it a requeue request is a programming error, because the batch path otherwise retries in-process.
    requeue_callback: Callable[[], Awaitable[None] | None] | None = None
    # The broker's headers, which is where the engine reads how often the message came back.
    headers: Mapping[str, Any] | None = None

    async def ack(self) -> None:
        result = self.ack_callback()
        if inspect.isawaitable(result):
            await result

    async def nack(self, *, requeue: bool) -> None:
        if requeue:
            if self.requeue_callback is None:
                raise ValueError("batch deliveries are retried in-process, not broker-requeued")
            result = self.requeue_callback()
        else:
            result = self.nack_callback()
        if inspect.isawaitable(result):
            await result


@dataclass
class _FlushState:
    started_at: float
    size: int
    settled: int = 0
    rejected: int = 0
    media_backfilled: int = 0
    identity_backfilled: int = 0
    error: BaseException | None = None
    record_ids: list[str] = field(default_factory=list)


class PostgreSQLBatchObserver:
    """Preserve Discogs SQL metrics and traces around shared-engine events."""

    def __init__(self) -> None:
        self._accepted_at = {data_type: deque[float]() for data_type in DATA_TYPES}
        # Monotonic time of each type's latest ACK or REJECT, the only settlements that mean a record
        # was dealt with. A REQUEUE hands the record back to the broker unwritten, so it is not
        # progress. The start counts as progress: a stall is measured from when this observer began
        # watching at the earliest.
        self.last_settled_at = dict.fromkeys(DATA_TYPES, time.monotonic())
        # Monotonic time the type's oldest unwritten record began waiting, kept across requeues: a
        # held record that goes back to the broker and comes back is the same wait. Set on the first
        # REQUEUE (back-dated to when that delivery was accepted), cleared on ACK or REJECT.
        self.held_since: dict[str, float | None] = dict.fromkeys(DATA_TYPES)
        self._states: dict[str, _FlushState] = {}
        self._had_dlq_nacks = dict.fromkeys(DATA_TYPES, False)
        self.processed_counts = dict.fromkeys(DATA_TYPES, 0)
        # Deliveries returned to the broker for redelivery (held too long, or lost with their channel).
        self.requeued_counts = dict.fromkeys(DATA_TYPES, 0)
        self.batch_counts = dict.fromkeys(DATA_TYPES, 0)
        self.media_backfilled_counts = dict.fromkeys(DATA_TYPES, 0)
        self.identity_backfilled_counts = dict.fromkeys(DATA_TYPES, 0)

    def accepted(self, data_type: str, started_at: float) -> None:
        self._accepted_at[data_type].append(started_at)

    def withdrawn(self, data_type: str, started_at: float) -> None:
        """Take back an :meth:`accepted` for a delivery the engine never took and will never settle."""
        with contextlib.suppress(ValueError):
            self._accepted_at[data_type].remove(started_at)

    def write_result(self, data_type: str, result: BatchWriteResult) -> None:
        state = self._states[data_type]
        state.media_backfilled += len(result.media_backfilled_ids)
        state.identity_backfilled += len(result.identity_backfilled_ids)

    def write_failed(self, data_type: str, error: BaseException, record_ids: Sequence[str] = ()) -> None:
        state = self._states[data_type]
        state.error = error
        state.record_ids = list(record_ids)

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
        """Record one terminal settlement.

        The engine settles a delivery released from a hold outside any flush, so there may be no
        flush state. A failure here is suppressed by the engine, so it must not depend on one: a
        rejection that missed the veto below would let the purge delete a record still current.
        """
        _ = duration_s, span
        state = self._states.get(entity)
        if state is not None:
            state.settled += 1
        # Every settlement owns exactly one accepted_at entry, whether the delivery was acked,
        # dead-lettered, requeued after a hold, or lost with its channel. Taking it first means no
        # branch below can leave one behind; an empty deque only follows a double report.
        accepted = self._accepted_at[entity]
        started_at = accepted.popleft() if accepted else time.perf_counter()
        terminal_duration = time.perf_counter() - started_at
        if result.settlement is Settlement.REQUEUE:
            # Only a hold that timed out is a record still waiting to be written. A delivery_lost
            # report names a delivery whose channel died, and it can arrive after the redelivered
            # copy was acked: starting a wait then would never be cleared.
            if result.outcome == "held_timeout" and self.held_since[entity] is None:
                self.held_since[entity] = time.monotonic() - terminal_duration
            telemetry.record_batch_requeued(entity, result.outcome)
        else:
            self.last_settled_at[entity] = time.monotonic()
            self.held_since[entity] = None
        error = state.error if state is not None else None
        if result.settlement is Settlement.ACK:
            outcome = result.outcome
            self.processed_counts[entity] += 1
            if outcome == "media_backfilled":
                self.media_backfilled_counts[entity] += 1
        else:
            outcome = "failed"
            if result.settlement is Settlement.REJECT:
                self._had_dlq_nacks[entity] = True
            elif result.settlement is Settlement.REQUEUE:
                # Back to the broker, not to the dead-letter queue: the purge veto must not read it as a
                # rejection, and the record is not lost.
                self.requeued_counts[entity] += 1
            if state is not None:
                state.rejected += 1
            if error is not None:
                telemetry.mark_span_error(span, error)

        error_type = type(error).__name__ if error is not None and outcome == "failed" else result.error_type
        telemetry.record_message(entity, outcome, terminal_duration)
        telemetry.record_consumed_message(entity, terminal_duration, error_type)

        if state is None:
            return
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
                    rejected=state.rejected,
                    record_ids=state.record_ids[:10],
                    sqlstate=getattr(state.error, "sqlstate", None),
                    error_type=type(state.error).__name__ if state.error is not None else None,
                )

    def oldest_unsettled_age_s(self, data_type: str) -> float:
        """Age of the oldest delivery accepted and not yet settled, including one inside a running write."""
        accepted = self._accepted_at[data_type]
        return time.perf_counter() - accepted[0] if accepted else 0.0

    def held_age_s(self, data_type: str) -> float:
        """How long the type's oldest unwritten record has waited, requeues included; 0 when none is held."""
        since = self.held_since[data_type]
        return time.monotonic() - since if since is not None else 0.0

    def outstanding_accepted(self) -> dict[str, int]:
        """Deliveries accepted but not yet settled, by type. Zero once the engine is drained."""
        return {data_type: len(entries) for data_type, entries in self._accepted_at.items()}

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
        except Exception as error:
            if len(payloads) > 1 and PostgreSQLFailureClassifier()(error) is FailureKind.DETERMINISTIC:
                return await self._isolate(key, list(payloads), error)
            self._observer.write_failed(key, error, [m.data_id for m in payloads])
            raise
        except BaseException as error:
            self._observer.write_failed(key, error)
            raise
        self._observer.write_result(key, result)
        return [BatchItemResult(Settlement.ACK, self._outcome(message, result)) for message in payloads]

    async def _isolate(self, key: str, payloads: list[PendingMessage], error: Exception) -> list[BatchItemResult]:
        """Narrow a deterministic batch failure to the offending record(s).

        The failed transaction rolled back whole, so bisecting rewrites nothing twice.
        Healthy records are committed and acked; only the records that fail on their
        own are rejected to the DLQ. Transient failures during isolation propagate so
        the shared engine retries the whole batch without dead-lettering anything.
        """
        if len(payloads) == 1:
            message = payloads[0]
            self._observer.write_failed(key, error, [message.data_id])
            logger.error(
                "☠️ Record dead-lettered after deterministic PostgreSQL failure",
                data_type=key,
                record_id=message.data_id,
                sqlstate=getattr(error, "sqlstate", None),
                error_type=type(error).__name__,
                error=str(error)[:300],
            )
            return [BatchItemResult(Settlement.REJECT, "poison")]
        middle = len(payloads) // 2
        results: list[BatchItemResult] = []
        for half in (payloads[:middle], payloads[middle:]):
            try:
                outcome = await self._processor._process_batch(key, half)
            except Exception as half_error:
                if PostgreSQLFailureClassifier()(half_error) is not FailureKind.DETERMINISTIC:
                    self._observer.write_failed(key, half_error)
                    raise
                results.extend(await self._isolate(key, half, half_error))
            else:
                self._observer.write_result(key, outcome)
                results.extend(BatchItemResult(Settlement.ACK, self._outcome(m, outcome)) for m in half)
        return results

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

    def __init__(self, connection_pool: Any, config: BatchConfig | None = None, admission: WriteAdmission | None = None) -> None:
        self.connection_pool = connection_pool
        self.admission = admission
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
        requeue_callback: Callable[[], Awaitable[None] | None] | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> bool:
        """Normalize and submit one delivery; invalid input is rejected locally.

        ``requeue_callback`` returns a held delivery to the broker once it exceeds ``max_hold_s``.
        ``headers`` are the broker's, which the requeue budget reads.
        """
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
        # Before submit: a delivery waiting for capacity can be settled inside it, and an entry added
        # after that settlement would never be taken, leaving health stalled until a restart.
        self._observer.accepted(data_type, started)
        try:
            await self._engine.submit(
                data_type,
                pending,
                _CallbackDelivery(ack_callback, nack_callback, requeue_callback, headers),
                span_context=span_context,
            )
        except BaseException:
            self._observer.withdrawn(data_type, started)
            raise
        snapshot = self._snapshot()
        threshold = min(snapshot["batch_sizes"][data_type], self.config.max_pending)
        if snapshot["pending"][data_type] >= threshold:
            await self._engine.flush(data_type)
        return True

    async def _process_batch(self, data_type: str, messages: list[PendingMessage]) -> BatchWriteResult:
        writer = PostgreSQLBatchWriter(self.connection_pool, logger, media_for_release, self.admission)
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

    def stalled(self, stall_s: float = DEFAULT_STALL_S) -> dict[str, float]:
        """Types whose oldest queued delivery is older than ``stall_s`` with no settlement for as long.

        Maps each stalled type to the age of its oldest queued delivery in seconds. A deep queue behind
        long but finishing flushes keeps settling and is not stalled; a write that never returns, or a
        capacity wait that never frees, stops settlement and is.
        """
        # The engine's age covers what is still queued. A batch inside a write that never returns has
        # left the queue, so it is the observer's accepted-and-unsettled age that shows it.
        queued = self._snapshot()["oldest_pending_age_s"]
        now = time.monotonic()
        ages = {
            data_type: max(float(queued.get(data_type, 0.0)), self._observer.oldest_unsettled_age_s(data_type), self._observer.held_age_s(data_type))
            for data_type in DATA_TYPES
        }
        return {data_type: age for data_type, age in ages.items() if age > stall_s and now - self._observer.last_settled_at[data_type] > stall_s}

    def report_stall_gauges(self, stall_s: float = DEFAULT_STALL_S) -> dict[str, float]:
        """Publish each type's stall state and oldest wait to telemetry; returns the stalled types."""
        stalled = self.stalled(stall_s)
        queued = self._snapshot()["oldest_pending_age_s"]
        for data_type in DATA_TYPES:
            age = max(
                float(queued.get(data_type, 0.0)),
                self._observer.oldest_unsettled_age_s(data_type),
                self._observer.held_age_s(data_type),
            )
            telemetry.record_batch_stall(data_type, data_type in stalled, age)
        return stalled

    def had_dlq_nacks(self, data_type: str) -> bool:
        return self._observer.had_dlq_nacks(data_type)

    def reset_dlq_nacks(self, data_type: str) -> None:
        self._observer.reset_dlq_nacks(data_type)
