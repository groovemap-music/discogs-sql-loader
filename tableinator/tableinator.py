import asyncio
import contextlib
import os
import signal
import time
from asyncio import run
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from common import (
    AsyncPostgreSQLPool,
    AsyncResilientRabbitMQ,
    DatabaseUnavailableError,
    DeliveryResult,
    HealthServer,
    OutageBackoff,
    Settlement,
    normalize_record,
    parse_postgres_host_port,
    run_delivery,
    setup_logging,
    setup_telemetry,
    shutdown_telemetry,
    start_event_loop_monitor,
)
from orjson import loads

from tableinator import telemetry
from tableinator.batch_processor import BatchConfig, PostgreSQLBatchProcessor, PostgreSQLFailureClassifier
from tableinator.catalog_contract import (
    AMQP_EXCHANGE_TYPE,
)
from tableinator.catalog_contract import (
    ENTITY_TYPES as DATA_TYPES,
)
from tableinator.catalog_contract import (
    exchange_name as catalog_exchange_name,
)
from tableinator.catalog_contract import (
    queue_name as catalog_queue_name,
)
from tableinator.config import TableinatorConfig
from tableinator.extraction_latch import LatchRelation, extraction_latch_key, probe_latch_relation, record_extraction_signal
from tableinator.graph_counters import refresh_derived_relations
from tableinator.media import media_for_release
from tableinator.queue_names import (
    dead_letter_exchange_name as catalog_dead_letter_exchange_name,
)
from tableinator.queue_names import (
    dead_letter_queue_name as catalog_dead_letter_queue_name,
)
from tableinator.record_persistence import PostgreSQLRecordPersistence


if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage


logger = structlog.get_logger(__name__)

SERVICE_NAME = "discogs-sql-loader"
LOG_PATH = Path("/logs") / f"{SERVICE_NAME}.log"
# The durable AMQP queues predate the repository split. Changing this consumer key
# would create a second set of queues and strand deliveries in the existing ones.
AMQP_CONSUMER_NAME = "tableinator"
# The docker-compose service key from the GrooveMap OpenTelemetry metrics conventions
# (design ADR-0006), distinct from SERVICE_NAME above which names logs and the health
# identity.
OTEL_SERVICE_NAME = "tableinator"

STARTUP_BANNER = r"""
+--------------------------------------------------+
| GrooveMap                                        |
| discogs-sql-loader                               |
+--------------------------------------------------+
""".strip("\n")

# Config will be initialized in main
config: TableinatorConfig | None = None

# Progress tracking
message_counts = {"artists": 0, "labels": 0, "masters": 0, "releases": 0}
progress_interval = 100  # Log progress every 100 messages
last_message_time = {"artists": 0.0, "labels": 0.0, "masters": 0.0, "releases": 0.0}
completed_files: set[str] = set()  # Track which files have completed processing

# Non-batch mode only: throttles requeues while PostgreSQL is unavailable, so an
# outage cannot burn the quorum queue's x-delivery-limit budget and dead-letter
# valid records. Batch mode gets the same protection from _flush_queue's
# re-enqueue+backoff, which never nacks (discogsography-rb05).
outage_backoff = OutageBackoff(SERVICE_NAME)
current_task = None
current_progress = 0.0

# Consumer management
consumer_tags: dict[str, str] = {}  # {"artists": "consumer-tag-123", ...}
consumer_cancel_tasks: dict[str, asyncio.Task[None]] = {}  # {"artists": asyncio.Task, ...}
queues: dict[str, Any] = {}  # {"artists": queue_object, ...}
CONSUMER_CANCEL_DELAY = int(os.environ.get("CONSUMER_CANCEL_DELAY", "300"))  # Default 5 minutes

# Safety cap for the stale-row purge. A resumed extraction skips files completed in an
# earlier session, so those data types receive zero messages this run while still getting
# an extraction_complete signal. Purging then would delete every row written before the
# restart. If a purge would remove at least this fraction of a non-empty table, it is
# almost certainly a resumed-extraction artifact (Discogs dumps grow, they do not shrink
# by ~100%), so we veto it rather than wipe the table.
PURGE_MAX_DELETE_FRACTION = float(
    os.environ.get("PURGE_MAX_DELETE_FRACTION", "0.90")
)  # Default 90% - refuse purges that would delete this share or more of a table

# ── gm-discogs-sql-loader-2eg.3: the derived-relation refresh ────────────────
# The counter, degree, and genre-aggregate relations are whole-catalog sums over the edge
# tables, so they are refreshed once — after every data type of ONE extraction has signalled
# extraction_complete, which is the latch `graphinator` defers its own post-import pass to
# (`handle_extraction_complete`: the four fanout queues drain at very different rates and
# releases finishes last, so a per-type refresh would sum a half-written catalog). The
# stale-row purge above needs no such latch, because a purge is scoped to the one table
# whose signal arrived; this is a new latch, not a reuse of anything the purge has.
#
# The latch itself lives in PostgreSQL, keyed on the extraction — see
# `tableinator.extraction_latch` for why neither an unkeyed count nor an in-memory set is
# sound. `completed_files` cannot stand in for it either: it is also written by
# `file_complete` and ERASED by `_recover_consumers`, so it answers "has this type's file
# finished" rather than "has this type signalled the end of THIS extraction".

# The declared latch relation, resolved once at startup by a read of `information_schema`.
# None means the schema in front of this loader does not declare it, and the loader then
# runs degraded: no signal is recorded and the pass never fires. This service never creates
# database objects — `docs/database-schema.md`, guarded by `tests/test_service_contract.py`.
extraction_latch: LatchRelation | None = None

# Single-flight within the process. The four consumers deliver their signals concurrently,
# so two of them can both observe a complete latch; the pass is idempotent, so the loser
# re-running would be correct but would take ACCESS EXCLUSIVE on seven tables for a second
# full sweep. The `refreshed_at` stamp the winner writes is what makes the loser a no-op.
derived_refresh_lock = asyncio.Lock()
# ── end gm-discogs-sql-loader-2eg.3 ──────────────────────────────────────────

# Periodic queue checking settings
QUEUE_CHECK_INTERVAL = int(
    os.environ.get("QUEUE_CHECK_INTERVAL", "3600")
)  # Default 1 hour - how often to check for new messages when connection is closed

# Interval for checking stuck state (consumers died unexpectedly)
STUCK_CHECK_INTERVAL = int(os.environ.get("STUCK_CHECK_INTERVAL", "30"))  # Default 30 seconds - how often to check for stuck state

# Idle mode settings - reduce log noise when no messages arrive after startup
STARTUP_IDLE_TIMEOUT = int(os.environ.get("STARTUP_IDLE_TIMEOUT", "30"))  # Seconds after startup with no messages before entering idle mode
IDLE_LOG_INTERVAL = int(os.environ.get("IDLE_LOG_INTERVAL", "300"))  # 5 min between idle status logs

# Idle mode state
idle_mode = False

# Connection parameters will be initialized in main
connection_params: dict[str, Any] = {}

# Connection state tracking
# Create async connection pool for concurrent access
connection_pool: AsyncPostgreSQLPool | None = None

rabbitmq_manager: Any = None  # Will hold AsyncResilientRabbitMQ instance
active_connection: Any = None  # Current active connection
active_channel: Any = None  # Current active channel
connection_check_task: asyncio.Task[None] | None = None  # Background task for periodic queue checks


def get_health_data() -> dict[str, Any]:
    """Get current health data for monitoring."""
    # Determine current task based on active consumers and recent activity
    active_task = None
    current_time = time.time()

    # Check for recent message activity (within last 10 seconds)
    for data_type, last_time in last_message_time.items():
        if last_time > 0 and (current_time - last_time) < 10:
            active_task = f"Processing {data_type}"
            break

    # If no recent activity but consumers exist, show as idle
    if active_task is None and len(consumer_tags) > 0:
        active_task = "Idle - waiting for messages"

    # Check for stuck state: no consumers but work remains (files not completed)
    no_active_consumers = len(consumer_tags) == 0
    files_incomplete = len(completed_files) < len(DATA_TYPES)
    has_processed_messages = any(count > 0 for count in message_counts.values())
    is_stuck = no_active_consumers and files_incomplete and has_processed_messages

    if is_stuck:
        active_task = "STUCK - consumers died, awaiting recovery"

    # Determine health status:
    # - "starting" if connection pool not yet initialized (startup in progress)
    # - "unhealthy" if connection pool was initialized but is now None (connection lost)
    # - "unhealthy" if in stuck state (consumers died unexpectedly)
    # - "healthy" if connection pool is initialized and ready
    if connection_pool is None:
        # Check if we're still in startup (no consumers registered yet)
        if len(consumer_tags) == 0 and all(c == 0 for c in message_counts.values()):
            status = "starting"
            active_task = "Initializing PostgreSQL connection"
        else:
            status = "unhealthy"
    elif is_stuck:
        status = "unhealthy"
    else:
        status = "healthy"

    return {
        "status": status,
        "service": SERVICE_NAME,
        "current_task": active_task,
        "progress": current_progress,
        "message_counts": message_counts.copy(),
        "last_message_time": last_message_time.copy(),
        "active_consumers": list(consumer_tags.keys()),
        "completed_files": list(completed_files),
        # A refresh that is silently not happening is the failure this field exists to surface.
        "derived_relation_refresh": "enabled" if extraction_latch is not None else "degraded",
        "timestamp": datetime.now(UTC).isoformat(),
    }


# Batch processor (optional, enabled via BATCH_MODE env var)
batch_processor: PostgreSQLBatchProcessor | None = None
BATCH_MODE = os.environ.get("POSTGRES_BATCH_MODE", "true").lower() == "true"
BATCH_SIZE = int(os.environ.get("POSTGRES_BATCH_SIZE", "100"))
BATCH_FLUSH_INTERVAL = float(os.environ.get("POSTGRES_BATCH_FLUSH_INTERVAL", "5.0"))

# Fallback pool max when config is not yet loaded (matches TableinatorConfig default).
_DEFAULT_POOL_MAX = 12


def channel_prefetch() -> tuple[int, bool]:
    """Resolve ``(prefetch_count, global_)`` for this channel's QoS.

    Two different shapes, because the two modes hold PostgreSQL connections very
    differently:

    * **Batch mode** — handlers hand the message to :class:`PostgreSQLBatchProcessor`
      and return immediately; only the flush task holds a pooled connection, and its
      concurrency is bounded by the batch processor's own semaphore. Each queue needs
      at least ``BATCH_SIZE`` unacked deliveries of its OWN data type for a batch to
      fill, so QoS stays per-consumer (``global_=False``). The channel-wide ceiling is
      therefore ``prefetch_count * len(DATA_TYPES)`` — logged explicitly so the 4x
      multiplier is not a surprise.
    * **Non-batch mode** (``POSTGRES_BATCH_MODE=false``) — every in-flight handler
      holds a pooled connection for the duration of its upsert, exactly the shape
      brainztableinator couples in ``_channel_prefetch``. Bound the channel's TOTAL
      unacked deliveries (``global_=True``) to the pool max so the broker, not the
      pool's ~15.5s exhausted-wait retry loop, applies backpressure. Without this,
      4 x 200 = 800 concurrent handlers raced for 12 connections; the losers raised,
      were nacked with ``requeue=True``, and burned the quorum queue's
      ``x-delivery-limit: 20`` until the message was silently dead-lettered
      (discogsography-4fio).
    """
    if BATCH_MODE:
        # prefetch_count must be >= batch_size to allow batches to fill before flushing
        return max(200, BATCH_SIZE * 2), False
    pool_max = config.postgres_pool_max_size if config is not None else _DEFAULT_POOL_MAX
    return pool_max, True


# Global shutdown flag
shutdown_requested = False


def signal_handler(signum: int, _frame: Any) -> None:
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    logger.info("🛑 Received signal, initiating graceful shutdown...", signum=signum)
    shutdown_requested = True


def get_connection() -> Any:
    """Get a database connection from the pool."""
    if connection_pool is None:
        raise RuntimeError("Connection pool not initialized")

    return connection_pool.connection()


async def schedule_consumer_cancellation(data_type: str, queue: Any) -> None:
    """Schedule cancellation of a consumer after a delay."""

    async def cancel_after_delay() -> None:
        try:
            await asyncio.sleep(CONSUMER_CANCEL_DELAY)

            if data_type in consumer_tags:
                consumer_tag = consumer_tags[data_type]
                logger.info(
                    f"🔧 Canceling consumer for {data_type} after {CONSUMER_CANCEL_DELAY}s grace period",
                    data_type=data_type,
                    CONSUMER_CANCEL_DELAY=CONSUMER_CANCEL_DELAY,
                )

                # Cancel the consumer with nowait to avoid hanging
                await queue.cancel(consumer_tag, nowait=True)

                # Remove from tracking
                del consumer_tags[data_type]
                telemetry.record_consumer_stopped()

                logger.info(
                    f"✅ Consumer for {data_type} successfully canceled",
                    data_type=data_type,
                )

                # Check if all consumers are now idle
                if await check_all_consumers_idle():
                    logger.info("🔧 All consumers idle, closing RabbitMQ connection")
                    await close_rabbitmq_connection()
        except Exception as e:
            logger.error("❌ Failed to cancel consumer", data_type=data_type, error=str(e))
        finally:
            # Clean up the task reference
            consumer_cancel_tasks.pop(data_type, None)

    # Cancel any existing scheduled cancellation
    if data_type in consumer_cancel_tasks:
        consumer_cancel_tasks[data_type].cancel()

    # Schedule new cancellation
    consumer_cancel_tasks[data_type] = asyncio.create_task(cancel_after_delay())


async def cancel_all_consumers() -> None:
    """Stop new deliveries at shutdown by cancelling every consumer.

    Shutdown previously had no deregistration phase at all: the flag flipped, the
    consumers stayed subscribed, and the per-message guard nacked whatever the
    broker kept pushing. Cancelling here closes the delivery tap BEFORE the
    seconds-long flush/teardown sequence, so nothing is redelivered into a
    service that is on its way out. Best-effort: teardown continues regardless.
    """
    for data_type, consumer_tag in list(consumer_tags.items()):
        queue = queues.get(data_type)
        if queue is None:
            consumer_tags.pop(data_type, None)
            telemetry.record_consumer_stopped()
            continue
        try:
            await queue.cancel(consumer_tag, nowait=True)
            consumer_tags.pop(data_type, None)
            telemetry.record_consumer_stopped()
        except Exception as e:
            logger.warning(
                "⚠️ Failed to cancel consumer during shutdown",
                data_type=data_type,
                error=str(e),
            )
    logger.info("✅ Consumers cancelled for shutdown")


async def close_rabbitmq_connection() -> None:
    """Close the RabbitMQ connection and channel when all consumers are idle."""
    global active_connection, active_channel

    try:
        if active_channel:
            try:
                await active_channel.close()
                logger.info("🔧 Closed RabbitMQ channel - all consumers idle")
            except Exception as e:
                logger.warning("⚠️ Error closing channel", error=str(e))
            active_channel = None

        if active_connection:
            try:
                await active_connection.close()
                logger.info("🔧 Closed RabbitMQ connection - all consumers idle")
            except Exception as e:
                logger.warning("⚠️ Error closing connection", error=str(e))
            active_connection = None

        logger.info(
            f"✅ RabbitMQ connection closed. Will check for new messages every {QUEUE_CHECK_INTERVAL}s",
            QUEUE_CHECK_INTERVAL=QUEUE_CHECK_INTERVAL,
        )
    except Exception as e:
        logger.error("❌ Error closing RabbitMQ connection", error=str(e))


async def check_all_consumers_idle() -> bool:
    """Check if all consumers are cancelled (idle) AND all files completed."""
    return len(consumer_tags) == 0 and len(DATA_TYPES) == len(completed_files)


async def check_consumers_unexpectedly_dead() -> bool:
    """Check if consumers have died unexpectedly (no consumers but files not completed).

    This detects the stuck state where:
    - No consumers are active (consumer_tags is empty)
    - Not all files are completed (some work remains)
    - We've processed at least some messages (not just starting up)

    Returns:
        True if consumers appear to have died unexpectedly
    """
    no_active_consumers = len(consumer_tags) == 0
    files_incomplete = len(completed_files) < len(DATA_TYPES)
    has_processed_messages = any(count > 0 for count in message_counts.values())

    return no_active_consumers and files_incomplete and has_processed_messages


async def periodic_queue_checker() -> None:
    """Periodically check queue health and recover from stuck states.

    This task handles two scenarios:
    1. Normal idle state: All files completed, check for new messages periodically
    2. Stuck state: Consumers died unexpectedly, need immediate recovery

    The stuck state check runs frequently (every STUCK_CHECK_INTERVAL seconds) to
    detect and recover quickly. The normal idle check runs at QUEUE_CHECK_INTERVAL.
    """

    last_full_check = 0.0  # Track when we last did a full queue check

    while not shutdown_requested:
        try:
            await asyncio.sleep(STUCK_CHECK_INTERVAL)

            current_time = time.time()

            # Check for stuck state (consumers died but work remains)
            if await check_consumers_unexpectedly_dead():
                logger.warning(
                    "⚠️ Detected stuck state: consumers died but files not completed. Attempting recovery...",
                    active_consumers=len(consumer_tags),
                    completed_files=list(completed_files),
                    message_counts=message_counts,
                )
                await _recover_consumers()
                continue

            # Normal idle check: only run at QUEUE_CHECK_INTERVAL
            time_since_last_check = current_time - last_full_check
            if time_since_last_check < QUEUE_CHECK_INTERVAL:
                continue

            # Only do full queue check if no active consumers and connection is closed
            if active_connection or len(consumer_tags) > 0:
                continue

            last_full_check = current_time
            logger.info("🔄 Checking all queues for new messages...")
            await _recover_consumers()

        except asyncio.CancelledError:
            logger.info("🛑 Queue checker task cancelled")
            break
        except Exception as e:
            logger.error("❌ Error in periodic queue checker", error=str(e))
            # Continue running despite errors


async def _recover_consumers() -> None:
    """Recover consumers by reconnecting to RabbitMQ and restarting consumption.

    This function handles the actual recovery logic for both:
    - Normal recovery after idle period
    - Emergency recovery after unexpected consumer death
    """
    global active_connection, active_channel, queues, idle_mode

    # Close any existing broken connection first
    if active_connection:
        try:
            await active_connection.close()
        except Exception as e:
            logger.warning("⚠️ Error closing broken connection during recovery", error=str(e))
        active_connection = None
        active_channel = None

    # Temporarily connect to check queue depths
    try:
        temp_connection = await rabbitmq_manager.connect()
        temp_channel = await temp_connection.channel()
    except Exception as e:
        logger.error("❌ Failed to connect to RabbitMQ for recovery", error=str(e))
        return

    try:
        # Check each queue for pending messages
        queues_with_messages = []
        for data_type in DATA_TYPES:
            queue_name = catalog_queue_name(AMQP_CONSUMER_NAME, data_type)

            # Use queue.declare with passive=True to get message count without affecting the queue
            declared_queue = await temp_channel.declare_queue(name=queue_name, passive=True)

            if declared_queue.declaration_result.message_count > 0:
                queues_with_messages.append((data_type, declared_queue.declaration_result.message_count))

        if queues_with_messages:
            total_messages = sum(count for _, count in queues_with_messages)
            logger.info(
                "📬 Found messages in queues, restarting consumers",
                queues=queues_with_messages,
                total_messages=total_messages,
            )

            # Re-establish full connection and start consuming
            active_connection = temp_connection
            active_channel = temp_channel

            # Set QoS - scale with batch_size in batch mode, couple to the PostgreSQL
            # pool capacity in non-batch mode (see channel_prefetch).
            prefetch_count, prefetch_global = channel_prefetch()
            await active_channel.set_qos(prefetch_count=prefetch_count, global_=prefetch_global)

            # Declare per-data-type fanout exchanges and consumer-owned queues
            queues = {}
            for data_type in DATA_TYPES:
                exchange_name = catalog_exchange_name(data_type)
                queue_name = catalog_queue_name(AMQP_CONSUMER_NAME, data_type)
                dlx_name = catalog_dead_letter_exchange_name(AMQP_CONSUMER_NAME, data_type)
                dlq_name = catalog_dead_letter_queue_name(AMQP_CONSUMER_NAME, data_type)

                # Declare fanout exchange (must match extractor)
                exchange = await active_channel.declare_exchange(
                    exchange_name,
                    AMQP_EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False,
                )

                # Declare consumer-owned dead-letter exchange
                dlx_exchange = await active_channel.declare_exchange(
                    dlx_name,
                    AMQP_EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False,
                )

                # Declare DLQ (classic queue for dead letters)
                dlq = await active_channel.declare_queue(
                    auto_delete=False,
                    durable=True,
                    name=dlq_name,
                    arguments={"x-queue-type": "classic"},
                )
                await dlq.bind(dlx_exchange)

                # Declare main quorum queue with consumer-owned DLX
                queue_args = {
                    "x-queue-type": "quorum",
                    "x-dead-letter-exchange": dlx_name,
                    "x-delivery-limit": 20,
                }
                queue = await active_channel.declare_queue(
                    auto_delete=False,
                    durable=True,
                    name=queue_name,
                    arguments=queue_args,
                )
                await queue.bind(exchange)
                queues[data_type] = queue

            # Start consumers for ALL data types lacking one — not just those
            # with a current backlog. A type whose queue was empty at the
            # passive-declare instant still needs a consumer; otherwise messages
            # that arrive later are never consumed, because once active_connection
            # is set and consumer_tags is non-empty both periodic recovery routes
            # are permanently gated off, silently starving that data type.
            pending_counts = dict(queues_with_messages)
            for data_type in DATA_TYPES:
                if data_type in queues and data_type not in consumer_tags:
                    handler = make_data_handler(data_type)
                    consumer_tag = await queues[data_type].consume(handler)
                    consumer_tags[data_type] = consumer_tag
                    telemetry.record_consumer_started()
                    # Only un-complete a type that actually has a backlog, so
                    # genuinely-finished types stay marked complete.
                    if data_type in pending_counts:
                        completed_files.discard(data_type)
                    last_message_time[data_type] = time.time()
                    logger.info(
                        f"✅ Started consumer for {data_type}",
                        data_type=data_type,
                        pending_messages=pending_counts.get(data_type, 0),
                    )

            logger.info(
                "✅ Recovery complete - consumers restarted",
                active_consumers=list(consumer_tags.keys()),
            )
            # Clear idle mode since we have active consumers again
            idle_mode = False
            # Don't close temp_connection since we're using it as active_connection
        else:
            logger.info("⏳ No messages in any queue, connection remains closed")
            # Close the temporary connection
            await temp_channel.close()
            await temp_connection.close()

    except Exception as e:
        logger.error("❌ Error during consumer recovery", error=str(e))
        # Make sure to close temporary connection on error
        try:
            await temp_channel.close()
            await temp_connection.close()
        except Exception as close_error:
            logger.warning(
                "⚠️ Error closing temporary connection after recovery failure",
                error=str(close_error),
            )
        active_connection = None
        active_channel = None
        queues = {}
        # Clear stale consumer tags: any consumers registered before the error
        # died with the now-closed connection. Leaving them behind would keep
        # len(consumer_tags) > 0 forever, permanently gating off both recovery
        # routes (stuck-check requires 0 tags) while health still reads healthy.
        for _ in range(len(consumer_tags)):
            telemetry.record_consumer_stopped()
        consumer_tags.clear()


async def purge_stale_rows(data_type: str, started_at: str, record_count: int | None = None) -> None:
    """Delete rows not refreshed by the current extraction when safe."""
    if connection_pool is None:
        return
    persistence = PostgreSQLRecordPersistence(
        connection_pool,
        logger,
        PURGE_MAX_DELETE_FRACTION,
        media_for_release,
    )
    await persistence.purge_stale_rows(data_type, started_at, record_count)


def make_data_handler(
    data_type: str,
) -> Any:
    """Create a per-data-type message handler that injects data_type context."""

    async def handler(message: AbstractIncomingMessage) -> None:
        await on_data_message(message, data_type)

    return handler


async def on_data_message(message: AbstractIncomingMessage, data_type: str) -> None:
    """Process one delivery inside its CONSUMER span.

    The span is opened from the message's own ``traceparent`` header, so this service's
    work continues the trace the extractor started when it published the record rather
    than starting a fresh one. A delivery that carries no readable context starts a new
    trace instead of failing (see tableinator.telemetry.consume_span).

    In batch mode the span closes as soon as the record is queued -- the write happens
    later, in whatever batch it lands in -- so the delivery is carried forward as a span
    LINK on that batch's ``flush postgresql {entity}`` span rather than as a parent.
    """
    with telemetry.consume_span(data_type, getattr(message, "headers", None)) as span:
        await _process_data_message(message, data_type, span)


async def _process_data_message(message: AbstractIncomingMessage, data_type: str, span: Any) -> None:
    message_started = time.perf_counter()

    def record_terminal(outcome: str, error_type: str | None = None) -> None:
        """Record this delivery's outcome once it reaches a terminal ack/nack.

        Covers both the domain groovemap.pipeline.messages/.message.duration metrics
        and, since this handler consumes via ``queue.consume`` directly rather than
        through ``common.process_message_with_retry``, the messaging.client.* metrics
        that wrapper would otherwise have emitted for free. A message handed to the
        batch processor (BATCH_MODE) is NOT terminal here — its outcome is recorded by
        batch_processor.py when the batch it lands in actually flushes.
        """
        duration = time.perf_counter() - message_started
        telemetry.record_message(data_type, outcome, duration)
        telemetry.record_consumed_message(data_type, duration, error_type)

    if shutdown_requested:
        # Leave the delivery UNACKED — never nack(requeue=True) here. The
        # consumer is still subscribed at this point, so a requeue is redelivered
        # within milliseconds and nacked again, burning a quorum x-delivery-count
        # per cycle; at x-delivery-limit=20 valid records are dead-lettered within
        # a second of a routine restart. Returning without settling lets the
        # connection close requeue them exactly once. See discogsography-lnn4.
        logger.debug("🛑 Shutdown requested, leaving message unacked for redelivery")
        return

    try:
        data: dict[str, Any] = loads(message.body)

        # Check if this is a file completion message
        if data.get("type") == "file_complete":
            total_processed = data.get("total_processed", 0)
            logger.info(f"✅ File processing complete for {data_type}! Total records processed: {total_processed}")

            # Flush remaining batches for this data type before cancellation
            if batch_processor is not None and not await batch_processor.flush_queue(data_type):
                # The drain gave up with rows still pending (typically a database
                # outage). Marking the file complete here would cancel the
                # consumer and let purge_stale_rows delete the very rows those
                # pending messages were about to refresh, while the service
                # reports success. Requeue the marker instead — the pending
                # records stay queued and periodic_flush retries them.
                # See discogsography-hh7r.
                logger.error(
                    "❌ Flush incomplete — requeueing file_complete instead of marking the file done",
                    data_type=data_type,
                )
                await message.nack(requeue=True)
                record_terminal("failed", "flush_incomplete")
                return

            # Mark complete only after flush to prevent premature idle detection
            completed_files.add(data_type)

            # Schedule consumer cancellation if enabled
            if CONSUMER_CANCEL_DELAY > 0 and data_type in queues:
                await schedule_consumer_cancellation(data_type, queues[data_type])

            await message.ack()
            record_terminal("processed")
            return

        # Check if this is an extraction completion message
        if data.get("type") == "extraction_complete":
            logger.info(
                "🏁 Received extraction_complete signal",
                data_type=data_type,
                version=data.get("version"),
            )

            # Flush remaining batches for this data type before cleanup. A purge
            # on top of an incomplete drain is actively destructive: the pending
            # messages' rows still carry an old updated_at, so purge_stale_rows
            # would DELETE exactly the records that were about to be refreshed.
            # See discogsography-hh7r.
            if batch_processor is not None and not await batch_processor.flush_queue(data_type):
                logger.error(
                    "❌ Flush incomplete — requeueing extraction_complete instead of purging stale rows",
                    data_type=data_type,
                )
                await message.nack(requeue=True)
                record_terminal("failed", "flush_incomplete")
                return

            # Purge stale rows from prior extractions. Skip entirely if any message
            # for this data_type was nacked to the DLQ this run (poison batch,
            # flush-retry exhaustion, or normalize/missing-id failure): a DLQ'd
            # record that is still present in the current dump was never upserted,
            # so its row's updated_at was never refreshed and would otherwise look
            # stale and get purged — deleting a still-current record beyond the DLQ.
            # See discogsography-x763.
            purge_ok = True
            if batch_processor is not None and batch_processor.had_dlq_nacks(data_type):
                logger.warning(
                    "⚠️ Skipping stale row purge — messages were nacked to the DLQ this run, so some dump-present rows may not have been refreshed",
                    data_type=data_type,
                )
                batch_processor.reset_dlq_nacks(data_type)
            elif connection_pool is not None:
                try:
                    record_counts = data.get("record_counts", {})
                    await purge_stale_rows(
                        data_type,
                        data.get("started_at", ""),
                        record_counts.get(data_type),
                    )
                except Exception as purge_exc:
                    logger.error(
                        "❌ Purge failed, nacking extraction_complete for retry",
                        data_type=data_type,
                        error=str(purge_exc),
                    )
                    purge_ok = False

            # ── gm-discogs-sql-loader-2eg.3: the derived-relation refresh ────
            # Beside the purge, on the same latch, and only once every type has
            # signalled. The pass reconciles `member_of` and `same_as` against the
            # documents present now and recomputes the seven counter relations from
            # the edge tables, in one transaction. It runs AFTER the purge, so the
            # rows a shrunk dump removed are already gone from the edge tables the
            # counters sum; a failure nacks this delivery exactly as a failed purge
            # does, and the whole pass is idempotent so the retry re-runs it safely.
            refresh_ok = True
            if purge_ok and connection_pool is not None and extraction_latch is None:
                logger.warning(
                    "⚠️ Skipping the derived-relation refresh — no extraction latch relation is declared",
                    data_type=data_type,
                )
            elif purge_ok and connection_pool is not None and extraction_latch is not None:
                async with derived_refresh_lock:
                    try:
                        version = extraction_latch_key(data)
                        latch = await record_extraction_signal(connection_pool, extraction_latch, version, data_type)
                        if latch.should_refresh(DATA_TYPES):
                            await refresh_derived_relations(connection_pool, logger, version, extraction_latch)
                        elif latch.already_refreshed:
                            logger.info(
                                "✅ Derived relations were already refreshed for this extraction",
                                data_type=data_type,
                                version=version,
                            )
                        elif latch.superseded:
                            logger.warning(
                                "⏭️ Ignoring a straggler extraction_complete — a later extraction has started",
                                data_type=data_type,
                                version=version,
                            )
                        else:
                            logger.info(
                                "⏳ Deferring the derived-relation refresh until every data type completes",
                                data_type=data_type,
                                version=version,
                                received=sorted(latch.signals),
                                pending=latch.pending(DATA_TYPES),
                            )
                    except Exception as refresh_exc:
                        logger.error(
                            "❌ Derived-relation refresh failed, nacking extraction_complete for retry",
                            data_type=data_type,
                            error=str(refresh_exc),
                        )
                        refresh_ok = False
            # ── end gm-discogs-sql-loader-2eg.3 ──────────────────────────────

            if purge_ok and refresh_ok:
                # extraction_complete is this type's terminal signal, so it must also
                # (re-)mark the type complete. completed_files is otherwise written
                # only by file_complete and ERASED by _recover_consumers for any type
                # whose queue still holds messages — and when the only pending message
                # IS this signal, nothing ever restored the flag. The service then
                # logged "Stalled consumers detected" at ERROR every 30s forever,
                # check_all_consumers_idle() could never return True, and the
                # connection plus four idle consumers were held open until restart.
                # A plain restart between the file_complete ack and this delivery
                # reaches the same terminal state (discogsography-ewvh).
                completed_files.add(data_type)

                # Re-arm cancellation: the file_complete that originally scheduled it
                # was consumed in an earlier session or before recovery.
                if CONSUMER_CANCEL_DELAY > 0 and data_type in queues:
                    await schedule_consumer_cancellation(data_type, queues[data_type])

                await message.ack()
                record_terminal("processed")
            else:
                await message.nack(requeue=True)
                record_terminal("failed", "purge_failed" if not purge_ok else "refresh_failed")
            return

        # Normal message processing - require a non-empty 'id' field.
        # Falsy (not just absent), matching every sibling site: graphinator.py's
        # `if not record.get("id")`, both batch processors' `if not data_id`, and the
        # brainz* consumers. Key-presence alone let `"id": null` through to an INSERT
        # into a NOT NULL PRIMARY KEY, whose deterministic IntegrityError landed in the
        # generic handler and was nacked with requeue=True — burning all 20 redeliveries
        # before dead-lettering — while `"id": ""` silently wrote a junk row keyed on the
        # empty string and acked it as success (discogsography-ria1).
        if not data.get("id"):
            logger.error("❌ Message missing 'id' field", data=data)
            await message.nack(requeue=False)
            record_terminal("failed", "missing_id")
            return

        # If batch mode is enabled, delegate to batch processor
        if BATCH_MODE and batch_processor is not None:
            accepted = await batch_processor.add_message(
                data_type=data_type,
                data=data,
                ack_callback=message.ack,
                # This delivery's CONSUMER span closes when the handler returns, long
                # before the batch it joined is written, so the flush span links back to
                # it instead of nesting under it.
                span_context=telemetry.span_context_of(span),
                # requeue=False: this callback nacks permanently-invalid input
                # (unknown data_type, missing 'id', normalize failure, poison
                # batch) — send it straight to the DLQ instead of cycling
                # x-delivery-limit (20) futile redeliveries. Matches the
                # non-batch validation path above. Transient failures are handled
                # by _flush_queue's re-enqueue+backoff, which never nacks.
                nack_callback=lambda: message.nack(requeue=False),
            )

            # Only update progress tracking when message was actually accepted
            if accepted and data_type in message_counts:
                message_counts[data_type] += 1
                last_message_time[data_type] = time.time()
            return

        # Non-batch mode: process individual messages
        data_id: str = data["id"]

        # Apply consumer-side normalization (year parsing from date strings)
        data = normalize_record(data_type, data)

        # Extract record details for logging
        record_name = None
        if data_type == "artists":
            record_name = data.get("name", "Unknown Artist")
        elif data_type == "labels":
            record_name = data.get("name", "Unknown Label")
        elif data_type == "releases":
            record_name = data.get("title", "Unknown Release")
        elif data_type == "masters":
            record_name = data.get("title", "Unknown Master")

        # Log at debug level to reduce noise
        if record_name:
            logger.debug(
                "🔄 Processing record",
                data_type=data_type[:-1],
                data_id=data_id,
                record_name=record_name,
            )
        else:
            logger.debug("🔄 Processing record", data_type=data_type[:-1], data_id=data_id)

    except Exception as e:
        logger.error("❌ Failed to parse message", error=str(e))
        await message.nack(requeue=False)
        record_terminal("failed", telemetry.error_type_of(e))
        return

    # Process record through the shared delivery runner. PostgreSQL policy stays
    # local; the runtime owns the single terminal ack/requeue/reject decision.
    async def persist_record() -> DeliveryResult:
        if connection_pool is None:
            raise DatabaseUnavailableError("Connection pool not initialized")

        persistence = PostgreSQLRecordPersistence(
            connection_pool,
            logger,
            PURGE_MAX_DELETE_FRACTION,
            media_for_release,
        )
        terminal_outcome = await persistence.persist_record(data_type, data_id, data)
        return DeliveryResult(Settlement.ACK, terminal_outcome)

    class DeliveryTelemetryObserver:
        """Bridge the current consumer span and metrics to ``run_delivery``."""

        @contextlib.contextmanager
        def consume(self, _destination: str, _headers: object | None) -> Any:
            yield span

        def settled(self, *, entity: str, result: DeliveryResult, duration_s: float, span: Any) -> None:
            outcome = result.outcome if result.settlement is Settlement.ACK else "failed"
            telemetry.record_message(entity, outcome, duration_s)
            telemetry.record_consumed_message(entity, duration_s, result.error_type)
            if result.error_type is not None:
                telemetry.mark_span_error_type(span, result.error_type)

    async def wait_before_requeue() -> None:
        await outage_backoff.wait()

    result = await run_delivery(
        message,
        persist_record,
        classifier=PostgreSQLFailureClassifier(),
        observer=DeliveryTelemetryObserver(),
        destination=data_type,
        entity=data_type,
        wait_before_requeue=wait_before_requeue,
    )

    if result.settlement is Settlement.ACK:
        # PostgreSQL answered — clear the outage backoff.
        outage_backoff.reset()

        # Increment counter and log progress only after successful DB write and ack
        if data_type in message_counts:
            message_counts[data_type] += 1
            last_message_time[data_type] = time.time()
            if message_counts[data_type] % progress_interval == 0:
                logger.info(
                    "📊 Processed records in PostgreSQL",
                    count=message_counts[data_type],
                    data_type=data_type,
                )
    elif result.settlement is Settlement.REQUEUE:
        logger.warning(
            "⚠️ Database connection issue, delivery requeued",
            data_type=data_type,
            error_type=result.error_type,
        )
    else:
        logger.error(
            "❌ Non-retryable data error, nacking without requeue",
            data_type=data_type,
            error_type=result.error_type,
        )


async def progress_reporter() -> None:
    global idle_mode

    report_count = 0
    startup_time = time.time()
    last_idle_log = 0.0

    while not shutdown_requested:
        # More frequent reports initially, then every 30 seconds
        if report_count < 3:
            await asyncio.sleep(10)  # First 3 reports every 10 seconds
        else:
            await asyncio.sleep(30)  # Then every 30 seconds
        report_count += 1

        # Skip all logging if all files are complete
        if len(completed_files) == len(DATA_TYPES):
            continue

        total = sum(message_counts.values())
        current_time = time.time()

        # Idle mode detection: no messages received after STARTUP_IDLE_TIMEOUT
        # Idle mode only suppresses reporting - consumers stay connected
        if not idle_mode and total == 0 and (current_time - startup_time) >= STARTUP_IDLE_TIMEOUT:
            idle_mode = True
            last_idle_log = current_time
            logger.info(
                f"😴 No messages received after {STARTUP_IDLE_TIMEOUT}s, entering idle mode. Consumers remain connected, reporting paused.",
                startup_idle_timeout=STARTUP_IDLE_TIMEOUT,
            )
            continue

        # While in idle mode, only log briefly every IDLE_LOG_INTERVAL
        if idle_mode:
            if total > 0:
                # Messages started flowing, exit idle mode
                idle_mode = False
                logger.info("🔄 Messages detected, resuming normal operation")
            elif (current_time - last_idle_log) >= IDLE_LOG_INTERVAL:
                last_idle_log = current_time
                logger.info(
                    "😴 Idle mode - waiting for messages. Consumers connected.",
                )
            continue

        # Check for stalled consumers (skip completed files)
        stalled_consumers = []
        for data_type, last_time in last_message_time.items():
            if data_type not in completed_files and last_time > 0 and (current_time - last_time) > 120:  # No messages for 2 minutes
                stalled_consumers.append(data_type)

        if stalled_consumers:
            logger.error(f"⚠️ Stalled consumers detected: {stalled_consumers}. No messages processed for >2 minutes.")

        # Always show progress, even if no messages processed yet
        # Build progress string with completion emojis
        progress_parts = []
        for data_type in ["artists", "labels", "masters", "releases"]:
            emoji = "✅ " if data_type in completed_files else ""
            progress_parts.append(f"{emoji}{data_type.capitalize()}: {message_counts[data_type]}")

        logger.info(f"📊 PostgreSQL Progress: {total} total messages processed ({', '.join(progress_parts)})")

        # Log current processing state
        if total == 0:
            logger.info("⏳ Waiting for messages to process...")
        elif all(current_time - last_time < 5 for last_time in last_message_time.values() if last_time > 0):
            logger.info("✅ All consumers actively processing")
        elif any(last_time > 0 and 5 < current_time - last_time < 120 for last_time in last_message_time.values()):
            slow_consumers = [dt for dt, lt in last_message_time.items() if lt > 0 and 5 < current_time - lt < 120]
            logger.warning(
                f"⚠️ Slow consumers detected: {slow_consumers}",
                slow_consumers=slow_consumers,
            )

        # Log consumer status
        active_consumers = list(consumer_tags.keys())
        canceled_consumers = [dt for dt in DATA_TYPES if dt not in consumer_tags and dt in completed_files]

        if canceled_consumers:
            logger.info(
                f"🔧 Canceled consumers: {canceled_consumers}",
                canceled_consumers=canceled_consumers,
            )
        if active_consumers:
            logger.info(
                f"✅ Active consumers: {active_consumers}",
                active_consumers=active_consumers,
            )


async def main() -> None:
    global \
        connection_pool, \
        config, \
        connection_params, \
        queues, \
        rabbitmq_manager, \
        active_connection, \
        active_channel, \
        connection_check_task, \
        batch_processor, \
        extraction_latch

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_logging(SERVICE_NAME, log_file=LOG_PATH)
    setup_telemetry(OTEL_SERVICE_NAME)
    # Sample this loop's scheduling delay into groovemap.runtime.event_loop.lag. It has to
    # be started from the running loop, so it belongs here and not next to setup_telemetry
    # in a module-level bootstrap; shutdown_telemetry() cancels it. Returns None -- and
    # logs one line -- whenever there is nothing to sample into, so no result to check.
    start_event_loop_monitor()
    logger.info("🚀 Starting GrooveMap discogs-sql-loader with PostgreSQL connection pooling")

    # Add startup delay for dependent services
    startup_delay = int(os.environ.get("STARTUP_DELAY", "5"))
    if startup_delay > 0:
        logger.info(
            f"⏳ Waiting {startup_delay} seconds for dependent services to start...",
            startup_delay=startup_delay,
        )
        await asyncio.sleep(startup_delay)

    # Start health server
    health_server = HealthServer(8002, get_health_data)
    health_server.start_background()
    logger.info("🏥 Health server started on port 8002")

    # Initialize configuration
    try:
        config = TableinatorConfig.from_env()
    except ValueError as e:
        logger.error("❌ Configuration error", error=str(e))
        return

    # Parse host and port from address (POSTGRES_HOST may embed a port, e.g. a pooler)
    host, port = parse_postgres_host_port(config.postgres_host)

    # Set connection parameters
    connection_params = {
        "host": str(host),
        "port": int(port),
        "dbname": str(config.postgres_database),
        "user": str(config.postgres_username),
        "password": str(config.postgres_password),
    }

    # Initialize async resilient connection pool for concurrent access.
    # Writes go through the BatchProcessor, whose semaphore (max_concurrent_flushes)
    # caps simultaneous PostgreSQL flushes — so a small pool is sufficient and keeps
    # the service within the shared PgBouncer backend budget (see resolve_postgres_pool_sizes).
    try:
        connection_pool = AsyncPostgreSQLPool(
            connection_params=connection_params,
            max_connections=config.postgres_pool_max_size,
            min_connections=config.postgres_pool_min_size,
            max_retries=5,
            health_check_interval=30,
        )
        await connection_pool.initialize()
        logger.info("🐘 Connected to PostgreSQL with async resilient connection pool")
        # gm-discogs-sql-loader-2eg.3: resolve the declared extraction latch relation once,
        # read-only. A miss is a logged degraded mode, never a reason to create the table.
        extraction_latch = await probe_latch_relation(connection_pool, logger)
        logger.info(
            "✅ Async connection pool initialized (min: %d, max: %d connections)",
            config.postgres_pool_min_size,
            config.postgres_pool_max_size,
        )
    except Exception as e:
        logger.error("❌ Failed to initialize connection pool", error=str(e))
        return

    # Initialize async batch processor if enabled
    if BATCH_MODE:
        batch_config = BatchConfig(
            batch_size=BATCH_SIZE,
            flush_interval=BATCH_FLUSH_INTERVAL,
        )
        batch_processor = PostgreSQLBatchProcessor(connection_pool, batch_config)
        logger.info(
            "🚀 Async batch processing enabled",
            batch_size=BATCH_SIZE,
            flush_interval=BATCH_FLUSH_INTERVAL,
        )
    else:
        logger.info("📝 Using per-message processing (batch mode disabled)")
    print(STARTUP_BANNER)

    # Initialize resilient RabbitMQ connection manager (not connecting yet)
    rabbitmq_manager = AsyncResilientRabbitMQ(
        connection_url=config.amqp_connection,
        max_retries=10,  # More retries for startup
        heartbeat=600,
        connection_attempts=10,
        retry_delay=5.0,
    )

    # Try to connect with additional retry logic for startup
    max_startup_retries = 5
    startup_retry = 0
    amqp_connection = None

    while startup_retry < max_startup_retries and not shutdown_requested:
        try:
            logger.info(
                "🐰 Attempting to connect to RabbitMQ",
                attempt=startup_retry + 1,
                max_attempts=max_startup_retries,
            )
            amqp_connection = await rabbitmq_manager.connect()
            active_connection = amqp_connection
            break
        except Exception as e:
            startup_retry += 1
            if startup_retry < max_startup_retries:
                wait_time = min(30, 5 * startup_retry)  # Exponential backoff up to 30s
                logger.warning(
                    "⚠️ RabbitMQ connection failed. Retrying...",
                    error=str(e),
                    wait_seconds=wait_time,
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(
                    "❌ Failed to connect to AMQP broker",
                    max_attempts=max_startup_retries,
                    error=str(e),
                )
                return

    if amqp_connection is None:
        logger.error("❌ No AMQP connection available")
        return

    async with amqp_connection:
        channel = await amqp_connection.channel()
        active_channel = channel

        # Set QoS to allow concurrent batch processing for better throughput, or to
        # couple in-flight deliveries to the PostgreSQL pool in non-batch mode.
        prefetch_count, prefetch_global = channel_prefetch()
        await channel.set_qos(prefetch_count=prefetch_count, global_=prefetch_global)
        logger.info(
            "🔧 QoS prefetch configured",
            prefetch_count=prefetch_count,
            channel_global=prefetch_global,
            # Per-consumer QoS means the real channel-wide ceiling is prefetch x consumers.
            channel_wide_max=prefetch_count if prefetch_global else prefetch_count * len(DATA_TYPES),
            batch_size=BATCH_SIZE if BATCH_MODE else "N/A",
        )

        # Declare per-data-type fanout exchanges and consumer-owned queues
        queues = {}
        for data_type in DATA_TYPES:
            exchange_name = catalog_exchange_name(data_type)
            queue_name = catalog_queue_name(AMQP_CONSUMER_NAME, data_type)
            dlx_name = catalog_dead_letter_exchange_name(AMQP_CONSUMER_NAME, data_type)
            dlq_name = catalog_dead_letter_queue_name(AMQP_CONSUMER_NAME, data_type)

            # Declare fanout exchange (must match extractor)
            exchange = await channel.declare_exchange(exchange_name, AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)

            # Declare consumer-owned dead-letter exchange
            dlx_exchange = await channel.declare_exchange(dlx_name, AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)

            # Declare DLQ (classic queue for dead letters)
            dlq = await channel.declare_queue(
                auto_delete=False,
                durable=True,
                name=dlq_name,
                arguments={"x-queue-type": "classic"},
            )
            await dlq.bind(dlx_exchange)

            # Declare main quorum queue with consumer-owned DLX
            queue_args = {
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": dlx_name,
                "x-delivery-limit": 20,
            }
            queue = await channel.declare_queue(
                auto_delete=False,
                durable=True,
                name=queue_name,
                arguments=queue_args,
            )
            await queue.bind(exchange)
            queues[data_type] = queue

        # Start consumers for all data types
        for data_type in DATA_TYPES:
            handler = make_data_handler(data_type)
            consumer_tags[data_type] = await queues[data_type].consume(handler)
            telemetry.record_consumer_started()

        logger.info(
            f"🚀 {SERVICE_NAME} started! Connected to AMQP broker ({len(DATA_TYPES)} fanout exchanges). "
            f"Consuming from {len(DATA_TYPES)} queues with connection pool "
            f"(max {config.postgres_pool_max_size} connections). "
            "Ready to process messages into PostgreSQL. Press CTRL+C to exit"
        )

        progress_task = asyncio.create_task(progress_reporter())

        # Start periodic queue checker task
        connection_check_task = asyncio.create_task(periodic_queue_checker())
        logger.info(
            f"🔄 Started periodic queue checker (interval: {QUEUE_CHECK_INTERVAL}s)",
            QUEUE_CHECK_INTERVAL=QUEUE_CHECK_INTERVAL,
        )

        # Start batch processor periodic flush task if enabled
        batch_flush_task = None
        if BATCH_MODE and batch_processor is not None:
            batch_flush_task = asyncio.create_task(batch_processor.periodic_flush())
            logger.info("🔄 Started batch processor periodic flush task")

        try:
            # Check for shutdown periodically
            while not shutdown_requested:
                await asyncio.sleep(1.0)

        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, shutting down gracefully")
        finally:
            # Stop new deliveries FIRST, before the multi-second flush/teardown
            # below: a still-subscribed consumer keeps being handed messages it
            # can only leave unacked (discogsography-lnn4).
            await cancel_all_consumers()

            # Cancel progress reporting
            progress_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await progress_task

            # Cancel connection check task
            if connection_check_task:
                connection_check_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await connection_check_task
                logger.info("✅ Queue checker task stopped")

            # Cancel batch flush task and flush remaining messages
            if batch_flush_task:
                batch_flush_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await batch_flush_task
            if batch_processor:
                batch_processor.shutdown()
                try:
                    await batch_processor.flush_all()
                    logger.info("✅ Batch processor flushed and stopped")
                except Exception as e:
                    logger.warning("⚠️ Error flushing batch processor", error=str(e))

            # Cancel any pending consumer cancellation tasks
            for task in list(consumer_cancel_tasks.values()):
                task.cancel()

            # Close RabbitMQ connection if still active
            await close_rabbitmq_connection()

            # Close async connection pool
            try:
                if connection_pool:
                    await connection_pool.close()
                    logger.info("✅ Async connection pool closed")
            except Exception as e:
                logger.warning("⚠️ Error closing connection pool", error=str(e))

        # Stop health server
        health_server.stop()

    # Force-flush and shut down the meter provider last, so any metrics recorded during
    # the teardown sequence above still land in the final export.
    shutdown_telemetry()


def cli() -> None:
    """Run the async service from its console-script entry point."""
    try:
        run(main())
    except KeyboardInterrupt:
        logger.warning("⚠️ Application interrupted")
    except Exception as e:
        logger.error("❌ Application error", error=str(e))
    finally:
        logger.info(f"✅ {SERVICE_NAME} shutdown complete")


if __name__ == "__main__":
    cli()
