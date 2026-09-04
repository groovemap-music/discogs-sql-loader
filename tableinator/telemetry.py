"""OpenTelemetry domain instruments for discogs-sql-loader (tableinator).

Metric names, units, and attribute keys follow the GrooveMap OpenTelemetry metrics
conventions (design ADR-0006; reproduced in the program epic and in
``groovemap-runtime``'s ``docs/runtime.md``). Instruments are created lazily from
``get_meter("groovemap.tableinator")`` and rebuilt whenever the installed
``MeterProvider`` changes (tracked via ``common.telemetry.provider_generation``), so:

* a process that never calls ``setup_telemetry`` pays only for one no-op instrument per
  metric, and
* code that runs before ``setup_telemetry`` (this module is imported well before
  ``main()`` calls it) does not permanently bind to a no-op meter -- the cache is
  rebuilt the first time a recording function runs after the real provider is
  installed.

Every recording function swallows its own errors: telemetry must never turn a working
message into a failure (see ``common.runtime_metrics``, which follows the same rule for
the shared wrappers).
"""

import logging
from threading import RLock
from typing import Any

from common.telemetry import get_meter, provider_generation


logger = logging.getLogger(__name__)

INSTRUMENTATION_SCOPE = "groovemap.tableinator"

# Domain metrics (groovemap.*), shared shape across every molecule in the OTEL-metrics
# program.
PIPELINE_MESSAGES = "groovemap.pipeline.messages"
PIPELINE_MESSAGE_DURATION = "groovemap.pipeline.message.duration"
PIPELINE_BATCH_SIZE = "groovemap.pipeline.batch.size"
PIPELINE_BATCH_FLUSH_DURATION = "groovemap.pipeline.batch.flush.duration"
PIPELINE_CONSUMERS_ACTIVE = "groovemap.pipeline.consumers.active"

# messaging.client.* -- the OTEL semantic-convention names the shared
# ``process_message_with_retry`` wrapper would emit. tableinator consumes via
# ``queue.consume(handler)`` directly (see tableinator.py::on_data_message), which never
# calls that wrapper, so these are recorded locally with the identical name and
# attribute keys instead of going unrecorded.
MESSAGING_CONSUMED_MESSAGES = "messaging.client.consumed.messages"
MESSAGING_OPERATION_DURATION = "messaging.client.operation.duration"

SOURCE = "discogs"
STORE = "postgresql"
MESSAGING_SYSTEM = "rabbitmq"

_lock = RLock()
_instruments: dict[str, Any] = {}
_instrument_generation = -1


def _build_instruments() -> dict[str, Any]:
    """Create one instrument per tableinator metric from the current provider."""
    meter = get_meter(INSTRUMENTATION_SCOPE)
    return {
        PIPELINE_MESSAGES: meter.create_counter(
            PIPELINE_MESSAGES,
            description="Catalog messages handled by the pipeline.",
        ),
        PIPELINE_MESSAGE_DURATION: meter.create_histogram(
            PIPELINE_MESSAGE_DURATION,
            unit="s",
            description="Duration of handling one catalog message.",
        ),
        PIPELINE_BATCH_SIZE: meter.create_histogram(
            PIPELINE_BATCH_SIZE,
            unit="{items}",
            description="Number of records in a batch flush attempt.",
        ),
        PIPELINE_BATCH_FLUSH_DURATION: meter.create_histogram(
            PIPELINE_BATCH_FLUSH_DURATION,
            unit="s",
            description="Duration of a batch flush to the store.",
        ),
        PIPELINE_CONSUMERS_ACTIVE: meter.create_up_down_counter(
            PIPELINE_CONSUMERS_ACTIVE,
            description="Active RabbitMQ consumers held by this service.",
        ),
        MESSAGING_CONSUMED_MESSAGES: meter.create_counter(
            MESSAGING_CONSUMED_MESSAGES,
            description="Messages consumed from the broker.",
        ),
        MESSAGING_OPERATION_DURATION: meter.create_histogram(
            MESSAGING_OPERATION_DURATION,
            unit="s",
            description="Duration of a messaging client operation.",
        ),
    }


def _instrument(name: str) -> Any:
    """Return one cached instrument, rebuilding the cache when the provider changed."""
    global _instrument_generation

    generation = provider_generation()
    with _lock:
        if _instrument_generation != generation or not _instruments:
            _instruments.clear()
            _instruments.update(_build_instruments())
            _instrument_generation = generation
        return _instruments[name]


def reset_instruments() -> None:
    """Drop the instrument cache. Test seam; production relies on the generation check."""
    global _instrument_generation

    with _lock:
        _instruments.clear()
        _instrument_generation = -1


def record_message(entity: str, outcome: str, duration_s: float) -> None:
    """Record one terminal disposition of a catalog message.

    ``outcome`` is one of ``processed`` (persisted, or a control signal handled),
    ``skipped`` (the record's hash matched, so only ``updated_at`` was refreshed),
    ``media_backfilled`` (the hash matched but the `releases` row's NULL ``media``
    column was filled in place -- ADR 0007), or ``failed`` (nacked, whether to the DLQ
    or for broker redelivery). Only terminal dispositions are recorded -- a message
    still awaiting an in-process batch retry has not concluded yet, so it is not counted
    here (see ``record_batch_flush`` for the batch-level view of that retry).
    """
    try:
        _instrument(PIPELINE_MESSAGES).add(1, {"source": SOURCE, "entity": entity, "outcome": outcome})
        _instrument(PIPELINE_MESSAGE_DURATION).record(duration_s, {"source": SOURCE, "entity": entity})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_MESSAGES, exc_info=True)


def record_batch_size(entity: str, size: int) -> None:
    """Record the size of one batch flush attempt."""
    try:
        _instrument(PIPELINE_BATCH_SIZE).record(size, {"store": STORE, "entity": entity})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_BATCH_SIZE, exc_info=True)


def record_batch_flush(entity: str, duration_s: float, outcome: str) -> None:
    """Record the duration of a terminal (successful or poisoned) batch flush.

    A transient PostgreSQL failure re-enqueues the batch for an in-process retry rather
    than concluding it, so it is not recorded here -- only ``success`` and ``failed``
    (poison batch nacked to the DLQ) are terminal outcomes for a flush.
    """
    try:
        _instrument(PIPELINE_BATCH_FLUSH_DURATION).record(duration_s, {"store": STORE, "entity": entity, "outcome": outcome})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_BATCH_FLUSH_DURATION, exc_info=True)


def record_consumer_started() -> None:
    """Count one RabbitMQ consumer starting."""
    try:
        _instrument(PIPELINE_CONSUMERS_ACTIVE).add(1, {"source": SOURCE})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s start", PIPELINE_CONSUMERS_ACTIVE, exc_info=True)


def record_consumer_stopped() -> None:
    """Count one RabbitMQ consumer stopping."""
    try:
        _instrument(PIPELINE_CONSUMERS_ACTIVE).add(-1, {"source": SOURCE})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s stop", PIPELINE_CONSUMERS_ACTIVE, exc_info=True)


def record_consumed_message(destination: str, duration_s: float, error_type: str | None = None) -> None:
    """Record ``messaging.client.consumed.messages`` / ``.operation.duration`` locally.

    Mirrors ``common.runtime_metrics.record_consumed_message`` exactly (same metric
    names, same attribute keys and values) for the code path that bypasses
    ``common.process_message_with_retry``.
    """
    attributes: dict[str, str] = {
        "messaging.system": MESSAGING_SYSTEM,
        "messaging.destination.name": destination,
        "messaging.operation.name": "process",
    }
    if error_type is not None:
        attributes["error.type"] = error_type
    try:
        _instrument(MESSAGING_CONSUMED_MESSAGES).add(1, attributes)
        _instrument(MESSAGING_OPERATION_DURATION).record(duration_s, attributes)
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record consumed-message metrics", exc_info=True)


def error_type_of(exc: BaseException) -> str:
    """Return the closed-set ``error.type`` value for an exception: its class name."""
    return type(exc).__name__
