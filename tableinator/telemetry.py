"""OpenTelemetry domain instruments and spans for discogs-sql-loader (tableinator).

Metric names, units, and attribute keys follow the GrooveMap OpenTelemetry metrics
conventions (design ADR-0006; reproduced in the program epic and in
``groovemap-runtime``'s ``docs/runtime.md``), and the span names, kinds, and attributes
follow the wave-2 tracing conventions in the same places. Instruments are created lazily from
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
the shared wrappers). The span helpers at the bottom of this module follow the same rule
and yield None when tracing is off, so a caller never has to check whether a provider is
installed.
"""

import logging
from contextlib import contextmanager
from threading import RLock
from typing import TYPE_CHECKING, Any

from common import extract_context, flush_span, get_tracer
from common.telemetry import get_meter, provider_generation


if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence


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


# ---------------------------------------------------------------------------------------
# Spans
#
# tableinator consumes via ``queue.consume(handler)`` directly, so it never reaches
# ``common.process_message_with_retry`` and never gets that wrapper's CONSUMER span for
# free. ``consume_span`` opens the identical span here -- same name, same kind, same
# attribute keys -- from the two stable helpers the runtime exports for exactly this case
# (``get_tracer`` and ``extract_context``), mirroring what ``record_consumed_message``
# above already does for the wrapper's metrics.
#
# The batch flush span comes straight from ``common.flush_span``; only the outcome
# attribute, which the library cannot know, is set here.
#
# Both follow the wave-2 span rules: exception recording and automatic status are OFF, and
# a failure sets status ERROR with ``error.type`` and nothing else -- no message, no stack
# trace, no span event carrying a payload.
# ---------------------------------------------------------------------------------------

# The span attribute that mirrors the ``outcome`` attribute on
# groovemap.pipeline.batch.flush.duration, and shares its closed set. It is set only where
# that metric is recorded, so a flush span without it is one that concluded in neither
# terminal state: the batch was re-enqueued for an in-process retry.
FLUSH_OUTCOME_ATTRIBUTE = "outcome"


def _trace_api() -> Any:
    """Return the ``opentelemetry.trace`` module, or None without the ``otel`` extra."""
    try:
        from opentelemetry import trace  # noqa: PLC0415
    except Exception:  # pragma: no cover - exercised only without the extra
        return None
    return trace


def mark_span_error(span: Any, exc: BaseException) -> None:
    """Fail a span with ``error.type`` only. Never raises, and a no-op for a None span."""
    if span is None:
        return
    trace = _trace_api()
    if trace is None:  # pragma: no cover - exercised only without the extra
        return
    try:
        span.set_attribute("error.type", error_type_of(exc))
        span.set_status(trace.Status(trace.StatusCode.ERROR))
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not mark a span as failed", exc_info=True)


def span_context_of(span: Any) -> Any:
    """Return a span's context when it is worth linking to, otherwise None.

    A span that is not recording is dropped here rather than being carried through the
    batch queue and filtered at the flush. That covers both ways a delivery can produce
    nothing worth linking: tracing is off, or the sampler dropped this span. In the second
    case the non-recording span still carries the REMOTE parent's context, which belongs
    to the extractor's publish and not to this delivery, so linking it would attach a
    stranger's span to the batch.
    """
    if span is None:
        return None
    try:
        if not span.is_recording():
            return None
        context = span.get_span_context()
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not read a span context", exc_info=True)
        return None
    return context if getattr(context, "is_valid", False) else None


@contextmanager
def consume_span(destination: str, headers: Mapping[str, Any] | None = None) -> Iterator[Any]:
    """Open the CONSUMER span for one delivery: ``process {destination}``.

    The span is a child of the trace context carried in ``headers``, which is what puts
    the extractor's ``publish`` span and this service's processing in one trace. Headers
    with no readable context -- absent, or a malformed ``traceparent`` -- simply start a
    new trace rather than failing the message that delivered them.

    ``destination`` is the entity name, the same closed-set value
    ``record_consumed_message`` records as ``messaging.destination.name``, so the span and
    the metric describe one destination rather than two.

    Yields None when tracing is off, so every caller must tolerate a None span.
    """
    trace = _trace_api()
    if trace is None:  # pragma: no cover - exercised only without the extra
        yield None
        return

    attributes = {
        "messaging.system": MESSAGING_SYSTEM,
        "messaging.destination.name": destination,
        "messaging.operation.name": "process",
    }
    try:
        manager = get_tracer(INSTRUMENTATION_SCOPE).start_as_current_span(
            f"process {destination}",
            context=extract_context(headers) if headers else None,
            kind=trace.SpanKind.CONSUMER,
            attributes=attributes,
            record_exception=False,
            set_status_on_exception=False,
        )
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not start the consume span for %s", destination, exc_info=True)
        yield None
        return

    with manager as span:
        try:
            yield span
        except BaseException as exc:
            mark_span_error(span, exc)
            raise


@contextmanager
def batch_flush_span(entity: str, links: Sequence[Any] | None = None) -> Iterator[Any]:
    """Open the INTERNAL span for one batch flush: ``flush postgresql {entity}``.

    ``links`` are the span contexts of the deliveries the batch carries; the runtime
    attaches at most ``common.tracing.MAX_FLUSH_LINKS`` (64) of them. The span stays open
    across ``_process_batch``, so the connection pool's CLIENT database spans nest inside
    it, and it covers exactly the window
    ``groovemap.pipeline.batch.flush.duration`` measures.
    """
    with flush_span(STORE, entity, links) as span:
        yield span


def set_flush_outcome(span: Any, outcome: str) -> None:
    """Record a flush span's terminal outcome, alongside the flush-duration metric."""
    if span is None:
        return
    try:
        span.set_attribute(FLUSH_OUTCOME_ATTRIBUTE, outcome)
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record the flush outcome on a span", exc_info=True)
