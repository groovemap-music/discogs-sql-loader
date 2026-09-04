"""Dead-letter name adapter over the promoted Discogs catalog-events binding.

``tableinator/catalog_contract.py`` is promoted byte-for-byte from the
discogs-ingestion producer (see ``contracts/catalog-events/v1/source.json``)
and must stay that way. The split, Discogs-only binding it now carries no
longer exposes ``dead_letter_exchange_name``/``dead_letter_queue_name``
helpers -- the pre-split, multi-source binding did. This module is the local,
non-generated adapter that reconstructs those two names with the exact
template the retired binding used: ``f"{queue_name(...)}.dlx"`` /
``f"{queue_name(...)}.dlq"``.

Runtime identifiers are frozen (ADR 0005): this service's durable AMQP
exchange, queue, dead-letter-exchange, and dead-letter-queue names must not
change across a producer promotion. ``tests/test_queue_names.py`` snapshots
every name this adapter produces for the ``tableinator`` consumer against
both the pre-promotion binding's output and
``contracts/catalog-events/v1/contract.json``'s ``runtime_identifiers``
block, so a future promotion that shifts a name is caught immediately.
"""

from tableinator.catalog_contract import queue_name


__all__ = ["dead_letter_exchange_name", "dead_letter_queue_name"]


def dead_letter_exchange_name(consumer: str, entity: str) -> str:
    """Build the dead-letter exchange name for a consumer queue."""
    return f"{queue_name(consumer, entity)}.dlx"


def dead_letter_queue_name(consumer: str, entity: str) -> str:
    """Build the dead-letter queue name for a consumer queue."""
    return f"{queue_name(consumer, entity)}.dlq"
