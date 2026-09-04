"""Frozen-identifier tests for the local queue-name adapter.

ADR 0005 freezes this service's runtime AMQP identifiers: a producer-contract
promotion must never rename a durable exchange, queue, dead-letter exchange,
or dead-letter queue this service already has messages under. FROZEN_NAMES
below is a snapshot of exactly what the pre-split, multi-source
``catalog_contract`` binding produced for the ``tableinator`` consumer before
it was replaced by the Discogs-only discogs-ingestion binding (see
``contracts/catalog-events/v1/source.json``). Every assertion here checks the
post-promotion binding plus the ``tableinator.queue_names`` adapter against
that frozen snapshot, and against the promoted contract's own
``runtime_identifiers`` block, so a future promotion that silently shifts a
name is caught immediately.
"""

import json
from pathlib import Path

from tableinator.catalog_contract import exchange_name, queue_name
from tableinator.queue_names import dead_letter_exchange_name, dead_letter_queue_name
from tableinator.tableinator import AMQP_CONSUMER_NAME, DATA_TYPES


ROOT = Path(__file__).parent.parent

# Snapshot of the retired, pre-split binding's output for this service
# (consumer="tableinator", source="discogs"), captured before the
# discogs-ingestion promotion. Do not update these values to match a new
# promotion -- a change here means a durable AMQP identifier moved.
FROZEN_NAMES = {
    "artists": {
        "exchange": "groovemap-discogs-artists",
        "queue": "groovemap-discogs-tableinator-artists",
        "dead_letter_exchange": "groovemap-discogs-tableinator-artists.dlx",
        "dead_letter_queue": "groovemap-discogs-tableinator-artists.dlq",
    },
    "labels": {
        "exchange": "groovemap-discogs-labels",
        "queue": "groovemap-discogs-tableinator-labels",
        "dead_letter_exchange": "groovemap-discogs-tableinator-labels.dlx",
        "dead_letter_queue": "groovemap-discogs-tableinator-labels.dlq",
    },
    "masters": {
        "exchange": "groovemap-discogs-masters",
        "queue": "groovemap-discogs-tableinator-masters",
        "dead_letter_exchange": "groovemap-discogs-tableinator-masters.dlx",
        "dead_letter_queue": "groovemap-discogs-tableinator-masters.dlq",
    },
    "releases": {
        "exchange": "groovemap-discogs-releases",
        "queue": "groovemap-discogs-tableinator-releases",
        "dead_letter_exchange": "groovemap-discogs-tableinator-releases.dlx",
        "dead_letter_queue": "groovemap-discogs-tableinator-releases.dlq",
    },
}


def test_frozen_names_cover_every_entity_type() -> None:
    assert set(FROZEN_NAMES) == set(DATA_TYPES)
    assert AMQP_CONSUMER_NAME == "tableinator"


def test_adapter_reproduces_the_frozen_identifiers() -> None:
    for entity, expected in FROZEN_NAMES.items():
        assert exchange_name(entity) == expected["exchange"]
        assert queue_name(AMQP_CONSUMER_NAME, entity) == expected["queue"]
        assert dead_letter_exchange_name(AMQP_CONSUMER_NAME, entity) == expected["dead_letter_exchange"]
        assert dead_letter_queue_name(AMQP_CONSUMER_NAME, entity) == expected["dead_letter_queue"]


def test_frozen_identifiers_match_the_promoted_contracts_runtime_identifiers() -> None:
    contract = json.loads((ROOT / "contracts/catalog-events/v1/contract.json").read_text())
    runtime_identifiers = contract["runtime_identifiers"]

    for entity, expected in FROZEN_NAMES.items():
        assert runtime_identifiers["exchanges"][entity] == expected["exchange"]
        queue = runtime_identifiers["queues"][AMQP_CONSUMER_NAME][entity]
        assert queue["name"] == expected["queue"]
        assert queue["dead_letter_exchange"] == expected["dead_letter_exchange"]
        assert queue["dead_letter_queue"] == expected["dead_letter_queue"]
