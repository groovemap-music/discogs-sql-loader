"""Generated from contracts/catalog-events/definitions/discogs.json; do not edit."""

from os import getenv

CONTRACT_NAME = "groovemap.catalog-events"
CONTRACT_VERSION = 1
SOURCE = "discogs"
AMQP_EXCHANGE_TYPE = "fanout"
ENTITY_TYPES = ["artists", "labels", "masters", "releases"]
CONSUMERS = ["graphinator", "tableinator"]
EXCHANGE_PREFIX = getenv("DISCOGS_EXCHANGE_PREFIX", "groovemap-discogs")


def exchange_name(entity: str) -> str:
    if entity not in ENTITY_TYPES:
        raise ValueError(f"Unknown Discogs entity: {entity}")
    return f"{EXCHANGE_PREFIX}-{entity}"


def queue_name(consumer: str, entity: str) -> str:
    if consumer not in CONSUMERS:
        raise ValueError(f"Unknown Discogs consumer: {consumer}")
    if entity not in ENTITY_TYPES:
        raise ValueError(f"Unknown Discogs entity: {entity}")
    return f"{EXCHANGE_PREFIX}-{consumer}-{entity}"
