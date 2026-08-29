"""Import and promoted catalog-contract smoke tests."""

import tableinator.tableinator as service
from tableinator.catalog_contract import AMQP_EXCHANGE_TYPE, DATA_TYPES, DISCOGS_EXCHANGE_PREFIX


def test_service_import_exposes_entry_point() -> None:
    assert callable(service.main)


def test_catalog_contract_matches_discogs_stream() -> None:
    assert DISCOGS_EXCHANGE_PREFIX == "groovemap-discogs"
    assert AMQP_EXCHANGE_TYPE == "fanout"
    assert DATA_TYPES == ["artists", "labels", "masters", "releases"]
