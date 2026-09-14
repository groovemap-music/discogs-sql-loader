"""Map a Discogs entity table onto the native identity vocabulary (ADR 0009).

Every row this loader writes carries the native catalog item its Discogs identifier
maps to. ``common.identity`` keys that mapping on ``(provider, entity_kind,
external_id)``, and the entity kind is the singular of the table name, so the one
translation both write paths need lives here rather than twice.
"""

from __future__ import annotations

from typing import Final

from common.identity import AliasRef


__all__ = ["DISCOGS_PROVIDER", "ENTITY_KIND_BY_DATA_TYPE", "alias_ref"]

DISCOGS_PROVIDER: Final = "discogs"

# The promoted catalog-events contract admits exactly these four tables, so a data type
# outside the mapping is a contract violation rather than a data error and must raise.
ENTITY_KIND_BY_DATA_TYPE: Final[dict[str, str]] = {
    "artists": "artist",
    "labels": "label",
    "masters": "master",
    "releases": "release",
}


def alias_ref(data_type: str, data_id: str) -> AliasRef:
    """Return the provider alias key for one Discogs row.

    Raises:
        KeyError: If ``data_type`` is not one of the four contract entity tables.
    """
    return AliasRef(DISCOGS_PROVIDER, ENTITY_KIND_BY_DATA_TYPE[data_type], data_id)
