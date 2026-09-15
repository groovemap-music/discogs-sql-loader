"""Map a Discogs entity table onto the native identity vocabulary (ADR 0009, ADR 0011).

Every row this loader writes carries the native catalog item its Discogs identifier
maps to. ``common.identity`` keys that mapping on ``(provider, entity_kind,
external_id)``, and the entity kind is the singular of the table name, so the one
translation both write paths need lives here rather than twice.

A release additionally carries a canonical ``identifiers`` block (ADR 0011), and the
barcode, catalogue-number, and matrix identifiers in it become provider aliases on the
release's native id so lookup by printed value resolves to the same item. Turning a
batch of release payloads into those aliases, and attaching them, is the second
translation both write paths need, so it lives here beside the first.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from common.identifiers import alias_refs_for_release
from common.identity import AliasRef, attach_aliases


if TYPE_CHECKING:
    import uuid
    from collections.abc import Iterable, Mapping

    from psycopg import AsyncConnection


__all__ = [
    "DISCOGS_PROVIDER",
    "ENTITY_KIND_BY_DATA_TYPE",
    "alias_ref",
    "alias_targets",
    "attach_alias_targets",
]

DISCOGS_PROVIDER: Final = "discogs"

# The promoted catalog-events contract admits exactly these four tables, so a data type
# outside the mapping is a contract violation rather than a data error and must raise.
ENTITY_KIND_BY_DATA_TYPE: Final[dict[str, str]] = {
    "artists": "artist",
    "labels": "label",
    "masters": "master",
    "releases": "release",
}

# Only releases carry an identifiers block, so only releases mint identifier aliases.
ALIAS_BEARING_DATA_TYPE: Final = "releases"


def alias_ref(data_type: str, data_id: str) -> AliasRef:
    """Return the provider alias key for one Discogs row.

    Raises:
        KeyError: If ``data_type`` is not one of the four contract entity tables.
    """
    return AliasRef(DISCOGS_PROVIDER, ENTITY_KIND_BY_DATA_TYPE[data_type], data_id)


def alias_targets(
    data_type: str,
    payloads: Iterable[tuple[dict[str, Any], uuid.UUID]],
) -> dict[AliasRef, uuid.UUID]:
    """Return the identifier aliases a set of release payloads mints, by native id.

    The ``identifiers`` block is additive within catalog-events v1, so a payload without
    one — every non-release row, and every release produced before ADR 0011 landed —
    contributes nothing rather than failing. A block that is present but carries no
    alias-bearing item likewise contributes nothing.

    Two payloads in the same batch can print the same barcode or catalogue number. The
    alias table holds one native id per ref, so the first payload in iteration order
    wins here and the collision surfaces as an attach conflict only when the alias
    already belongs to a third item.

    Args:
        data_type: The entity table the payloads belong to.
        payloads: The record payload and the native id it resolved to, in batch order.

    Returns:
        The native id each minted alias should attach to; empty for a non-release table.

    Raises:
        common.identifiers.IdentifierValidationError: If a payload carries a malformed
            identifiers block. A block that does not match the promoted contract is a
            producer defect, and the ``ValueError`` it subclasses is classified as a
            deterministic failure rather than retried as an outage.
    """
    if data_type != ALIAS_BEARING_DATA_TYPE:
        return {}

    targets: dict[AliasRef, uuid.UUID] = {}
    for data, native_id in payloads:
        block = data.get("identifiers")
        if not block:
            continue
        for ref in alias_refs_for_release(block):
            targets.setdefault(ref, native_id)
    return targets


async def attach_alias_targets(
    conn: AsyncConnection[Any],
    targets: Mapping[AliasRef, uuid.UUID],
    logger: Any,
    data_type: str,
) -> int:
    """Attach identifier aliases on the caller's transaction and report the conflicts.

    ``attach_aliases`` never overwrites an alias that is already present: it returns the
    native id the existing alias points at, which may not be the one supplied. That is a
    genuine identity collision — two releases printing one barcode — and it is counted
    and logged rather than raised, because the batch around it is otherwise correct and
    failing it would strand every other row.

    Args:
        conn: A psycopg ``AsyncConnection`` already inside the caller's transaction.
        targets: The mapping :func:`alias_targets` built.
        logger: The structured logger of the calling write path.
        data_type: The entity table, for the log line.

    Returns:
        The number of refs whose alias already pointed at a different native id.
    """
    if not targets:
        return 0

    attached = await attach_aliases(conn, targets)
    # A ref absent from the result had its alias closed between the insert and the
    # re-select; nothing is known to conflict there, so it is not counted as one.
    conflicts = sum(1 for ref, native_id in targets.items() if attached.get(ref, native_id) != native_id)

    if conflicts:
        logger.warning(
            "🏷️ Identifier aliases already pointed at a different native id",
            data_type=data_type,
            aliases=len(targets),
            alias_conflicts=conflicts,
        )
    else:
        logger.debug(
            "🏷️ Attached identifier aliases",
            data_type=data_type,
            aliases=len(targets),
            alias_conflicts=0,
        )
    return conflicts
