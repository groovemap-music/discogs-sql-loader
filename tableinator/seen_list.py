"""Reload seen lists: record that a reload saw a row without rewriting it.

The hash gate finds most rows of a monthly reload unchanged, and until now still ran
`UPDATE ... SET updated_at = NOW()` on each of them, because the stale-row purge deletes the rows
whose `updated_at` predates the extraction. That touch is about 230 µs and 7.8 KB of WAL a release.
`database-schema` (`reload_seen_lists`, gm-database-schema-uw6d) declares a narrow, unlogged,
index-free list per Discogs table in the `loader_seen` schema. The loader appends every id it
processes to it, in the write's own transaction, and the purge then deletes a row only when it is
older than the extraction AND absent from the list. See docs/database-schema.md, "Reload seen
lists", and database-schema's docs/architecture.md section of the same name.

Three modes, chosen by ``SEEN_LIST_MODE``:

- ``touch``: today's behaviour. No list is read or written.
- ``shadow``: append, keep the touch and the `updated_at` purge, and log
  `loader_seen.discogs_<type>_shadow` at every purge, so one real reload proves the two purge sets
  are equal before anything relies on the list.
- ``seen``: append, stop touching unchanged rows, and purge from
  `loader_seen.discogs_<type>_stale` behind the intact and count guards. Needs operator approval
  after a matching shadow reload.

This service never creates database objects. The startup probe reads the catalog, and if any
part of `loader_seen` is missing the loader runs in ``touch`` mode whatever was configured, so this
code can ship before the schema reaches a database.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from psycopg import sql


if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime


LOADER = "discogs"
SCHEMA = "loader_seen"
REGISTRY = "lists"
DATA_TYPES = ("artists", "labels", "masters", "releases")

# The purge's anti-join and distinct count hash a whole list. At lux's 64 MB the 5M-key anti-join
# spilled 198 MB to temp; at 256 MB it stayed in memory (database-schema's audit,
# docs/audits/2026-11-08-reload-seen-list). SET LOCAL, so it ends with the purge's transaction.
PURGE_WORK_MEM = "256MB"

# Ids sampled into each shadow difference, the function's own default.
SHADOW_SAMPLE = 20


class SeenMode(StrEnum):
    """How the loader proves a row was seen: by rewriting it, both ways, or by the list alone."""

    TOUCH = "touch"
    SHADOW = "shadow"
    SEEN = "seen"

    @property
    def appends(self) -> bool:
        """Whether every processed id is appended to its list."""
        return self is not SeenMode.TOUCH

    @property
    def touches(self) -> bool:
        """Whether hash-unchanged rows still get `updated_at = NOW()`."""
        return self is not SeenMode.SEEN


# Shadow until the switch: the probe already turns this into touch while the schema is absent.
DEFAULT_MODE = SeenMode.SHADOW
# The seen count must be at least this share of the extractor's record count for the type, or the
# purge is skipped. The registry catches a crash; this catches systematic loss, such as a write
# path that never appends. The first value is the design's; calibrate it from the shadow logs.
DEFAULT_MIN_FRACTION = 0.95


def configured_mode(raw: str | None = None) -> SeenMode:
    """Return the mode ``SEEN_LIST_MODE`` asks for. An unknown value is an error, never a fallback."""
    value = os.environ.get("SEEN_LIST_MODE") if raw is None else raw
    if value is None or not value.strip():
        return DEFAULT_MODE
    try:
        return SeenMode(value.strip().lower())
    except ValueError:
        choices = ", ".join(mode.value for mode in SeenMode)
        raise ValueError(f"SEEN_LIST_MODE must be one of {choices}, got {value!r}") from None


def configured_min_fraction(raw: str | None = None) -> float:
    """Return ``SEEN_MIN_FRACTION``, which must lie in (0, 1]."""
    value = os.environ.get("SEEN_MIN_FRACTION") if raw is None else raw
    if value is None or not value.strip():
        return DEFAULT_MIN_FRACTION
    try:
        fraction = float(value)
    except ValueError:
        fraction = 0.0
    if not 0.0 < fraction <= 1.0:
        raise ValueError(f"SEEN_MIN_FRACTION must be a number in (0, 1], got {value!r}")
    return fraction


def list_name(data_type: str) -> str:
    """Return the list reconciling one Discogs table, unqualified."""
    if data_type not in DATA_TYPES:
        raise ValueError(f"no seen list for data type {data_type!r}")
    return f"{LOADER}_{data_type}"


def _relation(data_type: str) -> sql.Identifier:
    return sql.Identifier(SCHEMA, list_name(data_type))


def _function(name: str) -> sql.Identifier:
    return sql.Identifier(SCHEMA, name)


# Everything the loader calls or appends to, as `to_regclass` / `to_regprocedure` spell it.
REQUIRED_RELATIONS: tuple[str, ...] = (f"{SCHEMA}.{REGISTRY}", *(f"{SCHEMA}.{LOADER}_{data_type}" for data_type in DATA_TYPES))
REQUIRED_FUNCTIONS: tuple[str, ...] = (
    f"{SCHEMA}.start(text, text)",
    f"{SCHEMA}.intact(text, text, timestamp with time zone)",
    f"{SCHEMA}.status(text, text, timestamp with time zone)",
    *(f"{SCHEMA}.{LOADER}_{data_type}_stale(timestamp with time zone)" for data_type in DATA_TYPES),
    *(f"{SCHEMA}.{LOADER}_{data_type}_shadow(timestamp with time zone, integer)" for data_type in DATA_TYPES),
)

_PROBE = sql.SQL(
    "SELECT name FROM unnest(%s::text[]) AS name WHERE pg_catalog.to_regclass(name) IS NULL "
    "UNION ALL "
    "SELECT name FROM unnest(%s::text[]) AS name WHERE pg_catalog.to_regprocedure(name) IS NULL"
)


async def probe_seen_lists(connection_pool: Any, logger: Any) -> bool:
    """Return whether every `loader_seen` object the loader uses is declared. Read-only.

    A miss is the expected state until database-schema's `reload_seen_lists` is deployed, so it is
    a warning, and the caller runs in touch mode rather than creating anything.
    """
    try:
        async with connection_pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(_PROBE, (list(REQUIRED_RELATIONS), list(REQUIRED_FUNCTIONS)))
            missing = [str(row[0]) for row in await cursor.fetchall()]
    except Exception as error:
        logger.warning("⚠️ Could not probe the reload seen lists — running in touch mode", error_type=type(error).__name__)
        return False
    if missing:
        logger.warning(
            "⚠️ The reload seen lists are not declared — running in touch mode. Apply a database-schema "
            "revision with reload_seen_lists; this service never creates database objects.",
            missing=missing,
        )
        return False
    return True


def effective_mode(requested: SeenMode, schema_present: bool, logger: Any) -> SeenMode:
    """Return the mode to run in: the requested one, or touch while the schema is absent."""
    if requested is SeenMode.TOUCH or schema_present:
        logger.info("👁️ Reload seen-list mode", mode=requested.value, requested=requested.value)
        return requested
    logger.warning("⚠️ Reload seen lists unavailable — falling back to touch mode", requested=requested.value, mode=SeenMode.TOUCH.value)
    return SeenMode.TOUCH


async def append_seen(cursor: Any, data_type: str, data_ids: Sequence[str]) -> None:
    """Append DATA_IDS to the type's list, on the caller's open write transaction.

    Duplicates are harmless: the purge's anti-join and the count's DISTINCT ignore them, so a
    redelivered batch needs no special case. An unlogged heap insert with no index or key, so
    concurrent batches never conflict.
    """
    if not data_ids:
        return
    await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        sql.SQL("INSERT INTO {list} (seen_key) SELECT unnest(%s::text[])").format(list=_relation(data_type)),
        (list(data_ids),),
    )


@dataclass(frozen=True)
class SeenStatus:
    """`loader_seen.status` for one list and extraction."""

    opened_at: datetime | None
    intact: bool
    seen_keys: int


async def read_status(cursor: Any, data_type: str, started_at: datetime) -> SeenStatus:
    """Return when the list started, whether it is intact for STARTED_AT, and its distinct keys."""
    await cursor.execute(
        sql.SQL("SELECT opened_at, intact, seen_keys FROM {status}(%s, %s, %s)").format(status=_function("status")),
        (LOADER, data_type, started_at),
    )
    row = await cursor.fetchone()
    if row is None:
        return SeenStatus(None, False, 0)
    return SeenStatus(row[0], bool(row[1]), int(row[2] or 0))


async def read_opened_at(cursor: Any, data_type: str) -> datetime | None:
    """Return when the list was last started, or None when its registry row is gone (a crash)."""
    await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        sql.SQL("SELECT opened_at FROM {registry} WHERE loader = %s AND data_type = %s").format(registry=sql.Identifier(SCHEMA, REGISTRY)),
        (LOADER, data_type),
    )
    row = await cursor.fetchone()
    return row[0] if row else None


def restarted_since(opened_at: datetime | None, started_at: datetime) -> bool:
    """Whether the list was started after this extraction began.

    Such a list was either restarted by an earlier delivery of this same terminal signal, which
    already settled the extraction, or it is already collecting a later extraction's keys. Starting
    it again would gain nothing in the first case and empty the next extraction's list in the
    second, so it is left alone. A list with no registry row (emptied by a crash) is started.
    """
    return opened_at is not None and opened_at > started_at


async def start_list(cursor: Any, data_type: str, started_at: datetime, logger: Any) -> bool:
    """Restart the type's list for the next extraction, on the caller's transaction.

    `loader_seen.start` is the only way a list is emptied: it truncates the list and stamps the
    registry in this transaction, so a purge that rolls back leaves both as they were. Returns
    whether the list was started.
    """
    opened_at = await read_opened_at(cursor, data_type)
    if restarted_since(opened_at, started_at):
        logger.info(
            "👁️ Seen list already restarted since this extraction began — leaving it",
            data_type=data_type,
            opened_at=opened_at.isoformat() if opened_at else None,
        )
        return False
    await cursor.execute(sql.SQL("SELECT {start}(%s, %s)").format(start=_function("start")), (LOADER, data_type))
    logger.info("👁️ Restarted the seen list for the next extraction", data_type=data_type)
    return True


@dataclass(frozen=True)
class ShadowComparison:
    """One row of `loader_seen.discogs_<type>_shadow`."""

    intact: bool
    total_rows: int
    touch_purge: int
    seen_purge: int
    refreshed_not_seen: int
    seen_not_refreshed: int
    refreshed_not_seen_sample: list[str]
    seen_not_refreshed_sample: list[str]

    @property
    def matches(self) -> bool:
        """Whether this reload supports the switch: intact, nothing live lost, equal purge sets."""
        return self.intact and self.refreshed_not_seen == 0 and self.touch_purge == self.seen_purge


async def read_shadow(cursor: Any, data_type: str, started_at: datetime) -> ShadowComparison:
    """Return the shadow comparison of the touch purge and the seen purge for STARTED_AT."""
    await cursor.execute(
        sql.SQL(
            "SELECT intact, total_rows, touch_purge, seen_purge, refreshed_not_seen, seen_not_refreshed, "
            "refreshed_not_seen_sample, seen_not_refreshed_sample FROM {shadow}(%s, %s)"
        ).format(shadow=_function(f"{list_name(data_type)}_shadow")),
        (started_at, SHADOW_SAMPLE),
    )
    row = await cursor.fetchone()
    if row is None:
        raise RuntimeError(f"{SCHEMA}.{list_name(data_type)}_shadow returned no row")
    return ShadowComparison(
        bool(row[0]),
        int(row[1]),
        int(row[2]),
        int(row[3]),
        int(row[4]),
        int(row[5]),
        list(row[6] or []),
        list(row[7] or []),
    )


# The purge set, computed once per purge. Named apart from the `stale` alias the graph purge gives
# the entity table, and dropped with the purge's transaction.
STALE_TABLE = "seen_stale"


async def create_stale_table(cursor: Any, data_type: str, started_at: datetime) -> None:
    """Materialize `loader_seen.discogs_<type>_stale(STARTED_AT)` into `seen_stale`.

    The function refuses a list that is not intact with SQLSTATE 55000, so even a caller that
    skipped its own intact check cannot purge from an emptied list.
    """
    await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        sql.SQL("CREATE TEMP TABLE {temp} ON COMMIT DROP AS SELECT data_id FROM {stale}(%s)").format(
            temp=sql.Identifier(STALE_TABLE),
            stale=_function(f"{list_name(data_type)}_stale"),
        ),
        (started_at,),
    )
    # A temporary table has no statistics until analyzed, and the deletes below join it.
    await cursor.execute(sql.SQL("ANALYZE {temp}").format(temp=sql.Identifier(STALE_TABLE)))


async def set_purge_work_mem(cursor: Any) -> None:
    """Give this purge transaction's list hashes room to stay in memory."""
    await cursor.execute(sql.SQL("SET LOCAL work_mem = {value}").format(value=sql.Literal(PURGE_WORK_MEM)))


def guard_reason(status: SeenStatus, record_count: int | None, min_fraction: float) -> str | None:
    """Return why a seen-mode purge must be skipped, or None when the list can be trusted.

    Not intact: the list was emptied by a crash, or started after the extraction began, so a row
    missing from it may still be live. Too few keys: something appended less than the extractor
    sent. No producer count: nothing to hold the list against, so it is not trusted either.
    """
    if not status.intact:
        return "the seen list is not intact for this extraction (lost in a crash, or started after it began)"
    if record_count is None:
        return "the extractor reported no record count for this type, so the seen list cannot be checked"
    required = min_fraction * record_count
    if status.seen_keys < required:
        return f"the seen list holds {status.seen_keys} ids, below {min_fraction:g} of the {record_count} records extracted"
    return None
