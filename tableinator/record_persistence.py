from datetime import UTC, datetime
from typing import Any

from common.identity import resolve_aliases
from psycopg import sql
from psycopg.types.json import Jsonb

from tableinator.graph_writer import purge_document_graph, write_document_graph
from tableinator.identity import alias_ref, alias_targets, attach_alias_targets
from tableinator.seen_list import (
    DEFAULT_MIN_FRACTION,
    STALE_TABLE,
    SeenMode,
    append_seen,
    create_stale_table,
    guard_reason,
    read_shadow,
    read_status,
    restarted_since,
    set_purge_work_mem,
    start_list,
)


def _parse_started_at(started_at: str) -> datetime:
    """Return the extraction's start as an aware datetime; a naive stamp is UTC."""
    started_at_dt = datetime.fromisoformat(started_at)
    if started_at_dt.tzinfo is None:
        started_at_dt = started_at_dt.replace(tzinfo=UTC)
    return started_at_dt


class PostgreSQLRecordPersistence:
    """Own record-specific PostgreSQL statements for the non-batch consumer."""

    def __init__(
        self,
        connection_pool: Any,
        logger: Any,
        purge_max_delete_fraction: float,
        media_resolver: Any,
        seen_mode: SeenMode = SeenMode.TOUCH,
        seen_min_fraction: float = DEFAULT_MIN_FRACTION,
    ) -> None:
        self.connection_pool = connection_pool
        self.logger = logger
        self.purge_max_delete_fraction = purge_max_delete_fraction
        self.media_resolver = media_resolver
        self.seen_mode = seen_mode
        self.seen_min_fraction = seen_min_fraction

    async def purge_stale_rows(
        self,
        data_type: str,
        started_at: str,
        record_count: int | None = None,
    ) -> str | None:
        """Delete rows not refreshed by the current extraction when safe.

        Returns why a seen-mode guard skipped the purge, or None. The caller records a returned
        reason exactly as it records a dead-letter veto (`purge_vetoed` on the latch, so the
        consecutive-veto alert counts it), and then restarts the list with `start_seen_list`.
        """
        if not started_at:
            self.logger.warning(
                "⚠️ No started_at in extraction_complete, skipping stale row purge",
                data_type=data_type,
            )
            return None

        started_at_dt = _parse_started_at(started_at)

        if record_count == 0:
            self.logger.warning(
                "⚠️ Skipping stale row purge — extractor reported 0 records this session (resumed extraction?)",
                data_type=data_type,
            )
            await self.start_seen_list(data_type, started_at)
            return None

        try:
            async with self.connection_pool.connection() as conn:
                await conn.set_autocommit(False)
                async with conn.transaction(), conn.cursor() as cursor:
                    if self.seen_mode is SeenMode.SEEN:
                        return await self._seen_purge(cursor, data_type, started_at_dt, record_count)
                    if self.seen_mode is SeenMode.SHADOW:
                        await set_purge_work_mem(cursor)
                        await self._log_shadow(conn, cursor, data_type, started_at_dt, record_count)
                    await self._touch_purge(cursor, data_type, started_at_dt)
                    if self.seen_mode is SeenMode.SHADOW:
                        # The shadow reload proves the list; it must never hold up the purge
                        # that is still the real one, so a failed restart is only logged. The
                        # list then stays intact for the next extraction, only larger.
                        try:
                            async with conn.transaction():
                                await start_list(cursor, data_type, started_at_dt, self.logger)
                        except Exception as error:
                            self.logger.error("❌ Could not restart the seen list", data_type=data_type, error_type=type(error).__name__)
        except Exception as exc:
            self.logger.error(
                f"❌ Failed to purge stale {data_type} rows",
                data_type=data_type,
                error=str(exc),
            )
            raise
        return None

    async def _touch_purge(self, cursor: Any, data_type: str, started_at_dt: datetime) -> None:
        """Delete the rows whose `updated_at` predates the extraction, under the delete cap."""
        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            sql.SQL("SELECT count(*) FROM {table}").format(table=sql.Identifier(data_type))
        )
        total_row = await cursor.fetchone()
        total_count = total_row[0] if total_row else 0

        if total_count == 0:
            self.logger.info(
                f"✅ No {data_type} rows to purge (table empty)",
                data_type=data_type,
            )
            return

        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            sql.SQL("SELECT count(*) FROM {table} WHERE updated_at < %s").format(table=sql.Identifier(data_type)),
            (started_at_dt,),
        )
        stale_row = await cursor.fetchone()
        stale_count = stale_row[0] if stale_row else 0

        if stale_count == 0:
            self.logger.info(
                f"✅ No stale {data_type} rows to purge",
                data_type=data_type,
            )
            return

        if self._refuse(data_type, stale_count, total_count):
            return

        # The edges of the documents this DELETE is about to remove, swept
        # first so the join that names them still has rows to join against.
        # Set-based and server-side: no deleted id is streamed back, which is
        # what keeps a large purge O(1) in the client.
        await purge_document_graph(cursor, data_type, started_at_dt)

        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            sql.SQL("DELETE FROM {table} WHERE updated_at < %s").format(table=sql.Identifier(data_type)),
            (started_at_dt,),
        )
        deleted_count = cursor.rowcount

        self.logger.info(
            f"🧹 Purged {deleted_count} stale {data_type} rows (not updated since extraction started)",
            data_type=data_type,
            deleted=deleted_count,
        )

    def _refuse(self, data_type: str, stale_count: int, total_count: int) -> bool:
        """Return whether PURGE_MAX_DELETE_FRACTION refuses this purge, logging the refusal."""
        delete_fraction = stale_count / total_count
        if delete_fraction < self.purge_max_delete_fraction:
            return False
        self.logger.error(
            f"🛡️ Refusing to purge {stale_count}/{total_count} "
            f"{data_type} rows ({delete_fraction:.1%} of table) — exceeds "
            f"safety cap, likely a resumed extraction not a dump shrink",
            data_type=data_type,
            stale=stale_count,
            total=total_count,
            fraction=round(delete_fraction, 4),
        )
        return True

    async def _log_shadow(self, conn: Any, cursor: Any, data_type: str, started_at_dt: datetime, record_count: int | None) -> None:
        """Log how the seen purge would differ from the touch purge about to run. Never deletes.

        In a savepoint, so a failure is logged and the touch purge still runs.
        """
        try:
            async with conn.transaction():
                status = await read_status(cursor, data_type, started_at_dt)
                shadow = await read_shadow(cursor, data_type, started_at_dt)
        except Exception as error:
            self.logger.error("❌ Could not compare the seen purge with the touch purge", data_type=data_type, error_type=type(error).__name__)
            return
        fields = {
            "data_type": data_type,
            "started_at": started_at_dt.isoformat(),
            "matches": shadow.matches,
            "intact": shadow.intact,
            "total_rows": shadow.total_rows,
            "touch_purge": shadow.touch_purge,
            "seen_purge": shadow.seen_purge,
            "refreshed_not_seen": shadow.refreshed_not_seen,
            "seen_not_refreshed": shadow.seen_not_refreshed,
            "refreshed_not_seen_sample": shadow.refreshed_not_seen_sample,
            "seen_not_refreshed_sample": shadow.seen_not_refreshed_sample,
            "seen_keys": status.seen_keys,
            "record_count": record_count,
            # What SEEN_MIN_FRACTION is calibrated from.
            "seen_ratio": round(status.seen_keys / record_count, 6) if record_count else None,
            "guard": guard_reason(status, record_count, self.seen_min_fraction),
        }
        if shadow.matches:
            self.logger.info(f"👁️ discogs_{data_type}_shadow: the seen purge matches the touch purge", **fields)
        else:
            self.logger.warning(f"⚠️ discogs_{data_type}_shadow: the seen purge differs from the touch purge", **fields)

    async def _seen_purge(self, cursor: Any, data_type: str, started_at_dt: datetime, record_count: int | None) -> str | None:
        """Purge the rows older than the extraction and absent from its seen list, then restart it.

        One transaction: the guards, the stale set, the graph and entity deletes by id, and
        `loader_seen.start`, so a failure anywhere leaves the rows, the list and its start time
        as they were.
        """
        await set_purge_work_mem(cursor)
        status = await read_status(cursor, data_type, started_at_dt)
        reason = guard_reason(status, record_count, self.seen_min_fraction)
        if reason is not None:
            if not status.intact and restarted_since(status.opened_at, started_at_dt) and status.seen_keys == 0:
                # An earlier delivery of this signal purged and restarted the list.
                self.logger.info(
                    f"✅ The {data_type} seen list was already settled for this extraction — nothing to purge",
                    data_type=data_type,
                )
                return None
            self.logger.warning(
                "⚠️ Skipping stale row purge — the seen list cannot be trusted",
                data_type=data_type,
                reason=reason,
                intact=status.intact,
                seen_keys=status.seen_keys,
                record_count=record_count,
            )
            return reason

        await create_stale_table(cursor, data_type, started_at_dt)
        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            sql.SQL("SELECT (SELECT count(*) FROM {table}), (SELECT count(*) FROM {stale})").format(
                table=sql.Identifier(data_type), stale=sql.Identifier(STALE_TABLE)
            )
        )
        counts = await cursor.fetchone()
        total_count, stale_count = (int(counts[0]), int(counts[1])) if counts else (0, 0)

        if stale_count == 0:
            self.logger.info(f"✅ No stale {data_type} rows to purge", data_type=data_type, seen_keys=status.seen_keys)
        elif not self._refuse(data_type, stale_count, total_count):
            await purge_document_graph(cursor, data_type, started_at_dt, stale_table=STALE_TABLE)
            await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                sql.SQL("DELETE FROM {table} AS entity USING {stale} AS stale WHERE entity.data_id = stale.data_id").format(
                    table=sql.Identifier(data_type), stale=sql.Identifier(STALE_TABLE)
                )
            )
            deleted_count = cursor.rowcount
            self.logger.info(
                f"🧹 Purged {deleted_count} stale {data_type} rows (not seen by this extraction)",
                data_type=data_type,
                deleted=deleted_count,
                seen_keys=status.seen_keys,
            )
        await start_list(cursor, data_type, started_at_dt, self.logger)
        return None

    async def start_seen_list(self, data_type: str, started_at: str) -> None:
        """Restart the type's seen list after the extraction settled without purging.

        Its own transaction. A failure is only logged: the list then stays intact for the next
        extraction, only larger, because the purge counts only keys appended since `started_at`.
        """
        if not self.seen_mode.appends or not started_at:
            return
        try:
            async with self.connection_pool.connection() as conn:
                await conn.set_autocommit(False)
                async with conn.transaction(), conn.cursor() as cursor:
                    await start_list(cursor, data_type, _parse_started_at(started_at), self.logger)
        except Exception as error:
            self.logger.error("❌ Could not restart the seen list", data_type=data_type, error_type=type(error).__name__)

    async def persist_record(
        self,
        data_type: str,
        data_id: str,
        data: dict[str, Any],
    ) -> str:
        """Persist one normalized record and return its telemetry outcome."""
        terminal_outcome = "processed"
        # The content-hash gate, answered by the upsert itself. Both statements below read
        # the row's prior hash in a `prior` CTE — evaluated on the pre-statement snapshot,
        # so it sees the hash the upsert is about to overwrite — and return whether this
        # event's payload differs from it. A separate SELECT would be a second round trip
        # for an answer the one statement already has, and an absent `prior` row is a first
        # sight of the document, which changes everything there is to change.
        content_changed = True
        # One transaction for the document, its aliases, and its graph rows, exactly as
        # `purge_stale_rows` above and `PostgreSQLBatchWriter.process_batch` open one. The
        # pool hands out an AUTOCOMMIT connection and resets it on return, so without this
        # every statement below would commit on its own — and the document-scoped DELETE
        # would land ahead of the edge INSERTs. A failure in between would leave the entity
        # row and its NEW content hash durable with the edges gone, and the next delivery
        # of the same event would read that hash, find it unchanged, and skip the
        # re-derivation that is the only thing that would have put them back.
        async with self.connection_pool.connection() as conn:
            await conn.set_autocommit(False)
            async with conn.transaction(), conn.cursor() as cursor:
                # Native identity (ADR 0009): resolve before the upsert, on the same
                # connection and therefore inside this transaction, so the row is written
                # with the native item it maps to or not written at all.
                ref = alias_ref(data_type, data_id)
                native_ids = await resolve_aliases(conn, [ref])
                if ref not in native_ids:
                    # Every Discogs entity kind is a catalog kind, so a miss is a broken
                    # assumption rather than a retryable outage.
                    raise RuntimeError(f"resolve_aliases returned no native id for {data_type}: {data_id}")
                gm_item_id = native_ids[ref]

                if data_type == "releases":
                    await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                        sql.SQL(
                            "WITH prior AS ("
                            "SELECT hash AS prior_hash, media IS NULL AS prior_media_is_null "
                            "FROM {table} WHERE data_id = %s"
                            "), upserted AS ("
                            "INSERT INTO {table} (hash, data_id, data, media, gm_item_id, updated_at) "
                            "VALUES (%s, %s, %s, %s, %s, NOW()) "
                            "ON CONFLICT (data_id) DO UPDATE "
                            "SET hash = CASE WHEN {table}.hash != EXCLUDED.hash "
                            "THEN EXCLUDED.hash ELSE {table}.hash END, "
                            "data = CASE WHEN {table}.hash != EXCLUDED.hash "
                            "THEN EXCLUDED.data ELSE {table}.data END, "
                            "media = CASE WHEN {table}.hash != EXCLUDED.hash OR {table}.media IS NULL "
                            "THEN EXCLUDED.media ELSE {table}.media END, "
                            "gm_item_id = EXCLUDED.gm_item_id, "
                            "updated_at = NOW() "
                            "RETURNING 1"
                            ") "
                            "SELECT COALESCE(prior.prior_hash = %s, false) AND prior.prior_media_is_null, "
                            "prior.prior_hash IS DISTINCT FROM %s "
                            "FROM prior;"
                        ).format(table=sql.Identifier(data_type)),
                        (
                            data_id,
                            data.get("sha256", ""),
                            data_id,
                            Jsonb(data),
                            Jsonb(self.media_resolver(data)),
                            gm_item_id,
                            data.get("sha256", ""),
                            data.get("sha256", ""),
                        ),
                    )
                    prior_state = await cursor.fetchone()
                    if prior_state is None:
                        content_changed = True
                    else:
                        if prior_state[0] is True:
                            terminal_outcome = "media_backfilled"
                        content_changed = bool(prior_state[1])
                else:
                    await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                        sql.SQL(
                            "WITH prior AS ("
                            "SELECT hash AS prior_hash FROM {table} WHERE data_id = %s"
                            "), upserted AS ("
                            "INSERT INTO {table} (hash, data_id, data, gm_item_id, updated_at) "
                            "VALUES (%s, %s, %s, %s, NOW()) "
                            "ON CONFLICT (data_id) DO UPDATE "
                            "SET hash = CASE WHEN {table}.hash != EXCLUDED.hash "
                            "THEN EXCLUDED.hash ELSE {table}.hash END, "
                            "data = CASE WHEN {table}.hash != EXCLUDED.hash "
                            "THEN EXCLUDED.data ELSE {table}.data END, "
                            "gm_item_id = EXCLUDED.gm_item_id, "
                            "updated_at = NOW() "
                            "RETURNING 1"
                            ") "
                            "SELECT prior.prior_hash IS DISTINCT FROM %s FROM prior;"
                        ).format(table=sql.Identifier(data_type)),
                        (
                            data_id,
                            data.get("sha256", ""),
                            data_id,
                            Jsonb(data),
                            gm_item_id,
                            data.get("sha256", ""),
                        ),
                    )
                    prior_row = await cursor.fetchone()
                    content_changed = prior_row is None or bool(prior_row[0])

                # Identifier aliases (ADR 0011): the same attach the batch path makes, one
                # record at a time, after the upsert and on this connection, so the aliases and
                # the row they point at commit together. A record whose hash did not change is
                # attached too, because the upsert above cannot report whether its aliases exist.
                await attach_alias_targets(conn, alias_targets(data_type, [(data, gm_item_id)]), self.logger, data_type)

                # The graph vertices and edges this document asserts, on this same connection
                # and therefore this same transaction. A document whose hash did not change
                # asserts nothing new, exactly as the enricher's own hash gate returns early
                # from `process_artist` and its three siblings.
                if content_changed:
                    await write_document_graph(cursor, data_type, [(data_id, data)])

                # The reload seen list, in this same transaction: the purge reads it to know
                # this document is still upstream (tableinator.seen_list).
                if self.seen_mode.appends:
                    await append_seen(cursor, data_type, [data_id])

                self.logger.debug(
                    "🐘 Updated record in PostgreSQL",
                    data_type=data_type[:-1],
                    data_id=data_id,
                )

        return terminal_outcome
