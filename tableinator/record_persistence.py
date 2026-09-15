from datetime import UTC, datetime
from typing import Any

from common.identity import resolve_aliases
from psycopg import sql
from psycopg.types.json import Jsonb

from tableinator.identity import alias_ref, alias_targets, attach_alias_targets


class PostgreSQLRecordPersistence:
    """Own record-specific PostgreSQL statements for the non-batch consumer."""

    def __init__(
        self,
        connection_pool: Any,
        logger: Any,
        purge_max_delete_fraction: float,
        media_resolver: Any,
    ) -> None:
        self.connection_pool = connection_pool
        self.logger = logger
        self.purge_max_delete_fraction = purge_max_delete_fraction
        self.media_resolver = media_resolver

    async def purge_stale_rows(
        self,
        data_type: str,
        started_at: str,
        record_count: int | None = None,
    ) -> None:
        """Delete rows not refreshed by the current extraction when safe."""
        if not started_at:
            self.logger.warning(
                "⚠️ No started_at in extraction_complete, skipping stale row purge",
                data_type=data_type,
            )
            return

        if record_count == 0:
            self.logger.warning(
                "⚠️ Skipping stale row purge — extractor reported 0 records this session (resumed extraction?)",
                data_type=data_type,
            )
            return

        started_at_dt = datetime.fromisoformat(started_at)
        if started_at_dt.tzinfo is None:
            started_at_dt = started_at_dt.replace(tzinfo=UTC)

        try:
            async with self.connection_pool.connection() as conn:
                await conn.set_autocommit(False)
                async with conn.transaction(), conn.cursor() as cursor:
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

                    delete_fraction = stale_count / total_count
                    if delete_fraction >= self.purge_max_delete_fraction:
                        self.logger.error(
                            f"🛡️ Refusing to purge {stale_count}/{total_count} "
                            f"{data_type} rows ({delete_fraction:.1%} of table) — exceeds "
                            f"safety cap, likely a resumed extraction not a dump shrink",
                            data_type=data_type,
                            stale=stale_count,
                            total=total_count,
                            fraction=round(delete_fraction, 4),
                        )
                        return

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
        except Exception as exc:
            self.logger.error(
                f"❌ Failed to purge stale {data_type} rows",
                data_type=data_type,
                error=str(exc),
            )
            raise

    async def persist_record(
        self,
        data_type: str,
        data_id: str,
        data: dict[str, Any],
    ) -> str:
        """Persist one normalized record and return its telemetry outcome."""
        terminal_outcome = "processed"
        async with self.connection_pool.connection() as conn, conn.cursor() as cursor:
            # Native identity (ADR 0009): resolve before the upsert, on the same connection
            # and therefore inside the same transaction, so the row is written with the
            # native item it maps to or not written at all.
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
                        "SELECT COALESCE(prior.prior_hash = %s, false) AND prior.prior_media_is_null "
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
                    ),
                )
                prior_state = await cursor.fetchone()
                if prior_state is not None and prior_state[0] is True:
                    terminal_outcome = "media_backfilled"
            else:
                await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                    sql.SQL(
                        "INSERT INTO {table} (hash, data_id, data, gm_item_id, updated_at) "
                        "VALUES (%s, %s, %s, %s, NOW()) "
                        "ON CONFLICT (data_id) DO UPDATE "
                        "SET hash = CASE WHEN {table}.hash != EXCLUDED.hash "
                        "THEN EXCLUDED.hash ELSE {table}.hash END, "
                        "data = CASE WHEN {table}.hash != EXCLUDED.hash "
                        "THEN EXCLUDED.data ELSE {table}.data END, "
                        "gm_item_id = EXCLUDED.gm_item_id, "
                        "updated_at = NOW();"
                    ).format(table=sql.Identifier(data_type)),
                    (data.get("sha256", ""), data_id, Jsonb(data), gm_item_id),
                )

            # Identifier aliases (ADR 0011): the same attach the batch path makes, one
            # record at a time, after the upsert and on this connection, so the aliases and
            # the row they point at commit together. A record whose hash did not change is
            # attached too, because the upsert above cannot report whether its aliases exist.
            await attach_alias_targets(conn, alias_targets(data_type, [(data, gm_item_id)]), self.logger, data_type)

            self.logger.debug(
                "🐘 Updated record in PostgreSQL",
                data_type=data_type[:-1],
                data_id=data_id,
            )

        return terminal_outcome
