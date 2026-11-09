"""Reload seen lists without a database: modes, the probe, the guard, and each write path's SQL.

The real-engine behaviour (shadow equality, a crash, a partial list, WAL) is in
tests/integration/test_seen_lists.py. These pin the decisions the code makes on the way there.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika.abc import AbstractIncomingMessage
from common import WriteAdmission

import tableinator.tableinator as service
from tableinator.batch_writer import PostgreSQLBatchWriter
from tableinator.extraction_latch import ExtractionLatch, LatchRelation
from tableinator.record_persistence import PostgreSQLRecordPersistence
from tableinator.seen_list import (
    DEFAULT_MIN_FRACTION,
    DEFAULT_MODE,
    REQUIRED_FUNCTIONS,
    REQUIRED_RELATIONS,
    SeenMode,
    SeenStatus,
    ShadowComparison,
    append_seen,
    configured_min_fraction,
    configured_mode,
    effective_mode,
    guard_reason,
    list_name,
    probe_seen_lists,
    restarted_since,
)


if TYPE_CHECKING:
    from collections.abc import Sequence


STARTED_AT = "2026-11-01T00:00:00+00:00"
STARTED = datetime.fromisoformat(STARTED_AT)


def _text(statement: Any) -> str:
    return statement.as_string() if hasattr(statement, "as_string") else str(statement)


def _statements(cursor: Any) -> list[str]:
    return [_text(call.args[0]) for call in cursor.execute.await_args_list]


class TestConfiguration:
    def test_the_default_mode_is_shadow(self) -> None:
        assert configured_mode("") == DEFAULT_MODE == SeenMode.SHADOW

    def test_the_environment_names_the_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SEEN_LIST_MODE", " Seen ")
        assert configured_mode() is SeenMode.SEEN
        monkeypatch.delenv("SEEN_LIST_MODE")
        assert configured_mode() is SeenMode.SHADOW

    def test_an_unknown_mode_is_an_error(self) -> None:
        with pytest.raises(ValueError, match="SEEN_LIST_MODE must be one of touch, shadow, seen"):
            configured_mode("delete-everything")

    def test_the_min_fraction_defaults_and_parses(self, monkeypatch: pytest.MonkeyPatch) -> None:
        assert configured_min_fraction("") == DEFAULT_MIN_FRACTION == 0.95
        monkeypatch.setenv("SEEN_MIN_FRACTION", "0.9")
        assert configured_min_fraction() == 0.9

    @pytest.mark.parametrize("raw", ["0", "1.5", "-0.1", "most"])
    def test_a_min_fraction_outside_0_1_is_an_error(self, raw: str) -> None:
        with pytest.raises(ValueError, match="SEEN_MIN_FRACTION"):
            configured_min_fraction(raw)

    def test_modes_say_what_they_write(self) -> None:
        assert (SeenMode.TOUCH.touches, SeenMode.TOUCH.appends) == (True, False)
        assert (SeenMode.SHADOW.touches, SeenMode.SHADOW.appends) == (True, True)
        assert (SeenMode.SEEN.touches, SeenMode.SEEN.appends) == (False, True)

    def test_only_the_four_discogs_tables_have_a_list(self) -> None:
        assert list_name("releases") == "discogs_releases"
        with pytest.raises(ValueError, match="no seen list"):
            list_name("relationships")


class TestProbe:
    @pytest.mark.asyncio
    async def test_every_object_present_enables_the_lists(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchall.return_value = []
        assert await probe_seen_lists(mock_async_pool(mock_postgres_connection), MagicMock()) is True
        relations, functions = cursor.execute.await_args.args[1]
        assert relations == list(REQUIRED_RELATIONS)
        assert functions == list(REQUIRED_FUNCTIONS)
        assert "loader_seen.discogs_releases_stale(timestamp with time zone)" in functions

    @pytest.mark.asyncio
    async def test_a_missing_object_falls_back_with_a_warning(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        mock_postgres_connection.cursor.return_value.fetchall.return_value = [("loader_seen.lists",)]
        logger = MagicMock()
        assert await probe_seen_lists(mock_async_pool(mock_postgres_connection), logger) is False
        assert logger.warning.call_args.kwargs["missing"] == ["loader_seen.lists"]

    @pytest.mark.asyncio
    async def test_a_failed_probe_falls_back(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        mock_postgres_connection.cursor.return_value.execute.side_effect = RuntimeError("down")
        assert await probe_seen_lists(mock_async_pool(mock_postgres_connection), MagicMock()) is False

    def test_the_effective_mode_is_touch_without_the_schema(self) -> None:
        logger = MagicMock()
        assert effective_mode(SeenMode.SEEN, False, logger) is SeenMode.TOUCH
        assert effective_mode(SeenMode.SHADOW, False, logger) is SeenMode.TOUCH
        assert effective_mode(SeenMode.SHADOW, True, logger) is SeenMode.SHADOW
        assert effective_mode(SeenMode.TOUCH, False, logger) is SeenMode.TOUCH


class TestGuard:
    def test_a_list_not_intact_is_never_trusted(self) -> None:
        reason = guard_reason(SeenStatus(None, False, 10_000), 100, 0.95)
        assert reason is not None
        assert "not intact" in reason

    def test_a_list_below_the_fraction_is_not_trusted(self) -> None:
        reason = guard_reason(SeenStatus(STARTED, True, 94), 100, 0.95)
        assert reason == "the seen list holds 94 ids, below 0.95 of the 100 records extracted"

    def test_without_a_producer_count_the_list_is_not_trusted(self) -> None:
        reason = guard_reason(SeenStatus(STARTED, True, 94), None, 0.95)
        assert reason is not None
        assert "no record count" in reason

    def test_an_intact_full_list_is_trusted(self) -> None:
        assert guard_reason(SeenStatus(STARTED, True, 95), 100, 0.95) is None

    def test_a_list_started_after_the_extraction_began_is_restarted_already(self) -> None:
        assert restarted_since(STARTED + timedelta(seconds=1), STARTED)
        assert not restarted_since(STARTED - timedelta(days=30), STARTED)
        assert not restarted_since(None, STARTED)

    def test_a_shadow_matches_only_when_intact_equal_and_nothing_live_lost(self) -> None:
        assert ShadowComparison(True, 10, 2, 2, 0, 0, [], []).matches
        assert not ShadowComparison(False, 10, 2, 2, 0, 0, [], []).matches
        assert not ShadowComparison(True, 10, 2, 3, 1, 0, ["1"], []).matches
        assert not ShadowComparison(True, 10, 3, 2, 0, 1, [], ["1"]).matches


class TestAppend:
    @pytest.mark.asyncio
    async def test_nothing_to_append_runs_nothing(self) -> None:
        cursor = AsyncMock()
        await append_seen(cursor, "labels", [])
        cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ids_are_appended_to_the_types_list(self) -> None:
        cursor = AsyncMock()
        await append_seen(cursor, "labels", ("1", "2"))
        statement, params = cursor.execute.await_args.args
        assert _text(statement) == 'INSERT INTO "loader_seen"."discogs_labels" (seen_key) SELECT unnest(%s::text[])'
        assert params == (["1", "2"],)


class Record:
    def __init__(self, data_id: str, sha256: str) -> None:
        self.data_id = data_id
        self.sha256 = sha256
        self.data = {"id": data_id, "sha256": sha256, "name": data_id}


async def _batch(mode: SeenMode, mock_async_pool: Any, connection: Any, existing: Sequence[tuple[Any, ...]], records: Sequence[Record]) -> list[str]:
    cursor = connection.cursor.return_value
    cursor.fetchall.return_value = list(existing)
    writer = PostgreSQLBatchWriter(mock_async_pool(connection), MagicMock(), lambda _data: {}, WriteAdmission(1), mode)
    with (
        patch("tableinator.batch_writer.resolve_aliases", new=AsyncMock(side_effect=lambda _conn, refs: dict.fromkeys(refs, "native"))),
        patch("tableinator.batch_writer.attach_alias_targets", new=AsyncMock()),
        patch("tableinator.batch_writer.write_document_graph", new=AsyncMock()),
    ):
        await writer.process_batch("labels", records)
    return _statements(cursor)


class TestBatchWriter:
    # "1" unchanged, "2" unchanged with no gm_item_id (backfilled), "3" changed, "4" new.
    EXISTING = (("1", "h1", False), ("2", "h2", True), ("3", "old", False))
    RECORDS = (Record("1", "h1"), Record("2", "h2"), Record("3", "h3"), Record("4", "h4"))

    @pytest.mark.asyncio
    async def test_touch_mode_refreshes_and_appends_nothing(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        statements = await _batch(SeenMode.TOUCH, mock_async_pool, mock_postgres_connection, self.EXISTING, self.RECORDS)
        assert any("SET updated_at = NOW() WHERE data_id = ANY" in s for s in statements)
        assert not any("loader_seen" in s for s in statements)

    @pytest.mark.asyncio
    async def test_shadow_mode_refreshes_and_appends_every_id(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        statements = await _batch(SeenMode.SHADOW, mock_async_pool, mock_postgres_connection, self.EXISTING, self.RECORDS)
        assert any("SET updated_at = NOW() WHERE data_id = ANY" in s for s in statements)
        appends = [call for call in mock_postgres_connection.cursor.return_value.execute.await_args_list if "loader_seen" in _text(call.args[0])]
        assert len(appends) == 1
        assert appends[0].args[1] == (["1", "2", "3", "4"],)

    @pytest.mark.asyncio
    async def test_seen_mode_appends_without_refreshing_but_still_backfills(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        statements = await _batch(SeenMode.SEEN, mock_async_pool, mock_postgres_connection, self.EXISTING, self.RECORDS)
        assert not any("SET updated_at = NOW() WHERE data_id = ANY" in s for s in statements)
        assert any('SET "gm_item_id" = incoming.value' in s for s in statements)
        assert any("ON CONFLICT (data_id) DO UPDATE" in s for s in statements)
        appends = [call for call in mock_postgres_connection.cursor.return_value.execute.await_args_list if "loader_seen" in _text(call.args[0])]
        assert appends[0].args[1] == (["1", "2", "3", "4"],)
        # The append is the batch's last statement, inside its one transaction.
        assert "loader_seen" in statements[-1]
        mock_postgres_connection.transaction.assert_called_once()


def _persistence(mock_async_pool: Any, connection: Any, mode: SeenMode, logger: Any = None) -> PostgreSQLRecordPersistence:
    return PostgreSQLRecordPersistence(mock_async_pool(connection), logger or MagicMock(), 0.9, lambda _data: {}, mode, 0.95)


class TestSeenPurge:
    @pytest.mark.asyncio
    async def test_an_intact_full_list_purges_by_id_and_restarts_in_one_transaction(
        self, mock_async_pool: Any, mock_postgres_connection: Any
    ) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.side_effect = [
            (STARTED - timedelta(days=30), True, 100),  # status
            (200, 5),  # total, stale
            (STARTED - timedelta(days=30),),  # registry before start
        ]
        cursor.rowcount = 5
        with patch("tableinator.record_persistence.purge_document_graph", new=AsyncMock()) as graph:
            veto = await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SEEN).purge_stale_rows("releases", STARTED_AT, 100)
        assert veto is None
        statements = _statements(cursor)
        assert statements[0] == "SET LOCAL work_mem = '256MB'"
        assert any('FROM "loader_seen"."status"' in s for s in statements)
        assert any(
            'CREATE TEMP TABLE "seen_stale" ON COMMIT DROP AS SELECT data_id FROM "loader_seen"."discogs_releases_stale"' in s for s in statements
        )
        assert any('DELETE FROM "releases" AS entity USING "seen_stale" AS stale WHERE entity.data_id = stale.data_id' in s for s in statements)
        assert not any("updated_at <" in s for s in statements)
        assert statements[-1] == 'SELECT "loader_seen"."start"(%s, %s)'
        graph.assert_awaited_once()
        assert graph.await_args.kwargs == {"stale_table": "seen_stale"}
        mock_postgres_connection.transaction.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_short_list_returns_a_veto_and_deletes_nothing(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.side_effect = [(STARTED - timedelta(days=30), True, 50)]
        veto = await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SEEN).purge_stale_rows("releases", STARTED_AT, 100)
        assert veto is not None
        assert "below 0.95" in veto
        assert not any("DELETE" in s or "start" in s for s in _statements(cursor))

    @pytest.mark.asyncio
    async def test_a_lost_list_returns_a_veto(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.side_effect = [(None, False, 0)]
        veto = await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SEEN).purge_stale_rows("releases", STARTED_AT, 100)
        assert veto is not None
        assert "not intact" in veto

    @pytest.mark.asyncio
    async def test_a_list_already_settled_by_an_earlier_delivery_is_left_alone(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.side_effect = [(STARTED + timedelta(hours=1), False, 0)]
        veto = await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SEEN).purge_stale_rows("releases", STARTED_AT, 100)
        assert veto is None
        assert not any("DELETE" in s or "start" in s for s in _statements(cursor))

    @pytest.mark.asyncio
    async def test_the_delete_cap_refuses_but_still_restarts(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.side_effect = [(STARTED - timedelta(days=30), True, 100), (1000, 950), (STARTED - timedelta(days=30),)]
        logger = MagicMock()
        veto = await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SEEN, logger).purge_stale_rows("releases", STARTED_AT, 100)
        assert veto is None
        statements = _statements(cursor)
        assert not any(s.startswith("DELETE") for s in statements)
        assert statements[-1] == 'SELECT "loader_seen"."start"(%s, %s)'
        assert "Refusing to purge 950/1000" in logger.error.call_args.args[0]

    @pytest.mark.asyncio
    async def test_nothing_stale_still_restarts(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.side_effect = [(STARTED - timedelta(days=30), True, 100), (100, 0), (None,)]
        assert await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SEEN).purge_stale_rows("releases", STARTED_AT, 100) is None
        assert _statements(cursor)[-1] == 'SELECT "loader_seen"."start"(%s, %s)'

    @pytest.mark.asyncio
    async def test_a_resumed_extraction_with_no_records_restarts_the_list_only(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.side_effect = [(STARTED - timedelta(days=30),)]
        assert await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SEEN).purge_stale_rows("releases", STARTED_AT, 0) is None
        statements = _statements(cursor)
        assert not any("DELETE" in s for s in statements)
        assert statements[-1] == 'SELECT "loader_seen"."start"(%s, %s)'


class TestShadowPurge:
    SHADOW = (True, 200, 5, 5, 0, 0, [], [])

    @pytest.mark.asyncio
    async def test_the_comparison_is_logged_before_the_touch_purge_which_still_deletes(
        self, mock_async_pool: Any, mock_postgres_connection: Any
    ) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.side_effect = [
            (STARTED - timedelta(days=30), True, 100),  # status
            self.SHADOW,
            (200,),  # total
            (5,),  # stale by updated_at
            (STARTED - timedelta(days=30),),  # registry before start
        ]
        logger = MagicMock()
        with patch("tableinator.record_persistence.purge_document_graph", new=AsyncMock()) as graph:
            veto = await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SHADOW, logger).purge_stale_rows(
                "releases", STARTED_AT, 100
            )
        assert veto is None
        statements = _statements(cursor)
        shadow_at = next(i for i, s in enumerate(statements) if "discogs_releases_shadow" in s)
        delete_at = next(i for i, s in enumerate(statements) if s.startswith("DELETE"))
        assert shadow_at < delete_at
        assert statements[delete_at] == 'DELETE FROM "releases" WHERE updated_at < %s'
        assert statements[-1] == 'SELECT "loader_seen"."start"(%s, %s)'
        assert graph.await_args.kwargs == {}
        logged = next(call for call in logger.info.call_args_list if "shadow" in call.args[0])
        assert logged.kwargs["matches"] is True
        assert logged.kwargs["seen_ratio"] == 1.0
        assert logged.kwargs["guard"] is None

    @pytest.mark.asyncio
    async def test_a_difference_is_a_warning(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.side_effect = [
            (STARTED - timedelta(days=30), True, 100),
            (True, 200, 5, 6, 1, 0, ["7"], []),
            (200,),
            (0,),
            (STARTED - timedelta(days=30),),
        ]
        logger = MagicMock()
        await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SHADOW, logger).purge_stale_rows("releases", STARTED_AT, 100)
        warned = next(call for call in logger.warning.call_args_list if "shadow" in call.args[0])
        assert warned.kwargs["refreshed_not_seen"] == 1
        assert warned.kwargs["refreshed_not_seen_sample"] == ["7"]

    @pytest.mark.asyncio
    async def test_a_failed_comparison_or_restart_never_holds_up_the_touch_purge(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        cursor = mock_postgres_connection.cursor.return_value

        async def execute(statement: Any, *_params: Any) -> None:
            if "loader_seen" in _text(statement):
                raise RuntimeError("loader_seen exploded")

        cursor.execute.side_effect = execute
        cursor.fetchone.side_effect = [(200,), (5,)]
        cursor.rowcount = 5
        logger = MagicMock()
        with patch("tableinator.record_persistence.purge_document_graph", new=AsyncMock()):
            assert (
                await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SHADOW, logger).purge_stale_rows("releases", STARTED_AT, 100)
                is None
            )
        assert any(_text(call.args[0]) == 'DELETE FROM "releases" WHERE updated_at < %s' for call in cursor.execute.await_args_list)
        errors = [call.args[0] for call in logger.error.call_args_list]
        assert any("Could not compare" in e for e in errors)
        assert any("Could not restart" in e for e in errors)


class TestStartAndPersist:
    @pytest.mark.asyncio
    async def test_touch_mode_never_starts_a_list(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.TOUCH).start_seen_list("releases", STARTED_AT)
        mock_postgres_connection.cursor.return_value.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_list_restarted_since_the_extraction_began_is_not_restarted_again(
        self, mock_async_pool: Any, mock_postgres_connection: Any
    ) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.return_value = (STARTED + timedelta(minutes=5),)
        await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SEEN).start_seen_list("releases", STARTED_AT)
        assert not any("start" in s for s in _statements(cursor))

    @pytest.mark.asyncio
    async def test_a_failed_restart_is_only_logged(self, mock_async_pool: Any, mock_postgres_connection: Any) -> None:
        mock_postgres_connection.cursor.return_value.execute.side_effect = RuntimeError("down")
        logger = MagicMock()
        await _persistence(mock_async_pool, mock_postgres_connection, SeenMode.SEEN, logger).start_seen_list("releases", STARTED_AT)
        assert "Could not restart" in logger.error.call_args.args[0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("mode", "appended"), [(SeenMode.TOUCH, False), (SeenMode.SHADOW, True), (SeenMode.SEEN, True)])
    async def test_the_single_record_path_appends_in_its_transaction(
        self, mock_async_pool: Any, mock_postgres_connection: Any, mode: SeenMode, appended: bool
    ) -> None:
        cursor = mock_postgres_connection.cursor.return_value
        cursor.fetchone.return_value = (False,)
        with (
            patch("tableinator.record_persistence.attach_alias_targets", new=AsyncMock()),
            patch("tableinator.record_persistence.write_document_graph", new=AsyncMock()),
        ):
            await _persistence(mock_async_pool, mock_postgres_connection, mode).persist_record("labels", "9", {"id": "9", "sha256": "h"})
        appends = [call for call in cursor.execute.await_args_list if "loader_seen" in _text(call.args[0])]
        assert bool(appends) is appended
        if appended:
            assert appends[0].args[1] == (["9"],)


_LATCH = LatchRelation(schema="public", table="loader_extraction_latch")


async def _one_type_signalled(_pool: Any, _latch: Any, version: str, data_type: str) -> ExtractionLatch:
    return ExtractionLatch(
        version=version,
        signals=frozenset({data_type}),
        already_signalled=False,
        already_refreshed=False,
        superseded=False,
    )


def _signal() -> AsyncMock:
    message = AsyncMock(spec=AbstractIncomingMessage)
    message.body = json.dumps(
        {"type": "extraction_complete", "version": "20261101", "started_at": STARTED_AT, "record_counts": {"releases": 100}}
    ).encode()
    return message


class TestTerminalSignal:
    @pytest.mark.asyncio
    async def test_a_seen_guard_is_recorded_as_a_veto_then_the_list_restarts(self) -> None:
        message = _signal()
        with (
            patch.object(service, "logger"),
            patch.object(service, "connection_pool", MagicMock()),
            patch.object(service, "extraction_latch", _LATCH),
            patch.object(service, "record_extraction_signal", new=_one_type_signalled),
            patch.object(service, "purge_stale_rows", new=AsyncMock(return_value="the seen list is short")) as purge,
            patch.object(service, "persist_purge_veto", new=AsyncMock(return_value=True)) as veto,
            patch.object(service, "start_seen_list", new=AsyncMock()) as start,
        ):
            await service.on_data_message(message, "releases")
        purge.assert_awaited_once_with("releases", STARTED_AT, 100)
        veto.assert_awaited_once()
        start.assert_awaited_once_with("releases", STARTED_AT)
        message.ack.assert_awaited_once()
        message.nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unrecorded_seen_veto_requeues_without_restarting(self) -> None:
        message = _signal()
        with (
            patch.object(service, "logger"),
            patch.object(service, "connection_pool", MagicMock()),
            patch.object(service, "extraction_latch", _LATCH),
            patch.object(service, "purge_stale_rows", new=AsyncMock(return_value="the seen list is short")),
            patch.object(service, "persist_purge_veto", new=AsyncMock(return_value=False)),
            patch.object(service, "start_seen_list", new=AsyncMock()) as start,
        ):
            await service.on_data_message(message, "releases")
        start.assert_not_awaited()
        message.nack.assert_awaited_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_a_dead_letter_veto_restarts_the_list_once_recorded(self) -> None:
        message = _signal()
        with (
            patch.object(service, "logger"),
            patch.object(service, "connection_pool", MagicMock()),
            patch.object(service, "extraction_latch", _LATCH),
            patch.object(service, "record_extraction_signal", new=_one_type_signalled),
            patch.object(service, "purge_veto_reason", new=AsyncMock(return_value="2 dead letters are waiting")),
            patch.object(service, "purge_stale_rows", new=AsyncMock()) as purge,
            patch.object(service, "persist_purge_veto", new=AsyncMock(return_value=True)),
            patch.object(service, "start_seen_list", new=AsyncMock()) as start,
        ):
            await service.on_data_message(message, "releases")
        purge.assert_not_awaited()
        start.assert_awaited_once_with("releases", STARTED_AT)
        message.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_service_wrappers_pass_the_mode_through(self) -> None:
        persistence = MagicMock()
        persistence.purge_stale_rows = AsyncMock(return_value=None)
        persistence.start_seen_list = AsyncMock()
        with (
            patch.object(service, "connection_pool", MagicMock()),
            patch.object(service, "seen_mode", SeenMode.SEEN),
            patch.object(service, "seen_min_fraction", 0.9),
            patch.object(service, "PostgreSQLRecordPersistence", return_value=persistence) as built,
        ):
            assert await service.purge_stale_rows("releases", STARTED_AT, 100) is None
            await service.start_seen_list("releases", STARTED_AT)
        assert built.call_args.args[4:] == (SeenMode.SEEN, 0.9)
        persistence.start_seen_list.assert_awaited_once_with("releases", STARTED_AT)

    @pytest.mark.asyncio
    async def test_touch_mode_never_restarts_a_list(self) -> None:
        with (
            patch.object(service, "connection_pool", MagicMock()),
            patch.object(service, "PostgreSQLRecordPersistence") as built,
        ):
            await service.start_seen_list("releases", STARTED_AT)
        built.assert_not_called()

    def test_health_reports_the_running_mode(self) -> None:
        with patch.object(service, "seen_mode", SeenMode.SHADOW):
            assert service.get_health_data()["seen_list_mode"] == "shadow"


def test_started_at_without_a_zone_is_utc() -> None:
    from tableinator.record_persistence import _parse_started_at

    assert _parse_started_at("2026-11-01T00:00:00") == datetime(2026, 11, 1, tzinfo=UTC)
