"""When a stale-row purge must not run, from every source of the veto."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import WriteAdmission

import tableinator.tableinator as service
from tableinator.extraction_latch import LatchRelation, consecutive_purge_vetoes


SIGNAL = {"type": "extraction_complete", "version": "20261101", "started_at": "2026-11-02T00:00:00Z"}


def processor(*, flagged: bool = False) -> MagicMock:
    batch = MagicMock()
    batch.had_dlq_nacks.return_value = flagged
    return batch


@pytest.mark.asyncio
async def test_a_clean_type_may_purge() -> None:
    with patch.object(service, "batch_processor", processor()):
        assert await service.purge_veto_reason("artists", SIGNAL) is None


@pytest.mark.asyncio
async def test_this_processes_flag_vetoes() -> None:
    with patch.object(service, "batch_processor", processor(flagged=True)):
        assert await service.purge_veto_reason("artists", SIGNAL) == "records were dead-lettered this run"


@pytest.mark.asyncio
async def test_dead_letters_on_the_broker_veto_with_no_flag_in_memory() -> None:
    with patch.object(service, "batch_processor", processor()), patch.object(service, "dead_letters_waiting", AsyncMock(return_value=3)):
        assert await service.purge_veto_reason("artists", SIGNAL) == "3 dead letters are waiting"


@pytest.mark.asyncio
async def test_a_broker_that_cannot_answer_vetoes() -> None:
    with patch.object(service, "batch_processor", processor()), patch.object(service, "dead_letters_waiting", AsyncMock(return_value=None)):
        assert await service.purge_veto_reason("artists", SIGNAL) == "the dead-letter queue could not be read"


@pytest.mark.asyncio
async def test_a_veto_on_the_latch_holds_for_a_redelivery_after_the_dlq_is_cleared() -> None:
    with (
        patch.object(service, "batch_processor", processor()),
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "extraction_latch", MagicMock()),
        patch.object(service, "purge_was_vetoed", AsyncMock(return_value=True)),
    ):
        assert await service.purge_veto_reason("artists", SIGNAL) == "an earlier delivery of this signal was vetoed"


@pytest.mark.asyncio
async def test_a_latch_that_cannot_answer_vetoes() -> None:
    with (
        patch.object(service, "batch_processor", processor()),
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "extraction_latch", MagicMock()),
        patch.object(service, "purge_was_vetoed", AsyncMock(side_effect=OSError("down"))),
    ):
        assert await service.purge_veto_reason("artists", SIGNAL) == "the recorded veto could not be read"


@pytest.mark.asyncio
async def test_dead_letters_with_no_connection_are_unknown() -> None:
    with patch.object(service, "active_connection", None):
        assert await service.__dict__["_real_dead_letters_waiting"]("artists") is None


@pytest.mark.asyncio
async def test_a_veto_that_cannot_be_recorded_goes_back_to_the_broker() -> None:
    with (
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "extraction_latch", MagicMock()),
        patch.object(service, "write_admission", WriteAdmission(1)),
        patch.object(service, "record_purge_veto", AsyncMock(side_effect=OSError("down"))),
    ):
        assert await service.persist_purge_veto("artists", SIGNAL) is False


class _Cursor:
    """The cursor shape consecutive_purge_vetoes reads."""

    def __init__(self, rows: list[tuple[list[str], list[str]]]) -> None:
        self.rows = rows
        self.execute = AsyncMock()

    async def fetchall(self) -> list[tuple[list[str], list[str]]]:
        return self.rows


def _pool(rows: list[tuple[list[str], list[str]]]) -> MagicMock:
    conn = MagicMock()
    conn.set_autocommit = AsyncMock()
    conn.transaction.return_value.__aenter__ = AsyncMock()
    conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=_Cursor(rows))
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([], 0),
        ([(["artists"], [])], 1),
        ([(["artists"], ["artists"]), (["artists"], ["artists"]), ([], ["artists"]), (["artists"], ["artists"])], 2),
        ([([], ["artists"]), (["artists"], ["artists"])], 0),
        ([(["artists"], ["artists"]), ([], ["labels"]), (["artists"], ["artists"])], 2),
        ([(["labels"], ["labels", "artists"])], 0),
    ],
    ids=["no-history", "one", "stops-at-a-clean-run", "clean-newest", "passes-over-a-run-that-missed-the-type", "other-type"],
)
async def test_consecutive_vetoes_count_back_from_the_newest_extraction(rows: list[tuple[list[str], list[str]]], expected: int) -> None:
    latch = LatchRelation("public", "loader_extraction_latch")
    assert await consecutive_purge_vetoes(_pool(rows), latch, "artists") == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("count", "alerts"), [(0, 0), (1, 0), (2, 1), (5, 1)])
async def test_the_alert_fires_at_the_stated_count(count: int, alerts: int) -> None:
    with (
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "extraction_latch", MagicMock()),
        patch.object(service, "write_admission", WriteAdmission(1)),
        patch.object(service, "consecutive_purge_vetoes", AsyncMock(return_value=count)),
        patch.object(service.telemetry, "record_purge_consecutive_vetoes") as gauge,
        patch.object(service, "logger") as logger,
    ):
        await service.report_purge_vetoes("artists")
    gauge.assert_called_once_with("artists", count)
    assert logger.error.call_count == alerts


@pytest.mark.asyncio
async def test_a_failed_count_does_not_hold_up_the_signal() -> None:
    with (
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "extraction_latch", MagicMock()),
        patch.object(service, "write_admission", WriteAdmission(1)),
        patch.object(service, "consecutive_purge_vetoes", AsyncMock(side_effect=OSError("down"))),
        patch.object(service.telemetry, "record_purge_consecutive_vetoes") as gauge,
    ):
        await service.report_purge_vetoes("artists")
    gauge.assert_not_called()
