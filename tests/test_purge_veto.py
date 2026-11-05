"""When a stale-row purge must not run, from every source of the veto."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import WriteAdmission

import tableinator.tableinator as service


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
