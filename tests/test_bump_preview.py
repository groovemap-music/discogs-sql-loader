from __future__ import annotations

import subprocess

from scripts import check_bump_preview


def test_accepts_valid_preview() -> None:
    assert check_bump_preview.accepted_result(0, "bump: version 0.1.0 -> 0.2.0")


def test_accepts_only_the_explicit_no_commits_result() -> None:
    output = "[NO_COMMITS_FOUND]\nNo new commits found."
    assert check_bump_preview.accepted_result(3, output)
    assert not check_bump_preview.accepted_result(4, output)
    assert not check_bump_preview.accepted_result(3, "No new commits found.")


def test_accepts_only_the_explicit_release_gap_result() -> None:
    output = """bump: version 0.1.1 → 0.2.0
tag to create: v0.2.0
increment detected: MINOR
No tag found to do an incremental changelog
"""
    assert check_bump_preview.accepted_result(16, output)
    assert not check_bump_preview.accepted_result(15, output)
    assert not check_bump_preview.accepted_result(16, "No tag found to do an incremental changelog")


def test_propagates_unexpected_commitizen_failure(monkeypatch) -> None:
    result = subprocess.CompletedProcess(check_bump_preview.COMMAND, 7, stdout="", stderr="invalid config\n")
    monkeypatch.setattr(check_bump_preview.subprocess, "run", lambda *_args, **_kwargs: result)

    assert check_bump_preview.main() == 7
