"""Validate the next Commitizen bump, including explicit release-boundary states."""

from __future__ import annotations

import subprocess
import sys
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Sequence


COMMAND = ("cz", "bump", "--dry-run", "--changelog", "--yes", "--check-consistency")
NO_COMMITS_EXIT = 3
NO_COMMITS_MARKERS = ("[NO_COMMITS_FOUND]", "No new commits found.")
RELEASE_GAP_EXIT = 16
RELEASE_GAP_MARKERS = (
    "bump: version ",
    "tag to create: v",
    "increment detected: ",
    "No tag found to do an incremental changelog",
)


def accepted_result(returncode: int, output: str) -> bool:
    """Return whether Commitizen produced a valid explicit preview state."""
    return (
        returncode == 0
        or (returncode == NO_COMMITS_EXIT and all(marker in output for marker in NO_COMMITS_MARKERS))
        or (returncode == RELEASE_GAP_EXIT and all(marker in output for marker in RELEASE_GAP_MARKERS))
    )


def main(command: Sequence[str] = COMMAND) -> int:
    """Run Commitizen and preserve its output and unexpected failure status."""
    result = subprocess.run(command, check=False, capture_output=True, text=True)  # noqa: S603
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    output = f"{result.stdout}\n{result.stderr}"
    return 0 if accepted_result(result.returncode, output) else result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
