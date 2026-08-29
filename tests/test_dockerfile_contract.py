"""Static regression tests for the repository-owned runtime image."""

import re
import shlex
from pathlib import Path


ROOT = Path(__file__).parent.parent
DOCKERFILE = (ROOT / "Dockerfile").read_text()
BUILD_SCRIPT = (ROOT / "scripts/build-image.sh").read_text()
RELEASE_WORKFLOW = (ROOT / ".github/workflows/release.yml").read_text()
SENSITIVE_ENV = re.compile(r"(?:PASSWORD|USERNAME|SECRET|TOKEN|CREDENTIAL|PRIVATE_KEY)(?:$|_)")


def _instructions() -> list[str]:
    instructions: list[str] = []
    parts: list[str] = []
    for raw_line in DOCKERFILE.splitlines():
        line = raw_line.strip()
        if not parts and (not line or line.startswith("#")):
            continue
        continued = line.endswith("\\")
        parts.append(line.removesuffix("\\").rstrip())
        if not continued:
            instructions.append(" ".join(parts))
            parts.clear()
    return instructions


def test_image_metadata_uses_repository_name() -> None:
    assert 'org.opencontainers.image.title="discogs-sql-loader"' in DOCKERFILE
    assert "github.com/groovemap-music/discogs-sql-loader" in DOCKERFILE


def test_local_and_published_images_use_repository_name() -> None:
    assert "--tag discogs-sql-loader:local" in BUILD_SCRIPT
    assert "image-name: discogs-sql-loader" in RELEASE_WORKFLOW


def test_runtime_user_is_numeric_and_non_root() -> None:
    users = [line.removeprefix("USER ") for line in _instructions() if line.startswith("USER ")]
    assert users
    assert users[-1] in {"1000:1000", "${UID}:${GID}"}


def test_healthcheck_uses_exec_form() -> None:
    healthchecks = [line for line in _instructions() if line.startswith("HEALTHCHECK ")]
    assert healthchecks
    assert 'CMD ["' in healthchecks[0]


def test_image_does_not_persist_credential_placeholders() -> None:
    for instruction in _instructions():
        if instruction.startswith("ENV "):
            keys = (assignment.split("=", 1)[0] for assignment in shlex.split(instruction.removeprefix("ENV ")))
            assert not [key for key in keys if SENSITIVE_ENV.search(key)]
