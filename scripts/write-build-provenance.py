"""Write non-publishing build metadata for the local release candidate."""

import json
import shutil
import subprocess
import sys
from hashlib import sha256
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"


def digest(path: Path) -> str:
    """Return the hexadecimal SHA-256 digest for an artifact."""
    return sha256(path.read_bytes()).hexdigest()


git = shutil.which("git")
if git is None:
    raise RuntimeError("git is required to write build provenance")
commit = subprocess.run(  # noqa: S603 -- executable is resolved from the operator's PATH
    [git, "rev-parse", "HEAD"],
    cwd=ROOT,
    capture_output=True,
    check=True,
    text=True,
    timeout=10,
).stdout.strip()
artifacts = [path for path in sorted(DIST.iterdir()) if path.suffix in {".gz", ".whl"}]
metadata = {
    "artifacts": [{"name": path.name, "sha256": digest(path)} for path in artifacts],
    "builder": "scripts/release-dry-run.sh",
    "commit": commit,
    "python": sys.version.split()[0],
    "repository": "https://github.com/groovemap-music/discogs-sql-loader",
}
(DIST / "provenance.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
