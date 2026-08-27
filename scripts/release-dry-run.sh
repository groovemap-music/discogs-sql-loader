#!/usr/bin/env bash
set -euo pipefail

uv build --out-dir dist --clear
(
  cd dist
  shasum -a 256 ./*.whl ./*.tar.gz > SHA256SUMS
)
uv run cyclonedx-py environment --output-file dist/sbom.json
uv run pip-licenses --format=json --output-file=dist/THIRD_PARTY_NOTICES.json
uv run python scripts/write-build-provenance.py
test -s dist/SHA256SUMS
test -s dist/sbom.json
test -s dist/THIRD_PARTY_NOTICES.json
test -s dist/provenance.json
