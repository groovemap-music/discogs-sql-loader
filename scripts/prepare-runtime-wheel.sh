#!/usr/bin/env bash
set -euo pipefail

runtime_repo="${GROOVEMAP_RUNTIME_REPO:-../python-libraries}"
expected="28fa329702bc76896cc54ab8d05ec5b1bd3d929e"

test -d "${runtime_repo}/.git"
actual="$(git -C "${runtime_repo}" rev-parse HEAD)"
test "${actual}" = "${expected}"
test -z "$(git -C "${runtime_repo}" status --short)"

mkdir -p .build/runtime
find .build/runtime -type f -name '*.whl' -delete
uv build --wheel --out-dir .build/runtime "${runtime_repo}"
