#!/usr/bin/env bash
set -euo pipefail

bash scripts/prepare-runtime-wheel.sh
loader_tmp="$(mktemp -d)"
trap 'rm -rf "${loader_tmp}"' EXIT

uv venv "${loader_tmp}/venv"
uv pip install --python "${loader_tmp}/venv/bin/python" ".build/runtime/$(basename "$(find .build/runtime -type f -name '*.whl' -print -quit)")[otel,postgres,rabbitmq]"
uv pip install --python "${loader_tmp}/venv/bin/python" --no-deps dist/*.whl
"${loader_tmp}/venv/bin/python" -c 'import tableinator.tableinator; import tableinator.config'
