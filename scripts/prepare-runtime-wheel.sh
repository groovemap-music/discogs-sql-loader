#!/usr/bin/env bash
set -euo pipefail

expected="41805b62520785f412e8f5d0db90f8d83838ec56"
runtime_repo="${GROOVEMAP_RUNTIME_REPO:-../python-libraries}"
runtime_checkout=

runtime_is_valid() {
  [[ -d "${runtime_repo}/.git" ]] &&
    [[ "$(git -C "${runtime_repo}" rev-parse HEAD)" = "${expected}" ]] &&
    [[ -z "$(git -C "${runtime_repo}" status --short)" ]]
}

if ! runtime_is_valid; then
  if [[ -n "${GROOVEMAP_RUNTIME_REPO:-}" ]]; then
    echo "GROOVEMAP_RUNTIME_REPO must be a clean checkout at ${expected}." >&2
    exit 2
  fi
  runtime_checkout="$(mktemp -d)"
  case "${runtime_checkout}" in
  /tmp/* | /private/tmp/* | /var/folders/*) ;;
  *)
    echo "Unexpected temporary checkout path: ${runtime_checkout}" >&2
    exit 2
    ;;
  esac
  trap 'rm -rf -- "${runtime_checkout}"' EXIT
  runtime_repo="${runtime_checkout}/python-libraries"
  git clone --quiet --filter=blob:none --no-checkout \
    https://github.com/groovemap-music/python-libraries.git "${runtime_repo}"
  git -C "${runtime_repo}" checkout --quiet "${expected}"
fi

mkdir -p .build/runtime
find .build/runtime -type f -name '*.whl' -delete
uv build --wheel --out-dir .build/runtime "${runtime_repo}"
