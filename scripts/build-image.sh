#!/usr/bin/env bash
set -euo pipefail

docker_config="$(mktemp -d)"
trap 'rm -rf "${docker_config}"' EXIT
active_context="$(docker context ls --format '{{if .Current}}{{.Name}}{{end}}' | sed -n '1p')"
docker_host="$(docker context inspect "${active_context}" --format '{{.Endpoints.docker.Host}}')"

DOCKER_HOST="${docker_host}" docker --config "${docker_config}" build --tag discogs-sql-loader:local .
