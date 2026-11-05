#!/usr/bin/env bash
# First-load shaped benchmark on a disposable PostgreSQL with optional server settings.
#
#   just benchmark-first-load --releases 60000
#   BENCHMARK_PG_ARGS="-c max_wal_size=8GB -c checkpoint_timeout=30min" just benchmark-first-load --defer-indexes
#
# BENCHMARK_PG_ARGS are extra `postgres` server arguments for the container. The container runs
# with --rm on a random loopback port and is removed, with its volume, on exit or SIGTERM.
set -euo pipefail

postgres_image="${POSTGRES_BENCHMARK_IMAGE:-postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2}"
container="discogs-sql-loader-firstload-$$"
child=""

cleanup() {
    docker rm -fv "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

free_kb="$(df -k / | awk 'NR==2{print $4}')"
if (( free_kb < 3000000 )); then
    echo "Less than 3 GB free; refusing to start a container" >&2
    exit 1
fi

# shellcheck disable=SC2086 -- BENCHMARK_PG_ARGS is a word list by design
docker run --detach --rm --name "${container}" --env POSTGRES_PASSWORD=benchmark \
    --publish 127.0.0.1::5432 "${postgres_image}" ${BENCHMARK_PG_ARGS:-} >/dev/null

ready="false"
for _attempt in $(seq 1 30); do
    if docker exec "${container}" pg_isready --quiet --username postgres; then
        ready="true"
        break
    fi
    sleep 1
done
if [[ "${ready}" != "true" ]]; then
    echo "Disposable PostgreSQL did not become ready" >&2
    exit 1
fi

port="$(docker port "${container}" 5432/tcp | head -n1)"
port="${port##*:}"
TEST_DATABASE_URL="postgresql://postgres:benchmark@127.0.0.1:${port}/postgres" \
    uv run python scripts/benchmark-first-load.py "$@" &
child=$!

stop_child() {
    kill -TERM "${child}" 2>/dev/null || true
    wait "${child}" 2>/dev/null || true
    exit 143
}
trap stop_child TERM INT

wait "${child}"
