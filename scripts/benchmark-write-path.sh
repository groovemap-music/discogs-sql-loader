#!/usr/bin/env bash
# Reproduce the loader's write-path throughput on a disposable PostgreSQL.
#
#   just benchmark-write-path                      # 5000 releases, batch 500, one worker
#   just benchmark-write-path --batch-size 2000 --workers 2
#
# Arguments pass through to scripts/benchmark-write-path.py. The container is started with
# --rm on a random loopback port, is stopped on exit, and its anonymous volume is removed.
# Outside `check` and the default test lane by design.
set -euo pipefail

postgres_image="${POSTGRES_BENCHMARK_IMAGE:-postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2}"
container="discogs-sql-loader-benchmark-$$"

cleanup() {
    docker rm -fv "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --detach --rm --name "${container}" --env POSTGRES_PASSWORD=benchmark \
    --publish 127.0.0.1::5432 "${postgres_image}" \
    -c shared_preload_libraries=pg_stat_statements >/dev/null

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
# Run the benchmark as a child we can stop: a signal to this script must not leave it writing
# into a container the trap is about to remove.
TEST_DATABASE_URL="postgresql://postgres:benchmark@127.0.0.1:${port}/postgres" \
    uv run python scripts/benchmark-write-path.py "$@" &
child=$!

stop_child() {
    kill -TERM "${child}" 2>/dev/null || true
    wait "${child}" 2>/dev/null || true
    exit 143
}
trap stop_child TERM INT

wait "${child}"
