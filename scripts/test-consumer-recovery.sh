#!/usr/bin/env bash
set -euo pipefail

# Same image digest as the homelab smoke that caught the v0.4.1 regression.
rabbitmq_image="${RABBITMQ_RECOVERY_IMAGE:-rabbitmq:4-management@sha256:c200fd496482cda18c412be4e6f244830e1e75949b8cf5f0e887cea9ea456109}"
rabbitmq_container="discogs-consumer-recovery-$$"
temp_dir="$(mktemp -d)"

cleanup() {
    docker rm --force --volumes "${rabbitmq_container}" >/dev/null 2>&1 || true
    rm -rf -- "${temp_dir}"
}
trap cleanup EXIT
trap "exit 130" INT
trap "exit 143" TERM

# The tests make the broker time out a delivery, so they need a short ack deadline.
cat >"${temp_dir}/rabbitmq.conf" <<'EOF'
consumer_timeout = 2000
EOF

docker create --name "${rabbitmq_container}" --rm \
    --publish 127.0.0.1::5672 "${rabbitmq_image}" >/dev/null
docker cp "${temp_dir}/rabbitmq.conf" "${rabbitmq_container}:/etc/rabbitmq/rabbitmq.conf"
docker start "${rabbitmq_container}" >/dev/null

rabbitmq_ready=false
for _attempt in $(seq 1 90); do
    if docker exec --user rabbitmq "${rabbitmq_container}" rabbitmq-diagnostics -q check_port_listener 5672 >/dev/null 2>&1; then
        rabbitmq_ready=true
        break
    fi
    sleep 1
done
if [[ "${rabbitmq_ready}" != true ]]; then
    docker logs "${rabbitmq_container}" >&2
    echo "Owned RabbitMQ did not become ready" >&2
    exit 1
fi

rabbitmq_port="$(docker port "${rabbitmq_container}" 5672/tcp)"
export TEST_AMQP_URL="amqp://guest:guest@127.0.0.1:${rabbitmq_port##*:}/"

uv run pytest tests/integration/test_consumer_recovery.py tests/integration/test_redelivery_bound.py -m integration --no-cov "$@"
