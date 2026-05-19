#!/usr/bin/env bash
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Long-lived entrypoint for the Antithesis Test Composer client.
#
# Responsibilities:
#   1. Wait for materialized SQL/HTTP and the Kafka + Schema Registry endpoints.
#   2. Emit a bootstrap reachable assertion through the Antithesis Python SDK.
#   3. Emit the lifecycle `setup_complete` signal.
#   4. Sleep so Antithesis Test Composer can exec individual commands against
#      this container.

set -euo pipefail

export NO_COLOR=1
export FORCE_COLOR=0
export PY_COLORS=0

MZ_HOST="${MZ_HOST:-materialized}"
MZ_SQL_PORT="${MZ_SQL_PORT:-6875}"
MZ_HTTP_PORT="${MZ_HTTP_PORT:-6878}"
KAFKA_HOST="${KAFKA_HOST:-kafka}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
SR_HOST="${SR_HOST:-schema-registry}"
SR_PORT="${SR_PORT:-8081}"

# Total budget for the entire readiness phase. Antithesis bring-up under fault
# injection can be slow; bias toward a generous deadline.
WAIT_DEADLINE_SECONDS="${MZ_ANTITHESIS_WAIT_DEADLINE:-600}"

start_epoch=$(date +%s)
deadline=$(( start_epoch + WAIT_DEADLINE_SECONDS ))

now() { date +%s; }

wait_tcp() {
    local host=$1 port=$2 desc=$3
    while ! nc -z "$host" "$port" 2>/dev/null; do
        if [[ "$(now)" -gt "$deadline" ]]; then
            echo "[entrypoint] TIMEOUT waiting for $desc ($host:$port)" >&2
            exit 1
        fi
        sleep 1
    done
    echo "[entrypoint] ready: $desc ($host:$port)"
}

wait_http_ok() {
    local url=$1 desc=$2
    while ! curl -fsS -o /dev/null "$url"; do
        if [[ "$(now)" -gt "$deadline" ]]; then
            echo "[entrypoint] TIMEOUT waiting for $desc ($url)" >&2
            exit 1
        fi
        sleep 1
    done
    echo "[entrypoint] ready: $desc ($url)"
}

echo "[entrypoint] waiting for SUT and dependencies"

wait_tcp  "$MZ_HOST"    "$MZ_SQL_PORT"   "materialized SQL"
wait_tcp  "$MZ_HOST"    "$MZ_HTTP_PORT"  "materialized HTTP"
wait_http_ok "http://${MZ_HOST}:${MZ_HTTP_PORT}/api/readyz" "materialized readyz"

wait_tcp  "$KAFKA_HOST" "$KAFKA_PORT"    "kafka"
wait_tcp  "$SR_HOST"    "$SR_PORT"       "schema-registry"

# Smoke-check the SQL connection so missing auth/config surfaces here, not in
# every test command.
PGPASSWORD='' psql -h "$MZ_HOST" -p "$MZ_SQL_PORT" -U materialize -d materialize \
    -v ON_ERROR_STOP=1 -c "SELECT 1;" >/dev/null
echo "[entrypoint] materialized accepts SQL"

# Bootstrap: prove that the Antithesis Python SDK is reachable from inside this
# container, then emit setup_complete. Both calls go through
# materialize.antithesis.sdk so they tolerate running outside Antithesis (the
# wrapper falls back to local stubs).
python3 - <<'PYEOF'
import os
import sys

sys.path.insert(0, os.environ.get("PYTHONPATH", "/opt/materialize/misc/python"))

from materialize.antithesis.sdk import reachable, setup_complete

reachable(
    "antithesis test-driver entrypoint reached",
    {"phase": "bootstrap"},
)
setup_complete({"service": "antithesis-test-driver"})
PYEOF

echo "[entrypoint] setup_complete emitted; sleeping"

# Stay alive so Antithesis Test Composer can exec commands. `sleep infinity`
# is a foreground process so signals propagate cleanly.
exec sleep infinity
