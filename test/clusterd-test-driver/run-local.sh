#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Run the clusterd-test-driver entirely on the host, without docker images.
#
# Brings up (or reuses) CockroachDB for persist consensus, launches a local
# clusterd, and runs the headless-driver against it. Everything is on
# localhost, so a single PubSub address and a single persist location work for
# both the driver and clusterd.
#
# To profile clusterd instead, run it yourself under heaptrack/perf using the
# command this script prints ("clusterd command:"), then run the driver with
# RUN_CLUSTERD=0 (it connects to the clusterd you launched). See
# doc/developer/design/20260612_headless_clusterd_test_driver.md.

set -euo pipefail
cd "$(dirname "$0")/../.."

# --- configuration (override via env) ---
COMPUTE_ADDR=${CLUSTERD_COMPUTE_ADDR:-127.0.0.1:2101}
STORAGE_ADDR=${CLUSTERD_STORAGE_ADDR:-127.0.0.1:2100}
PUBSUB_PORT=${PUBSUB_PORT:-6879}
COCKROACH_PORT=${COCKROACH_PORT:-26257}
CONSENSUS_URL=${PERSIST_CONSENSUS_URL:-postgres://root@127.0.0.1:${COCKROACH_PORT}?options=--search_path=consensus}
BLOB_DIR=${BLOB_DIR:-/tmp/clusterd-test-driver-blob}
SCRATCH_DIR=${SCRATCH_DIR:-/tmp/clusterd-test-driver-scratch}
SECRETS_DIR=${SECRETS_DIR:-/tmp/clusterd-test-driver-secrets}
TARGET_BYTES=${TARGET_BYTES:-1000000}
SCENARIO=${SCENARIO:-index}
N_TIMESTAMPS=${N_TIMESTAMPS:-64}
RUN_CLUSTERD=${RUN_CLUSTERD:-1}
ENVIRONMENT_ID="mzcompose-us-east-1-00000000-0000-0000-0000-000000000000-0"

mkdir -p "$BLOB_DIR" "$SCRATCH_DIR" "$SECRETS_DIR"

# --- CockroachDB ---
if ! docker exec cockroach true >/dev/null 2>&1; then
    docker start cockroach >/dev/null 2>&1 || docker run --name=cockroach -d \
        -p "${COCKROACH_PORT}:26257" -p 26258:8080 \
        cockroachdb/cockroach:latest start-single-node --insecure \
        --store=type=mem,size=2G >/dev/null
    sleep 3
fi
docker exec cockroach cockroach sql --insecure -e \
    "CREATE SCHEMA IF NOT EXISTS consensus" >/dev/null

# --- Timely configs (single process, one worker) ---
timely_config() { # $1 = comm port
    cat <<EOF
{"workers":1,"process":0,"addresses":["127.0.0.1:$1"],"arrangement_exert_proportionality":0,"enable_zero_copy":false,"enable_zero_copy_lgalloc":false,"zero_copy_limit":null}
EOF
}

clusterd_pid=""
cleanup() { [[ -n "$clusterd_pid" ]] && kill "$clusterd_pid" 2>/dev/null || true; }
trap cleanup EXIT

if [[ "$RUN_CLUSTERD" == "1" ]]; then
    echo "Building clusterd..."
    cargo build --bin clusterd
    echo "clusterd command:"
    echo "  PERSIST_PUBSUB_URL=http://127.0.0.1:${PUBSUB_PORT} \\"
    echo "  target/debug/clusterd \\"
    echo "    --compute-controller-listen-addr ${COMPUTE_ADDR} \\"
    echo "    --storage-controller-listen-addr ${STORAGE_ADDR} \\"
    echo "    --compute-timely-config '$(timely_config 2102)' \\"
    echo "    --storage-timely-config '$(timely_config 2103)' \\"
    echo "    --process 0 --environment-id ${ENVIRONMENT_ID} \\"
    echo "    --secrets-reader local-file --secrets-reader-local-file-dir ${SECRETS_DIR} \\"
    echo "    --scratch-directory ${SCRATCH_DIR}"
    PERSIST_PUBSUB_URL="http://127.0.0.1:${PUBSUB_PORT}" \
        target/debug/clusterd \
        --compute-controller-listen-addr "${COMPUTE_ADDR}" \
        --storage-controller-listen-addr "${STORAGE_ADDR}" \
        --compute-timely-config "$(timely_config 2102)" \
        --storage-timely-config "$(timely_config 2103)" \
        --process 0 --environment-id "${ENVIRONMENT_ID}" \
        --secrets-reader local-file --secrets-reader-local-file-dir "${SECRETS_DIR}" \
        --scratch-directory "${SCRATCH_DIR}" \
        >/tmp/clusterd-test-driver-clusterd.log 2>&1 &
    clusterd_pid=$!

    # Wait for the compute controller port.
    for _ in $(seq 1 60); do
        if (echo >"/dev/tcp/${COMPUTE_ADDR%:*}/${COMPUTE_ADDR#*:}") 2>/dev/null; then
            break
        fi
        sleep 0.5
    done
fi

echo "Building headless-driver..."
cargo build -p mz-clusterd-test-driver --bin headless-driver

echo "Running driver..."
CLUSTERD_COMPUTE_ADDR="${COMPUTE_ADDR}" \
    PERSIST_BLOB_URL="file://${BLOB_DIR}" \
    PERSIST_CONSENSUS_URL="${CONSENSUS_URL}" \
    DRIVER_PUBSUB_BIND="0.0.0.0:${PUBSUB_PORT}" \
    TARGET_BYTES="${TARGET_BYTES}" \
    SCENARIO="${SCENARIO}" \
    N_TIMESTAMPS="${N_TIMESTAMPS}" \
    target/debug/headless-driver
