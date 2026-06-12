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
# Command prepended to clusterd, e.g. WRAPPER="heaptrack" or
# WRAPPER="perf record -g --". On cleanup we terminate the inner clusterd (not
# the wrapper) so the wrapper flushes its output and exits on its own.
WRAPPER=${WRAPPER:-}
# Cargo profile for clusterd and the driver. `optimized` (release-like, fast
# build, keeps debug symbols) gives representative profiling numbers; override
# with PROFILE=dev for a quicker unoptimized build.
PROFILE=${PROFILE:-optimized}
PROFILE_DIR=$([[ "$PROFILE" == "dev" ]] && echo debug || echo "$PROFILE")
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

# `launched_pid` is whatever we backgrounded (the wrapper, if any, else
# clusterd). `clusterd_pid` is the inner clusterd, resolved after launch.
launched_pid=""
clusterd_pid=""
cleanup() {
    # Terminate the inner clusterd first, so a wrapper (heaptrack/perf) sees its
    # child exit and flushes its own output, then exits on its own. clusterd
    # handles SIGTERM (~1s); escalate to SIGKILL only if it ignores it.
    if [[ -n "$clusterd_pid" ]]; then
        kill "$clusterd_pid" 2>/dev/null || true
        for _ in $(seq 1 10); do
            kill -0 "$clusterd_pid" 2>/dev/null || break
            sleep 0.3
        done
        kill -9 "$clusterd_pid" 2>/dev/null || true
    fi
    # Only touch the wrapper if it's a distinct process; give it time to flush,
    # then force it if it's still hanging after the inner exited.
    if [[ -n "$launched_pid" && "$launched_pid" != "$clusterd_pid" ]]; then
        for _ in $(seq 1 20); do
            kill -0 "$launched_pid" 2>/dev/null || break
            sleep 0.3
        done
        kill "$launched_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

if [[ "$RUN_CLUSTERD" == "1" ]]; then
    echo "Building clusterd (profile: ${PROFILE})..."
    cargo build --profile "$PROFILE" --bin clusterd
    echo "clusterd command:"
    echo "  PERSIST_PUBSUB_URL=http://127.0.0.1:${PUBSUB_PORT} \\"
    echo "  ${WRAPPER} target/${PROFILE_DIR}/clusterd \\"
    echo "    --compute-controller-listen-addr ${COMPUTE_ADDR} \\"
    echo "    --storage-controller-listen-addr ${STORAGE_ADDR} \\"
    echo "    --compute-timely-config '$(timely_config 2102)' \\"
    echo "    --storage-timely-config '$(timely_config 2103)' \\"
    echo "    --process 0 --environment-id ${ENVIRONMENT_ID} \\"
    echo "    --secrets-reader local-file --secrets-reader-local-file-dir ${SECRETS_DIR} \\"
    echo "    --scratch-directory ${SCRATCH_DIR}"
    # ${WRAPPER} is intentionally unquoted so a multi-word wrapper (e.g.
    # "perf record -g --") splits into separate arguments.
    # shellcheck disable=SC2086
    PERSIST_PUBSUB_URL="http://127.0.0.1:${PUBSUB_PORT}" \
        ${WRAPPER} "target/${PROFILE_DIR}/clusterd" \
        --compute-controller-listen-addr "${COMPUTE_ADDR}" \
        --storage-controller-listen-addr "${STORAGE_ADDR}" \
        --compute-timely-config "$(timely_config 2102)" \
        --storage-timely-config "$(timely_config 2103)" \
        --process 0 --environment-id "${ENVIRONMENT_ID}" \
        --secrets-reader local-file --secrets-reader-local-file-dir "${SECRETS_DIR}" \
        --scratch-directory "${SCRATCH_DIR}" \
        >/tmp/clusterd-test-driver-clusterd.log 2>&1 &
    launched_pid=$!

    # Resolve the inner clusterd PID. With no wrapper, what we launched *is*
    # clusterd. With a wrapper, the wrapper's argv also contains the clusterd
    # path, so exclude the launched (wrapper) pid when matching, and retry
    # until the wrapper has forked clusterd.
    if [[ -z "$WRAPPER" ]]; then
        clusterd_pid="$launched_pid"
    else
        for _ in $(seq 1 60); do
            clusterd_pid=$(pgrep -f "target/${PROFILE_DIR}/clusterd --compute-controller-listen-addr ${COMPUTE_ADDR}" \
                | grep -vx "$launched_pid" | head -1 || true)
            [[ -n "$clusterd_pid" ]] && break
            sleep 0.5
        done
        [[ -z "$clusterd_pid" ]] && clusterd_pid="$launched_pid"
    fi

    # Wait for the compute controller port.
    for _ in $(seq 1 60); do
        if (echo >"/dev/tcp/${COMPUTE_ADDR%:*}/${COMPUTE_ADDR#*:}") 2>/dev/null; then
            break
        fi
        sleep 0.5
    done
fi

echo "Building headless-driver (profile: ${PROFILE})..."
cargo build --profile "$PROFILE" -p mz-clusterd-test-driver --bin headless-driver

echo "Running driver..."
CLUSTERD_COMPUTE_ADDR="${COMPUTE_ADDR}" \
    PERSIST_BLOB_URL="file://${BLOB_DIR}" \
    PERSIST_CONSENSUS_URL="${CONSENSUS_URL}" \
    DRIVER_PUBSUB_BIND="0.0.0.0:${PUBSUB_PORT}" \
    TARGET_BYTES="${TARGET_BYTES}" \
    SCENARIO="${SCENARIO}" \
    N_TIMESTAMPS="${N_TIMESTAMPS}" \
    "target/${PROFILE_DIR}/headless-driver"
