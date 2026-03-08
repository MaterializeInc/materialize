#!/usr/bin/env bash
# Sweep value_size across the scale-out-no-contention scenario.
# Usage: ./sweep-value-size.sh [extra specsheet flags...]
#
# Example:
#   ./sweep-value-size.sh --duration 15 --warmup 3

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCENARIO="$SCRIPT_DIR/scale-out-no-contention.yaml"
SIZES=(1 64 256 1024 4096 16384 65536)

printf "%-10s %10s %10s %10s %10s %10s %10s %12s\n" \
    "val_size" "CAS/s" "CAS p50" "CAS p99" "flushes/s" "ops/flush" "bytes/flush" "memory"

for size in "${SIZES[@]}"; do
    json=$(cargo run --release -p mz-persist-consensus-svc --example specsheet -- \
        --workload "$SCENARIO" --value-size "$size" --json "$@" 2>/dev/null)

    cas_per_sec=$(echo "$json" | jq -r '.throughput.ops_per_sec_by_type["CAS committed"].actual // 0')
    cas_p50=$(echo "$json" | jq -r '.latencies["CAS committed"].p50_ms // 0')
    cas_p99=$(echo "$json" | jq -r '.latencies["CAS committed"].p99_ms // 0')
    flushes=$(echo "$json" | jq -r '.throughput.flushes_per_sec')
    ops_flush=$(echo "$json" | jq -r '.throughput.ops_per_flush')
    bytes_flush=$(echo "$json" | jq -r '.throughput.bytes_per_flush')
    memory=$(echo "$json" | jq -r '.actor_state.approx_bytes')

    # Human-readable bytes
    if (( bytes_flush >= 1073741824 )); then
        bf=$(awk "BEGIN{printf \"%.1f GiB\", $bytes_flush/1073741824}")
    elif (( bytes_flush >= 1048576 )); then
        bf=$(awk "BEGIN{printf \"%.1f MiB\", $bytes_flush/1048576}")
    elif (( bytes_flush >= 1024 )); then
        bf=$(awk "BEGIN{printf \"%.1f KiB\", $bytes_flush/1024}")
    else
        bf="${bytes_flush} B"
    fi

    if (( memory >= 1073741824 )); then
        mem=$(awk "BEGIN{printf \"%.1f GiB\", $memory/1073741824}")
    elif (( memory >= 1048576 )); then
        mem=$(awk "BEGIN{printf \"%.1f MiB\", $memory/1048576}")
    elif (( memory >= 1024 )); then
        mem=$(awk "BEGIN{printf \"%.1f KiB\", $memory/1024}")
    else
        mem="${memory} B"
    fi

    # Human-readable value size
    if (( size >= 1024 )); then
        vs="$((size / 1024)) KiB"
    else
        vs="${size} B"
    fi

    printf "%-10s %10s %8.2f ms %8.2f ms %10.1f %10.1f %12s %12s\n" \
        "$vs" "$cas_per_sec" "$cas_p50" "$cas_p99" "$flushes" "$ops_flush" "$bf" "$mem"
done
