#!/usr/bin/env bash
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
set -euo pipefail

GIT_ROOT="$(git rev-parse --show-toplevel)"

OUTPUT="idle_memory.csv"

echo -n "nproc," > "$OUTPUT"
tail -n +2 "/proc/self/smaps_rollup" | awk '{printf "%s,",$1;}' >> "$OUTPUT"
echo "" >> "$OUTPUT"

cargo build --release --bin materialized
NPROC="$(nproc)"
#NPROC=128

MZDATA="$GIT_ROOT/mzdata"

for ((i=1;i<="$NPROC";i=2*i)); do
    echo "${i}" >&2
    [ -d "$MZDATA" ] && rm -r "$MZDATA"
    cargo run --release --bin materialized -- --workers "${i}" &
    PID=$!
    sleep 20
    echo -n "${i}," >> "$OUTPUT"
    tail -n +2 "/proc/$PID/smaps_rollup" | awk '{printf "%s,",$2;}' >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    kill "$PID"
    wait "$PID" || echo "$?"
done
