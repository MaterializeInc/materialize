#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Runs the pool_extents measurement matrix on the scratch instance.
set -uo pipefail
BIN=~/materialize/target/release/examples/pool_extents
OUT=~/runs
mkdir -p "$OUT"
BASE=(--backend file --dir /scratch/pool --chunks 16384 --budget-mib 4096
      --spill-threads 4 --compressibility 0.5 --die-young 0.3 --die-lag 4096
      --churn 8192 --readers 16 --reads 2000 --drain-timeout-secs 180)

run() {
  local name=$1; shift
  rm -rf /scratch/pool; mkdir -p /scratch/pool
  echo "=== $name: $*" > "$OUT/$name.log"
  "$BIN" "$@" >> "$OUT/$name.log" 2>&1
  echo "exit=$?" >> "$OUT/$name.log"
  echo "done $name"
}

run base         "${BASE[@]}" --rss-target-mib 8192
run rss-0.1      "${BASE[@]}" --rss-target-mib 4506
run rss-0.25     "${BASE[@]}" --rss-target-mib 5120
run rss-0.5      "${BASE[@]}" --rss-target-mib 6144
run readers-1    "${BASE[@]}" --rss-target-mib 8192 --readers 1 --reads 20000
run readers-64   "${BASE[@]}" --rss-target-mib 8192 --readers 64 --reads 1000
run spill-1      "${BASE[@]}" --rss-target-mib 8192 --spill-threads 1
run spill-2      "${BASE[@]}" --rss-target-mib 8192 --spill-threads 2
run spill-8      "${BASE[@]}" --rss-target-mib 8192 --spill-threads 8
run identity-0.2 "${BASE[@]}" --rss-target-mib 8192 --identity-fraction 0.2
run capacity-8g  "${BASE[@]}" --rss-target-mib 8192 --file-capacity-mib 8192
echo MATRIXDONE
