#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Runs no-backing, file and swap arms of the parked workload under the same
# settings, including the persist_source read-ahead bound, so the backings
# compare like for like. File and no-backing arms run with swap off, swap arms
# with the file store off.
#
# Usage: park-fair.sh PREFIX "MEM_GIB..." [NOSPILL_MEM_GIB]
# Environment: INFLIGHT (bytes, default 512 MiB), SWAP_GIB (swapfile size,
# default 64), ARMS (default "noback file swap").
set -uo pipefail
PREFIX=$1 MEMS=$2 NOSPILL=${3:-}
ARMS=${ARMS:-noback file swap}
SYS="psql -h localhost -p 6877 -U mz_system materialize -qAtX"
USR="psql -h localhost -p 6875 -U materialize materialize -qAtX"
for c in $($USR -c "SELECT name FROM mz_clusters WHERE name LIKE 'c\_%'"); do
  $USR -c "DROP CLUSTER $c CASCADE"
done
$SYS -c "ALTER SYSTEM SET enable_compute_temporal_bucketing = true"
$SYS -c "ALTER SYSTEM SET compute_dataflow_max_inflight_bytes_cc = ${INFLIGHT:-536870912}"
has_arm() { [[ " $ARMS " == *" $1 "* ]]; }
sudo swapoff -a
for kind in index mv; do
  k=${kind:0:3}
  if [ -n "$NOSPILL" ]; then
    ./park-arm.sh "$PREFIX-$k-nospill$NOSPILL" "$kind" false false "$NOSPILL"
  fi
  for mem in $MEMS; do
    if has_arm noback; then ./park-arm.sh "$PREFIX-$k-noback$mem" "$kind" true false "$mem"; fi
    if has_arm file; then ./park-arm.sh "$PREFIX-$k-file$mem" "$kind" true true "$mem"; fi
  done
done
if has_arm swap; then
  GIB=${SWAP_GIB:-64}
  if [ "$(stat -c %s /scratch/swapfile 2>/dev/null || echo 0)" -lt $((GIB << 30)) ]; then
    sudo rm -f /scratch/swapfile
    sudo fallocate -l "${GIB}G" /scratch/swapfile
    sudo chmod 600 /scratch/swapfile
    sudo mkswap /scratch/swapfile >/dev/null
  fi
  sudo swapon /scratch/swapfile
  for kind in index mv; do
    k=${kind:0:3}
    for mem in $MEMS; do
      ./park-arm.sh "$PREFIX-$k-swap$mem" "$kind" true false "$mem"
    done
  done
  sudo swapoff -a
fi
echo PARKFAIRDONE
