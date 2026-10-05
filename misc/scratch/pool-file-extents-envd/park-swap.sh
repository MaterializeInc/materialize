#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Runs the unspilled reference on a 32 GiB replica, then the spill arms against
# real swap on the scratch NVMe with the file store off. persist_source
# read-ahead is unbounded here, as with the production defaults.
set -uo pipefail
SYS="psql -h localhost -p 6877 -U mz_system materialize -qAtX"
USR="psql -h localhost -p 6875 -U materialize materialize -qAtX"
for c in $($USR -c "SELECT name FROM mz_clusters WHERE name LIKE 'c\_%'"); do
  $USR -c "DROP CLUSTER $c CASCADE"
done
$SYS -c "ALTER SYSTEM SET enable_compute_temporal_bucketing = true"
$SYS -c "ALTER SYSTEM RESET compute_dataflow_max_inflight_bytes_cc"
sudo swapoff -a
for kind in index mv; do
  k=${kind:0:3}
  ./park-arm.sh "q-$k-nospill" "$kind" false false 32
done
if ! swapon --show | grep -q /scratch/swapfile; then
  sudo fallocate -l 64G /scratch/swapfile
  sudo chmod 600 /scratch/swapfile
  sudo mkswap /scratch/swapfile >/dev/null
fi
sudo swapon /scratch/swapfile
for kind in index mv; do
  k=${kind:0:3}
  ./park-arm.sh "q-$k-swap8" "$kind" true false 8
  ./park-arm.sh "q-$k-swap4" "$kind" true false 4
done
sudo swapoff -a
echo PARKSWAPDONE
