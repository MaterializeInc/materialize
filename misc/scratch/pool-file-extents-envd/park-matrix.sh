#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Runs the swap-less parked-data arms with persist_source backpressure on.
# With lgalloc off, nothing else bounds persist_source read-ahead on cc
# replicas, and the snapshot fetch OOM-kills small replicas in the first second.
set -uo pipefail
SYS="psql -h localhost -p 6877 -U mz_system materialize -qAtX"
USR="psql -h localhost -p 6875 -U materialize materialize -qAtX"
for c in $($USR -c "SELECT name FROM mz_clusters WHERE name LIKE 'c\_%'"); do
  $USR -c "DROP CLUSTER $c CASCADE"
done
$SYS -c "ALTER SYSTEM SET enable_compute_temporal_bucketing = true"
$SYS -c "ALTER SYSTEM SET compute_dataflow_max_inflight_bytes_cc = ${INFLIGHT:-536870912}"
sudo swapoff -a
for kind in index mv; do
  k=${kind:0:3}
  ./park-arm.sh "r-$k-noback8" "$kind" true false 8
  ./park-arm.sh "r-$k-file8" "$kind" true true 8
  ./park-arm.sh "r-$k-noback4" "$kind" true false 4
  ./park-arm.sh "r-$k-file4" "$kind" true true 4
done
echo PARK4DONE
