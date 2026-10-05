#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Ingests TPC-H at the given scale factor and waits for lineitem's snapshot.
set -uo pipefail
SF=${1:-10}
USR="psql -h localhost -p 6875 -U materialize materialize -qAtX"
$USR -c "CREATE CLUSTER src SIZE 'scale=1,workers=8,mem=32GiB'"
$USR -c "CREATE SOURCE tpch IN CLUSTER src FROM LOAD GENERATOR TPCH (SCALE FACTOR $SF)"
$USR -c "CREATE TABLE lineitem FROM SOURCE tpch (REFERENCE lineitem)"
$USR -c "CREATE TABLE orders FROM SOURCE tpch (REFERENCE orders)"
START=$(date +%s)
for _ in $(seq 7200); do
  S=$($USR -c "SELECT bool_and(snapshot_committed) FROM mz_internal.mz_source_statistics s JOIN mz_tables t ON s.id = t.id WHERE t.name IN ('lineitem', 'orders')")
  [ "$S" = "t" ] && break
  sleep 5
done
echo "snapshot committed after $(( $(date +%s) - START ))s"
$USR -c "SELECT count(*) FROM lineitem" 2>&1 | head -1
