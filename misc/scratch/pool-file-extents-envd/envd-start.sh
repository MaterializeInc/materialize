#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Starts CockroachDB and a fresh environmentd on the scratch instance.
set -uo pipefail
cd ~/materialize || exit 1
docker start cockroach >/dev/null 2>&1 || docker run --name=cockroach -d -p 26257:26257 -p 26258:8080 \
  cockroachdb/cockroach:latest start-single-node --insecure --store=type=mem,size=8G >/dev/null
for _ in $(seq 60); do
  docker exec cockroach ./cockroach sql --insecure -e 'select 1' >/dev/null 2>&1 && break
  sleep 1
done
PARAMS="enable_column_paged_batcher=true"
PARAMS+=";enable_column_paged_batcher_spill=true"
PARAMS+=";column_paged_batcher_lz4=true"
PARAMS+=";column_paged_batcher_budget_fraction=${BUDGET_FRACTION:-0.01}"
PARAMS+=";column_paged_batcher_pool_rss_target_fraction=${RSS_FRACTION:-0.02}"
PARAMS+=";enable_column_paged_batcher_file_extents=false"
PARAMS+=";enable_upsert_paged_spill=false"
PARAMS+=";enable_compute_correction_v2_spill=false"
setsid nohup bin/environmentd --release --reset -- "--system-parameter-default=$PARAMS" \
  > ~/envd.log 2>&1 < /dev/null &
for _ in $(seq 300); do
  psql -h localhost -p 6875 -U materialize materialize -c 'select 1' >/dev/null 2>&1 && { echo envd up; exit 0; }
  sleep 2
done
echo envd did not come up; tail -20 ~/envd.log; exit 1
