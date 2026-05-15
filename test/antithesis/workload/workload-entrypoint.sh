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

PGHOST="${PGHOST:-materialized}"
PGPORT="${PGPORT:-6875}"
PGUSER="${PGUSER:-materialize}"
PGPORT_INTERNAL="${PGPORT_INTERNAL:-6877}"
PGUSER_INTERNAL="${PGUSER_INTERNAL:-mz_system}"
CLUSTER="${MZ_ANTITHESIS_CLUSTER:-antithesis_cluster}"
# Number of long-lived pool clusters to bootstrap, each bound to its own
# clusterd-pool-{i} container. Must match `ANTITHESIS_CLUSTERD_POOL_SIZE`
# in mzcompose.py and `CLUSTERD_POOL_SIZE` in the parallel-workload driver.
CLUSTERD_POOL_SIZE="${ANTITHESIS_CLUSTERD_POOL_SIZE:-2}"
# Timely worker threads per clusterd process — must equal the `workers=`
# argument every `Clusterd(...)` Service in mzcompose.py passes, because
# the controller reads worker count from the WORKERS clause we put in
# CREATE CLUSTER REPLICAS, not from clusterd's runtime config. Plumbed
# in via the Workload service's environment.
CLUSTERD_WORKERS="${CLUSTERD_WORKERS:-16}"

# Wait for materialized to be ready.
echo "Waiting for materialized to become healthy..."
until curl -sf http://materialized:6878/api/readyz > /dev/null 2>&1; do
    sleep 1
done
echo "materialized is healthy."

# Provision an unmanaged cluster with one replica per external clusterd
# process. Multi-replica gives Antithesis the option to kill one
# clusterd at a time without taking the workload offline, and exercises
# the multi-replica compute/storage code paths (notably
# `compute-replica-epoch-isolation`).
#
# This must run before setup-complete so Test Composer assertions can
# target the cluster from the start. Idempotent — `IF NOT EXISTS` is
# unsupported on `CREATE CLUSTER REPLICAS (...)`, so we query
# mz_clusters first.
existing=$(
    psql -h "$PGHOST" -p "$PGPORT_INTERNAL" -U "$PGUSER_INTERNAL" -tAc \
        "SELECT 1 FROM mz_clusters WHERE name = '$CLUSTER'"
)
if [[ -z "$existing" ]]; then
    echo "Provisioning cluster '$CLUSTER' with replicas on clusterd1 + clusterd2..."
    psql -h "$PGHOST" -p "$PGPORT_INTERNAL" -U "$PGUSER_INTERNAL" <<SQL
CREATE CLUSTER ${CLUSTER} REPLICAS (
    replica1 (
        STORAGECTL ADDRESSES ['clusterd1:2100'],
        STORAGE ADDRESSES ['clusterd1:2103'],
        COMPUTECTL ADDRESSES ['clusterd1:2101'],
        COMPUTE ADDRESSES ['clusterd1:2102'],
        WORKERS ${CLUSTERD_WORKERS}
    ),
    replica2 (
        STORAGECTL ADDRESSES ['clusterd2:2100'],
        STORAGE ADDRESSES ['clusterd2:2103'],
        COMPUTECTL ADDRESSES ['clusterd2:2101'],
        COMPUTE ADDRESSES ['clusterd2:2102'],
        WORKERS ${CLUSTERD_WORKERS}
    )
);
GRANT ALL ON CLUSTER ${CLUSTER} TO ${PGUSER};
SQL
else
    echo "Cluster '$CLUSTER' already exists; skipping provisioning."
fi

# Bootstrap a long-lived `pool_cluster_{i}` for each clusterd-pool-{i}
# container. Each pool cluster has exactly one replica wired to its
# matching pool clusterd. Parallel-workload driver invocations pick a
# slot at random and run against `pool_cluster_{slot}`; concurrent
# invocations may share a pool cluster (every workload object is in a
# seed-scoped database so they don't collide). The cluster identity is
# tied to the clusterd identity, so reconnects don't trip clusterd's
# `instance configuration not compatible` halt; only the seed-scoped
# database / roles get dropped between invocations.
#
# Idempotent: skip pool clusters that already exist (the SUT's catalog
# survives across `docker compose up` if metadata volumes aren't wiped).
for i in $(seq 0 $((CLUSTERD_POOL_SIZE - 1))); do
    POOL_CLUSTER="pool_cluster_$i"
    existing_pool=$(
        psql -h "$PGHOST" -p "$PGPORT_INTERNAL" -U "$PGUSER_INTERNAL" -tAc \
            "SELECT 1 FROM mz_clusters WHERE name = '$POOL_CLUSTER'"
    )
    if [[ -n "$existing_pool" ]]; then
        echo "Pool cluster '$POOL_CLUSTER' already exists; skipping provisioning."
        continue
    fi
    echo "Provisioning pool cluster '$POOL_CLUSTER' on clusterd-pool-$i..."
    psql -h "$PGHOST" -p "$PGPORT_INTERNAL" -U "$PGUSER_INTERNAL" <<SQL
CREATE CLUSTER ${POOL_CLUSTER} REPLICAS (
    r1 (
        STORAGECTL ADDRESSES ['clusterd-pool-${i}:2100'],
        STORAGE ADDRESSES ['clusterd-pool-${i}:2103'],
        COMPUTECTL ADDRESSES ['clusterd-pool-${i}:2101'],
        COMPUTE ADDRESSES ['clusterd-pool-${i}:2102'],
        WORKERS ${CLUSTERD_WORKERS}
    )
);
GRANT ALL ON CLUSTER ${POOL_CLUSTER} TO ${PGUSER};
SQL
done

# Emit setup_complete — Antithesis begins test commands after this.
/usr/local/bin/setup-complete.sh

# Sleep forever — Test Composer runs the test commands, not this entrypoint.
echo "Setup complete. Sleeping while Test Composer runs commands."
exec sleep infinity
