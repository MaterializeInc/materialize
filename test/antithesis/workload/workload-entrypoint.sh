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

# Active workload group — set on the workload service in each per-group
# docker-compose.yaml by export-compose.py, and also embedded into this
# image at build time by the Dockerfile (`ENV ANTITHESIS_WORKLOAD_GROUP`).
# Defaults to `combined` if both are unset (e.g. someone runs the image
# directly).  The group only controls cluster-bootstrap branching at
# this point; the test-template directory was pre-baked at image-build
# time so Test Composer's first-scan sees the right scripts immediately.
ANTITHESIS_WORKLOAD_GROUP="${ANTITHESIS_WORKLOAD_GROUP:-combined}"
echo "Workload group: $ANTITHESIS_WORKLOAD_GROUP"

# Number of long-lived pool clusters to bootstrap, each bound to its own
# clusterd-pool-{i} container. Must match `ANTITHESIS_CLUSTERD_POOL_SIZE`
# in mzcompose.py and `CLUSTERD_POOL_SIZE` in the parallel-workload driver.
# Only consulted when the active group needs pool clusters.
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

# Provision the antithesis_cluster with replicas on clusterd1 +
# clusterd2.  Every workload group includes both clusterds so multi-
# replica fault windows (compute-replica-epoch-isolation) get
# exercised regardless of which group is running.  Idempotent —
# `IF NOT EXISTS` is unsupported on `CREATE CLUSTER REPLICAS (...)`,
# so we query mz_clusters first.  Must run before setup-complete so
# Test Composer assertions can target the cluster from the start.
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

# Pool clusters are only needed by groups that run the parallel-workload
# driver.  Bootstrapping them in other groups is wasted setup time and
# wasted clusterd containers the topology didn't ask for.
case "$ANTITHESIS_WORKLOAD_GROUP" in
    parallel-workload|combined)
        # Bootstrap a long-lived `pool_cluster_{i}` for each clusterd-pool-{i}
        # container.  Each pool cluster has exactly one replica wired to its
        # matching pool clusterd.  Parallel-workload driver invocations pick a
        # slot at random and run against `pool_cluster_{slot}`; concurrent
        # invocations may share a pool cluster (every workload object is in a
        # seed-scoped database so they don't collide).  The cluster identity is
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
        ;;
    *)
        echo "Skipping pool-cluster bootstrap for group '$ANTITHESIS_WORKLOAD_GROUP'."
        ;;
esac

# Emit setup_complete — Antithesis begins test commands after this.
/usr/local/bin/setup-complete.sh

# Sentinel file the workload healthcheck looks at, so other services
# can gate their start on this entrypoint finishing the cluster +
# setup_complete handshake.  Currently consumed by the upsert-stress
# group's upsert-hammer-{i} containers, which would otherwise race
# the first_* setup drivers and start producing into Kafka before
# the source exists.
touch /tmp/workload-ready
echo "Workload ready sentinel created at /tmp/workload-ready."

# Sleep forever — Test Composer runs the test commands, not this entrypoint.
echo "Setup complete. Sleeping while Test Composer runs commands."
exec sleep infinity
