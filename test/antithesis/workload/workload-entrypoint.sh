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

# Wait for materialized to be ready.
echo "Waiting for materialized to become healthy..."
until curl -sf http://materialized:6878/api/readyz > /dev/null 2>&1; do
    sleep 1
done
echo "materialized is healthy."

# Provision an unmanaged cluster backed by the external clusterd1 process.
# This must run before setup-complete so Test Composer assertions can target
# the cluster from the start. Idempotent — `IF NOT EXISTS` is unsupported on
# `CREATE CLUSTER REPLICAS (...)`, so we query mz_clusters first.
existing=$(
    psql -h "$PGHOST" -p "$PGPORT_INTERNAL" -U "$PGUSER_INTERNAL" -tAc \
        "SELECT 1 FROM mz_clusters WHERE name = '$CLUSTER'"
)
if [[ -z "$existing" ]]; then
    echo "Provisioning cluster '$CLUSTER' against clusterd1..."
    psql -h "$PGHOST" -p "$PGPORT_INTERNAL" -U "$PGUSER_INTERNAL" <<SQL
CREATE CLUSTER ${CLUSTER} REPLICAS (replica1 (
    STORAGECTL ADDRESSES ['clusterd1:2100'],
    STORAGE ADDRESSES ['clusterd1:2103'],
    COMPUTECTL ADDRESSES ['clusterd1:2101'],
    COMPUTE ADDRESSES ['clusterd1:2102'],
    WORKERS 1
));
GRANT ALL ON CLUSTER ${CLUSTER} TO ${PGUSER};
SQL
else
    echo "Cluster '$CLUSTER' already exists; skipping provisioning."
fi

# Emit setup_complete — Antithesis begins test commands after this.
/usr/local/bin/setup-complete.sh

# Sleep forever — Test Composer runs the test commands, not this entrypoint.
echo "Setup complete. Sleeping while Test Composer runs commands."
exec sleep infinity
